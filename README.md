# ai_wrkforch — Durable workflow orchestration and bounded dataflow for Haskell

**Make long-running computations survive crashes, wait for people and services, run in parallel, and remain explainable after the fact.**

`ai_wrkforch` is the FUDD Haskell workflow engine. It combines a **typed hierarchical finite-state machine (HFSM)** for deciding *what happens next* with a **durable dataflow engine (DF)** for executing *what can run in parallel*. PostgreSQL provides authoritative state, transactional progress, leases, audit history, and work admission.

This is an orchestration library, not a background-job wrapper or a visual-only workflow editor. A workflow is a typed program with inspectable control structure and durable execution records. It can last longer than any worker process running it.

**Read according to your goal:** [Why workflows are difficult](#1-the-problem-a-function-call-is-not-a-workflow) · [Run your first HFSM](#2-the-fastest-way-to-see-it-work) · [Choose HFSM vs dataflow](#3-one-engine-two-complementary-kinds-of-work) · [Static and dynamic dataflow](#4-dataflow-in-five-minutes-df-01-and-df-02) · [Build and test](#5-getting-the-repository-running) · [Atomic persistence](#7-how-a-durable-hfsm-turn-works) · [Operations](#9-operating-the-runtime-ct-06) · [Limits and roadmap](#13-what-remains-before-broad-production-deployment).

> **Current development stage:** The typed HFSM runtime, PostgreSQL execution, durable activities, history/replay/migration, production-oriented operations, static DAGs (**DF-01**), and bounded dynamic scatter/gather (**DF-02**) are implemented. The team reports that the integrated CT-01–CT-06 and DF-01/DF-02 test suites pass. This is a substantial functional foundation, **not yet a certification for arbitrary untrusted multi-tenant or high-scale production deployment**. See [What remains](#13-what-remains-before-broad-production-deployment).

## 1. The problem: a function call is not a workflow

A simple pipeline is easy to express in Haskell:

```haskell
result <- fetchInputs request
        >>= analyze
        >>= verify
        >>= publish
```

It stops being simple when the execution has to **outlive the calling process**:

- `analyze` takes hours; its worker may die halfway through.
- `verify` waits days for a human decision or a third-party callback.
- The service may respond just before the connection drops. Did the operation happen?
- Two workers may both believe they own the same task after a network partition.
- `publish` must not happen twice simply because an acknowledgement was lost.
- Some inputs should be processed in parallel, but aggregation must wait for all the required outputs.
- A new version is deployed while instances are still running the old one.
- An operator needs to answer *why* an instance reached a state, *which* evidence it used, and *which* actions it emitted.

Adding retry loops and a queue to the original function does not solve these problems by itself. The system needs a durable **control model**, a clearly defined **effect boundary**, a **concurrency protocol**, and an **execution history**.

### The approach in one picture

```text
                 Haskell machine specification
                states • signals • handoffs • routes
                               │
                     compile + validate
                               │
                               ▼
                    ┌──────────────────┐
 signals / approvals│   HFSM CONTROL   │ lifecycle, waits, children,
        ──────────► │                  │ timers, decisions, cancellation
                    └───────┬──────────┘
                            │ durable intended effects
                ┌───────────┴──────────────┐
                ▼                          ▼
        ┌──────────────────┐       ┌────────────────────┐
        │ ACTIVITY WORKERS │       │  DATAFLOW WORKERS  │
        │ external APIs,   │       │ static DAGs,       │
        │ idempotent calls │       │ partitions, gather │
        └────────┬─────────┘       └────────┬───────────┘
                 │                          │
                 └───────── outcome ────────┘
                            │ durable signal
                            ▼
                    next HFSM transition

         PostgreSQL: releases • instances • steps • snapshots
          signals • outbox • tasks • tokens • artifacts • audit
```

**HFSM governs control; dataflow handles computation.** Both have fenced workers and persistent records. External effects are delivered *after* the state transition that authorized them has committed.

### What does it solve?

| Challenge | Engine mechanism | Practical result |
|---|---|---|
| Worker/process crash | PostgreSQL snapshots, leases and fenced commits | Progress can be resumed without rerunning already committed transitions |
| Lost acknowledgements | Stable effect keys, deduplication, activity receipts | Safe retry where the external provider supports idempotency |
| Competing workers | Per-instance ordering, lease UUIDs, revision/snapshot checks | A stale worker cannot commit over a newer owner |
| Hours/days of waiting | Durable signals, timers, child/join/wait control | No thread must remain alive while a workflow is waiting |
| Parallel computation | Task DAGs, dependency tokens, CPU/memory reservations | Independent tasks run concurrently; consumers wait for required artifacts |
| Variable workload size | Bounded one-shot scatter, partition tasks, reduction tree | Parallelism can be derived from an immutable runtime manifest |
| Audit and diagnosis | Immutable steps, snapshots, causal IDs and replay inputs | Operators can inspect and verify a historical transition |
| Deployment changes | Immutable machine release identity, explicit migration | Running instances do not silently acquire new behavior |
| Operational supervision | Tenants, quotas, priorities, heartbeats, dead letters, traces | The engine can be managed as a service, not just called as a library |

### Should I use it?

Use `ai_wrkforch` when your application has **durable business or computational control**: multiple steps, waits, retries, parallel branches, external actions, or a requirement to explain/recover an execution. Typical FUDD applications include AI-assisted research, Batcher pipelines, document processing, Skill Factory workflows, and venture-development processes.

You may **not need it** for a short, pure, synchronous calculation where a failed process can simply restart the whole function. It is also **not** a distributed stream processor, an unrestricted dynamic task scheduler, a capability sandbox, or a replacement for domain authorization.

## 2. The fastest way to see it work

The repository includes a small **approval workflow**. Its states and events are ordinary Haskell types:

```haskell
data Phase   = DraftPh | ReviewPh | ReworkPh | DonePh

data Signal  = SubmitSg Text
             | ApproveSg Text
             | RejectSg Text Text
             | ResubmitSg Text

data Handoff = SubmittedHf | ApprovedHf | RejectedHf | ResubmittedHf
```

The control path is familiar:

```text
Draft ── Submit ──► Review ── Approve ──► Done
                      │
                    Reject
                      ▼
                    Rework ── Resubmit ──► Review
```

Once your project is configured and built (see [Getting the repository running](#5-getting-the-repository-running)), execute:

```sh
stack exec -- thfsm test approval
```

That runs the machine against its **in-memory store**, enqueues a sequence of four signals, and renders the resulting execution. To exercise the **real PostgreSQL** store with the same machine:

```sh
stack exec -- thfsm test approval --db
```

The database version verifies persisted revisions, applied signals, steps, outbox rows, and the final typed context. It is a *development/test command*, not a general-purpose production worker launcher.

### The smallest program using the HFSM engine

Here is the essential code path, extracted and condensed from `Demo.ApprovalRuntime`. It deliberately uses the existing demo's machine and initial context, so the example focuses on **using the engine**, not constructing a domain DSL first:

```haskell

module Example where

import Control.Monad (forM_, replicateM_, unless)
import qualified Data.UUID.V4 as UUID

import Demo.ApprovalHfsm (Signal(..), approvalSpec)
import Demo.ApprovalRuntime (initialCtx)
import qualified Demo.MemoryStore as Memory
import Demo.Print (printRuntimeStore)
import WfOrch.Compile (CompiledMachine(..), compile)
import WfOrch.Runtime.Engine.Worker (WorkerCfg(..), runTick)
import WfOrch.Runtime.Registry (emptyRegistry, register)
import WfOrch.Validate.Report (ok, validate)

require :: Show err => Either err a -> IO a
require = either (fail . show) pure

example :: IO ()
example = do
  spec <- require approvalSpec
  compiled <- require (compile spec)
  unless (ok (validate compiled.graph)) $
    fail "Invalid HFSM graph"
  registry <- require (register compiled emptyRegistry)

  memory   <- Memory.newMemoryStore
  instanceId <- Memory.createInstance memory compiled initialCtx

  forM_ [ SubmitSg "alice"
        , RejectSg "bob" "needs legal note"
        , ResubmitSg "alice"
        , ApproveSg "carol"
        ] $ \signal -> do
          _ <- Memory.enqueueSignal memory instanceId signal
          pure ()

  workerId <- UUID.nextRandom
  let worker = WorkerCfg
        { node = workerId, batchSize = 1
        , leaseSeconds = 30, pollDelayMs = 10 }

  replicateM_ 4 $ runTick worker (Memory.storeOf memory) registry
  view <- Memory.loadInstanceView memory instanceId
  printRuntimeStore view
```

Four `runTick` invocations process four committed inputs. The resulting view shows the current context and the underlying signals, steps, snapshots, outbox, and projections. No ad hoc checkpointing is needed inside the state handler.

For a durable deployment, the application substitutes the PostgreSQL-backed `Store IO`, registers its machine release, creates a persisted instance, and runs supervised workers. **The authored machine and transition planner do not change.**

### The crucial separation: handler versus route

A state handler computes an outcome; a route declares the structural consequence. This is the central authoring decision in `ai_wrkforch`:

```haskell
-- From the approval demonstration; simplified to the successful case.
react = \_ signal ctx ->
  case signal of
    ApproveSg reviewer ->
      Right ReactRez
        { next    = ctx { approvedBy = Just reviewer }
        , handoff = ApprovedHf
        , cmds    = [NotifyCm ctx.author ("approved by " <> reviewer)]
        , note    = ["approved"]
        }
    _ -> Left (RejectREr "unexpected signal")

-- The route is a separate declaration, conceptually:
-- ApprovedHf -> goto DonePh
```

The handler is **pure**: it does not send the notification. It returns a command. The engine records the intended command alongside the successful state transition; an activity worker performs delivery later. This is why a retry of the transition does not need to repeat an external effect.

## 3. One engine, two complementary kinds of work

The package is easiest to understand by choosing the right primitive for the job.

| You need to... | Prefer | Why |
|---|---|---|
| Wait for an approval, event, timer, or external callback | **HFSM** | The important state is *which decision/control point is active* |
| Coordinate steps that may take days | **HFSM** | Revisions, snapshots and waiting conditions survive worker restarts |
| Safely initiate an HTTP call or other external operation | **HFSM + activity** | Side effects are separate from the committed control transition |
| Transform 100 independent documents | **DF-01 dataflow** | Dependencies, parallel resources and produced artifacts are central |
| Split an input into a data-dependent number of partitions | **DF-02 dataflow** | One bounded, immutable scatter expansion creates the needed task graph |
| Run computations, inspect the result, then approve publication | **HFSM → dataflow → HFSM** | Parallel calculation and business authorization remain separate |

A common real-world composition:

```text
HFSM: RequestReceived
  │
  ├── CT-04 durable flow-start command
  ▼
DF: Fetch → [Analyze A, Analyze B, Analyze C] → Combine
  │
  ├── immutable result artifact + deduplicated completion signal
  ▼
HFSM: AwaitHumanReview → Approved → Publish
```

**Do not model 10,000 data partitions as 10,000 HFSM states.** Use the workflow to own lifecycle and the dataflow engine to own task dependencies and artifacts. Conversely, do not use a DAG task as the authority for an irreversible business decision.

## 4. Dataflow in five minutes (DF-01 and DF-02)

Dataflow is not an experimental appendix: it is a **first-class execution subsystem** beside HFSM, sharing PostgreSQL operational infrastructure and the worker/resource model.

### 4.1 Static DAGs: tasks, ports, edges, tokens, artifacts (DF-01)

A `FlowSpec` defines a finite directed acyclic graph. A `TaskDef` declares its executor, required input ports, output ports, resources, attempt limits, timeout, capabilities, and priority. An `EdgeDef` connects a producer's output port to one consumer input port.

This **diamond graph** is taken from `Demo.FlowRegression`:

```haskell
import Data.Text (Text)
import WfOrch.Flow.Spec

fixtureTask :: Text -> [Text] -> [Text] -> TaskDef
fixtureTask name inputPorts outputPorts = TaskDef
  { key            = name
  , executor       = "fixture"
  , inputs         = inputPorts
  , outputs        = outputPorts
  , cpu            = 1
  , memoryMb       = 64
  , capabilities   = []
  , maxAttempts    = 3
  , timeoutSeconds = 5
  , priority       = 0
  }

diamond :: FlowSpec
diamond = FlowSpec
  { tasks =
      [ fixtureTask "A" []          ["a"]
      , fixtureTask "B" ["a"]       ["b"]
      , fixtureTask "C" ["a"]       ["c"]
      , fixtureTask "D" ["b", "c"] ["result"]
      ]
  , edges =
      [ EdgeDef "A" "a" "B" "a"
      , EdgeDef "A" "a" "C" "a"
      , EdgeDef "B" "b" "D" "b"
      , EdgeDef "C" "c" "D" "c"
      ]
  }
```

```text
              ┌──► B ──┐
        A ────┤        ├──► D
              └──► C ──┘
```

`B` and `C` become ready after `A` commits. `D` becomes ready **only after both** `b` and `c` tokens exist. A token carries a reference to an immutable artifact already published by its producing task. Completion order never substitutes for an explicit dependency.

Validation is available independently of the database:

```haskell
validateFlowSpec diamond  -- Right ()
topologicalOrder diamond -- Right ["A","B","C","D"]
```

DF-01 validates a finite, acyclic, fully connected input-port contract: **1–256 tasks**, at most **4,096 edges**, and at most **64 inputs or outputs per task**. Each declared input port has exactly one producer. Root tasks have no input ports and start ready.

The production execution path is:

```text
register immutable FlowSpec
        │
start flow run (with a stable source effect key)
        │
claim ready tasks, subject to quotas/resources/capabilities
        │
execute provider using input artifact references
        │
verify and publish output bytes to external CAS
        │
publish immutable artifact manifest in PostgreSQL
        │
fenced task completion transaction:
  output bindings + downstream tokens + readiness + attempt + run status
        │
optional HFSM completion signal
```

#### A task is a computation, not an implicit side effect

`WfOrch.Flow.Worker.TaskProvider` has the essential shape:

```haskell
TaskProvider
  { executor = "fixture"
  , execute  = \leasedTask -> do
      -- Read immutable input artifacts, calculate the result,
      -- write and VERIFY bytes in your durable CAS,
      -- publish the artifact manifest, then return its UUID.
      ...
  }
```

A provider returns `Either TaskError [(Text, UUID)]`, a complete map of declared output ports to **published artifact UUIDs**. The fenced SQL settlement validates the whole map before inserting outputs or tokens. A stale worker cannot overwrite successful outputs.

Execution may be retried, so task callbacks must be repeatable and must not perform unreconciled irreversible actions. Send business effects through the **HFSM/CT-04 outbox**, not directly from a retryable dataflow callback.

### 4.2 Dynamic scatter and partitioned aggregation (DF-02)

DF-01 requires a fixed task graph. DF-02 adds a **bounded, one-shot expansion** when the number of partitions depends on an input discovered during execution.

```text
                  [scatter]
                      │ materialize immutable manifest
            ┌─────────┼──────────┐
            ▼         ▼          ▼
          part 1    part 2     part N      (parallel, resource-limited)
            │         │          │
            └─────────┼──────────┘
                      ▼
               ordered reducers
               (fan-in 2–64)
                      │
                 result artifact
```

The application builds a `ScatterPlan` containing:

- a stable `groupKey`;
- an **ordered** list of `PartitionDef` entries (partition key + already-published artifact UUID);
- `partitionExecutor` and `aggregateExecutor`;
- resource/retry profiles for each;
- reduction `fanIn`.

The pure planner `WfOrch.Flow.Dynamic.Spec.planExpansion` derives deterministic task keys, partition bindings, reducer tasks and dependency edges. For example:

```text
df2:analysis:p:000001        leaf partition
df2:analysis:p:000002        leaf partition
df2:analysis:r:0:000001      level-0 reducer
df2:analysis:r:1:000001      later reducer
```

The existing DF-01 resource scheduler handles the generated tasks. The DF-02 PostgreSQL extension **atomically creates the expansion and completes the fenced scatter source**. There is no window where a partially inserted dynamic graph becomes visible as committed execution. Repeating the same committed expansion is idempotent; an inconsistent plan is rejected.

Partition tasks receive an immutable `item` artifact binding. Reducers receive ordinary DF-01 tokens on ordered ports such as `i000001`, `i000002`, etc. **Manifest order, not task completion order, defines aggregation order**. The last reducer produces the `result` artifact.

Current limits are deliberately conservative:

| DF-02 property | Current contract |
|---|---|
| Scatter groups | One per flow run |
| Partition count | 0–512 |
| Reducer fan-in | 2–64 |
| Total tasks after expansion | Up to 4,096 per run |
| Expansion | One-shot and immutable |
| Recursive/nested scatters | Not supported |
| Distributed streaming/windows | Not supported |

A zero-partition scatter creates a ready identity reducer; the application must define the correct identity artifact. A singleton still goes through a reducer. For non-associative reductions, the **tree shape matters**: choose a deterministic fold, pin the fan-in, and do not claim that different reduction trees necessarily produce bit-for-bit equivalent answers.

#### DF-02 worker/provider integration

The primary entry points are:

```haskell
WfOrch.Flow.Dynamic.Runtime.runManagedDynamicWorker
WfOrch.Flow.Dynamic.Postgres.dynamicFlowStore
WfOrch.Flow.Dynamic.Provider.newScatterProvider
WfOrch.Flow.Dynamic.Provider.newPartitionProvider
WfOrch.Flow.Dynamic.Provider.newOrderedReducerProvider
```

Use `runManagedDynamicWorker` for workers serving DF-02 tasks: it registers CT-06 worker identity/resources and uses the dynamic store adapter to add partition `item` bindings and to recognize the scatter's already-committed completion. DF-01 workers alone do **not** supply that DF-02 behavior.

Your worker must advertise `flow:task` plus executor-specific capabilities such as `flow:execute:df2.scatter`, `flow:execute:df2.partition` and `flow:execute:df2.reduce` (or an explicitly authorized wildcard). Reducer code is supplied by the application; `ai_wrkforch` builds and executes the dependency tree, not the aggregation mathematics.

### 4.3 Artifacts: database metadata versus stored bytes

`WfOrch.Flow.Types.ArtifactRef` records identity, SHA-256 digest, byte length, storage URI, media type, and metadata. PostgreSQL holds **immutable manifests**, not the full payload bytes. The caller must upload bytes to durable content-addressed storage and verify them **before** publishing the manifest. SQL validates the manifest and tenant linkage; it cannot attest that a remote object was actually stored or will remain available.

Artifact lineage is inspectable via task output records, dependency tokens, run events, and DF-02 partition bindings. Orphaned CAS objects after aborted attempts need a separate storage reconciliation/GC policy.

### 4.4 HFSM ⇄ dataflow: one durable boundary

A workflow can start a flow run through `WfOrch.Flow.Provider.Start.newFlowStartProvider`. It consumes a **previously committed HFSM outbox intent**, checks its source release and tenant, and starts an idempotent flow run. At terminal flow completion, `WfOrch.Flow.Worker.runCompletionWorker` (or `runCompletionTick`) delivers a signal to the source HFSM through the CT-06 deduplicated enqueue path:

```text
hfsm/flow/completion/<run-uuid>
```

The application supplies the **correctly encoded HFSM success/failure signals**. Flow does not interpret arbitrary JSON as a typed signal. A second completion delivery returns the earlier signal ID, not another signal for the same run.

A production application with several command operations should use **one command dispatcher/router** for an outbox `kind` (the default planner emits `cmd`), rather than register multiple competing providers for that same kind.

Read more: [DF-01 design](docs/df1_boundedAcyclic.md), [DF-02 design](docs/df2_scatterGather.md), and the executable regression examples in `src/Demo/FlowPostgresRegression.hs` and `src/Demo/FlowDynamicPostgresRegression.hs`.

## 5. Getting the repository running

This section describes the **current repository**, not a published, turnkey package distribution. The Cabal/Stack package and executable are named `thfsm`; the source repository is `ai_wrkforch`.

### Prerequisites

- **GHC / Stack** compatible with `stack.yaml` (currently Stackage LTS 24.65; `system-ghc: true`).
- **PostgreSQL 16+** for the durable runtime and SQL migrations.
- The Haskell dependencies in `package.yaml`, including Hasql, Hasql-TH, Hasql-Pool, Aeson, UUID, HTTP client, and related packages.
- A configured database for database-backed tests. Hasql-TH statement checking can require the target schema to be available when compiling.

**Repository-specific note:** the current `stack.yaml` includes a relative local `minio-hs` `extra-deps` path alongside its pinned dependencies. A fresh checkout outside the existing FUDD development directory may need that local dependency to be made available or the Stack configuration adjusted before it can build. This is a packaging limitation, not an HFSM runtime requirement.

### Database schema and migration order

Run the forward migrations **once, in order**, against a dedicated development database before building/testing DB-dependent modules:

| Order | File | Introduces |
|---|---|---|
| 1 | `Support/001_hfsm_runtime.sql` | Durable machine instances, snapshots, signals, steps, outbox and coordination |
| 2 | `Support/002_hfsm_activity.sql` | Durable activity attempts, retry and failure semantics |
| 3 | `Support/003_hfsm_history_migration.sql` | Pinned releases, replay input evidence, explicit migrations |
| 4 | `Support/004_hfsm_operations.sql` | Tenant/worker admission, quotas, cancellation, dead letters and projections |
| 5 | `Support/005_hfsm_finite_flow.sql` | Static DAGs, tasks, edges, tokens and artifact manifests |
| 6 | `Support/006_hfsm_dynamic_scatter_aggregation.sql` | Scatter groups, partition bindings and bounded graph expansion |

For a **fresh disposable test DB**, a straightforward `psql` sequence is:

```sh
export PGDATABASE=hfsm_test    # point to YOUR dedicated test database
psql -X -v ON_ERROR_STOP=1 -1 -f Support/001_hfsm_runtime.sql
psql -X -v ON_ERROR_STOP=1 -1 -f Support/002_hfsm_activity.sql
psql -X -v ON_ERROR_STOP=1 -1 -f Support/003_hfsm_history_migration.sql
psql -X -v ON_ERROR_STOP=1 -1 -f Support/004_hfsm_operations.sql
psql -X -v ON_ERROR_STOP=1 -1 -f Support/005_hfsm_finite_flow.sql
psql -X -v ON_ERROR_STOP=1 -1 -f Support/006_hfsm_dynamic_scatter_aggregation.sql
```

These are schema migrations, **not repeatable seed scripts**. Use your normal migration ledger in real deployments. `Support/001_initial_instance.sql` is not a seventh migration; `Support/006_diagnostics.sql` is an optional **read-only** DF-02 diagnostic script (it expects a `flow_run_id` `psql` variable).

### Configure the development CLI

The executable parses YAML configuration. Create a local `config.yaml` with the connection parameters for your test environment:

```yaml
debug: 0
db:
  host: localhost
  port: 5432
  user: hfsm_test
  passwd: YOUR_LOCAL_TEST_PASSWORD
  dbase: hfsm_test
```

Then run:

```sh
stack build
stack exec -- thfsm test approval
stack exec -- thfsm test approval --db
```

Pass `--config ./config.yaml` **before** the subcommand if you want to use a custom configuration. The executable otherwise looks at `thfsmCONF` or its current default configuration path (`~/.fudd/twfeng/config.yaml`). The CLI still loads configuration for the in-memory demo even though that branch does not connect to PostgreSQL.

The CLI currently provides `help`, `version` and `test` commands. **Production workers are library-level entry points**, not separate `thfsm worker` subcommands.

### Run the regressions

The names below come from `src/Commands/Test.hs`. Run database tests against **isolated test schemas/databases**, without competing production workers:

```sh
# Pure/in-memory checks
stack exec -- thfsm test approval
stack exec -- thfsm test commitTurn
stack exec -- thfsm test distributed
stack exec -- thfsm test activity
stack exec -- thfsm test history
stack exec -- thfsm test operations
stack exec -- thfsm test flow
stack exec -- thfsm test flowDynamic

# Durable/integration checks
stack exec -- thfsm test approval --db
stack exec -- thfsm test distributedDb --db
stack exec -- thfsm test activityDb --db
stack exec -- thfsm test historyDb --db
stack exec -- thfsm test operationsDb --db
stack exec -- thfsm test flowDb --db
stack exec -- thfsm test flowDynamicDb --db
stack exec -- thfsm test chaosDb --db
```

The separate-process crash harness is **destructive by design** and belongs only in a dedicated test database:

```sh
bash scripts/ct06_kill_matrix.sh
```

Consult the script and its environment requirements before running it. The repo's tests cover substantial correctness properties; they do not replace security audits, performance testing, external-provider fault simulation or backup/restore drills.

## 6. How HFSMs are represented

### Typed authoring model

A machine is a Haskell value parameterized by application types:

```haskell
MachineSpec st sg hf ctx cmd child
```

| Parameter | Meaning |
|---|---|
| `st` | Authored state key |
| `sg` | Incoming signal type |
| `hf` | Result/handoff of a reaction |
| `ctx` | Durable machine context |
| `cmd` | Intended external command |
| `child` | Child workflow key |

The relevant modules are `WfOrch.Spec.Def`, `WfOrch.Spec.Case`, and `WfOrch.Spec.Route`.

- `MachineSpec` identifies the machine, version, root region, codecs and metadata.
- `RegionSpec` identifies its initial state and children.
- `StateSpec` declares state kind, entry behavior, cases, routes and optional nested region.
- `CaseSpec` supplies signal matching and a pure reaction.
- `RouteSpec` maps a handoff to a structural control plan.

A machine can describe atomic, composite and terminal states; control plans include staying or moving, waiting, timers, child spawning, joins and breakpoints. The graph can be validated before the first instance exists.

### Why route handoffs separately?

A reaction produces:

```text
(current context, incoming signal)
      → (new context, handoff, commands, notes)
```

The route table then interprets the handoff as **control movement**. This keeps business decisions local to the relevant state and keeps orchestration structure inspectable. It also makes state logic easier to test without a database or worker process.

### Compilation, validation, and rendering

`WfOrch.Compile.compile` produces a `CompiledMachine` containing:

```text
original typed specification
   + normalized, serializable MachineGraph
   + executable Haskell reaction table
   + compiled digest
```

The graph has no Haskell closures. Its nodes/edges expose regions, states, cases, routes, timers and joins. Run `WfOrch.Validate.Report.validate` and require `ok` before registration. The validation modules check structure, routes, reachability, waits, joins, and compatibility concerns.

For inspection and tooling, the repository includes `WfOrch.Render.Print`, `WfOrch.Export.Graphviz`, `WfOrch.Export.Mermaid`, `WfOrch.Export.Json` and `WfOrch.Export.Scxml`. Exported representations describe machine structure; they are **not a replacement for the typed Haskell executable reactions**.

The authoring model is Haskell-first. FUDD's proposed FWL/FWIR compiler is a **separate integration project**, not an already-provided `ai_wrkforch` source parser.

## 7. How a durable HFSM turn works

The key correctness contract is in `WfOrch.Store.Class`:

```haskell
data Store m = Store
  { leaseSignal  :: LeaseRq -> m LeaseRez
  , loadInstance :: UUID -> m (Maybe InstanceRw)
  , loadSnapshot :: UUID -> m (Maybe SnapshotRw)
  , commitTurn   :: CommitRq -> m (Either StoreErr CommitRez)
  , ackSignal    :: AckRq -> m (Either StoreErr ())
  , renewLease   :: RenewRq -> m (Either StoreErr UTCTime)
  , listSignals  :: PollRq -> m [SignalRw]
  }
```

**`commitTurn` is the only successful-turn persistence boundary.** A successful input is not acknowledged in a separate operation. The transaction validates the expected revision/snapshot and leased input, then commits the new step, snapshot, outbox, child/break/projection evidence, instance advance **and signal acknowledgement together**. A conflict or expired fence commits no partial turn.

```text
ordered durable signal
         │
     acquire lease ◄──── worker UUID + expiry + unique fencing UUID
         │
 load instance + snapshot
         │
 resolve EXACT machine version/digest from Registry
         │
 pure planStep                 no HTTP, no DB mutations, no AI calls
         │
 atomic commitTurn             revision/snapshot CAS + lease check
         │
 persisted new state + history + intended commands
```

The pure planner lives in `WfOrch.Runtime.Engine.Step`; the worker loop lives in `WfOrch.Runtime.Engine.Worker`. A stale worker may finish calculating, but a lost lease or changed revision prevents it from winning the commit. If a signal cannot produce a valid turn, `ackSignal` handles **rejection/failure/requeue only**, under its own lease check.

The durable data model contains machine releases, instances, signals, steps, snapshots, an outbox, child relationships, breakpoint events, projection events, and supporting operational records. `WfOrch.Store.Postgres` provides the PostgreSQL implementation; `Demo.MemoryStore` supplies the test implementation.

### Activity delivery is deliberately separate (CT-04)

Suppose a reaction emits `SendEmail` or `CallAIModel`. That intent is persisted with the HFSM turn. A separate `WfOrch.Activity.Worker` claims the outbox item and calls a registered `Provider`:

```haskell
data Provider = Provider
  { kind                :: Text
  , supportsIdempotency :: Bool
  , deliver             :: ActivityLease -> IO (Either ProviderFailure Value)
  }
```

The effect has a stable `effectKey`, attempt history, bounded retry settings, a lease and a provider-result record. A successful HTTP response whose acknowledgement is lost is an **ambiguous outcome**. The runtime can retry automatically only under the configured replay-safety/idempotency policy; it cannot guarantee that an arbitrary external service executes exactly once.

A provider should support a stable idempotency key whenever possible. Activity outcomes that drive subsequent control movement are enqueued back as **new durable signals**, not executed inside the pure planner. The implementation includes HTTP and internal-enqueue provider adapters.

### Waiting, timers, children, and joins

Long waits do not require a live Haskell continuation. The snapshot records the relevant wait/control information; future signals, child completions or timer-driven events reactivate the machine. Parent/child and join semantics are represented structurally in compiled control plans and persisted supporting records. The operationally important rule is the same: **the next state change occurs only through a fenced durable turn**.

## 8. History, replay, releases, and migration (CT-05)

A long-lived workflow must remain understandable after an upgrade or incident.

### Immutable identity

Each registered executable release has a machine name, version and spec/graph digests. `WfOrch.History.Release.registerPinnedRelease` adds an explicit **implementation fingerprint** covering the actual Haskell executable/codec build, because a serialized graph digest by itself cannot identify arbitrary Haskell closures.

An instance remains pinned to its recorded identity. Never silently load an old snapshot into a new implementation with different semantics.

### Replay is verification, not effect re-execution

`WfOrch.History.Postgres` exposes paginated history. `WfOrch.History.Replay.verifyTurn` / `verifyTimeline` use recorded pre-commit input evidence to rerun the **pure planner** and compare expected state, handoffs, routes, output intents and projections to durable history. It does not re-send emails, re-query AI services, or repeat dataflow computations.

Historical turns created **before** CT-05 did not necessarily capture every transient planner input. Those turns remain auditable but are not guaranteed to meet strict replay's evidence requirements. Replaying against another binary also requires that binary's appropriate pinned release implementation.

### Migration is a controlled operation

`WfOrch.Migration.Plan` and `WfOrch.Migration.Postgres` provide explicit migration planning/commit. The application supplies mappings for old context, control state, waits, children and lifecycle status into a target validated release. The migration checks revision/snapshot identity and outstanding-work restrictions, persists a new revision/snapshot, and appends immutable migration evidence.

This is different from resuming the same instance under changed Haskell code. Treat migration as a deliberate, tested business operation.

Related modules also include `WfOrch.Migrate.Diff`, `WfOrch.Migrate.Check` and `WfOrch.Migrate.Run` for graph comparison and compatibility analysis.

## 9. Operating the runtime (CT-06)

The engine has a **production-oriented execution path** above the lower-level stores. Use the CT-06 APIs instead of the older, policy-bypassing stores for production admission:

```haskell
WfOrch.Operations.Postgres.productionStore
WfOrch.Operations.Postgres.productionActivityStore
WfOrch.Operations.Runtime.runSignalWorker
WfOrch.Operations.Runtime.runActivityWorker
```

Or use the traced worker variants. These share the existing transactional commit/finish logic but use controlled claims and supervised worker registration.

### Tenants, queues, and resources

- **Tenants:** associated instances, signals, effects and flow runs; quotas for outstanding work and concurrency.
- **Concurrency groups:** caps on competing signals, activities and dataflow tasks within a group.
- **Priority and aging:** priority across eligible instances/tasks, with bounded aging. Signals retain per-instance ordering.
- **Workers:** UUID identity, capability names, generation fences, TTL-based heartbeats, active/draining/stopped states.
- **Dataflow resources:** worker CPU units, memory units and task slots; claims respect available reservations and declared executor capabilities.

The current scheduler uses a **shared PostgreSQL admission gate** to serialize competing quota/resource decisions. It favors correctness and clear fencing over maximum admission throughput. Benchmark it with real workloads before deciding whether to shard admission.

### Cancellation and dead letters

`WfOrch.Operations.Postgres.cancelInstance` uses an expected revision and records an immutable operator action. Failed signals/effects are inspectable through dead-letter records. `redriveSignal` creates a **new input** linked to the failure rather than modifying the original; effects with ambiguous outcomes require explicit provider reconciliation instead of automatic blind re-execution.

Cancellation cannot undo a side effect that has already happened at an external provider. Treat operator actor identifiers as audit data, **not authentication credentials**.

### Correlation, telemetry, and projections

Correlation and causation IDs, together with W3C `traceparent`, propagate through the relevant signal/outbox paths. `WfOrch.Operations.Trace` and `WfOrch.Operations.Traced` provide optional OpenTelemetry-compatible OTLP/HTTP tracing; telemetry export failures must not alter committed workflow semantics.

Authoritative historical records feed **asynchronous, revision-ordered, rebuildable projections** via `WfOrch.Operations.Projection`. Operational views can be reset/rebuilt without rewriting the step/snapshot history. Full queue and child-tree materializations can require additional handling because live lease and child status also change outside normal successful turns.

### Production-facing API examples

```haskell
-- Illustrative boundary selection, not a complete server:
let hfsmStore = Ops.productionStore runner
let actStore  = Ops.productionActivityStore runner
let flowApi   = Dynamic.dynamicFlowStore runner
```

Here `Ops` is `WfOrch.Operations.Postgres`, `Dynamic` is `WfOrch.Flow.Dynamic.Postgres`, and `runner` is the `Runner` created from the application's Hasql pool. Register workers with matching capabilities and resource limits before claiming work. See `WfOrch.Operations.Runtime`, `WfOrch.Flow.Dynamic.Runtime` and the integration regressions for concrete lifecycles.

## 10. API/module map

Most applications will start with a small set of entry points rather than import the entire source tree.

| Goal | Begin with |
|---|---|
| Define states, typed reactions and routes | `WfOrch.Spec.Def`, `WfOrch.Spec.Case`, `WfOrch.Spec.Route` |
| Compile and validate | `WfOrch.Compile`, `WfOrch.Validate.Report` |
| Register a compiled release | `WfOrch.Runtime.Registry`, `WfOrch.History.Release` |
| Execute the core machine | `WfOrch.Runtime.Engine.Worker`, `WfOrch.Store.Class` |
| Persist machine execution | `WfOrch.Store.Postgres`, `WfOrch.Operations.Postgres` |
| Deliver durable external effects | `WfOrch.Activity.Worker`, `WfOrch.Activity.Types` |
| Build a static computation DAG | `WfOrch.Flow.Spec`, `WfOrch.Flow.Postgres` |
| Implement dataflow computations | `WfOrch.Flow.Worker`, `WfOrch.Flow.Types` |
| Generate bounded scatter/gather | `WfOrch.Flow.Dynamic.Spec`, `WfOrch.Flow.Dynamic.Provider` |
| Run resource-managed dynamic workers | `WfOrch.Flow.Dynamic.Runtime`, `WfOrch.Flow.Dynamic.Postgres` |
| Start a flow from an HFSM | `WfOrch.Flow.Provider.Start` |
| Inspect and verify history | `WfOrch.History.Postgres`, `WfOrch.History.Replay` |
| Plan an instance migration | `WfOrch.Migration.Plan`, `WfOrch.Migration.Postgres` |
| Coordinate workers and operations | `WfOrch.Operations.Runtime`, `WfOrch.Operations.Projection` |
| Trace, export and render | `WfOrch.Operations.Trace`, `WfOrch.Export.*`, `WfOrch.Render.Print` |
| Learn from actual end-to-end tests | `src/Demo/ApprovalRuntime.hs`, `src/Demo/FlowPostgresRegression.hs`, `src/Demo/FlowDynamicPostgresRegression.hs` |

### Semantic layers

```text
WfOrch.Core / WfOrch.Spec        authored typed workflow
         ↓
WfOrch.Compile / Graph         normalized structure + Haskell execution table
         ↓
WfOrch.Validate                check before execution
         ↓
WfOrch.Runtime / Store         pure planning + atomic, fenced persistence
         ├── WfOrch.Activity   durable external effects
         ├── WfOrch.Flow       task graph / tokens / artifact lineage
         │      └── Dynamic  bounded expansion and reduction
         ├── WfOrch.History    audit and replay
         └── WfOrch.Operations admission, workers, telemetry, projections
```

## 11. Development milestones and evidence

This repository has progressed through the following implemented workstreams:

| Stage | Principal result | Tests/examples |
|---|---|---|
| Core foundation | Typed authoring, compilation, graph validation, rendering | `approval` |
| CT-01 | Atomic successful turns | `commitTurn` |
| CT-02 / CT-02A | PostgreSQL schema and transactional store | `approval --db` |
| CT-03 | Distributed leases, ordering and fencing | `distributed`, `distributedDb` |
| CT-04 | Durable outbox activities and retry policy | `activity`, `activityDb` |
| CT-05 | Immutable releases, strict replay and explicit migration | `history`, `historyDb` |
| CT-06 | Tenant operations, managed workers, dead letters, tracing, projections and faults | `operations`, `operationsDb`, `chaosDb` |
| DF-01 | Finite DAGs, tokens, artifacts and worker resources | `flow`, `flowDb` |
| DF-02 | Bounded scatter/gather and partitioned aggregation | `flowDynamic`, `flowDynamicDb` |

The tests have been reported passing in the integrated repository. This table lists **implemented scope and its intended tests**, not a guarantee that every workload, SQL privilege arrangement, external provider and deployment topology has been exercised.

## 12. Design guarantees—and their limits

The package draws several boundaries deliberately:

1. **Atomic control, not atomic external reality.** `commitTurn` atomically records state and intended effects. An HTTP call or email is not part of that database transaction. End-to-end exactly-once *external* effects require provider-side idempotency or reconciliation.
2. **Pure replay, not re-running the outside world.** Replay verifies the control planner using recorded inputs; it does not guarantee the same nondeterministic AI response would be generated today.
3. **Immutable manifests, not magically durable bytes.** SQL checks the identity of output artifacts; storage adapters must upload, verify and retain bytes separately.
4. **Bounded dynamic graphs, not unbounded computation.** DF-02 materializes a finite expansion. Nested scatter, streaming windows and incremental cross-run recomputation are not provided.
5. **Tenant metadata and quotas, not full application authorization.** CT-06 assumes a trusted service, appropriate database grants, and an external authentication/authorization layer. The SQL migrations do **not** establish a complete PostgreSQL RLS policy perimeter.
6. **Capabilities describe admission, not a security sandbox.** A worker capability name controls task assignment; a trusted capability registry, input validation, permissions and execution isolation belong to the host platform.
7. **An executable graph digest is not a complete binary attestation.** CT-05 adds an implementation ID so closures and codecs can be tied to a deployed build. Keep the corresponding release artifacts available when strict replay or migration requires them.

These limits are meaningful engineering contracts. Preserving them is preferable to promising exactly-once behavior or isolation that the underlying systems cannot provide.

## 13. What remains before broad production deployment

`ai_wrkforch` is suitable for internal FUDD applications and for beginning VIK/Skill Factory integration. Wider production deployment calls for additional work:

- **Security:** service authentication/authorization, tenant-scoped read enforcement, database-role separation/RLS where appropriate, operator RBAC and secret management.
- **Provider governance:** typed capability registry, authorization at invocation time, idempotency reconciliation, audit receipts, sandboxing for untrusted executors, and AI/API expenditure limits.
- **Operations:** production load tests, backpressure and fair scheduling benchmarks, recovery drills, metrics/alerts, SLOs, backups and retention/GC policies.
- **Runtime integration:** ergonomic handling of many independent machine type families; deployment tooling for pinned implementations; mature heterogeneous workflow APIs.
- **Dataflow evolution (as warranted by workloads):** multiple/nested dynamic scatters, streaming/partial aggregates, caching and incremental recomputation, larger-scale scheduler partitioning.
- **Developer experience:** richer examples, deployment manifests, an admin Wapp, conformance fixtures and continuously executable integration documentation.

For **VIK**, the main next work is authoritative goal/evidence/decision models and controlled AI/domain capabilities; for the **FUDD Skill Factory**, it is typed FWIR/FWL compilation, source provenance and trusted capability binding. Neither product currently needs a new general workflow engine.

## 14. Repository guides and examples

- [Atomic turn design](docs/ct1_atomicStep.md)
- [Distributed safety](docs/ct3_distributedSafety.md)
- [History, replay and migration](docs/ct5_historyReplay.md)
- [Operational runtime](docs/ct06_operImpr.md)
- [DF-01: bounded static dataflow](docs/df1_boundedAcyclic.md)
- [DF-02: dynamic scatter/gather and partitioned aggregation](docs/df2_scatterGather.md)
- `src/Demo/ApprovalWfOrch.hs` — full typed machine and pure interpreter example
- `src/Demo/ApprovalRuntime.hs` — memory and PostgreSQL execution of the same workflow
- `src/Demo/FlowRegression.hs` — static diamond DAG
- `src/Demo/FlowPostgresRegression.hs` — tokens, artifact lineage, resource claims and HFSM completion
- `src/Demo/FlowDynamicRegression.hs` — deterministic bounded expansion and reduction-tree generation
- `src/Demo/FlowDynamicPostgresRegression.hs` — SQL atomic expansion, two-worker races, recovery, providers and HFSM completion
- `scripts/ct06_kill_matrix.sh` — **test-only** process termination harness

---

**Bottom line:** If your Haskell application must coordinate decisions and computations across crashes, people, services, and worker processes, `ai_wrkforch` gives you a typed, auditable control engine with a complementary parallel dataflow executor. Start with the approval example, graduate to the diamond DAG, and then compose HFSM with bounded scatter/gather for workflows whose computation is larger than their control graph.
