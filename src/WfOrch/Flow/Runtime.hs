{-# LANGUAGE NoFieldSelectors #-}
-- | One registered CT-06 worker identity + resources + heartbeats.
-- Workflow/flow workers should be separate processes or carry both capability
-- sets explicitly. The lifecycle wrapper rejects a mismatched worker UUID.
module WfOrch.Flow.Runtime (runManagedFlowWorker) where

import WfOrch.Operations.Types (WorkerRegistration(..))
import WfOrch.Operations.Runtime (withRegisteredWorker)
import WfOrch.Flow.Types (WorkerResources(..))
import WfOrch.Flow.Worker (FlowWorkerCfg(..), TaskProvider)
import qualified WfOrch.Flow.Worker as Worker
import qualified WfOrch.Flow.Postgres as Flow
import WfOrch.Store.Postgres.Types (Runner)

runManagedFlowWorker :: Runner -> WorkerRegistration -> WorkerResources
                     -> FlowWorkerCfg -> [TaskProvider] -> IO ()
runManagedFlowWorker runner registration resources cfg providers
  | registration.workerId /= resources.workerId ||
    registration.workerId /= cfg.workerId =
      fail "DF-01 worker IDs must match CT-06 registry identity"
  | otherwise = withRegisteredWorker runner registration $ do
      applied <- Flow.registerResources runner resources
      case applied of
        Left err -> fail ("DF-01 resource registration failed: " <> show err)
        Right False -> fail "DF-01 resource registration refused"
        Right True -> Worker.runWorker cfg (Flow.flowStore runner) providers
