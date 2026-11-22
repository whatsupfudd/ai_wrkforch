{-# LANGUAGE NoFieldSelectors #-}

-- | Production CT-06 registration and resource supervision for DF-02.
-- Use this instead of DF-01's runManagedFlowWorker when dynamic partitions
-- need their persisted "item" input injected into FlowTaskLease.
module WfOrch.Flow.Dynamic.Runtime
  ( runManagedDynamicWorker ) where

import WfOrch.Operations.Types (WorkerRegistration(..))
import WfOrch.Operations.Runtime (withRegisteredWorker)
import WfOrch.Flow.Types (WorkerResources(..))
import WfOrch.Flow.Worker (FlowWorkerCfg(..), TaskProvider)
import qualified WfOrch.Flow.Worker as Worker
import qualified WfOrch.Flow.Postgres as Flow
import qualified WfOrch.Flow.Dynamic.Postgres as Dynamic
import WfOrch.Store.Postgres.Types (Runner)

runManagedDynamicWorker :: Runner -> WorkerRegistration -> WorkerResources
                        -> FlowWorkerCfg -> [TaskProvider] -> IO ()
runManagedDynamicWorker runner registration resources config providers
  | registration.workerId /= resources.workerId ||
    registration.workerId /= config.workerId =
      fail "DF-02 worker identity must match CT-06 registration and resource reservation"
  | otherwise = withRegisteredWorker runner registration $ do
      registered <- Flow.registerResources runner resources
      case registered of
        Left err -> fail ("DF-02 resource registration failed: " <> show err)
        Right False -> fail "DF-02 resource reservation refused"
        Right True -> Worker.runWorker config (Dynamic.dynamicFlowStore runner) providers
