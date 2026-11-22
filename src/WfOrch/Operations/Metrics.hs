-- | Lightweight durable tenant counters suitable for scraping or dashboard
-- materialization. Metrics never mutate authoritative or projection rows.
module WfOrch.Operations.Metrics (tenantMetrics) where

import Data.Aeson (Value)
import qualified Data.Text as T
import Data.UUID (UUID)
import qualified Hasql.Session as Session
import WfOrch.Operations.Types (OpErr(..))
import WfOrch.Store.Postgres.Types (Runner(..))
import qualified WfOrch.Operations.Postgres.Statements as S

tenantMetrics :: Runner -> UUID -> IO (Either OpErr Value)
tenantMetrics runner tenant = do
  result <- runSession runner $ Session.statement tenant S.tenantMetrics
  pure $ either (Left . OpBackend . T.pack . show) Right result
