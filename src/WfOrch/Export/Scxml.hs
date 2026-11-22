{-# LANGUAGE DerivingStrategies #-}
module WfOrch.Export.Scxml ( exportScxml ) where

import Data.Text (Text, pack)

import WfOrch.Graph.Def (MachineGraph)

data ScxmlErr =
    EmptyEr
  | InvalidCharEr Int Char
  deriving stock (Eq, Ord, Show, Read)

exportScxml :: MachineGraph -> Either ScxmlErr Text
exportScxml graph =
  -- TODO: provide a proper SCXML export.
  pure (pack $ show graph)
