module WfOrch.Export.Graphviz ( exportDot ) where

import Data.Text (Text, pack)

import WfOrch.Graph.Def (MachineGraph(..))
import WfOrch.Core (machineNameText)
import WfOrch.Core.Name (regionNameText)

exportDot :: MachineGraph -> Text
exportDot graph =
  -- TODO: provide a proper DOT export.
  pack $ show graph
