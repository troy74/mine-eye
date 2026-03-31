import { createContext, useContext } from "react";

export type InspectorTab =
  | "summary"
  | "diagnostics"
  | "mapping"
  | "crs"
  | "output";

type Ctx = {
  openInspector: (nodeId: string, tab?: InspectorTab) => void;
  /** Focus the 2D map tab on a `plan_view_2d` node (data comes only from its wired inputs). */
  openPlanMapViewer: (nodeId: string) => void;
};

export const GraphInspectorContext = createContext<Ctx | null>(null);

export function useGraphInspector(): Ctx {
  const v = useContext(GraphInspectorContext);
  if (!v) throw new Error("useGraphInspector: missing provider");
  return v;
}
