import { createContext, useContext } from "react";

export type InspectorTab =
  | "summary"
  | "diagnostics"
  | "mapping"
  | "crs"
  | "output";

type Ctx = {
  openInspector: (nodeId: string, tab?: InspectorTab) => void;
  /** Open/focus a preview tab for this node. */
  openNodeViewer: (nodeId: string) => void;
  /** Queue this node (and downstream) for execution. */
  queueNodeRun: (nodeId: string, opts?: { includeManual?: boolean }) => Promise<void>;
};

export const GraphInspectorContext = createContext<Ctx | null>(null);

export function useGraphInspector(): Ctx {
  const v = useContext(GraphInspectorContext);
  if (!v) throw new Error("useGraphInspector: missing provider");
  return v;
}
