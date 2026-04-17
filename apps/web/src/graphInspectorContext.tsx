import { createContext, useContext } from "react";

export type InspectorTab =
  | "summary"
  | "preview"
  | "diagnostics"
  | "config"
  | "mapping"
  | "crs"
  | "output";

type Ctx = {
  openInspector: (nodeId: string, tab?: InspectorTab) => void;
  /** Open/focus a preview tab for this node. */
  openNodeViewer: (nodeId: string) => void;
  /** Queue this node (and downstream) for execution. */
  queueNodeRun: (nodeId: string, opts?: { includeManual?: boolean }) => Promise<void>;
  /** Save an alias/display name for this node. Pass empty string to clear. */
  renameNode: (nodeId: string, alias: string) => Promise<void>;
  /** Toggle the recompute policy between manual (locked) and auto. */
  toggleLock: (nodeId: string, isCurrentlyLocked: boolean) => Promise<void>;
  /** Open a dedicated node editor surface when available. */
  openNodeEditor?: (nodeId: string) => void;
  /** Open the AOI bbox map editor tab for this node. */
  openAoiEditor?: (nodeId: string) => void;
};

export const GraphInspectorContext = createContext<Ctx | null>(null);

export function useGraphInspector(): Ctx {
  const v = useContext(GraphInspectorContext);
  if (!v) throw new Error("useGraphInspector: missing provider");
  return v;
}
