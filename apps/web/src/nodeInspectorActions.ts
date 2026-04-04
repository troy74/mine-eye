import type { ApiNode } from "./graphApi";
import type { InspectorTab } from "./graphInspectorContext";
import type { RegistryNodeSpec } from "./nodeRegistry";

export type NodeActionKey = "run" | "lock" | "edit" | "config" | "preview";

export type NodeInspectorCapabilities = {
  canRun: boolean;
  canLock: boolean;
  canEdit: boolean;
  canConfig: boolean;
  canPreview: boolean;
  editTab: InspectorTab;
  configTab: InspectorTab;
  previewTab: InspectorTab;
};

export function resolveNodeInspectorCapabilities(
  node: ApiNode,
  opts: {
    csvCapable: boolean;
    hasConfigTab: boolean;
    hasMappingTab: boolean;
    hasCrsTab: boolean;
    nodeSpec?: RegistryNodeSpec;
  }
): NodeInspectorCapabilities {
  const canEdit = opts.csvCapable || opts.hasConfigTab;
  const editTab: InspectorTab = opts.csvCapable
    ? "mapping"
    : opts.hasConfigTab
      ? "config"
      : "summary";
  const configTab: InspectorTab = opts.hasConfigTab
    ? "config"
    : opts.hasCrsTab
      ? "crs"
      : opts.hasMappingTab
        ? "mapping"
        : "diagnostics";

  const fallback: NodeInspectorCapabilities = {
    canRun: true,
    canLock: true,
    canEdit,
    canConfig: true,
    canPreview: true,
    editTab,
    configTab,
    previewTab: "preview",
  };

  const ia = opts.nodeSpec?.interaction?.actions;
  if (!ia) return fallback;
  const tabOr = (raw: unknown, dflt: InspectorTab): InspectorTab =>
    raw === "summary" ||
    raw === "preview" ||
    raw === "diagnostics" ||
    raw === "config" ||
    raw === "mapping" ||
    raw === "crs" ||
    raw === "output"
      ? raw
      : dflt;
  return {
    canRun: ia.run?.allowed ?? fallback.canRun,
    canLock: ia.lock_toggle?.allowed ?? fallback.canLock,
    canEdit: ia.edit?.allowed ?? fallback.canEdit,
    canConfig: ia.config?.allowed ?? fallback.canConfig,
    canPreview: ia.preview?.allowed ?? fallback.canPreview,
    editTab: tabOr(ia.edit?.tab, fallback.editTab),
    configTab: tabOr(ia.config?.tab, fallback.configTab),
    previewTab: tabOr(ia.preview?.tab, fallback.previewTab),
  };
}

export function lockLabel(node: ApiNode): string {
  return node.policy.recompute === "manual" ? "Unlock" : "Lock";
}
