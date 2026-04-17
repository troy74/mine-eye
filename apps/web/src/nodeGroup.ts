import type { ApiNode } from "./graphApi";

export type GroupPort = {
  id: string;
  label: string;
  semantic: string;
  optional?: boolean;
};

export type GroupNodeDef = {
  id: string;
  kind: string;
  category: string;
  params?: Record<string, unknown>;
  position?: {
    x: number;
    y: number;
  };
};

export type GroupEdgeDef = {
  from_node_id: string;
  from_port_id: string;
  from_output_index?: number;
  to_node_id: string;
  to_port_id: string;
};

export type GroupInputBinding = {
  group_input_id: string;
  internal_node_id: string;
  internal_port_id: string;
};

export type GroupOutputBinding = {
  group_output_id: string;
  internal_node_id: string;
  internal_port_id: string;
  internal_output_index?: number;
};

export type GroupDefinition = {
  version: number;
  label: string;
  display?: {
    icon?: string;
    accent?: string;
    badge?: string;
  };
  inputs: GroupPort[];
  outputs: GroupPort[];
  internal_nodes: GroupNodeDef[];
  internal_edges: GroupEdgeDef[];
  input_bindings: GroupInputBinding[];
  output_bindings: GroupOutputBinding[];
};

export type GroupTemplate = {
  id: string;
  label: string;
  definition: GroupDefinition;
};

export const NODE_GROUP_MAX_DEPTH = 3;

export function defaultGroupNodePosition(index: number): { x: number; y: number } {
  return {
    x: 60 + (index % 4) * 220,
    y: 60 + Math.floor(index / 4) * 140,
  };
}

export function groupDefinitionFromParams(
  params: Record<string, unknown> | null | undefined
): GroupDefinition | null {
  const raw =
    params?.group_definition ??
    ((params?.ui as Record<string, unknown> | undefined)?.group_definition as
      | Record<string, unknown>
      | undefined);
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return null;
  return ensureGroupDefinitionLayout(raw as GroupDefinition);
}

export function ensureGroupDefinitionLayout(def: GroupDefinition): GroupDefinition {
  return {
    ...def,
    internal_nodes: (def.internal_nodes ?? []).map((node, idx) => ({
      ...node,
      position: node.position ?? defaultGroupNodePosition(idx),
    })),
  };
}

export function groupDefinitionFromNode(node: ApiNode | null | undefined): GroupDefinition | null {
  return groupDefinitionFromParams(node?.config?.params);
}

export function groupTemplateIdFromNode(node: ApiNode | null | undefined): string {
  const raw =
    node?.config?.params?.group_template_id ??
    ((node?.config?.params?.ui as Record<string, unknown> | undefined)?.group_template_id as
      | string
      | undefined);
  return typeof raw === "string" ? raw : "";
}

export function cloneGroupDefinition(def: GroupDefinition): GroupDefinition {
  return ensureGroupDefinitionLayout(JSON.parse(JSON.stringify(def)) as GroupDefinition);
}

export function groupDefinitionForInternalNode(
  node: GroupNodeDef | null | undefined
): GroupDefinition | null {
  if (!node) return null;
  const params =
    node.params && typeof node.params === "object" && !Array.isArray(node.params)
      ? (node.params as Record<string, unknown>)
      : {};
  return groupDefinitionFromParams(params);
}

export function setInternalNodeGroupDefinition(
  node: GroupNodeDef,
  definition: GroupDefinition,
  templateId?: string
): GroupNodeDef {
  const params =
    node.params && typeof node.params === "object" && !Array.isArray(node.params)
      ? ({ ...node.params } as Record<string, unknown>)
      : {};
  const ui =
    params.ui && typeof params.ui === "object" && !Array.isArray(params.ui)
      ? ({ ...(params.ui as Record<string, unknown>) } as Record<string, unknown>)
      : {};
  if (templateId) ui.group_template_id = templateId;
  ui.group_definition = ensureGroupDefinitionLayout(definition);
  params.ui = ui;
  return { ...node, params };
}

export function maxGroupDefinitionDepth(def: GroupDefinition): number {
  let deepest = 1;
  for (const node of def.internal_nodes ?? []) {
    if (node.kind !== "node_group") continue;
    const nested = groupDefinitionForInternalNode(node);
    if (!nested) continue;
    deepest = Math.max(deepest, 1 + maxGroupDefinitionDepth(nested));
  }
  return deepest;
}

export function resolveGroupDefinitionAtPath(
  root: GroupDefinition,
  path: string[]
): GroupDefinition | null {
  let current: GroupDefinition | null = ensureGroupDefinitionLayout(root);
  for (const nodeId of path) {
    if (!current) return null;
    const internal = current.internal_nodes.find((node) => node.id === nodeId) ?? null;
    current = groupDefinitionForInternalNode(internal);
    if (!current) return null;
  }
  return current;
}

export function updateGroupDefinitionAtPath(
  root: GroupDefinition,
  path: string[],
  updater: (current: GroupDefinition) => GroupDefinition
): GroupDefinition {
  if (path.length === 0) {
    return ensureGroupDefinitionLayout(updater(ensureGroupDefinitionLayout(root)));
  }
  const [head, ...rest] = path;
  const safeRoot = ensureGroupDefinitionLayout(root);
  return ensureGroupDefinitionLayout({
    ...safeRoot,
    internal_nodes: safeRoot.internal_nodes.map((node) => {
      if (node.id !== head || node.kind !== "node_group") return node;
      const nested = groupDefinitionForInternalNode(node);
      if (!nested) return node;
      const nextNested = updateGroupDefinitionAtPath(nested, rest, updater);
      return setInternalNodeGroupDefinition(node, nextNested);
    }),
  });
}

export function groupVisualMeta(node: ApiNode | null | undefined): {
  icon: string;
  accent: string;
  badge: string;
} {
  const templateId = groupTemplateIdFromNode(node);
  const template = GROUP_TEMPLATES.find((item) => item.id === templateId) ?? null;
  const def = groupDefinitionFromNode(node);
  const display = def?.display ?? template?.definition.display ?? {};
  return {
    icon: display.icon ?? "▣",
    accent: display.accent ?? "#ff9e3d",
    badge: display.badge ?? "GROUP",
  };
}

export function groupPortsFromNode(
  node: ApiNode | null | undefined,
  direction: "in" | "out"
): GroupPort[] {
  const def = groupDefinitionFromNode(node);
  if (!def) return [];
  return direction === "in" ? def.inputs ?? [] : def.outputs ?? [];
}

export const GROUP_TEMPLATES: GroupTemplate[] = [
  {
    id: "ip_model",
    label: "IP model",
    definition: {
      version: 1,
      label: "IP model",
      display: { icon: "∿", accent: "#e85aad", badge: "GROUP" },
      inputs: [
        { id: "clean_observations", label: "Clean observations", semantic: "data_table" },
      ],
      outputs: [
        { id: "inversion_voxels", label: "Inversion voxels", semantic: "mesh" },
        { id: "inversion_result", label: "Inversion result", semantic: "data_table" },
        { id: "diagnostics", label: "Diagnostics", semantic: "semantic_json" },
        { id: "inversion_report", label: "Inversion report", semantic: "semantic_json" },
        { id: "pseudo_rows", label: "Pseudo rows", semantic: "data_table", optional: true },
        { id: "ip_mesh", label: "IP mesh", semantic: "data_table", optional: true },
      ],
      internal_nodes: [
        { id: "pseudo", kind: "ip_pseudosection", category: "transform", params: {}, position: { x: 60, y: 120 } },
        { id: "mesh", kind: "ip_inversion_mesh", category: "model", params: {}, position: { x: 320, y: 260 } },
        { id: "input", kind: "ip_inversion_input", category: "model", params: {}, position: { x: 590, y: 190 } },
        { id: "invert", kind: "ip_invert", category: "model", params: {}, position: { x: 860, y: 190 } },
      ],
      internal_edges: [
        { from_node_id: "pseudo", from_port_id: "pseudo_rows", from_output_index: 1, to_node_id: "mesh", to_port_id: "pseudo_in" },
        { from_node_id: "pseudo", from_port_id: "pseudo_rows", from_output_index: 1, to_node_id: "input", to_port_id: "pseudo_in" },
        { from_node_id: "mesh", from_port_id: "mesh_rows", from_output_index: 0, to_node_id: "input", to_port_id: "mesh_in" },
        { from_node_id: "input", from_port_id: "inversion_input", from_output_index: 0, to_node_id: "invert", to_port_id: "inversion_input" },
      ],
      input_bindings: [
        { group_input_id: "clean_observations", internal_node_id: "pseudo", internal_port_id: "survey_in" },
        { group_input_id: "clean_observations", internal_node_id: "input", internal_port_id: "observations_in" },
      ],
      output_bindings: [
        { group_output_id: "inversion_voxels", internal_node_id: "invert", internal_port_id: "inversion_voxels", internal_output_index: 0 },
        { group_output_id: "inversion_result", internal_node_id: "invert", internal_port_id: "inversion_result", internal_output_index: 2 },
        { group_output_id: "diagnostics", internal_node_id: "invert", internal_port_id: "diagnostics", internal_output_index: 3 },
        { group_output_id: "inversion_report", internal_node_id: "invert", internal_port_id: "report", internal_output_index: 4 },
        { group_output_id: "pseudo_rows", internal_node_id: "pseudo", internal_port_id: "pseudo_rows", internal_output_index: 1 },
        { group_output_id: "ip_mesh", internal_node_id: "mesh", internal_port_id: "mesh_rows", internal_output_index: 0 },
      ],
    },
  },
  {
    id: "drillhole_ingest",
    label: "Drillhole ingest",
    definition: {
      version: 1,
      label: "Drillhole ingest",
      display: { icon: "⌁", accent: "#4cbf6b", badge: "GROUP" },
      inputs: [],
      outputs: [
        { id: "collars", label: "Collars", semantic: "point_set" },
        { id: "survey_rows", label: "Survey rows", semantic: "data_table" },
      ],
      internal_nodes: [
        { id: "collars", kind: "collar_ingest", category: "input", params: {}, position: { x: 80, y: 180 } },
        { id: "survey", kind: "survey_ingest", category: "input", params: {}, position: { x: 360, y: 180 } },
      ],
      internal_edges: [],
      input_bindings: [],
      output_bindings: [
        { group_output_id: "collars", internal_node_id: "collars", internal_port_id: "collars", internal_output_index: 0 },
        { group_output_id: "survey_rows", internal_node_id: "survey", internal_port_id: "survey_rows", internal_output_index: 0 },
      ],
    },
  },
];
