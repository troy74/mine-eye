/**
 * Port / link typing for the graph UI (V1SPEC semantic ports + logical refinements).
 *
 * Rust `SemanticPortType` is the wire contract. In the product we also care about **logical**
 * refinements on a link — e.g. collars are a PointSet (or Table in legacy graphs) tagged by
 * `from_port` / `to_port` names like `collars`. Consumers that accept PointSet or Table can
 * still consume collar outputs; a dedicated collar-only consumer would check the port name.
 *
 * Future: add `CollarSet` etc. to the backend enum + migrations; until then refinements are
 * derived from port ids on the edge.
 */

export type SemanticPortKey =
  | "point_set"
  | "interval_set"
  | "trajectory_set"
  | "surface"
  | "raster"
  | "mesh"
  | "block_model"
  | "table"
  | string;

/** Stroke colour for edges (and handle accents) by semantic type. */
export const SEMANTIC_EDGE_COLORS: Record<string, string> = {
  any: "#e6edf3",
  point_set: "#38bdf8",
  interval_set: "#f97316",
  trajectory_set: "#a78bfa",
  surface: "#4ade80",
  raster: "#facc15",
  mesh: "#fb7185",
  block_model: "#c084fc",
  table: "#94a3b8",
};

/** When the graph marks a collar-bearing output (port id), tint the link. */
export const COLLAR_LINK_COLOR = "#34d399";

export function normalizeSemantic(raw: string): SemanticPortKey {
  const s = raw.trim().toLowerCase().replace(/([A-Z])/g, "_$1").replace(/^_/, "");
  const snake = s.replace(/([a-z])([A-Z])/g, "$1_$2").toLowerCase();
  return snake;
}

export function baseColorForSemantic(sem: string): string {
  const k = normalizeSemantic(sem);
  return SEMANTIC_EDGE_COLORS[k] ?? "#64748b";
}

/**
 * Edge colour: semantic base, with optional logical refinement (collars / surveys / assays).
 */
export function edgeColorForApiEdge(edge: {
  semantic_type: string;
  from_port: string;
  to_port: string;
}): string {
  const fp = edge.from_port.toLowerCase();
  if (fp === "collars" || fp.includes("collar")) {
    return COLLAR_LINK_COLOR;
  }
  if (fp === "surveys" || fp.includes("survey")) {
    return "#818cf8";
  }
  if (fp === "assays" || fp.includes("assay")) {
    return "#fb923c";
  }
  return baseColorForSemantic(edge.semantic_type);
}

export function pickHandleColor<
  E extends {
    from_node: string;
    to_node: string;
    semantic_type: string;
    from_port: string;
    to_port: string;
  },
>(edges: E[], nodeId: string, direction: "in" | "out"): string {
  if (direction === "in") {
    const e = edges.find((x) => x.to_node === nodeId);
    return e ? edgeColorForApiEdge(e) : "#484f58";
  }
  const e = edges.find((x) => x.from_node === nodeId);
  return e ? edgeColorForApiEdge(e) : "#484f58";
}

export function pickHandleColorForPort<
  E extends {
    from_node: string;
    to_node: string;
    semantic_type: string;
    from_port: string;
    to_port: string;
  },
>(edges: E[], nodeId: string, direction: "in" | "out", portId: string): string {
  if (direction === "in") {
    const e = edges.find((x) => x.to_node === nodeId && x.to_port === portId);
    return e ? edgeColorForApiEdge(e) : "#484f58";
  }
  const e = edges.find((x) => x.from_node === nodeId && x.from_port === portId);
  return e ? edgeColorForApiEdge(e) : "#484f58";
}

export function pickHandleColorForPortWithSemantic<
  E extends {
    from_node: string;
    to_node: string;
    semantic_type: string;
    from_port: string;
    to_port: string;
  },
>(
  edges: E[],
  nodeId: string,
  direction: "in" | "out",
  portId: string,
  fallbackSemantic?: string | null
): string {
  const linked = pickHandleColorForPort(edges, nodeId, direction, portId);
  if (linked !== "#484f58") return linked;
  if (fallbackSemantic) return baseColorForSemantic(fallbackSemantic);
  return linked;
}

/** @deprecated use PORT_TAXONOMY_SUMMARY in portTaxonomy.ts */
export const PORT_COMPATIBILITY_NOTES = `
See port taxonomy (V1SPEC §16): base dataframes vs 2d/3d frames, artifacts, and domain refinements
(collar, assay, survey). Plan map viewer uses only edges wired into plan_view_2d.
`.trim();
