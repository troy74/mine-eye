/**
 * Scene3DTypes.ts
 * ────────────────────────────────────────────────────────────────────────────
 * Domain-aware 3D scene type system for mine-eye.
 *
 * Philosophy
 * ──────────
 * Every input arriving at a viewer node carries a *semantic type* (point_set,
 * trajectory_set, interval_set, surface, raster, mesh …) plus optional port
 * names and schema identifiers.  Together these tell us exactly what kind of
 * mining data we're looking at and therefore how to render it.
 *
 * This file encodes that domain knowledge in one place so that:
 *  • The panel can show the right controls for each layer
 *  • The Canvas can pick the right renderer / geometry
 *  • Defaults are sensible for each data class
 *  • The system is extensible: add a new DisplayKind → one place to update
 *
 * Planned usage path
 * ──────────────────
 * SceneUiState (current flat blob) → SceneUiStateV2 (layers: Record<id, LayerState>)
 * Map3DThreePanel will drive layer rendering from LayerState rather than the
 * current hardcoded LAYER_KEYS enum.
 */

// ─── Port semantic types (mirrors portTaxonomy.ts) ───────────────────────────

export type SemanticType =
  | "point_set"
  | "trajectory_set"
  | "interval_set"
  | "surface"
  | "raster"
  | "mesh"
  | "block_model"
  | "table"
  | "semantic_json"
  | "any";

// ─── Display kinds ────────────────────────────────────────────────────────────
//
// A DisplayKind is the *rendering archetype* for a layer.  It is derived from
// the combination of semantic type + port name + schema_id.  Multiple semantic
// types can share a display kind (e.g. "collar_points" and "generic_points"
// both render as point clouds, but with different default colours and controls).

export type DisplayKind =
  // ── Drillhole domain ─────────────────────────────────────────────────────
  | "collar_points"       // point_set with hole_id + xy + optional z + optional azimuth/dip
  | "drillhole_traces"    // trajectory_set: desurveyed 3D paths (tubes)
  | "grade_intervals"     // interval_set: grade/lithology along depth (coloured tubes)
  | "assay_points"        // point_set with measure attributes (scatter / bubble)

  // ── Topographic domain ───────────────────────────────────────────────────
  | "terrain_surface"     // surface: DEM grid → mesh, also Z-snap source
  | "contour_lines"       // interval_set or point_set coming from a contour port
  | "imagery_drape"       // raster: satellite / topo image draped on terrain

  // ── Geological domain ────────────────────────────────────────────────────
  | "block_model_voxels"  // block_model: 3D voxel cloud
  | "solid_mesh"          // mesh: wireframe / solid geological surface

  // ── Generic fallback ─────────────────────────────────────────────────────
  | "point_cloud"         // point_set without domain-specific context
  | "line_set"            // generic line segments
  | "generic_table";      // table: no geometry, shown as attribute label bubbles

// ─── Domain colour palette ────────────────────────────────────────────────────
//
// Each DisplayKind has a canonical colour used for:
//   • Layer dot in the panel
//   • Default geometry colour when no attribute is mapped
//   • Edge colour inheritance hint

export const DISPLAY_KIND_COLOR: Record<DisplayKind, string> = {
  collar_points:      "#34d399",  // emerald — the "start" of a drillhole
  drillhole_traces:   "#a78bfa",  // purple  — trajectory / path
  grade_intervals:    "#f97316",  // orange  — grade / interval data
  assay_points:       "#60a5fa",  // blue    — sample / assay scatter
  terrain_surface:    "#4ade80",  // green   — terrain
  contour_lines:      "#38bdf8",  // sky     — contour elevation lines
  imagery_drape:      "#facc15",  // yellow  — satellite imagery
  block_model_voxels: "#c084fc",  // violet  — block model
  solid_mesh:         "#fb7185",  // rose    — geological surface mesh
  point_cloud:        "#38bdf8",  // sky     — generic points
  line_set:           "#64748b",  // slate   — generic lines
  generic_table:      "#94a3b8",  // grey    — tabular / no-geometry
};

export const DISPLAY_KIND_LABEL: Record<DisplayKind, string> = {
  collar_points:      "Collar points",
  drillhole_traces:   "Drillhole traces",
  grade_intervals:    "Grade intervals",
  assay_points:       "Assay points",
  terrain_surface:    "Terrain surface",
  contour_lines:      "Contour lines",
  imagery_drape:      "Imagery drape",
  block_model_voxels: "Block model",
  solid_mesh:         "Solid mesh",
  point_cloud:        "Point cloud",
  line_set:           "Line set",
  generic_table:      "Data table",
};

// ─── Domain knowledge: semantic → display kind mapping ───────────────────────
//
// Mining domain semantics:
//
//  "collars" port    → collar_points
//    Raw drillhole collar data: xy + optional z (elevation), hole_id,
//    optional azimuth, dip, depth.  The XY coordinates are the surface
//    piercing point; Z may be absent if collars come from a 2D survey.
//    Z-snap to terrain surface if Z is missing.
//
//  "trajectories" port → drillhole_traces
//    Desurveyed 3D paths produced by a survey desurvey node.  Each path
//    is a polyline in 3D space from collar to EOH.  Rendered as tubes.
//
//  "assays" / "intervals" port → grade_intervals
//    Grade / lithology data sampled along a drillhole at from-to depth
//    intervals.  Rendered as coloured tube segments along the trajectory.
//    If the port name matches "contour" it maps to contour_lines instead.
//
//  "surface" semantic → terrain_surface
//    DEM grid data.  Renders as a mesh and serves as Z-snap source for
//    points that lack an explicit Z.
//
//  "raster" semantic → imagery_drape
//    Satellite / topo imagery for drapin on terrain.
//
//  Default point_set with no port context → point_cloud

export function deriveDisplayKind(opts: {
  semanticType: SemanticType | string;
  fromPort?: string;
  toPort?: string;
  schemaId?: string;
  sourceKind?: string;     // upstream node kind (e.g. "collar_ingest")
}): DisplayKind {
  const { semanticType, fromPort = "", toPort = "", schemaId = "", sourceKind = "" } = opts;
  const sem = semanticType.toLowerCase().trim();
  const port = (fromPort || toPort).toLowerCase().trim();
  const schema = schemaId.toLowerCase().trim();
  const kind = sourceKind.toLowerCase().trim();

  // ── Imagery ──────────────────────────────────────────────────────────────
  if (sem === "raster") return "imagery_drape";
  if (schema.includes("imagery_drape") || schema.includes("tilebroker")) return "imagery_drape";

  // ── Terrain surface ──────────────────────────────────────────────────────
  if (sem === "surface") return "terrain_surface";
  if (schema.includes("surface") || schema.includes("dem") || schema.includes("terrain")) {
    return "terrain_surface";
  }

  // ── Mesh ─────────────────────────────────────────────────────────────────
  if (sem === "mesh") return "solid_mesh";

  // ── Block model ──────────────────────────────────────────────────────────
  if (sem === "block_model") return "block_model_voxels";

  // ── Table ────────────────────────────────────────────────────────────────
  if (sem === "table") return "generic_table";

  // ── Trajectory set ───────────────────────────────────────────────────────
  if (sem === "trajectory_set") return "drillhole_traces";

  // ── Interval set ─────────────────────────────────────────────────────────
  if (sem === "interval_set") {
    // Port name hint: contours come in as interval_set on a "contour" port
    if (port.includes("contour") || port.includes("iso")) return "contour_lines";
    return "grade_intervals";
  }

  // ── Point set ────────────────────────────────────────────────────────────
  if (sem === "point_set" || sem === "wire.point_set") {
    // Port name hints
    if (port.includes("collar") || kind.includes("collar")) return "collar_points";
    if (port.includes("assay") || port.includes("sample") || kind.includes("assay")) return "assay_points";
    if (port.includes("contour") || port.includes("iso")) return "contour_lines";
    if (port.includes("trajectory") || port.includes("survey")) return "drillhole_traces";

    // Source node kind hints
    if (kind.includes("collar")) return "collar_points";
    if (kind.includes("assay") || kind.includes("sample_ingest")) return "assay_points";
    if (kind.includes("contour") || kind.includes("isoline")) return "contour_lines";

    // Schema hints
    if (schema.includes("collar")) return "collar_points";
    if (schema.includes("assay")) return "assay_points";
    if (schema.includes("contour")) return "contour_lines";

    return "point_cloud";
  }

  // ── Fallback ──────────────────────────────────────────────────────────────
  return "point_cloud";
}

// ─── Per-layer state ──────────────────────────────────────────────────────────
//
// This is the target shape for SceneUiState v2.  Each input to the viewer
// gets its own LayerState keyed by a stable id (e.g. "edge:<edgeId>").
//
// Currently unused by the rendering code — the transition from the flat blob
// happens once we migrate Map3DThreePanel to use this structure.

export type LayerVizPreset = {
  // Attribute to map to colour; "" = constant colour
  attributeKey: string;
  palette: "inferno" | "viridis" | "turbo" | "red_blue";
  colorMode: "continuous" | "categorical";
  transform: "linear" | "log10" | "ln";
  clampLowPct: number;
  clampHighPct: number;
  categoricalColorMap: string;       // JSON string: { "ore": "#f97316" }
};

export type LayerState = {
  id: string;                        // stable key: "edge:<edgeId>" or "src:<nodeId>"
  displayKind: DisplayKind;
  label: string;                     // human label (derived from upstream node label)
  visible: boolean;
  opacity: number;                   // 0 – 1
  expanded: boolean;                 // panel card expanded?
  viz: LayerVizPreset;

  // Kind-specific overrides (null = use scene default)
  pointSize?: number | null;         // for point_cloud / collar_points / assay_points
  tubeRadius?: number | null;        // for drillhole_traces / grade_intervals
  lineWidth?: number | null;         // for contour_lines / line_set
  constantColor?: string | null;     // override colour when attributeKey is empty
};

// ─── Scene globals ────────────────────────────────────────────────────────────

export type BgPreset = "night" | "dusk" | "dawn" | "overcast";

export type SceneGlobals = {
  ambientIntensity: number;   // 0 – 2.0
  fogEnabled: boolean;
  gridEnabled: boolean;
  bgPreset: BgPreset;
  invertDepth: boolean;       // depth-down view (Y flipped)
  radiusScale: number;        // global radius multiplier
  groundLayerId: string;      // layer id of the terrain_surface used for Z-snap
};

// ─── SceneUiState v2 (target) ─────────────────────────────────────────────────
//
// Not yet used — kept here as the design target.
// Migration: flatten SceneUiState (current) → SceneUiStateV2 via adapter
// function in Map3DThreePanel.  Alpha: no migration needed, wipe state.

export type SceneUiStateV2 = {
  layers: Record<string, LayerState>;
  layerOrder: string[];
  layerOrderMode: "contract" | "override";
  globals: SceneGlobals;
  panelCollapsed: boolean;
};

// ─── Default constructors ─────────────────────────────────────────────────────

export function defaultViz(displayKind: DisplayKind): LayerVizPreset {
  // Grade data benefits from inferno; terrain/imagery are constant
  const pal: LayerVizPreset["palette"] =
    displayKind === "grade_intervals" || displayKind === "assay_points"
      ? "inferno"
      : "turbo";
  return {
    attributeKey: "",
    palette: pal,
    colorMode: "continuous",
    transform: "linear",
    clampLowPct: 2,
    clampHighPct: 98,
    categoricalColorMap: "{}",
  };
}

export function defaultLayerState(
  id: string,
  displayKind: DisplayKind,
  label?: string
): LayerState {
  const color = DISPLAY_KIND_COLOR[displayKind];
  const kindLabel = DISPLAY_KIND_LABEL[displayKind];
  return {
    id,
    displayKind,
    label: label ?? kindLabel,
    visible: true,
    opacity: 1,
    expanded: false,
    viz: defaultViz(displayKind),
    constantColor: color,
    pointSize: null,
    tubeRadius: null,
    lineWidth: null,
  };
}

export const DEFAULT_GLOBALS: SceneGlobals = {
  ambientIntensity: 0.92,
  fogEnabled: true,
  gridEnabled: true,
  bgPreset: "night",
  invertDepth: false,
  radiusScale: 1.35,
  groundLayerId: "",
};

// ─── Mining domain port name helpers ─────────────────────────────────────────
//
// Canonical port name fragments for common mining data flows.
// Used by deriveDisplayKind() and future UI labelling.

export const MINING_PORT_HINTS = {
  collars:      ["collar", "collars", "drillhole_collar", "dh_collar"],
  trajectories: ["trajectory", "trajectories", "survey", "desurvey", "dh_trace"],
  assays:       ["assay", "assays", "sample", "samples", "grade", "interval", "intervals"],
  contours:     ["contour", "contours", "iso", "isoline", "isolines", "level"],
  dem:          ["dem", "terrain", "surface", "dtm", "topo", "elevation"],
  imagery:      ["imagery", "image", "drape", "tile", "satellite", "ortho"],
} as const;

/** Returns a human-readable description of what this layer represents. */
export function layerTooltip(layer: LayerState): string {
  switch (layer.displayKind) {
    case "collar_points":
      return "Drillhole collar locations — the surface piercing points of each drillhole.";
    case "drillhole_traces":
      return "Desurveyed drillhole traces — 3D paths from collar to end of hole.";
    case "grade_intervals":
      return "Grade / lithology intervals — colour-coded segments along each drillhole.";
    case "assay_points":
      return "Assay sample points — individual measurements at specific depths.";
    case "terrain_surface":
      return "Terrain surface (DEM) — also used as Z-snap source for 2D data.";
    case "contour_lines":
      return "Contour lines — topographic elevation levels.";
    case "imagery_drape":
      return "Satellite / topo imagery draped on terrain.";
    case "block_model_voxels":
      return "Block model — geological grade or property voxels.";
    case "solid_mesh":
      return "Solid mesh — geological surface or wireframe.";
    case "point_cloud":
      return "Generic point cloud.";
    case "line_set":
      return "Generic line set.";
    case "generic_table":
      return "Tabular data — no geometry.";
  }
}
