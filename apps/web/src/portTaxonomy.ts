/**
 * Extensible port taxonomy (product model). Rust `SemanticPortType` remains the wire enum
 * until we migrate; this module maps wire → logical types and documents inheritance.
 */

import { normalizeSemantic } from "./portTypes";

/** Logical base / constrained port type ids (stable strings). */
export const PortTaxon = {
  // Scalars
  text: "scalar.text",
  number: "scalar.number",
  boolean: "scalar.boolean",
  datetime: "scalar.datetime",
  // Tabular
  dataframe: "dataframe",
  dataframe2d: "dataframe.2d",
  dataframe3d: "dataframe.3d",
  // Artifacts
  artifactFile: "artifact.file",
  artifactImage: "artifact.image",
  artifactShapefile: "artifact.shapefile",
  // Constrained (inherit tabular + geometry hints)
  collarTable: "domain.collar",
  assayTable: "domain.assay",
  surveyTable: "domain.survey",
  // Wire aliases (legacy / transport)
  wirePointSet: "wire.point_set",
  wireTable: "wire.table",
  wireSemanticJson: "wire.semantic_json",
  wireIntervalSet: "wire.interval_set",
  wireTrajectorySet: "wire.trajectory_set",
} as const;

/** Parent link: subtype → supertype (single inheritance for assignability lattice). */
export const INHERITS: Record<string, string | null> = {
  [PortTaxon.text]: null,
  [PortTaxon.number]: null,
  [PortTaxon.boolean]: null,
  [PortTaxon.datetime]: null,
  [PortTaxon.dataframe]: null,
  [PortTaxon.dataframe2d]: PortTaxon.dataframe,
  [PortTaxon.dataframe3d]: PortTaxon.dataframe,
  [PortTaxon.artifactFile]: null,
  [PortTaxon.artifactImage]: PortTaxon.artifactFile,
  [PortTaxon.artifactShapefile]: PortTaxon.artifactFile,
  [PortTaxon.collarTable]: PortTaxon.dataframe3d,
  [PortTaxon.assayTable]: PortTaxon.dataframe,
  [PortTaxon.surveyTable]: PortTaxon.dataframe,
  [PortTaxon.wirePointSet]: PortTaxon.dataframe2d,
  [PortTaxon.wireTable]: PortTaxon.dataframe,
  [PortTaxon.wireSemanticJson]: PortTaxon.dataframe,
  [PortTaxon.wireIntervalSet]: PortTaxon.dataframe,
  [PortTaxon.wireTrajectorySet]: PortTaxon.dataframe3d,
};

export function walkSupertypes(id: string): Set<string> {
  const out = new Set<string>();
  let cur: string | null | undefined = id;
  while (cur) {
    out.add(cur);
    cur = INHERITS[cur] ?? null;
  }
  return out;
}

/** True if producer type `from` can satisfy consumer port typed `to` (by id). */
export function canAssignPortType(from: string, to: string): boolean {
  if (from === to) return true;
  const supers = walkSupertypes(from);
  return supers.has(to);
}

/**
 * Map API `semantic_type` (snake_case) to a taxonomy id for docs / future validation.
 */
export function wireSemanticToTaxonomy(semantic: string): string {
  const k = normalizeSemantic(semantic);
  switch (k) {
    case "point_set":
      return PortTaxon.wirePointSet;
    case "table":
      return PortTaxon.wireTable;
    case "semantic_json":
      return PortTaxon.wireSemanticJson;
    case "interval_set":
      return PortTaxon.wireIntervalSet;
    case "trajectory_set":
      return PortTaxon.wireTrajectorySet;
    default:
      return `wire.${k}`;
  }
}

/**
 * Semantics allowed into `plan_view_2d` inputs (v1): anything we can try to read x,y from JSON for.
 * Raster/mesh/block connect elsewhere until we add layer compositing.
 */
export function isPlanViewInputSemantic(semantic: string): boolean {
  const k = normalizeSemantic(semantic);
  return (
    k === "point_set" ||
    k === "table" ||
    k === "semantic_json" ||
    k === "interval_set" ||
    k === "trajectory_set" ||
    k === "surface" ||
    k === "raster"
  );
}

/** Semantics allowed into 3D scene viewer inputs in v1. */
export function isSceneViewInputSemantic(semantic: string): boolean {
  const k = normalizeSemantic(semantic);
  return (
    k === "point_set" ||
    k === "table" ||
    k === "interval_set" ||
    k === "trajectory_set" ||
    k === "surface" ||
    k === "mesh" ||
    k === "block_model"
  );
}

/** Short doc block for inspector / V1SPEC alignment. */
export const PORT_TAXONOMY_SUMMARY = `
Base types: text, number, boolean, datetime; unconstrained dataframe; dataframe.2d (x,y + attrs);
dataframe.3d (x,y,z + attrs); artifacts (file, image, shapefile, …).

Constrained types inherit from these (e.g. collar ⇒ 3d dataframe with required x,y and optional
id/az/dip; assays ⇒ tabular with hole/interval codes and grade columns). A collar stream can feed a
2d consumer (plan view uses x,y only) or 3d; the reverse needs the consumer’s minimum columns.

Wire enum (Rust today): point_set, table, interval_set, trajectory_set, … maps to the taxonomy for UI
and future strict validation.
`.trim();
