import type { ArtifactEntry } from "./graphApi";

/** Filename suffix (last path segment) we expect per node kind for V1 workers. */
export function preferredArtifactSuffix(kind: string): string | null {
  switch (kind) {
    case "collar_ingest":
      return "collars.json";
    case "survey_ingest":
      return "surveys.json";
    case "surface_sample_ingest":
      return "surface_samples.json";
    case "assay_ingest":
      return "assays.json";
    case "artifact_ingest":
      return "artifact_ingest_table_pointer.json";
    case "desurvey_trajectory":
      return "trajectory.json";
    case "drillhole_model":
      return "drillhole_meshes.json";
    case "assay_heatmap":
      return "heatmap.json";
    case "plan_view_2d":
      return "plan_view.json";
    case "dem_integrate":
      return "dem_stub.json";
    case "block_model_basic":
      return "block_model_meta.json";
    case "md_viewer":
      return "md_view_doc.json";
    case "plot_chart":
      return "plot_chart_view.json";
    default:
      return null;
  }
}

export function artifactEndsWithSuffix(key: string, suffix: string): boolean {
  return key === suffix || key.endsWith(`/${suffix}`);
}

/** Pick a default artifact to preview for this node kind. */
export function defaultArtifactForKind(
  artifacts: ArtifactEntry[],
  kind: string
): ArtifactEntry | null {
  if (artifacts.length === 0) return null;
  const suffix = preferredArtifactSuffix(kind);
  if (suffix) {
    const hit = artifacts.find((a) => artifactEndsWithSuffix(a.key, suffix));
    if (hit) return hit;
  }
  const jsonFirst = artifacts
    .filter((a) => a.key.endsWith(".json"))
    .sort((a, b) => a.key.localeCompare(b.key));
  return jsonFirst[0] ?? artifacts[0] ?? null;
}

export function isLikelyJsonKey(key: string): boolean {
  return key.endsWith(".json");
}

export function isLikelyBinaryKey(key: string): boolean {
  return /\.(bin|raw|dat)$/i.test(key);
}
