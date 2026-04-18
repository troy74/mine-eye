import type { ArtifactEntry } from "./graphApi";

/** Preferred filename suffixes (in order) per node kind for V1 workers. */
export function preferredArtifactSuffixes(kind: string): string[] {
  switch (kind) {
    case "collar_ingest":
      return ["collars.json"];
    case "survey_ingest":
      return ["surveys.json"];
    case "surface_sample_ingest":
      return ["surface_samples.json"];
    case "assay_ingest":
      return ["assays.json"];
    case "lithology_ingest":
      return ["lithology_intervals.json"];
    case "orientation_ingest":
      return ["formation_orientations.json"];
    case "observation_ingest":
      return ["observation_table_pointer.json"];
    case "magnetic_model":
      return ["magnetic_points.preview.json", "magnetic_grid.preview.json", "magnetic_points.json"];
    case "desurvey_trajectory":
    case "vertical_trajectory":
      return ["trajectory.json"];
    case "formation_interface_extract":
      return ["interface_points.json"];
    case "formation_catalog_build":
      return ["formation_catalog.json"];
    case "stratigraphic_order_define":
      return ["stratigraphic_order.json"];
    case "model_domain_define":
      return ["model_domain.json"];
    case "constraint_merge":
      return ["interpolation_constraints.json"];
    case "structural_frame_builder":
      return ["structural_frame.json"];
    case "stratigraphic_interpolator":
      return ["scalar_field.json"];
    case "lith_block_model_build":
      return ["lith_block_model_report.json", "lith_block_model_voxels.json"];
    case "stratigraphic_surface_model":
      return ["surface_report.json"];
    case "drillhole_model":
      return ["drillhole_meshes.json"];
    case "assay_heatmap":
      return ["heatmap.json"];
    case "heatmap_raster_tile_cache":
      return ["raster_tile_manifest.json", "heatmap_imagery_drape.json"];
    case "plan_view_2d":
      return ["plan_view.json"];
    case "dem_integrate":
      return ["dem_stub.json"];
    case "block_model_basic":
      return ["block_model_meta.json"];
    case "md_viewer":
      return ["md_view_doc.json"];
    case "plot_chart":
      return ["plot_chart_view.json"];
    default:
      return [];
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
  for (const suffix of preferredArtifactSuffixes(kind)) {
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
