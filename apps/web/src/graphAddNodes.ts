export type AddNodePreset = {
  category: string;
  kind: string;
  label: string;
  policy?: Record<string, string>;
};

/** Right-click “Add node” presets (matches orchestrator `add_node` categories). */
export const ADD_NODE_PRESETS: AddNodePreset[] = [
  { category: "input", kind: "collar_ingest", label: "Collar ingest" },
  { category: "input", kind: "survey_ingest", label: "Survey ingest" },
  { category: "input", kind: "assay_ingest", label: "Assay ingest" },
  { category: "input", kind: "drillhole_ingest", label: "Drillhole ingest" },
  { category: "transform", kind: "drillhole_merge", label: "Drillhole merge" },
  { category: "transform", kind: "desurvey_trajectory", label: "Desurvey trajectory" },
  { category: "transform", kind: "dem_integrate", label: "DEM integrate" },
  { category: "model", kind: "block_model_basic", label: "Block model (stub)" },
  {
    category: "visualisation",
    kind: "plan_view_2d",
    label: "2D plan map",
    policy: { recompute: "manual", propagation: "hold", quality: "preview" },
  },
];
