# Node Workflow Fragments

## Ingest Nodes

### `collar_ingest`
- Required mapping: `hole_id`, `x`, `y`
- Optional: `z`, `azimuth_deg`, `dip_deg`
- Typical aliases: `hole`, `holeid`, `easting`, `northing`, `rl`, `elev`.

### `survey_ingest`
- Required mapping: `hole_id`, `depth_or_length_m`, `azimuth_deg`, `dip_deg`
- Optional: `segment_id`
- Typical aliases: `depth`, `md`, `azi`, `bearing`, `inclination`.

### `assay_ingest`
- Required mapping: `hole_id`, `from_m`, `to_m`
- Optional analyte columns retained under interval attributes.
- Typical analyte hints: `au`, `au_ppm`, `cu`, `zn`, `pb`, `ag`, `grade`, `value`.

### `surface_sample_ingest`
- Required mapping: `x`, `y`
- Optional: `z`, `sample_id`

## Transform/Model Chain
- `desurvey_trajectory` depends on `collars_in + surveys_in`.
- `drillhole_model` depends on `trajectory_in + assays_in`.
- Missing ingest payloads propagate as failures downstream.

## Viewer Routing
- Primary viewer is `threejs_display_node`.
- Cesium viewers are archival/historic unless user explicitly requests Cesium.
- Prefer semantic-safe wiring from registry and validate before mutate.

## Execution Nudge
- After mapping patches, run ingest nodes first, then transform/model nodes.
- Use targeted `run_node` for rapid feedback before full `run_graph`.
