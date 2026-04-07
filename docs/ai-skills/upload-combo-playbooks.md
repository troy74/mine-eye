# Upload Combo Playbooks

## Purpose
Short operational nudges for common uploaded file bundles so chat can move from files to runnable graph state with minimal back-and-forth.

## Collar + Survey + Assay (Typical Drillhole)
- Objective: Produce desurveyed trajectories and drillhole assay geometry in Three.js.
- Sequence:
1. `list_uploaded_files`
2. `graph_audit_bundle`
3. Ensure/locate: `collar_ingest`, `survey_ingest`, `assay_ingest`, `desurvey_trajectory`, `drillhole_model`, `threejs_display_node`.
4. For each file: `uploaded_csv_profile` -> `suggest_ingest_mapping_from_upload` -> `apply_upload_to_ingest_node`.
5. Validate wiring:
`collars -> desurvey.collars_in`
`surveys -> desurvey.surveys_in`
`desurvey.trajectory -> drillhole_model.trajectory_in`
`assays -> drillhole_model.assays_in`
`drillhole_meshes -> threejs_display_node`
6. `run_node` on each ingest if needed, then `run_node` desurvey, then `run_node` drillhole model.
- Guardrails:
- Required mappings:
`collar_ingest`: `hole_id`, `x`, `y`
`survey_ingest`: `hole_id`, `depth_or_length_m`, `azimuth_deg`, `dip_deg`
`assay_ingest`: `hole_id`, `from_m`, `to_m`
- If uncertain mapping, present top 2 likely mappings with confidence and proceed with best unless user vetoes.

## Collar + Survey (No Assay Yet)
- Objective: Build trajectory-only 3D context and verify directional control.
- Sequence: map collar + survey, wire to desurvey, run desurvey, wire trajectory-capable viewer inputs where available.
- Explain that drillhole assay meshes will remain absent until assay intervals are mapped.

## Surface Samples (+ Optional AOI)
- Objective: Spatial point QA and 2D/3D visual context.
- Sequence:
1. Map `surface_sample_ingest` (`x`,`y`, optional `z`,`sample_id`).
2. If AOI requested, seed/patch AOI from points.
3. Add terrain chain (`aoi -> tilebroker -> dem_fetch`) when 3D terrain requested.
4. Wire output to viewer compatible ports and run.

## Unknown Tabular Upload
- Objective: classify file role quickly.
- Sequence:
1. `uploaded_csv_profile`.
2. Identify likely role by headers:
   - Collar-like: easting/northing/rl/hole_id.
   - Survey-like: depth/azimuth/dip.
   - Assay-like: from/to + analyte columns.
3. Propose best-fit ingest node and mapping.

## Response Style for Playbooks
- First: concise operator summary (what will happen now).
- Second: compact system detail with tool actions and confidence.
