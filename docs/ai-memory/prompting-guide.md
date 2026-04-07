# Prompting Guide: Getting Strong Results from Mine-Eye AI

Use this format for best outcomes:

1. Objective
- "I want to build a [workflow outcome] from [data sources]."

2. Data + CRS
- "Inputs available: [node ids or data types]."
- "CRS is EPSG:[code] (or unknown)."

3. Constraints
- "Must keep: [existing nodes/edges], avoid: [expensive step/manual lock/etc]."

4. Action Preference
- "Propose only" or "Apply changes directly."

5. Validation Expectation
- "After changes, tell me what to run and what success/failure should look like."

## High-Value Prompts
- "Inspect this graph and suggest a robust AOI -> DEM -> imagery -> 3D pipeline. Apply wiring only after showing the plan."
- "Read top/tail of assay-related artifacts and propose the exact transform config patch for value cleaning and unit normalization."
- "Patch `data_model_transform` to keep only fields needed for heatmap, then wire output into `assay_heatmap` with correct semantic port."
- "I uploaded collar/survey/assay CSVs. Build the standard ingest -> desurvey -> drillhole -> threejs 3D pipeline in plan-only mode, then ask me what to apply."

## Recommended Configurable Transforms (Near-Term)
- Data normalization transform:
  - column mapping, type coercion, null policy, unit conversion
- Spatial conditioning transform:
  - CRS override/validation, extent clipping, AOI mask
- Geochem conditioning transform:
  - thresholding, log scaling, outlier handling, compositing mode
- Surface interpolation transform:
  - method selection (IDW/kriging/TIN), search radius, anisotropy hints

## Obvious Candidate Files/Fields Strategy
- Ask AI to inspect likely artifacts first:
  - `list_node_artifacts` on likely source nodes (surface samples, assays, transformed tables)
  - then `csv_profile` + `suggest_measure_fields`
- Preferred field-candidate ranking:
  - numeric density first
  - assay/element naming hints (`au`, `cu`, `zn`, `grade`, `ppm`, `%`)
  - non-empty row coverage
- Example prompt:
  - "Inspect surface sample artifacts, rank likely heatmap value fields, and configure assay_heatmap with your top candidate in plan-only mode."

## Default Workflow Heuristic
- If attached file names indicate `collar`, `survey`, `assay`, assume baseline drillhole pipeline:
  - `collar_ingest`, `survey_ingest`, `assay_ingest`
  - `desurvey_trajectory`
  - `drillhole_model`
  - `threejs_display_node`
- Ask only the minimum needed clarifications:
  - preferred 3D output (drill traces only vs full terrain + imagery context)
  - whether to apply immediately or return plan first.
