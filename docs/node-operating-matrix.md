# Node Operating Matrix (Exploration Workflow)

This matrix is the practical operating guide for what we currently do with node kinds in `mine-eye`.

## Status Legend
- `Primary`: current recommended path
- `Secondary`: valid but situational
- `Historic`: archive/legacy path, avoid unless explicitly required

## Core Exploration Pipelines

| Workflow Goal | Recommended Node Chain | Status | Notes |
|---|---|---|---|
| Drillhole 3D interpretation | `collar_ingest` + `survey_ingest` -> `desurvey_trajectory` -> `drillhole_model` -> `threejs_display_node` | Primary | Baseline for collars/surveys/assays into 3D scene context. |
| Terrain + imagery context in 3D | `aoi` -> `dem_fetch` -> `tilebroker` -> `threejs_display_node` | Primary | Use with drillhole outputs for draped exploration context. |
| Block resource estimation (voxel) | `drillhole_model`/grade points -> `block_grade_model` -> `threejs_display_node` | Primary | Emits block voxels + center points + resource summary report; clips to topography when terrain is wired. |
| Surface geochem heatmap | `surface_sample_ingest` -> (`data_model_transform`) -> `assay_heatmap` -> `plan_view_2d`/`threejs_display_node` | Primary | Keep measure-column config explicit and validated from source schema. |
| Contour export / topography QA | `dem_fetch`/`terrain_adjust` -> `surface_iso_extract` or `dem_contour_xyz` | Secondary | Use when iso/contour artifacts are required downstream. |
| Scene contract composition | `scene3d_layer_stack` -> `threejs_display_node` | Secondary | Useful for explicit multi-layer 3D composition and UI capabilities. |

## Node-Level Intent Matrix

| Kind | Category | Typical Use | Inputs Needed | Outputs Produced | Status |
|---|---|---|---|---|---|
| `collar_ingest` | input | Import collar table | file/config mapping | `collars` (`point_set`) | Primary |
| `survey_ingest` | input | Import survey stations | file/config mapping | `surveys` (`trajectory_set`) | Primary |
| `assay_ingest` | input | Import assays/geochem intervals | file/config mapping | `assays` (`interval_set`) | Primary |
| `surface_sample_ingest` | input | Import surface point samples | file/config mapping | `surface_samples` (`point_set`) | Primary |
| `desurvey_trajectory` | transform | Build downhole trajectories | collars + surveys | `trajectory` | Primary |
| `drillhole_model` | model | Build drill traces/meshes and assay points | trajectory + assays | meshes + points | Primary |
| `block_grade_model` | model | Build voxel block grades + resource summary | grade points (+ optional terrain) | block voxels + centers + report | Primary |
| `aoi` | transform | Stable AOI definition | seed geometry | AOI contract (`table`) | Primary |
| `dem_fetch` | transform | DEM fetch/fit from AOI | AOI geometry | terrain surface | Primary |
| `tilebroker` | transform | Imagery provider normalization | AOI + terrain | imagery contract | Primary |
| `assay_heatmap` | transform | Surface interpolation of measures | point/table input | heatmap surface | Primary |
| `data_model_transform` | transform | Schema/field cleanup and transform | tabular input | tabular output | Primary |
| `surface_iso_extract` | transform | DEM contour extraction | surface | contour + meta | Secondary |
| `dem_contour_xyz` | transform | DEM contour XYZ export | surface | points + lines | Secondary |
| `terrain_adjust` | transform | Terrain control-point adjustment | terrain + controls | adjusted terrain | Secondary |
| `xyz_to_surface` | transform | Generic XYZ -> surface interpolation | point geometry | surface | Secondary |
| `imagery_provider` | transform | Direct imagery contract generation | AOI + terrain | imagery contract | Secondary |
| `scene3d_layer_stack` | transform | Explicit scene3d layer stack build | mixed layer inputs | layer stack | Secondary |
| `plan_view_2d` | visualisation | 2D map viewer | wired inputs | none | Primary |
| `threejs_display_node` | visualisation | Active 3D viewer path | wired inputs/contracts | none | Primary |
| `plan_view_3d` | visualisation | Cesium legacy viewer | wired inputs | none | Historic |
| `cesium_display_node` | visualisation | Cesium legacy viewer | wired inputs | none | Historic |

## Operational Rules

1. Default 3D viewer for new work is `threejs_display_node`.
2. Treat Cesium nodes as historic/archived unless a user explicitly requests Cesium.
3. For uploaded collar/survey/assay-style files, default graph setup should be ingest -> desurvey -> drillhole model -> threejs viewer.
4. Keep CRS and semantic-port compatibility explicit before applying wires.
