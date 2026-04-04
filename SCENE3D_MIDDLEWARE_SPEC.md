# 3D Imagery + AOI Middleware Spec (Drift-Resistant)

Status: Draft for implementation planning
Owner: mine-eye core
Date: 2026-04-02

## 1) Goals

- Move imagery/provider logic out of frontend rendering and into graph middleware contracts.
- Make AOI a first-class, reusable project concept with deterministic inference when missing.
- Keep frontend flexible for UX overrides while preserving reproducible data semantics.
- Ensure 2D and 3D views consume equivalent geospatial extents/sources to reduce drift.

## 2) Non-goals (for this phase)

- Full provider-specific optimization for every imagery source.
- Block model generation (future phase).
- Time-series imagery UI workflows beyond basic contract fields.

## 3) Core principle: strict contract boundary

Frontend should consume only versioned scene contracts, not parse provider internals.

Contract families:
- `spatial.aoi.v1`
- `terrain.surface_grid.v1`
- `scene3d.imagery_drape.v1`
- `scene3d.layer_stack.v1`

All include:
- `schema_id`
- `schema_version`
- `crs` (`epsg`, optional `vertical_datum`)
- `provenance` (node id, provider id, timestamp)

## 4) New/updated nodes

### 4.1 `aoi` node (new)

Purpose:
- Emit canonical AOI artifact for downstream nodes.

Inputs:
- optional `xyz_points` / `xy_points` / other spatial artifacts

Params:
- `mode`: `project_default | inferred | manual`
- `margin_pct` (default 8)
- `max_expand_pct_per_run` (default 20)
- `outlier_sigma` (default 4.0)
- `hard_bounds` (optional)

Output:
- `spatial.aoi.v1`

### 4.2 `imagery_provider` node (new)

Purpose:
- Resolve imagery provider request into drape-ready, georeferenced contract.

Inputs:
- `area_of_interest` (`spatial.aoi.v1`, required)
- optional `terrain.surface_grid.v1` (for alignment metadata and QC)

Params:
- `provider_id` (`esri_world_imagery`, `esri_world_topo`, `esri_natgeo`, `usgs_imagery`, extensible)
- `style_id` (optional)
- `target_resolution_m` or `max_pixels`
- `acquisition_window` (optional)
- `fallback_provider_ids` (ordered)
- `license_mode` (`strict | permissive`)

Output:
- `scene3d.imagery_drape.v1`

Policy note:
- Default provider should be unpaid/public (`esri_world_imagery`).
- Paid provider requests should be debounced in UI-triggered fetch paths.

### 4.3 `scene3d_layer_stack` node (new)

Purpose:
- Compose an ordered, capability-annotated layer contract from upstream scene artifacts.

Inputs:
- imagery drape contract + terrain + traces + segments + assays + contours (optional mix)

Output:
- `scene3d.layer_stack.v1`

### 4.4 `threejs_display_node` (existing; behavior update)

Purpose:
- Compose scene only from middle-layer artifacts (`layer_stack`, `imagery_drape`, `surface_grid`, etc.).
- No provider endpoint logic in viewer once imagery node is enabled.

## 5) AOI behavior spec

### 5.1 Project-level AOI

Project metadata should store canonical AOI:
- geometry (polygon, allow quadrilateral convenience)
- CRS
- defaults (`margin_pct`, preferred resolution)
- lock flag (`locked: true/false`)

If project AOI exists and locked, downstream nodes use it unless explicit override.

### 5.2 AOI inference when missing

When AOI not set:
1. Collect first available spatial points from wired artifacts.
2. Remove obvious outliers using robust z-score/IQR guard.
3. Build bbox of inliers.
4. Inflate by `margin_pct`.
5. Cap growth with `max_expand_pct_per_run` if previous AOI exists.
6. If incoming points exceed AOI moderately, auto-expand.
7. If points are "crazy" (far beyond guard), do not auto-expand; emit warning.

### 5.3 Error/outlier policy

- `warn` event if dropped outlier count > threshold.
- `error` only if no valid inliers remain.

## 6) `scene3d.imagery_drape.v1` contract

Required fields:
- `schema_id`: `scene3d.imagery_drape.v1`
- `provider_id`
- `provider_label`
- `attribution`
- `license_terms_url` (if available)
- `coverage_polygon`
- `source_crs` and `target_crs`
- `texture_mode`: `single_image | tile_template`
- `image_url` or `tile_url_template`
- `bounds`
- `pixel_size` / `resolution_m_est`
- `z_mode`: `drape_on_surface | flat`
- `quality_flags` (e.g. reprojection fallback used)
- `fingerprint` (content hash for cache + reproducibility)

Optional fields:
- `acquired_at`
- `cloud_cover_est`
- `nodata_mask`
- `fallback_chain_used`

## 7) `scene3d.layer_stack.v1` contract

Purpose:
- One stable scene composition payload.

Includes ordered `layers[]` with:
- `layer_id`
- `kind`: `imagery_drape | contours | drill_segments | assay_points | mesh | volume`
- `source_artifact_ref`
- `style_defaults` (middle-layer defaults)
- `ui_capabilities` (which properties UI may override)
- `priority`
- `visibility_default`

## 8) UI override boundary (important)

Three-tier rule:

1. Semantic data (middle, immutable from UI):
- geometry, measure values, CRS, AOI, provider provenance, scale units.

2. Style defaults (middle, persisted/reproducible):
- default palette, default opacity, default radius scale, contour interval default.

3. UI overrides (viewer, persisted per viewer node):
- opacity sliders
- color/palette selection
- attribute chosen for coloring/sizing
- symbol scale multipliers
- layer visibility/order

Rule: UI can override only properties listed in `ui_capabilities` for each layer.

## 9) 2D/3D parity requirements

- Both viewers use same AOI artifact id by default.
- Both viewers resolve basemap/imagery through same `imagery_provider` output (or equivalent 2D adapter from same contract).
- Status line should display provider + fingerprint short hash for parity checks.

## 10) Edit-window UX requirements

AOI editor in node panel should include:
- place search input (geocoder)
- draw/edit quadrilateral over base Esri layer
- numeric bounds editor (min/max x/y) with CRS display
- "Use current viewport" action
- "Lock project AOI" toggle

## 11) Toast messaging (future design note)

Add a project-wide toast/event channel for:
- AOI auto-expand occurred
- outliers ignored
- imagery fallback provider used
- reprojection fallback used
- contract validation warnings

(Implement later; include as dependency in UX roadmap.)

## 12) Drift-protection enforcement

- Validate artifacts against JSON schema at node output boundary.
- Reject/flag unknown schema versions unless compatibility adapter exists.
- Keep backward adapters in middleware, not frontend.
- Add golden fixtures for contract snapshots in CI.

## 13) Suggested rollout

1. Introduce schemas + validators.
2. Implement `aoi` node + project AOI storage.
3. Implement `imagery_provider` node with Esri + USGS.
4. Update 3D viewer to consume `scene3d.imagery_drape.v1` only.
5. Add 2D adapter to same imagery contract.
6. Add `layer_stack` composer node and migrate viewer config.

## 14) Open decisions

- Project AOI lock default: `false` or `true` after first manual set.
- Where geocoder runs (server-side preferred for key control).
- Whether `layer_stack` is explicit node or orchestrator-generated virtual artifact.
