# mine-eye Node & Port Inventory

> Design reference — connection types, port semantics, required/optional status,
> and framework recommendations.

---

## Connection Types (Semantic Wire Colours)

| Semantic | Colour | Meaning |
|---|---|---|
| `point_set` | `#38bdf8` sky-blue | Discrete 3-D point cloud (collars, samples, contour XYZ) |
| `interval_set` | `#f97316` orange | Depth-tagged intervals on a trace (assays, lithology, contours) |
| `trajectory_set` | `#a78bfa` violet | Survey trajectory / desurvey product |
| `surface` | `#4ade80` green | Gridded or mesh surface (DEM, iso-surface, heatmap) |
| `raster` | `#facc15` yellow | Image or classified raster tile |
| `mesh` | `#fb7185` rose | Triangulated 3-D solid (drillhole cylinders, geology bodies) |
| `table` | `#94a3b8` slate | Generic tabular contract (AOI spec, imagery drape, transforms) |
| `any` | `#484f58` grey | Untyped / viewer dynamic input |

> **Rule:** a port will only accept a connection if the upstream semantic matches or both ends are `table`/`any`. The handle colour is the definitive visual indicator — wires inherit the source semantic colour.

---

## Nodes

### INPUT category (green border)

---

#### `collar_ingest`
Import collar survey CSV or API feed.

| Dir | Port | Label | Semantic | Required |
|---|---|---|---|---|
| OUT | `collars` | Collars | `point_set` | — |

*Recommendations:*
- Currently input is configured entirely via the node's CSV-mapping panel; no wired inputs.
- **Proposed**: add optional `source_in: table` for a live data-connector node so the ingest can be driven programmatically.

---

#### `survey_ingest`
Import downhole survey (dip/azimuth intervals).

| Dir | Port | Label | Semantic | Required |
|---|---|---|---|---|
| OUT | `surveys` | Surveys | `trajectory_set` | — |

*Recommendations:*
- Same as collar — optional `source_in: table` for connector-driven loading.

---

#### `surface_sample_ingest`
Import surface geochemistry / chip sample data.

| Dir | Port | Label | Semantic | Required |
|---|---|---|---|---|
| OUT | `surface_samples` | Surface samples | `point_set` | — |

---

#### `assay_ingest`
Import downhole assay CSV.

| Dir | Port | Label | Semantic | Required |
|---|---|---|---|---|
| OUT | `assays` | Assays | `interval_set` | — |

---

### TRANSFORM category (blue border)

---

#### `desurvey_trajectory`
Desurvey collars + survey angles into a 3-D trajectory.

| Dir | Port | Label | Semantic | Required |
|---|---|---|---|---|
| IN | `collars_in` | Collars | `point_set` | ✓ |
| IN | `surveys_in` | Surveys | `trajectory_set` | ✓ |
| OUT | `trajectory` | Trajectory | `trajectory_set` | — |

*Recommendations:*
- Both inputs are required — node remains "unset" until both are connected.
- Future: add optional `lithology_in: interval_set` to allow geology-aware desurvey methods.
- For vertical-contact datasets without surveys, pair collars with a small helper that emits straight traces so downstream interface extraction can stay in middleware.

---

#### `drillhole_model`
Build 3-D cylinder mesh and assay point cloud from trajectory + assays.

| Dir | Port | Label | Semantic | Required |
|---|---|---|---|---|
| IN | `trajectory_in` | Trajectory | `trajectory_set` | ✓ |
| IN | `assays_in` | Assays | `interval_set` | ✓ |
| OUT | `drillhole_meshes` | Drillhole meshes | `mesh` | — |
| OUT | `assay_points` | Assay points | `table` | — |

*Recommendations:*
- `assay_points` output semantic should be changed from `table` → `point_set` to get correct wire colour and enable direct connection to point-aware viewers.
- Add optional `lithology_in: interval_set` for separate lith rendering pass.
- Add optional `collar_labels_in: point_set` so collar IDs can be shown as 3-D text.
- Keep assay-driven drillhole rendering separate from future stratigraphy/interface extraction so geologic contacts do not inherit grade-specific assumptions.

---

#### `assay_heatmap`
Interpolate assay or sample points into a surface heatmap.

| Dir | Port | Label | Semantic | Required |
|---|---|---|---|---|
| IN | `in` | Input points | `table` | ✓ |
| OUT | `heatmap` | Heatmap | `surface` | — |

*Recommendations:*
- Input semantic should be `point_set` not `table`.
- Add optional `bounds_in: surface` to constrain interpolation extent to a DEM.
- Add optional `aoi_in: table` to constrain to an AOI polygon.

---

#### `surface_iso_extract`
Extract contour polylines from a DEM / surface at configured level intervals.

| Dir | Port | Label | Semantic | Required |
|---|---|---|---|---|
| IN | `surface_in` | Surface | `surface` | ✓ |
| OUT | `contours` | Contours | `interval_set` | — |
| OUT | `meta` | Iso meta | `table` | — |

*Recommendations:*
- `meta` output is rarely useful downstream; could be hidden by default.
- Add optional `mask_in: surface` to restrict contour extent.

---

#### `dem_contour_xyz`
Export contour lines from a DEM as an XYZ point file with z=elevation.

| Dir | Port | Label | Semantic | Required |
|---|---|---|---|---|
| IN | `terrain_in` | Terrain surface | `surface` | ✓ |
| OUT | `xyz_points` | Contour XYZ | `point_set` | — |
| OUT | `contour_lines` | Contour lines | `interval_set` | — |

*Recommendations:*
- Z-order in viewers should be above base terrain so lines aren't z-fighting.

---

#### `terrain_adjust`
Shift/warp a DEM to match ground-truth control points.

| Dir | Port | Label | Semantic | Required |
|---|---|---|---|---|
| IN | `terrain_in` | Terrain | `surface` | ✓ |
| IN | `control_points` | Control points | `point_set` | ✓ |
| OUT | `terrain_out` | Adjusted terrain | `surface` | — |

*Recommendations:*
- `control_points` could be made optional with a "no adjustment" passthrough.

---

#### `xyz_to_surface`
Interpolate XYZ scatter points into a surface grid (kriging / IDW / TIN).

| Dir | Port | Label | Semantic | Required |
|---|---|---|---|---|
| IN | `xyz_in` | XYZ input | `table` | ✓ |
| IN | `xyz_in_2` | XYZ input 2 | `point_set` | opt |
| IN | `xyz_in_3` | XYZ input 3 | `trajectory_set` | opt |
| IN | `xyz_in_4` | XYZ input 4 | `mesh` | opt |
| OUT | `surface_out` | Surface | `surface` | — |

*Recommendations:*
- `xyz_in` semantic should be `point_set`. The multiple typed inputs (`xyz_in_2..4`) are a workaround for multi-type acceptance — see "Smart AOI inputs" pattern below.
- Consider collapsing into a single `point_set` primary input + algorithm config.

---

#### `dem_fetch`
Fetch a DEM tile from an online or local tile source for the given AOI.

| Dir | Port | Label | Semantic | Required |
|---|---|---|---|---|
| IN | `aoi_in` | AOI / seed geometry | `table` | ✓* |
| IN | `aoi_in_2` | AOI / seed geometry 2 | `point_set` | opt |
| IN | `aoi_in_3` | AOI / seed geometry 3 | `trajectory_set` | opt |
| IN | `aoi_in_4` | AOI / seed geometry 4 | `mesh` | opt |
| IN | `tileserver_in` | Imagery / tile provider | `table` | opt |
| OUT | `terrain_out` | Terrain DEM | `surface` | — |

*One of `aoi_in` through `aoi_in_4` must be connected; they accept any geometry type.

*Recommendations:*
- The `aoi_in_*` fan-in pattern (same concept, different accepted semantics) is a recurring workaround. See **Smart Multi-Semantic Input** pattern below.
- `tileserver_in` is optional; when absent, a plain shaded DEM is rendered.

---

#### `aoi`
Derive an AOI (bounding box or convex hull) from seed geometry.

| Dir | Port | Label | Semantic | Required |
|---|---|---|---|---|
| IN | `seed_in` | Seed geometry | `table` | ✓* |
| IN | `seed_in_2` | Seed geometry 2 | `point_set` | opt |
| IN | `seed_in_3` | Seed geometry 3 | `trajectory_set` | opt |
| IN | `seed_in_4` | Seed geometry 4 | `mesh` | opt |
| OUT | `aoi_out` | AOI contract | `table` | — |

---

#### `data_model_transform`
Generic Python/Rust data transform (configurable).

| Dir | Port | Label | Semantic | Required |
|---|---|---|---|---|
| IN | `in` | Input table | `table` | ✓ |
| IN | `in_2` | Input table 2 | `table` | opt |
| IN | `in_3` | Input point set | `point_set` | opt |
| IN | `in_4` | Input intervals | `interval_set` | opt |
| OUT | `out` | Transformed table | `table` | — |

*Recommendations:*
- Output semantic should reflect actual transform, not always `table`.
- Could benefit from dynamic typed outputs declared at config time.

---

#### `imagery_provider` / `tilebroker`
Produce an imagery drape contract for a given AOI + terrain.

| Dir | Port | Label | Semantic | Required |
|---|---|---|---|---|
| IN | `aoi_in` | AOI | `table` | ✓ |
| IN | `terrain_in` | Terrain | `surface` | ✓ |
| OUT | `imagery_out` | Imagery drape contract | `table` | — |

*Recommendations:*
- These two nodes are functionally similar; consider merging or a `source` config enum.
- Output semantic `table` is a workaround — a dedicated `imagery` semantic would give better wire colour (`#facc15` raster).

---

#### `scene3d_layer_stack`
Assemble a collection of typed layers into a named stack for a 3-D viewer.

| Dir | Port | Label | Semantic | Required |
|---|---|---|---|---|
| IN | `imagery_in` | Imagery drape | `table` | opt |
| IN | `terrain_in` | Terrain | `surface` | opt |
| IN | `traces_in` | Traces | `trajectory_set` | opt |
| IN | `segments_in` | Segments | `mesh` | opt |
| IN | `samples_in` | Samples | `table` | opt |
| IN | `contours_in` | Contours | `table` | opt |
| OUT | `layer_stack_out` | Scene3D layer stack | `table` | — |

*Recommendations:*
- `samples_in` and `contours_in` should be `point_set` and `interval_set` respectively.
- Consider making this node the **canonical aggregator** upstream of all viewers, replacing direct fan-in to viewer nodes.

---

### VISUALISATION category (pink border)

---

#### `plan_view_2d`
2-D Leaflet plan view — up to 8 layers.

| Dir | Port | Label | Semantic | Required |
|---|---|---|---|---|
| IN | `in` … `in_8` | In 1 … In 8 | `table` | opt |

*Recommendations:*
- All inputs are `table`; should accept any semantic (the viewer renders what it understands).
- **Implement smart dynamic ports** (same as `threejs_display_node`) so empty slots aren't shown.
- Layer ordering should be explicit — drag to reorder or numbered.

---

#### `plan_view_3d`
Cesium 3-D globe viewer.

| Dir | Port | Label | Semantic | Required |
|---|---|---|---|---|
| IN | `terrain_in` | Terrain | `surface` | opt |
| IN | `traces_in` | Traces | `trajectory_set` | opt |
| IN | `segments_in` | Segments | `mesh` | opt |
| IN | `samples_in` | Samples | `table` | opt |
| IN | `in_5` … `in_8` | In 5 … In 8 | `table` | opt |

*Recommendations:*
- `samples_in` → `point_set`.
- Typed named ports + dynamic overflow slots is the right model.

---

#### `cesium_display_node`
Same port signature as `plan_view_3d` (Cesium variant).

*Recommendations:* Merge with `plan_view_3d` or differentiate with an explicit config flag.

---

#### `threejs_display_node`
Three.js 3-D scene — **smart dynamic ports**.

| Dir | Port | Label | Semantic | Required |
|---|---|---|---|---|
| IN | `in_1` | Input 1 | `any` | opt |
| IN | `in_2` | Input 2 | `any` | opt |
| IN | `in_N+1` | *(auto-created)* | `any` | opt |

*Currently the ONLY viewer implementing dynamic port expansion (in_1..in_N+1).*

*Recommendations:*
- **Migrate other viewers to the same dynamic model** — typed named primary ports (`terrain`, `traces`, `meshes`, `points`, `contours`) followed by generic overflow slots.
- Port name auto-labelling: when connected, show the upstream node's output port label as the slot label.

---

## Design Patterns & Recommendations

### 1 — Smart Multi-Semantic Input (replace fan-in workaround)

Several nodes duplicate a port concept across 4 typed variants (`aoi_in`, `aoi_in_2`, `aoi_in_3`, `aoi_in_4`). This is noisy and hard to explain to users.

**Proposed approach**: Introduce a union semantic `geometry` that the wire system can resolve at connect-time. A single `aoi_in: geometry` port accepts `point_set | trajectory_set | mesh | table`. Display the accepted union in the port tooltip. If multiple geometry sources are needed, use a dynamic expansion (like threejs_display_node).

### 2 — Dynamic Typed Viewer Ports

The `threejs_display_node` expansion pattern should become the standard for all visualisation nodes. Named typed primary slots are discoverable; dynamic overflow slots allow arbitrary stacking.

Suggested standard viewer port layout:

```
IN  terrain     surface      opt   (terrain DEM)
IN  imagery     table        opt   (drape contract)
IN  drillholes  mesh         opt   (cylinder meshes)
IN  traces      trajectory_set opt
IN  points      point_set    opt   (samples, collars, contour XYZ)
IN  contours    interval_set opt
IN  layer_N+1   any          opt   (dynamic expansion)
```

### 3 — Semantic Audit (current mismatches)

| Node | Port | Current | Should Be |
|---|---|---|---|
| `drillhole_model` | `assay_points` | `table` | `point_set` |
| `assay_heatmap` | `in` | `table` | `point_set` |
| `xyz_to_surface` | `xyz_in` | `table` | `point_set` |
| `scene3d_layer_stack` | `samples_in` | `table` | `point_set` |
| `scene3d_layer_stack` | `contours_in` | `table` | `interval_set` |
| `plan_view_3d` | `samples_in` | `table` | `point_set` |
| `imagery_provider` | `imagery_out` | `table` | `raster` |
| `tilebroker` | `imagery_out` | `table` | `raster` |

### 4 — Missing Nodes (suggested future)

| Kind | Category | Purpose |
|---|---|---|
| `lithology_ingest` | input | Import downhole lithology intervals |
| `formation_interface_extract` | transform | Convert borehole intervals + traces into underground interface points |
| `structure_ingest` | input | Import structural measurements (orientation) |
| `polygon_ingest` | input | Import shapefile / GeoJSON polygons (domains, extents) |
| `data_connector` | input | Generic REST/DB source that drives ingest nodes |
| `drillhole_composite` | transform | Composite assays to regular intervals |
| `block_model_import` | input | Import CSV/Datamine block model |
| `grade_estimation` | model | Inverse-distance / kriging grade estimate to block model |
| `resource_report` | export | Generate JORC/NI43-101 style tonnage × grade table |
| `section_cut` | transform | Slice 3-D geometry along a cross-section plane |
| `variogram` | qa | Compute and display experimental variogram |
| `qaqc_summary` | qa | Duplicate / blank / standard performance charts |
| `csv_export` | export | Export any port output to CSV/GeoJSON/shapefile |
| `web_tile_server` | visualisation | Serve a raster layer as XYZ tiles for external GIS tools |

### 5 — Connection Validation

The wire system currently allows any semantic → `table` connection (implicit cast). This is useful but masks mis-wiring. Suggested rules:

- **Hard block**: `surface` → `trajectory_set` (no reasonable conversion)
- **Soft warn** (yellow edge): `table` → typed port (implicit cast — works but unusual)
- **Accept always**: typed → `any` port, typed → `table` port
- **Accept with promotion**: `point_set` → `trajectory_set` if upstream is a desurvey trajectory

### 6 — Port Defaults

Ports that have a usable fallback when unconnected should show a `◆` indicator in the label (currently not implemented). Examples:

- `tileserver_in` on `dem_fetch` — defaults to Mapbox hillshade
- `bounds_in` on heatmap — defaults to data extent
- `imagery_in` on viewers — defaults to no-drape shaded DEM

---

*Generated by mine-eye design review — update when node-registry.json changes.*
