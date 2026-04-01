# Geo-Scry Framework Checklist (Working Draft)

Status date: 2026-04-01  
Scope: End-to-end foundation for mining/exploration workflows before broad expansion.

This document has been adopted into this repository as the active framework checklist and contract freeze candidate.

## Project Assessment Snapshot (mine-eye repo)

Assessment date: 2026-04-01.

### What is already present

- Core graph + node + edge persistence exists in Postgres (`graphs`, `nodes`, `edges`) with a queued job table (`job_queue`) in [crates/mine-eye-store/migrations/20250331000001_init.sql](/Users/troytravlos/mine-eye/crates/mine-eye-store/migrations/20250331000001_init.sql).
- Artifact records exist with `artifact_key` and `content_hash` in `node_artifacts`, but version metadata is still minimal (same migration file above).
- Semantic port wiring exists end-to-end with a narrow enum (`point_set`, `interval_set`, `trajectory_set`, `surface`, `raster`, `mesh`, `block_model`, `table`) in [crates/mine-eye-types/src/ports.rs](/Users/troytravlos/mine-eye/crates/mine-eye-types/src/ports.rs).
- UI has an extensible taxonomy overlay for future richer contracts in [apps/web/src/portTaxonomy.ts](/Users/troytravlos/mine-eye/apps/web/src/portTaxonomy.ts).
- Node execution and cache states exist, but lifecycle labels differ from this checklist (`idle/pending/running/failed/succeeded`) in [crates/mine-eye-types/src/node.rs](/Users/troytravlos/mine-eye/crates/mine-eye-types/src/node.rs).

### Major gaps vs this checklist

- No branch/revision model tables yet (`graph_branches`, `graph_revisions`, `branch_promotions` not present).
- No frozen primitive + structural + semantic contract system implemented as enforceable backend schema.
- No formal compatibility/coercion matrix enforcement in backend edge validation.
- No explicit preview/final artifact version fields (`variant`, `schema_version`, `manifest_version`) in persistence.
- No immutable revision lineage model yet (lineage exists per node, not graph-branch revisioned in the described form).

### Implemented in this pass (2026-04-01)

- Added persistence schema for `graph_branches`, `graph_revisions`, `branch_promotions`.
- Added artifact version metadata columns (`variant`, `schema_version`, `manifest_version`, `lineage_hash`, `payload_hash`, `supersedes_artifact_id`).
- Added store APIs for branch/revision creation/listing and promotion audit logging.
- Added orchestrator endpoints:
  - `GET/POST /graphs/{graph_id}/branches`
  - `POST /graphs/{graph_id}/branches/{branch_id}/commit-current`
  - `GET /graphs/{graph_id}/revisions`
  - `GET/POST /graphs/{graph_id}/promotions`
  - `POST /graphs/{graph_id}/promotions/execute` (fast-forward and 3-way merge with conflict report)
- Added automatic revision commits on graph-mutating API operations (`add_node`, `patch_node_params`, `add_edge`, `delete_node`, `delete_edge`).
- Added merge apply path that can materialize a promoted merged revision back onto the active graph definition.
- Added sidebar branch/revision/promotion controls in the web app to create branch, commit current graph state, and promote to `main` for manual testing.
- Added viewer manifest endpoint (`GET /graphs/{graph_id}/viewers/{viewer_node_id}/manifest`) as middleware-owned rendering contract.
- Added backend presentation broker module (orchestrator) to infer per-layer UI metadata from artifacts (`display_contract`, `heatmap_config`, `measure_candidates`, contour/surface hints).
- Web 2D viewer now consumes manifest presentation metadata first (with legacy fallback parsing), reducing UI-specific rendering drift.
- Added `plan_view_3d` node type and runtime support (`mine-eye-nodes` executor + worker artifact output).
- Added web `Map3DPanel` (Cesium) using viewer manifest + wired input artifacts to render terrain baseline, drill traces, grade segments, and assay points with persisted layer/measure/palette controls.
- Added `terrain_adjust` node for control-point DEM nudging (vertical bias + affine XY/Z fit), with QC residual metrics and adjusted terrain output.
- Added `surface_iso_extract` node for contour extraction from `surface_grid`, including 3D Z projection controls.
- Extended 3D viewer controls with display-only radius scaling and contour/terrain overlay toggles.

---

## 1) Delivery Checklist

Use this as the active build checklist. Mark items `[x]` only when implemented and verified.

### A. Core Contracts (Freeze First)

- [ ] Freeze primitive data contracts (scalar, vector, time, CRS, quality, units).
- [ ] Freeze canonical geospatial payload contract (`geo_table_xyzt`) and derived geometry/materializations.
- [ ] Freeze tabular contracts (typed columns, optional/required fields, null policy, code lists).
- [ ] Freeze semantic contracts for mining datasets (collar, survey, assay, lithology, geotech, geophysics).
- [ ] Freeze artifact envelope contract (lineage hash, schema hash, CRS bounds, quality summary, variant).
- [ ] Freeze node state model + transition rules (`idle -> queued -> running -> preview_ready/completed/failed` etc.).
- [ ] Freeze error contract (validation errors, runtime errors, retryability, remediation hints).
- [ ] Freeze compatibility matrix: source output port -> target input port.

### B. Graph + Revision + Branching

- [ ] Middleware remains source of truth for committed graph revision.
- [ ] Frontend supports draft state and merge-to-commit.
- [ ] Straightforward merge policy implemented.
- [ ] Conflict policy implemented: auto-create branch on conflict.
- [x] Branch promotion path defined (`draft -> qa -> approved -> promoted`).
- [ ] Revision diff and branch lineage visible in UI.

### C. Runtime and Orchestration

- [ ] DAG planner executes topologically with explicit dependency checks.
- [ ] Async job queue for heavy compute nodes.
- [ ] Cancellation tokens propagate gateway -> orchestrator -> worker.
- [ ] Retry policy per node class (max attempts, backoff, retryable error classes).
- [ ] Cache policy v1 implemented (strict lineage cache key + preview/final variants).
- [ ] Immutable run history persisted for all runs.

### D. End-to-End Pipeline A (Surface Samples -> Heatmap)

- [ ] Ingest surface samples (`csv-input-generic` + semantic mapper).
- [ ] Clean/normalize node (CRS harmonization + null/outlier policy).
- [ ] Interpolation node (surface heatmap output + quality metadata).
- [ ] Display adapters for 2D heatmap and 3D drape layer.
- [ ] Node status and outputs visible and accurate in UI.

### E. End-to-End Pipeline B (Drill -> Block Model)

- [ ] Ingest collars, surveys, assays (+ optional lithology).
- [ ] Survey normalization and desurvey pipeline.
- [ ] Assay projection/compositing pipeline.
- [ ] Block model generation node (preview + final).
- [ ] 3D viewer layer controls for traces, assays, block model, terrain.

### F. Usability Baseline

- [ ] Graph canvas shows stale/running/failed/locked states clearly.
- [ ] Node panel shows: inputs, required semantic fields, outputs, last run, quality flags.
- [ ] Run timeline panel shows queued/running/completed/failed with timestamps.
- [ ] Errors are actionable (what failed, why, how to fix).
- [ ] First-run guided flow for import -> map fields -> validate -> run -> view.

### G. AI Readiness (After A + B are Solid)

- [ ] Graph/schema context packs are deterministic and compact.
- [ ] AI can propose import mapping with confidence + questions.
- [ ] AI actions compile to auditable graph operations only.
- [ ] Human confirmation required for graph-modifying actions.

---

## 2) Core Contract Type List (Draft Freeze Candidate)

This section is the initial "frozen list" candidate for discussion and lock-down.

### 2.1 Primitive Types

- `number_f64`
- `integer_i64`
- `boolean`
- `text_utf8`
- `enum_code`
- `timestamp_utc`
- `date`
- `duration`
- `json_object`
- `binary_blob`
- `crs_ref` (authority+code, optional proj string)
- `unit_ref` (canonical unit ID)
- `quality_flag` (valid/suspect/rejected + reason code)

### 2.2 Structural Types

- `scalar`
- `vector_n`
- `table`
- `key_value_map`
- `timeseries_1d`
- `image_2d`
- `raster`

### 2.3 Geospatial Core Types

Canonical first approach:

- `geo_table_xyzt` (x and y required; z optional; t optional; attributes bag always allowed)
- `geo_row` (single record view of `geo_table_xyzt`)

Derived/materialized forms (produced by specific nodes, not required as base ingest shape):

- `point_set_xyzt`
- `polyline_set`
- `polygon_set`
- `tri_mesh`
- `point_cloud`
- `voxel_grid`
- `volume_grid`
- `raster_grid`
- `scene_layer`

### 2.4 Mining Semantic Types (High Value v1)

- `collar_table`
- `survey_table`
- `downhole_measure_table` (generic interval/point measures along hole trajectory)
- `assay_table` (semantic profile of `downhole_measure_table`)
- `lithology_table` (semantic profile of `downhole_measure_table`)
- `geotech_table`
- `structure_table` (fault/fracture/vein intervals or picks)
- `density_table`
- `composite_table`
- `desurveyed_trace_set`
- `hole_segment_set` (canonical segmented representation of a hole with segment IDs)
- `wireframe_mesh`
- `block_model_volume`
- `surface_sample_table`
- `channel_sample_table`
- `soil_sample_table`
- `geochem_table`
- `geophysics_ground_table`
- `geophysics_airborne_table`
- `geophysics_downhole_table`
- `magnetics_grid`
- `radiometrics_grid`
- `gravity_grid`
- `em_conductivity_volume`
- `seismic_section`
- `terrain_dem`
- `imagery_layer`
- `document_bundle`
- `report_table`

### 2.5 Metadata That Must Travel With Data

- `crs` (required for all spatial outputs)
- `units` (per field/measure)
- `field_dictionary` (name, type, semantic meaning)
- `provenance` (source URI/hash/import settings)
- `lineage_hash` (upstream artifacts + params + code version)
- `quality_summary` (null rate, ranges, distribution checks, QA flags)
- `bounds` (2D/3D extents)
- `time_range` (where applicable)

---

## 3) Mining Data Ingestion Inventory (Likely Inputs)

### 3.1 Underground and Drill-Centric

- Collar files (planned + as-drilled).
- Survey files (depth/azimuth/dip; multiple survey tools).
- Assay intervals and point assays.
- Lithology and alteration logs.
- Geotech logs (RQD, recovery, fractures, strengths).
- Downhole geophysics (mag, density, EM, gamma, televiewer derivatives).
- Deviation corrections and survey QC files.

### 3.2 Surface and Near-Surface

- Surface sample points/channels.
- Trenching logs.
- Mapping polygons/lines (lith boundaries, structures, domains).
- Topography/DEM and derived slope/aspect products.
- Satellite/orthophoto/drone imagery.

### 3.3 Airborne and Regional

- Airborne magnetics.
- Airborne radiometrics.
- Airborne EM.
- Airborne gravity/gradiometry.
- Regional vector layers and target catalogs.

### 3.4 Supporting Non-Spatial Inputs

- Lab certificates and QAQC tables.
- Domain dictionaries/code mappings.
- Economic parameters for scenario runs.
- Compliance docs, notes, and report attachments.

### 3.5 Tabular Format Coverage (v1 ingest requirement)

- Delimited text (`csv`, `tsv`, `psv`, custom delimiter).
- Fixed-width text.
- Header/footer row offsets and row-skip rules.
- Typed parsing hints and null tokens.
- Mapping/export config JSON for deterministic re-import and replay.

---

## 4) Node Family List (for Buildout)

### 4.1 Ingest Nodes

- `tabular-input-generic` (delimited + fixed-width + parser config persistence)
- `collar-input`
- `survey-input`
- `assay-input`
- `lithology-input`
- `surface-sample-input`
- `dem-fetch`
- `esri-tiles` / imagery source
- `document-input`

### 4.2 Converter and Cleaner Nodes

- CRS transform
- Schema map (column mapping + semantic tagging)
- Dataframe transform script (Polars-style expressions)
- Unit convert
- Null/invalid cleaner
- Outlier detector/clipper
- Interval normalizer (from/to depth checks)
- Survey normalize
- Domain/code map
- JSON hierarchy mapper/exporter (table -> nested model contract)

### 4.3 Processor Nodes

- Desurvey
- Downhole measure project (assay/litho/other measures onto trace/segments)
- Composite intervals
- Surface interpolation (IDW/kriging variants)
- Mesh generation
- Voxelization
- Block model build
- Domain modeling helpers (wireframe/domain assignment)
- Ground/underground geophysics interpretation (2D and 3D starter set)
- Airborne geophysics interpretation (2D and 3D starter set)

### 4.4 Display/Output Nodes

- Viewer 2D (layers, controls, legends)
- Viewer 3D (layers, controls, legends)
- Artifact preview/QC viewer (table/profile/summary for any node output)
- Table summary
- Report/document generator

---

## 5) Port Type System Draft (Parallel Workstream)

### 5.1 Port Classes

- `primitive_port`: scalar/text/bool/time/json.
- `spatial_port`: `geo_table_xyzt` or derived geometry/raster/volume with CRS required.
- `semantic_port`: spatial or table payload plus required semantic fields.
- `control_port`: non-data controls (run mode, quality gates, approvals).

### 5.2 Port Compatibility Rules (v1)

Deterministic rule order (evaluate top to bottom):

1. Class check:

- `primitive_port` can connect only to `primitive_port`.
- `spatial_port` can connect only to `spatial_port`.
- `semantic_port` can connect only to `semantic_port` (or to declared semantic-compatible spatial/table adapters).
- `control_port` can connect only to `control_port`.

2. Exact type pass:

- If source and target types are identical, allow.

3. Approved coercion pass:

- Allow only conversions in the v1 safe coercion matrix below.
- If conversion is lossy or ambiguous, reject and require explicit converter node.

4. Semantic contract pass:

- For semantic ports, required-field contract must be satisfied.
- Missing required fields => reject with field-level diagnostics.

5. CRS pass (for spatial/semantic-spatial payloads):

- Allow if CRS exact match.
- Allow if target accepts transform and a CRS transform policy is declared.
- Otherwise reject and require explicit CRS transform node.

6. Field preservation pass:

- Dropping semantic required fields is rejected by default.
- Dropping optional fields requires explicit node config (`allow_field_drop=true`) and audit log entry.
- Extra fields are passed through by default (`passthrough_additional_fields=true` default).

#### 5.2.1 Safe Coercion Matrix (Auto-Allowed)

Primitive safe coercions:

- `integer_i64` -> `number_f64`
- `date` -> `timestamp_utc` (normalized at midnight UTC unless timezone provided)
- `enum_code` -> `text_utf8`
- `boolean` -> `text_utf8` (`true`/`false`)

Structural/spatial safe coercions:

- `geo_table_xyzt` -> `point_set_xyzt` (materialize points)
- `point_set_xyzt` -> `geo_table_xyzt` (tabular projection)
- `polyline_set` -> `geo_table_xyzt` (vertex flattening with path identifiers)
- `polygon_set` -> `geo_table_xyzt` (ring vertex flattening with polygon/ring identifiers)

Semantic safe coercions:

- `downhole_measure_table` -> `assay_table` only when assay dictionary contract is present.
- `downhole_measure_table` -> `lithology_table` only when lithology dictionary contract is present.

#### 5.2.2 Explicit Converter Required (Not Auto-Allowed)

- `text_utf8` -> numeric/date/time types.
- `number_f64` -> `integer_i64`.
- Any primitive -> `enum_code` (requires dictionary map).
- Any geometry family to another geometry family where topology is inferred (for example point->polyline, polyline->polygon).
- `raster_grid` <-> vector families.
- Any CRS-changing conversion without explicit transform declaration.
- Any semantic narrowing that removes required fields.

#### 5.2.3 Standard Rejection Codes

- `port_class_mismatch`
- `port_type_mismatch`
- `coercion_not_allowed`
- `semantic_contract_unsatisfied`
- `crs_incompatible`
- `field_loss_not_allowed`
- `field_passthrough_disabled`

### 5.3 Semantic Port Contracts (V1 Baseline)

These are the first locked contracts for implementation.

#### 5.3.1 `collar_required` (table semantics)

Required fields:

- `hole_id` (`text_utf8`, non-empty, normalized uppercase trim)
- `x` (`number_f64`)
- `y` (`number_f64`)

Optional fields:

- `z` (`number_f64`)
- `planned_azimuth` (`number_f64`)
- `planned_dip` (`number_f64`)
- `planned_length` (`number_f64`)
- `collar_date` (`date` or `timestamp_utc`)
- `attrs.*` (free attributes)

Validation rules:

- `hole_id` unique within dataset (or duplicates flagged as `quality_flag=suspect`).
- `x,y` required and finite.
- `z` optional in ingest, but must be present before desurvey unless an elevation resolver node is inserted.
- CRS required on artifact.

#### 5.3.2 `survey_table_required` (table semantics)

Required fields:

- `hole_id` (`text_utf8`)
- One trajectory pattern must be satisfied:
  - Pattern A: `depth`, `azimuth`, `dip`
  - Pattern B: `from_depth`, `to_depth`, `azimuth`, `dip`

Optional fields:

- `survey_date` (`date` or `timestamp_utc`)
- `tool_type` (`enum_code`)
- `quality_code` (`enum_code`)
- `attrs.*`

Validation rules:

- `azimuth` normalized to `[0, 360)`.
- `dip` within `[-90, 90]`.
- Depth monotonic by `hole_id` after normalization.
- For Pattern B: `to_depth > from_depth` for every row.
- Rows failing these checks are retained with `quality_flag` unless strict mode is enabled.

#### 5.3.3 `downhole_measure_required` (generic interval/point along hole)

Required fields:

- `hole_id` (`text_utf8`)
- One location pattern:
  - Interval: `from_depth`, `to_depth`
  - Point: `depth`
- At least one measure field in `measures.*`

Optional fields:

- `measure_group` (`enum_code`, examples: `assay`, `lithology`, `geotech`, `geophysics`)
- `sample_id` (`text_utf8`)
- `segment_id` (`text_utf8`)
- `support` (`enum_code`, examples: `interval`, `point`, `composited`)
- `attrs.*`

Validation rules:

- Interval rows require `to_depth > from_depth`.
- Point rows require finite `depth`.
- Mixed interval/point rows allowed in one artifact only if `support` is explicit.
- Node must declare whether midpoint projection or interval projection is used downstream.

#### 5.3.4 Specialized profiles on `downhole_measure_required`

- `assay_interval_required`
  - Base: `downhole_measure_required`
  - Plus: assay measure dictionary (for example `au_ppm`, `cu_pct`) and unit refs.

- `lithology_interval_required`
  - Base: `downhole_measure_required`
  - Plus: domain/lith code dictionary (for example `lith_code`, `alteration_code`).

#### 5.3.5 Composite contracts

- `desurvey_input_required`
  - Inputs: `collar_required` + `survey_table_required`

- `hole_segment_binding_required`
  - `downhole_measure_required` rows must resolve to either:
    - explicit `segment_id`, or
    - deterministic interval-to-trajectory binding at runtime.

### 5.4 Geo Node Definition Formula

For a node to qualify as a `geo_node`, require:

- At least one `spatial_port` input or output.
- Explicit CRS policy (`pass-through`, `transform`, `project-default`, `required-match`).
- Declared quality checks (minimum one of schema, range, topology, or coverage checks).
- Declared lineage contribution (params hash + code version).

### 5.5 Field Propagation Contract (Locked)

Default rule:

- Nodes must preserve all unknown/additional fields unless explicitly configured to drop them.

Required behavior:

- Preserve `attrs.*`, `measures.*`, and unrecognized columns through node execution.
- Preserve field order metadata where available.
- Preserve unit metadata for passthrough fields.
- Preserve quality flags for passthrough fields.

When a field is dropped or renamed:

- Node must emit a field transform manifest in artifact metadata:
  - `dropped_fields[]`
  - `renamed_fields[{from,to}]`
  - `derived_fields[]`
- Run event must include a summary of schema delta.
- If any required downstream semantic field is dropped, validation fails before execution.

### 5.6 Canonical Naming and Unit Policy (Locked)

Field naming:

- Canonical internal field names use `snake_case`.
- Semantic aliases are mapped at ingest via schema map node.
- Reserved semantic fields:
  - `hole_id`, `x`, `y`, `z`, `t`
  - `from_depth`, `to_depth`, `depth`
  - `segment_id`, `sample_id`
- Free fields go under `attrs.*` or `measures.*` namespaces after normalization.

Units:

- Units are tracked in metadata per field (not encoded into field names).
- Preferred canonical examples:
  - `depth_m`, `elevation_m`, `au_ppm`, `cu_pct` may exist as source names,
  - but canonical contract stores logical field + unit mapping (for example `depth` + `m`).
- Conversion nodes must update both values and unit metadata.
- Missing units are allowed only with `quality_flag=suspect` unless strict mode.

---

## 6) Open Decisions To Finalize Next

- [x] Mandatory vs optional semantic fields per dataset type (v1 baseline locked for collar/survey/downhole-measure).
- [x] Canonical measure naming and unit policy.
- [x] Coercion matrix (safe automatic conversions vs explicit converter nodes).
- [x] Preview vs final artifact retention policy in first release (v1 baseline).
- [x] Branch promotion and conflict strategy (v1 baseline).

---

## 7) Artifact Versioning and Branch Management (Locked V1)

### 7.1 Core Principles

- Graph revisions are immutable and append-only.
- Artifacts are immutable and content-addressed.
- Branches are first-class objects with explicit base and head revisions.
- Promotion to mainline is explicit and auditable.

### 7.2 Branch Model

- `graph_branch` has:
  - `id`
  - `graph_id`
  - `name`
  - `base_revision_id`
  - `head_revision_id`
  - `status` (`draft`, `qa`, `approved`, `promoted`, `archived`)
  - `created_by`, `created_at`, `updated_at`

- Every `graph_revision` belongs to exactly one branch (`branch_id` required).

### 7.3 Artifact Version Identity

Each produced artifact version is immutable and identified by:

- `payload_hash`
- `lineage_hash`
- `schema_version`
- `node_manifest_version`
- `variant` (`preview` or `final`)

Recommended stable key:

- `artifact_key = hash(payload_hash + lineage_hash + schema_version + node_manifest_version + variant)`

Lineage must include:

- upstream artifact IDs/hashes
- normalized node params
- node code/runtime version
- source revision ID

### 7.4 Promotion Behavior

- Fast-forward promote when target branch head equals source branch base.
- Otherwise perform 3-way merge at graph revision level.
- On conflicts, create conflict report and keep source branch open for resolution.

### 7.5 Conflict Rules

Auto-merge allowed:

- Non-overlapping node additions/deletions.
- Non-overlapping edge additions/deletions.
- Param edits on different nodes.

Manual resolution required:

- Same node param edited differently.
- Node edited in one branch and deleted in another.
- Any semantic contract break.
- Any required-field loss along active downstream paths.

### 7.6 Retention Policy (V1)

- Keep all `final` artifacts for promoted revisions.
- Keep all revisions and run history (no destructive rewrite).
- Keep `preview` artifacts with TTL (default recommendation: 60 days), except:
  - previews referenced by checkpoints are retained.
- Introduce hard checkpoints that are never pruned.

### 7.7 Minimum Data Additions (Implementation Guide)

- `graph_branches` table.
- `graph_revisions.branch_id`.
- `artifacts` version metadata:
  - `artifact_key`
  - `variant`
  - `schema_version`
  - `manifest_version`
  - `supersedes_artifact_id` (optional lineage pointer)
- `branch_promotions` audit table:
  - source/target branch IDs
  - source head revision
  - promoted revision
  - status
  - conflict/merge report payload
