# V1SPEC.md

## Exploration Modelling Platform — V1 Decision Spec

### 1. Node Abstraction
Stateful nodes with:
- id (immutable)
- versioned config
- typed input/output ports
- execution state
- cache state
- lineage metadata

Node categories:
- Input, Transform, Model, QA, Visualisation, Export

Ports use semantic types:
PointSet, IntervalSet, TrajectorySet, Surface, Raster, Mesh, BlockModel, Table

---

### 2. Canonical Data Model
Acquisition primitives:
- Collar
- SurveyStation
- IntervalSample
- PointSample
- SurfacePoint / DEM

Derived primitives:
- Trajectory
- DesurveyedInterval
- PointCloud
- Surface
- Mesh
- BlockModel
- ScalarField

Each includes:
geometry, CRS, units, attributes, QA flags, lineage

---

### 3. Graph Execution Model
- Incremental DAG
- Hash-based staleness detection
- Node policies:
  - recompute: auto/manual
  - propagation: eager/debounce/hold
  - quality: preview/final

---

### 4. Persistence Model
Three layers:
1. Postgres + PostGIS (graph + metadata)
2. Object storage (artifacts)
3. Local cache (offline + recent data)

---

### 5. Dataset vs Artifact Pipeline
artifact → ingestion → dataset → transform → dataset/artifact

---

### 6. AI Roles
- Ingestion assistant
- Graph wiring assistant
- Transformation assistant
- Modelling assistant

AI suggests, user confirms.

---

### 7. Node Extensibility
- Core nodes (Rust)
- Plugin nodes (future)
- Isolated execution for plugins

---

### 8. Geometry Model
Separate:
- Analytic (points, polylines, rasters, grids)
- Render (meshes, tiles)

---

### 9. Compute Architecture
Frontend:
- UI, viewport, previews

Orchestrator:
- graph state, scheduling, AI

Workers:
- heavy compute

---

### 10. Versioning
- Node-level versioning
- Hash-based change detection
- Snapshots on key events

---

### 11. AI Graph Wiring
AI suggests pipelines and connections based on semantic types.

---

### 12. V1 Scope
- Drillhole ingestion (collars, surveys, assays)
- Surface samples
- Desurvey + trajectory
- DEM integration
- Basic block model
- 3D visualisation

---

### 13. CRS Strategy
- Preserve source CRS
- Normalize to project CRS

---

### 14. Roles & Approval
Metadata included:
owner, status, lock, approval fields

---

### 15. Economic Layer
Deferred to V2

---

### 16. Port taxonomy (extensible)

Ports are chained between nodes. Types are **extensible** and support **inheritance** for validation and UI colouring.

**Base / generic families**

1. **Scalars:** text, number, boolean, datetime  
2. **Dataframe:** tabular data with unconstrained columns (e.g. raw file import)  
3. **2D dataframe:** requires plan-position columns **x, y** (and optional ids, measures, attributes)  
4. **3D dataframe:** **x, y, z** (and optional nested tabular payloads)  
5. **Artifacts:** opaque or semi-typed blobs — file, image, shapefile, etc.

**Constrained / domain types (inherit from the above)**

Examples (non-exhaustive):

- **Collar:** 3D dataframe (or effectively 2D if elevation is relative / omitted for plan view) with required **x, y**; optional **hole id**, **azimuth**, **dip**, and passthrough attributes. May feed **2D** consumers (plan map uses x,y) or **3D** consumers; the reverse is only valid if the consumer’s minimum columns are satisfied.  
- **Assay / interval table:** hole or sample codes, interval bounds, element grades; can join to geometry for **2D or 3D** plotting when linked to spatial data.  
- **Survey:** stations along holes (depth, az, dip, …) as tabular / trajectory-oriented types.

**Wire format (today)**  
Rust `SemanticPortType` (`point_set`, `table`, `interval_set`, `trajectory_set`, …) remains the persisted edge contract. The UI maps these to the taxonomy above for documentation, colours, and future strict validation.

**Plan map viewer (`plan_view_2d`)**  
A visualisation node with **inputs only**. The 2D map **must not** infer data from node kinds or hunt for “collars” globally. It displays only artifacts reachable via **edges into** the viewer’s input ports, for semantics agreed as plan-view-compatible (e.g. table, point_set, interval_set, trajectory_set). Rendering starts with **points** (x,y); line styles and port-colour composition on the map are deferred.
