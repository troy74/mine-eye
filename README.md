# mine-eye

Geo-Scry's graph-native exploration platform for mining workflows.

This repository contains a componentized backend graph/orchestration runtime, worker execution engine, and the Vite web client used to design pipelines, run nodes, and preview 2D/3D outputs.

## Middleware-First Design

`mine-eye` is intentionally middleware-first to prevent frontend drift:

- backend services own graph truth, execution/cache state, CRS policy, and lineage
- versioned contracts (`spatial.aoi.v1`, `terrain.surface_grid.v1`, `scene3d.*`) are the handoff boundary
- orchestrator viewer manifests broker render intent so clients do not duplicate inference logic
- clients should render contracts/manifests, not provider internals

This is the core parity rule for web, iOS, and desktop clients.

## What This Project Is

`mine-eye` is an early-stage, end-to-end framework for exploration data workflows:

- ingest collars, surveys, assays, and surface samples
- normalize borehole lithology/contact data into interface-ready artifacts for future formation modelling
- run transform/model nodes (desurvey, drillhole model, heatmap, terrain helpers)
- compose higher-level workflow wrappers with `node_group` while keeping strict typed ports at the canvas boundary
- route outputs through typed graph ports
- render node-scoped previews (2D/3D and generic artifact preview)
- persist graph, branch/revision history, artifacts, and node configuration in backend storage

## Repository Layout

- `apps/web` — React + Vite web client (graph canvas, inspector, viewers)
- `services/orchestrator` — Axum API, graph mutation endpoints, scheduling/enqueue, viewer manifest broker
- `services/worker` — job runner that executes node kinds and writes artifacts
- `crates/mine-eye-types` — shared domain/types/contracts
- `crates/mine-eye-graph` — graph logic/validation helpers
- `crates/mine-eye-scheduler` — dirty-node planning/scheduling helpers
- `crates/mine-eye-nodes` — node executors (ingest/transform/model/visualization)
- `crates/mine-eye-store` — Postgres persistence layer and migrations
- `contracts/scene3d` — JSON schema contracts for AOI/terrain/imagery/layer-stack payloads
- `contracts/geology` — JSON schema contracts for borehole lithology, interface points, stratigraphic surfaces, and touching-volume outputs
- `sample-data` — sample inputs for local testing
- `sample-data/kimberlina_borehole_dataset` — compact public borehole fixture subset for GemPy-inspired stratigraphy/interface tests
- `ARCHITECTURE.md` — architecture and design principles

### Node Kind Taxonomy (Current)

`crates/mine-eye-nodes/src/kinds` is split by domain so public node behavior stays explicit and maintainable:

- `acquisition` (ingest primitives)
- `data_model` (table/dataset transforms)
- `spatial` (AOI and spatial contracts)
- `surface` (heatmap/iso/terrain/DEM/surface interpolation)
- `imagery_raster` (imagery provider + tilebroker contracts)
- `trajectory` (desurvey)
- `drillhole` (merge/model)
- near-term extension: add a dedicated stratigraphy/formation-modelling path that consumes collars + lithology/contact intervals + surface controls on the Rust middleware side
- `resource_model` (block grade modeling + resource summaries)
- `magnetic_model` (airborne magnetics cleanup, gridding, derivatives)
- `magnetic_depth` (Euler deconvolution — 3D depth/susceptibility voxels from magnetic grid)
- `scene_contract` (scene layer composition)
- `visualization` (viewer payload nodes)
- `node_group` (wrapper execution over an internal subgraph with explicitly exposed ports)
- `stubs` (alpha placeholders)

Shared utilities in `kinds/`:

- `parse_util.rs` — canonical numeric parsing and percentile helpers reused across node kinds
- `colour.rs` — canonical colour ramp interpolation (inferno, viridis, terrain, grayscale) reused across node kinds

`runtime.rs` is internal helper/runtime support. New public node entrypoints should be added to the domain module, not directly exposed from `runtime.rs`.

## Componentized Middleware Brokers

To keep frontends efficient and consistent, behavior is split into broker components:

- **Graph broker** (`services/orchestrator`): graph CRUD, branch/revision state, run planning.
- **Execution broker** (`mine-eye-scheduler` + worker): dirty propagation, queue discipline, deterministic execution.
- **Scene broker** (viewer manifest + `scene_contract` nodes): layer composition, renderer intent, UI capability boundary.
- **Spatial broker** (`spatial` nodes + CRS APIs): AOI inference/lock, CRS normalization, bounds provenance.
- **Terrain/imagery broker** (`surface`, `imagery_raster`): DEM fetch/fit, imagery fallback strategy, cache metadata.
- **Artifact broker** (`mine-eye-store`): immutable artifacts, lineage/content hash, schema/variant metadata.

Frontend rule: fetch one brokered contract/manifest and render, rather than recomputing semantics in the UI.

## Node Groups (V1)

`node_group` lets us wrap a small internal workflow behind a cleaner top-level node on the main canvas.

Current goals:

- reduce wiring clutter for standard patterns such as IP modelling and drillhole ingest
- preserve strong middle-layer contracts instead of hiding semantics in frontend-only glue
- allow selective exposure of intermediary internal outputs when they are operationally useful

Current behavior:

- group definitions are persisted in node config as data, not hard-coded runtime state
- group input/output ports are dynamic and derived from the saved group definition
- the worker executes the internal DAG in topological order and reuses the normal node registry for each internal node
- the web client now provides a dedicated drill-in group editor surface with a persisted internal layout, breadcrumb navigation for nested groups, and canvas-level editing of internal topology
- group templates are exposed directly in the add-node menu as a distinct `Groups` section, with their own icon/accent treatment on the main canvas
- the group editor is opened on demand rather than mounted globally, which keeps normal project-open flow isolated from group-edit state

Built-in templates at this stage:

- `IP model` — pseudosection, inversion mesh, inversion input, inversion
- `Drillhole ingest` — collars plus survey ingest

Current V1 limits:

- nested groups are supported with breadcrumb drill-in, capped to a maximum depth of `3`
- internal editing still uses one editor surface rather than spawning separate windows per nesting level
- plugins should still target core semantics/contracts first

## AI Chat (Current)

AI chat is backend-orchestrated and tool-driven (`services/orchestrator/src/ai_chat.rs`) using OpenRouter (`openai/gpt-5.4` by default).

Key behaviors now implemented:

- graph-first diagnostics via one-shot `graph_audit_bundle`
- upload-aware tabular inspection/mapping tools
- direct ingest patching from uploaded files
- wiring/validation tools with semantic-port checks
- execution tools from chat (`run_node`, `run_graph`)
- compact tool reporting in UI (expandable details)

### AI Chat Tool Families

- Graph context: `graph_audit_bundle`, `list_nodes`, `list_edges`, `read_node`, `read_registry_kind`, `registry_capability_matrix`
- Artifacts: `list_node_artifacts`, `artifact_top_tail`, `json_path_extract`, `csv_profile`, `suggest_measure_fields`, `profile_numeric_distribution`
- Uploads (chat attachments): `list_uploaded_files`, `uploaded_file_top_tail`, `uploaded_csv_profile`, `suggest_ingest_mapping_from_upload`, `apply_upload_to_ingest_node`
- Mutations: `add_node`, `patch_node_config`, `wire_nodes`, `unwire_edge`, `preview_graph_diff_for_plan`
- Execution/readiness: `validate_pipeline_for_goal`, `run_node`, `run_graph`

### AI Memory + Skill/Playbook Docs

AI system prompt context is loaded from:

- `docs/ai-memory/project-context.md`
- `docs/ai-memory/geology-summary.md`
- `docs/ai-memory/activities-log.md`
- `docs/ai-memory/system-prompt-notes.md`
- `docs/ai-skills/upload-combo-playbooks.md`
- `docs/ai-skills/node-workflow-fragments.md`
- `docs/node-operating-matrix.md`

The `docs/ai-skills/*` files are designed as reusable playbook fragments for common upload combinations (for example collar+survey+assay) and node-level workflow nudges.

### Chat UX Notes

- Assistant responses support a dual format:
  - `<plain>...</plain>` concise operator-facing answer
  - `<system>...</system>` deeper diagnostics/actions (collapsible in UI)
- Tool events are rendered as compact expandable bubbles.
- A `New Chat` button resets chat thread state for the current project while preserving project/graph context.

## Design Principles

- Backend is source of truth: graph state, node configs, artifacts, branches/revisions, project CRS.
- UI is a renderer/editor: clients should consume backend contracts (especially viewer manifest) and avoid hidden client-only inference.
- Node registry is centralized and backend-driven: web loads registry from API.
- Typed port semantics gate wiring and keep workflows deterministic.
- Persisted viewer + node UI state is part of graph config, so behavior is reproducible.
- Prefer immutable artifact/version records with lineage-aware execution.

## Application Layers

`mine-eye` is intentionally split so we can support multiple front ends without duplicating business logic:

1. **Domain and contracts layer** (`crates/mine-eye-types`, `crates/mine-eye-graph`):
- shared graph/node/port/artifact types
- execution and compatibility semantics

2. **Data and persistence layer** (`crates/mine-eye-store`):
- Postgres persistence, migrations, artifact references
- branch/revision/promotion records and execution state

3. **Application services layer** (`services/orchestrator`, `services/worker`):
- API and orchestration logic
- queue, scheduler, execution dispatch
- viewer manifest and CRS/search APIs

4. **Presentation clients layer** (currently `apps/web`):
- graph editing UX
- node inspectors and viewer tabs
- no authoritative state ownership

The current priority is the Vite web frontend, but all new workflow/business behavior should be implemented in backend contracts first so iOS/desktop clients can consume the same APIs.

## Quick Start

### 1) Start Postgres

```bash
docker compose up -d postgres
```

### 2) Start orchestrator (API)

```bash
cargo run -p mine-eye-orchestrator
```

Defaults:
- `LISTEN=0.0.0.0:3000`
- `DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5433/mine_eye`
- `ARTIFACT_ROOT=./data/artifacts`
- `.env.dev` / `.env` are auto-loaded at startup when present

### 3) Start worker

```bash
cargo run -p mine-eye-worker
```

Notes:
- `.env.dev` / `.env` are auto-loaded at startup when present.
- DEM fetch prefers OpenTopography when a key is available via
  `OPENTTOPOGRAPHY_API_KEY` (also accepts `OPENTOPOGRAPHY_API_KEY`).
- `dem_fetch` accepts any upstream XYZ-bearing artifacts (collars, point sets,
  trajectories, meshes) as fit controls.
- `dem_fetch` supports `node_ui.fit_mode`:
  - `none` (provider DEM only)
  - `vertical_bias` (default)
  - `affine_xy_z` (bias + tilt)
- `dem_fetch` now emits `confidence_grid` (`class_ids` + `scores`) for overlay
  rendering in viewers.
- `dem_fetch` default timeout is 60 s (configurable via `node_ui.timeout_ms`).
  Large AOIs (~55 km × 55 km) can take 30+ s from OpenTopography; the old 7 s
  default caused silent fallback to open-meteo for every request.
- `dem_fetch` tile cache guards against poisoned all-null entries: API error
  responses (e.g. rate-limit JSON with no `"elevation"` key) are never cached,
  preventing stale failures from persisting across retries.
- `heatmap_raster_tile_cache` builds cached heatmap rasters + XYZ tile pyramids from generic XY point measures and emits both:
  - `raster_tile_manifest.json` (cache metadata + render defaults)
  - `heatmap_imagery_drape.json` (`scene3d.tilebroker_response.v1` contract for 3D drape wiring)
- Workspace-level cache defaults are available from the top-right settings cog
  (`/workspaces/{ws_id}/cache-settings`) and control defaults for max bytes, tile count, zoom range, retention, and auto-prune behavior.

### 4) Start web app

```bash
cd apps/web
npm install
npm run dev
```

Default web URL: `http://localhost:5174`

### 5) Configure Clerk auth (required)

The web app requires Clerk at startup and will throw if no publishable key is present.

Set one of these in your shell or `.env.dev`:

- `VITE_CLERK_PUBLISHABLE_KEY`
- `NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY`

Orchestrator verifies Clerk session tokens/JWTs and enforces organization-scoped access for workspaces and graphs.

Optional orchestrator env overrides:

- `CLERK_AUTHORIZED_PARTIES` (comma-separated `azp` allow-list; localhost defaults are built in)
- `CLERK_FRONTEND_API_URL` (derive JWKS endpoint)
- `CLERK_JWKS_URL` (explicit JWKS endpoint; useful for custom domains)

## Core Runtime Flows

- Graph edits in web call orchestrator graph endpoints.
- Orchestrator persists graph mutations and branch/revision metadata.
- `runGraph` queues jobs for dirty nodes; worker claims jobs from `job_queue`.
- Worker executes node kind logic, writes artifacts, updates node execution/cache state.
- Viewer tabs use node inputs + artifact refs and (for visualization nodes) consume viewer manifests.

## Multi-Frontend Direction (Web First, iOS Next)

Planned clients:

- `apps/web` (current primary delivery path)
- future field iOS client for in-field validation, review, and lightweight edits
- possible desktop client for heavier review/model workflows

To keep this sustainable:

- all graph truth, CRS logic, node registry, and viewer-layer contracts stay backend-owned
- clients render from shared contracts and persist user-intent settings via API
- avoid client-only "magic" transforms that cannot be replayed by another client

## CRS and Viewer Notes

- Workspace project CRS is persisted in backend and editable from UI.
- CRS selection uses a single unified picker that merges:
  - project CRS
  - common CRS
  - workspace-used CRS
  - cached picks
  - live EPSG search
- `plan_view_2d` is the current 2D visualization node.
- `threejs_display_node` is the active 3D workflow node for current scene/layer UX iteration.
- `cesium_display_node` remains supported as a legacy/parallel 3D path (and older graphs may still reference `plan_view_3d` alias flows).

## Recommended 3D Pipeline (Current)

For deterministic draped terrain scenes:

1. `aoi` (manual or inferred from connected XY/XYZ)
2. `dem_fetch` (provider DEM with optional fit to known XYZ controls)
3. `tilebroker` / `imagery_provider` (imagery contract + provider fallback chain)
4. optional `xyz_to_surface` / mesh/model nodes
5. optional `block_grade_model` for voxelized grade/resource estimation
6. `threejs_display_node` consuming wired contracts/layers

This ordering keeps AOI, terrain, and imagery explicit and reproducible.

## Recommended Magnetic Pipeline (Current)

For airborne magnetic workflows, including 3D depth inversion:

1. `observation_ingest` (pointer-first ingest of large source table + schema/audit)
2. `magnetic_model` (cleanup, despike/smoothing, interpolation, derivatives)
3. `magnetic_depth_model` (Euler deconvolution — derives 3D source depth voxels from mag grid)
4. `plan_view_2d` for 2D review
5. `threejs_display_node` for 3D context — wires both terrain and depth voxels into a single draped scene

Notes:
- `magnetic_model` emits both full and lightweight `.preview` artifacts (for responsive viewer behavior).
- In magnetic 2D heatmap controls, measure options are intentionally constrained to:
  - `M`
  - `TMF`
  - `fvd`
  - `grad_mag`
  - `tilt`
- `magnetic_depth_model` outputs `block_grade_model_voxels.v1` and renders immediately
  in the 3D viewer via the existing block voxels renderer (no new renderer needed).
- Voxel size scales with estimated depth: `max(depth_m × voxel_scale, resolution_m)` so
  deeper (more uncertain) sources are represented with proportionally larger blocks.
- `structural_index_mode` config accepts `"multi"` (tries N=0,1,2,3 — contact/dyke/cylinder/sphere,
  picks best fit) or `"fixed"` with `structural_index: N` for a single-pass solve.

## 3D Viewer Performance Notes

The Three.js viewer (`Map3DThreePanel.tsx`) has been optimized to avoid unnecessary GPU work:

- **Canvas key stability**: the R3F `<Canvas>` is keyed only on `viewerNodeId`, not on camera reset tokens. Previously, every data reload destroyed and recreated the entire WebGL context (all GPU textures, geometries).
- **Drape texture persistence** (`DrapeTextureCache`): a module-level 20-entry LRU caches `THREE.Texture` objects across Canvas mounts/unmounts. Tab switches and camera resets no longer re-fetch ESRI imagery tiles from the network.
- **Imperative camera reset** (`CameraAutoFit`): uses `useThree()` to reposition the camera without remounting the Canvas. Connects the camera reset token to a `useEffect` that fires only when the token changes.
- **Drape transparency** (`depthWrite={false}`): the imagery drape material does not write to the depth buffer. This allows semi-transparent drape to reveal underground voxels (e.g. from `magnetic_depth_model`) without depth-culling geometry behind the terrain surface.
- **LRU artifact text cache** (`LruTextCache`): bounded 200-entry Map-based LRU for immutable artifact JSON text, replacing an unbounded Map.
- **Shared palette module** (`apps/web/src/palettes.ts`): single source of truth for named colour ramps (mineeye, inferno, viridis, terrain, grayscale). Used by both 2D and 3D viewers and eliminates duplicated interpolation logic.

## Current Scope

This codebase is in active early development. Compatibility with old example workspaces is not guaranteed unless explicitly stated.

## Security/Auth and AI Governance

Current implementation:

- **Authentication**:
  - Clerk-backed session/JWT verification in orchestrator middleware.
  - web app is wrapped in `ClerkProvider` and gates app shell behind sign-in.
- **Authorization**:
  - request-scoped `AuthContext` (`organization_id`, `user_id`, role) is required on API routes.
  - workspace/graph access is checked against organization ownership before reads/mutations/runs.
  - personal users are mapped to deterministic personal org ids (`personal:{user_id}`).
- **Identity persistence**:
  - database tables for `users`, `organizations`, and `organization_memberships`.
  - workspace rows now carry `organization_id`; graph metadata stores organization and creator ids.

Remaining roadmap items:

- optional envelope encryption for sensitive artifact/config classes
- AI budget/credit policy service (org/suballocation controls, metering, and guardrails)

See [ARCHITECTURE.md](ARCHITECTURE.md) for proposed component boundaries and service-level responsibilities.

## Useful Commands

```bash
cargo check
cd apps/web && npm run build
```

## Engineering Playbooks

- Plugin/analytic delivery guide:
  [docs/plugin-analytic-implementation-guide.md](docs/plugin-analytic-implementation-guide.md)

## License

Workspace license is `MIT OR Apache-2.0`.
