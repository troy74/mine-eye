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
- run transform/model nodes (desurvey, drillhole model, heatmap, terrain helpers)
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
- `sample-data` — sample inputs for local testing
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
- `scene_contract` (scene layer composition)
- `visualization` (viewer payload nodes)
- `stubs` (alpha placeholders)

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

### 4) Start web app

```bash
cd apps/web
npm install
npm run dev
```

Default web URL: `http://localhost:5174`

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
- avoid client-only “magic” transforms that cannot be replayed by another client

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
5. `threejs_display_node` consuming wired contracts/layers

This ordering keeps AOI, terrain, and imagery explicit and reproducible.

## Current Scope

This codebase is in active early development. Compatibility with old example workspaces is not guaranteed unless explicitly stated.

## Security/Auth and AI Governance Placeholders

These are intentionally scaffolded as architecture requirements for upcoming phases:

- **Authentication/authorization**:
  - move toward token-based auth and per-request identity context in orchestrator
  - support role/scope checks (user, org, workspace/project permissions)
- **Encryption options**:
  - baseline TLS in transit
  - optional envelope encryption at rest for sensitive artifacts/config metadata
- **AI usage governance**:
  - per-org and per-suballocation AI credit ledgers
  - policy checks before AI actions (budget, role, allowed tool scope)
  - auditable AI action logs linked to graph/project context

See [ARCHITECTURE.md](/Users/troytravlos/mine-eye/ARCHITECTURE.md) for proposed component boundaries and service-level responsibilities.

## Useful Commands

```bash
cargo check
cd apps/web && npm run build
```

## License

Workspace license is `MIT OR Apache-2.0`.
