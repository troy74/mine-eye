# mine-eye

Geo-Scry's graph-native exploration platform for mining workflows.

This repository contains the backend graph/orchestration runtime, worker execution engine, and the web client used to design pipelines, run nodes, and preview 2D/3D outputs.

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
- `sample-data` — sample inputs for local testing
- `GEO_SCRY_FRAMEWORK_CHECKLIST.md` — active implementation checklist
- `V1SPEC.md` — v1 behavior/spec notes
- `ARCHITECTURE.md` — architecture and design principles

## Design Principles

- Backend is source of truth: graph state, node configs, artifacts, branches/revisions, project CRS.
- UI is a renderer/editor: clients should consume backend contracts (especially viewer manifest) and avoid hidden client-only inference.
- Node registry is centralized and backend-driven: web loads registry from API.
- Typed port semantics gate wiring and keep workflows deterministic.
- Persisted viewer + node UI state is part of graph config, so behavior is reproducible.
- Prefer immutable artifact/version records with lineage-aware execution.

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

### 3) Start worker

```bash
cargo run -p mine-eye-worker
```

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

## CRS and Viewer Notes

- Workspace project CRS is persisted in backend and editable from UI.
- CRS selection uses a single unified picker that merges:
  - project CRS
  - common CRS
  - workspace-used CRS
  - cached picks
  - live EPSG search
- `plan_view_2d` is the current 2D visualization node.
- `cesium_display_node` is the current Cesium-backed 3D visualization node (legacy alias path exists for `plan_view_3d`).

## Current Scope

This codebase is in active early development. Compatibility with old example workspaces is not guaranteed unless explicitly stated.

## Useful Commands

```bash
cargo check
cd apps/web && npm run build
```

## License

Workspace license is `MIT OR Apache-2.0`.
