# Architecture

## 1. Purpose

`mine-eye` is a graph-native execution platform for exploration/mining workflows.

The architecture is designed so that:

- backend owns graph truth, execution truth, and persistence
- clients remain thin render/editor layers
- node behavior is deterministic from persisted config + upstream artifacts
- outputs are portable across multiple clients (web, and future iOS/desktop)

## 2. High-Level System

```text
Web UI (React) ──HTTP/SSE──> Orchestrator (Axum)
                                │
                                ├── Postgres (graphs, nodes, edges, branches, revisions, jobs, artifacts)
                                ├── Artifact storage (filesystem)
                                └── Job queue rows -> Worker (Rust)

Worker (mine-eye-worker) executes node kinds from mine-eye-nodes and writes artifacts + execution state.
```

## 3. Main Components

### 3.1 Web Client (`apps/web`)

Responsibilities:

- graph canvas editing (nodes/edges)
- node inspector for node-specific config (mapping, CRS, run/config/output)
- workspace/project controls (including project CRS)
- viewer tabs (workspace + node-scoped previews)
- rendering from backend contracts (especially viewer manifest)

Non-goals:

- no authoritative graph state
- no hidden business logic that should live in backend contracts

### 3.2 Orchestrator (`services/orchestrator`)

Responsibilities:

- API surface for graph CRUD, branch/revision flows, promotions
- edge validation and scheduling preparation
- job enqueueing (`runGraph`)
- viewer manifest generation (`/graphs/{graph_id}/viewers/{viewer_node_id}/manifest`)
- project CRS persistence (`PATCH /workspaces/{ws_id}/project-crs`)
- EPSG lookup proxy (`/epsg/search`)

Design role:

- middleware source of truth for committed graph and presentation contracts

### 3.3 Worker (`services/worker`)

Responsibilities:

- poll and claim queued jobs
- resolve latest input artifacts for node execution
- execute node kind via `NodeExecutorRegistry`
- write artifact refs, content hashes, and execution/cache state
- persist failures with error context

### 3.4 Shared Crates

- `mine-eye-types`: canonical structs/enums for graph, ports, jobs, artifacts
- `mine-eye-graph`: graph logic helpers
- `mine-eye-scheduler`: dirty-node planning/scheduling helpers
- `mine-eye-nodes`: executable node implementations
- `mine-eye-store`: Postgres access + migrations

## 4. Core Data Model

At minimum:

- `workspaces` (includes `project_crs`)
- `graphs`
- `nodes`
- `edges`
- `job_queue`
- `node_artifacts`
- `graph_branches`
- `graph_revisions`
- `branch_promotions`

Principles:

- graph and branch history are backend-owned
- artifacts are immutable content-addressed records
- node UI/runtime config is persisted with nodes

## 5. Execution Model

1. User edits graph/config in UI.
2. Orchestrator persists mutation and marks downstream stale where needed.
3. `runGraph` computes dirty set and enqueues jobs.
4. Worker claims jobs and executes node kinds.
5. Worker writes artifacts + node execution status.
6. UI refreshes graph/artifacts and renders latest or stale-last-good outputs.

## 6. Node and Port Contracts

Node kinds are registry-driven (backend node registry JSON loaded by clients).

Port semantics are typed (for example: `point_set`, `interval_set`, `trajectory_set`, `surface`, `mesh`, `table`) and used for:

- compatibility checks
- graph wiring UX
- viewer layer interpretation

This keeps add-node menus and wiring behavior consistent across clients.

## 7. Viewer Architecture

Viewer nodes are graph nodes (not global panels):

- `plan_view_2d`
- `cesium_display_node` (current Cesium-backed 3D display; legacy alias path exists for `plan_view_3d`)

A viewer renders only artifacts reachable through its input edges.

### 7.1 Viewer Manifest Contract

Manifest is generated in orchestrator and consumed by clients as rendering truth.

Manifest includes:

- artifact identity/provenance
- layer hints (`display_contract`, `measure_candidates`, heatmap/contour metadata)
- render metadata for 2D and 3D layer types

This is the key anti-drift mechanism for multi-client support.

## 8. CRS Strategy

- workspace has a persisted project CRS
- acquisition/source nodes can use project CRS or explicit source CRS
- transform/model nodes preserve CRS metadata in artifacts
- UI uses a single CRS picker control with merged sources:
  - project CRS
  - common CRS
  - workspace-used CRS
  - cached selections
  - full EPSG search

Goal: no client-specific CRS hidden state; CRS intent is explicit in node/workspace config.

## 9. State and UX Semantics

Node execution states (current model in code) are shown on canvas and used in run decisions:

- idle/pending/running/succeeded/failed
- cache state tracks hit/miss/stale context
- lock state prevents recalculation where configured

Target UX behavior:

- preserve last-good outputs even when stale
- make stale vs current explicit
- allow node-scoped run and viewer-scoped preview

## 10. Multi-Client Readiness

To support future iOS/desktop clients without logic duplication:

- keep graph truth, node registry, viewer manifests, and CRS state in backend
- avoid hardcoding rendering rules in any one UI
- persist UI-affecting config on nodes/workspace (not local-only)
- treat web as one consumer of backend contracts, not the contract owner

## 11. Extension Points

- add new node kinds in `mine-eye-nodes` + registry
- expand semantic port taxonomy and compatibility rules
- strengthen branch/revision conflict semantics
- add richer artifact envelope/version metadata
- evolve from polling to event-driven updates (SSE/websocket)
- introduce next-gen 3D renderer node (threejs/WGSL path) while retaining Cesium display node as legacy/parallel option

## 12. Tradeoffs (Current)

- Fast iteration over strict formalization: some contract freeze items are still draft.
- Polling is still present in parts of UI; real-time push is partially implemented.
- Backward compatibility for old demo workspaces is intentionally de-prioritized in this phase.
