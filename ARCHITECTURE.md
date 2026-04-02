# Architecture

## 1. Architecture Goals

`mine-eye` is designed as a backend-first graph execution platform for exploration workflows.

Primary goals:

- keep domain behavior deterministic and auditable
- support multiple clients (web now, iOS field app later, desktop possible) without contract drift
- isolate business rules from UI-specific implementation details
- preserve reproducibility via persisted graph/node/viewer configuration and immutable artifacts

## 2. Layered System Model

The platform is intentionally componentized into four layers:

1. **Domain/Contract Layer**
2. **Data/Persistence Layer**
3. **Application Services Layer**
4. **Presentation Layer**

```text
Presentation Clients (Web, future iOS/Desktop)
        │
        ▼
Application Services (Orchestrator API + Worker Execution)
        │
        ▼
Data/Persistence (Postgres + Artifact Store)
        │
        ▼
Domain Contracts (shared Rust types/semantics/registry)
```

The key rule: clients do not define truth; they consume and mutate truth through service contracts.

## 3. Domain and Contract Layer

Current modules:

- `crates/mine-eye-types`
- `crates/mine-eye-graph`
- `crates/mine-eye-scheduler`
- `crates/mine-eye-nodes` (executor implementations and node-kind behavior)

Responsibilities:

- canonical graph/node/edge/job/artifact types
- typed semantic ports and compatibility behavior
- node execution semantics and envelope behavior
- shared contracts that every client depends on indirectly via API

Design constraints:

- avoid “web-only” contract interpretation
- promote node registry and viewer manifest contracts to first-class backend outputs

## 4. Data and Persistence Layer

Current module:

- `crates/mine-eye-store`

Backing systems:

- Postgres for graph/workspace/branch/revision/job metadata
- filesystem artifact root for produced artifacts

Core persisted entities:

- workspaces (includes project CRS)
- graphs, nodes, edges
- job queue and execution status
- node artifacts (content and keys)
- branch/revision/promotion records

Data-layer principles:

- graph revision history is append-oriented and backend-owned
- artifacts are immutable records keyed by content/lineage semantics
- UI state needed for deterministic behavior is persisted in node/workspace config

## 5. Application Services Layer

### 5.1 Orchestrator (`services/orchestrator`)

Responsibilities:

- graph CRUD and wiring validation
- branch/revision/promotion orchestration
- dirty-node run planning and job enqueueing
- viewer manifest generation (`/graphs/{graph_id}/viewers/{viewer_node_id}/manifest`)
- workspace CRS management (`PATCH /workspaces/{ws_id}/project-crs`)
- EPSG search proxy and normalization (`/epsg/search`)

Role:

- middleware source of truth for graph and presentation contracts

### 5.2 Worker (`services/worker`)

Responsibilities:

- claim queued jobs
- resolve latest input artifacts
- execute node kinds through `NodeExecutorRegistry`
- persist artifacts, hashes, execution status, and errors

Role:

- deterministic runtime executor isolated from UI concerns

## 6. Presentation Layer (Web First, Multi-Client Ready)

Current client:

- `apps/web` (Vite + React)

Responsibilities:

- graph editing and inspector UX
- node-scoped previews (2D/3D/generic)
- running nodes/pipeline and displaying execution state
- rendering viewer layers from backend contracts

Future clients:

- iOS field app (offline-aware review + limited edit/run workflows)
- desktop app for heavier visualization/analysis workflows

Presentation-layer rules:

- no hidden client-only transforms for core business logic
- no client-owned “source of truth”
- all clients consume same graph/manifest/CRS contracts

## 7. Viewer Architecture

Viewer nodes are graph-native visualization nodes, not global singleton panels.

Current viewer node kinds:

- `plan_view_2d`
- `cesium_display_node` (active Cesium-backed 3D display node)
- `plan_view_3d` remains as a compatibility alias path

A viewer renders only connected upstream artifacts by edge semantics.

### 7.1 Viewer Manifest as Stable Broker Contract

Manifest produced by orchestrator must contain sufficient layer metadata so each client can render consistently:

- artifact identity/provenance
- display contract and measure candidates
- heatmap/contour/terrain/traces hints
- renderer-neutral layer intent

This is the anti-drift mechanism for web/iOS/desktop parity.

## 8. CRS Architecture

CRS behavior is backend-backed, not UI-local:

- workspace stores project CRS
- node configs declare source/output CRS intent
- artifacts preserve CRS metadata

UI pattern:

- one unified CRS control with:
  - project CRS
  - common shortlist
  - workspace-used CRS
  - cached picks
  - full EPSG search

This control model should be shared behavior across clients even if UI widgets differ.

## 9. Execution and State Semantics

Current execution lifecycle in code:

- `idle`, `pending`, `running`, `succeeded`, `failed`
- cache states (hit/miss/stale) and lock state influence run behavior

Expected UX semantics:

- stale-last-good outputs should remain viewable
- running and failed states should be explicit and actionable
- node-level run and graph-level run remain first-class controls

## 10. Security and Identity Roadmap (Placeholder Contracts)

The following are planned architecture components and should be treated as future backend services/contracts:

### 10.1 Authentication

- token/session-based identity for all mutating API endpoints
- standardized request identity context (`user_id`, `org_id`, role/scopes)

### 10.2 Authorization

- policy checks at service boundary:
  - workspace/project access
  - branch promotion rights
  - run/execute rights
  - viewer/artifact read rights

### 10.3 Auditability

- immutable audit events for:
  - graph mutations
  - promotions
  - run requests
  - AI-assisted actions

## 11. Optional Additional Encryption (Placeholder)

Baseline:

- TLS for transport
- database credentials and secrets managed via environment

Planned optional controls:

- envelope encryption for selected artifact classes
- encrypted-at-rest columns for sensitive configuration metadata
- key management abstraction with rotation support

## 12. AI Credits and Organizational Allocation (Placeholder)

Planned model:

- org-level AI budget/credits
- sub-allocation buckets (team/project/workspace/user)
- policy enforcement before AI action execution
- usage metering events (tokens/cost/feature/tool scope)
- admin visibility and guardrails (hard/soft limits, alerts)

Service split recommendation:

- keep AI budget ledger and policy evaluation in backend services
- expose read/write APIs for allocation and usage views
- keep clients presentation-only for budget visuals and approvals

## 13. Scalability and Evolution

Near-term evolution priorities:

- move from mixed polling to more reactive event delivery where practical
- strengthen port compatibility matrix and semantic contract validation
- continue registry-driven node metadata and backend-owned viewer contracts
- introduce next-gen 3D renderer path (threejs/WGSL) as a new node/presentation contract without breaking current Cesium path

## 14. Non-Goals (Current Phase)

- strict backward compatibility for old demo/example workspaces
- perfect final contract freeze before iterative usability delivery
- embedding core business logic inside any single frontend

## 15. Guiding Rule

If a behavior must be consistent across web, iOS, and desktop, it belongs in contracts and services, not in a single UI implementation.
