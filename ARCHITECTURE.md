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

Node implementation structure (current):

- `crates/mine-eye-nodes/src/kinds/mod.rs` is the taxonomy entrypoint.
- Domain modules own public node execution APIs:
  - `acquisition`
  - `data_model`
  - `spatial`
  - `surface`
  - `imagery_raster`
  - `trajectory`
  - `drillhole`
  - `resource_model`
  - `scene_contract`
  - `visualization`
  - `stubs`
- `crates/mine-eye-nodes/src/kinds/runtime.rs` is internal helper/runtime logic and is not a domain API surface.

Rule:

- New node behavior should be implemented in the relevant domain module, not by adding new public `run_*` entrypoints in `runtime.rs`.

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

### 5.3 Componentized Middleware Brokers (Design Pointer)

The preferred architecture is a brokered middleware chain that keeps frontend logic small:

- **Graph broker**: graph CRUD, branch/revision state, run intent.
- **Execution broker**: dirty propagation, queue scheduling, lock-aware execution ordering.
- **Scene broker**: viewer manifest + `scene_contract` layer composition from connected artifacts.
- **Spatial broker**: AOI contracts, CRS normalization, bounds provenance, and project CRS policy.
- **Terrain/imagery broker**: DEM provider/fitting, tilebroker/provider fallback ladders, quality flags.
- **Artifact broker**: immutable artifact lineage/content hashes and schema/variant metadata.
- **AI assistant broker** (`services/orchestrator/src/ai_chat.rs`): tool-orchestrated conversational planning/mutation/execution over graph state, artifacts, and uploaded chat files.

Efficiency constraints:

- push expensive inference/provider logic into middleware and emit compact render-ready contracts
- make cache keys content+lineage based (never timestamp-only)
- keep UI overrides scoped to explicitly permitted `ui_capabilities` in contracts
- avoid duplicate “guessing” logic across web/iOS/desktop

### 5.4 AI Assistant Architecture (Current)

The in-app assistant is implemented as a backend-run tool harness:

- model/provider: OpenRouter with `openai/gpt-5.4` default
- tool loop: bounded iterative tool-calling with structured tool events
- mutation control: explicit plan-only vs apply mode
- branch awareness: branch id is passed for graph mutations

Core tool groups:

- graph audit/introspection
- artifact profiling/extraction
- upload-file inspection/mapping
- graph mutation (add/patch/wire/unwire)
- execution control (`run_node`, `run_graph`)

The assistant should discover context through tools before asking users for low-level identifiers or mapping keys.

## 6. AI Prompt, Memory, and Playbooks

Prompting is layered:

1. Base persona/behavior system prompt (concise, operator-first, professional exploration tone).
2. Dynamic session context (user/mutation mode/chat transcript).
3. Memory and playbook files loaded from repository docs.

Current memory/playbook sources include:

- `docs/ai-memory/*`
- `docs/ai-skills/upload-combo-playbooks.md`
- `docs/ai-skills/node-workflow-fragments.md`
- `docs/node-operating-matrix.md`
- top-level `README.md` and `ARCHITECTURE.md`

These files are treated as durable project memory + operational skill fragments.

## 7. Chat Interaction Contract

Assistant response contract supports two parts:

- `<plain>...</plain>`: short plain-meaning reply for user-facing guidance
- `<system>...</system>`: detailed diagnostic/tool narrative (rendered as expandable UI detail)

UI behavior:

- tool events are compact and expandable
- chat can be reset to a fresh thread via `New Chat`
- attachments are upload-aware, and tabular content can be profiled/mapped by assistant tools

## 8. Presentation Layer (Web First, Multi-Client Ready)

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

## 9. Viewer Architecture

Viewer nodes are graph-native visualization nodes, not global singleton panels.

Current viewer node kinds:

- `plan_view_2d`
- `threejs_display_node` (active 3D scene workflow)
- `cesium_display_node` (supported legacy/parallel 3D path)
- `plan_view_3d` may still appear as compatibility alias in older graphs

A viewer renders only connected upstream artifacts by edge semantics.

### 7.1 Viewer Manifest as Stable Broker Contract

Manifest produced by orchestrator must contain sufficient layer metadata so each client can render consistently:

- artifact identity/provenance
- display contract and measure candidates
- heatmap/contour/terrain/traces hints
- renderer-neutral layer intent

This is the anti-drift mechanism for web/iOS/desktop parity.

## 10. CRS Architecture

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

## 11. Execution and State Semantics

Current execution lifecycle in code:

- `idle`, `pending`, `running`, `succeeded`, `failed`
- cache states (hit/miss/stale) and lock state influence run behavior

Expected UX semantics:

- stale-last-good outputs should remain viewable
- running and failed states should be explicit and actionable
- node-level run and graph-level run remain first-class controls

## 12. Security and Identity (Current)

### 12.1 Authentication

Implemented:

- orchestrator runs an auth middleware (`require_auth`) over API routes.
- Clerk session/JWT tokens are accepted via `Authorization: Bearer ...` or `__session` cookie.
- RS256 verification uses Clerk JWKS with cache/refresh behavior.
- authorized-party checks use `CLERK_AUTHORIZED_PARTIES` with localhost defaults.

### 12.2 Authorization and Tenant Isolation

Implemented:

- every request gets an `AuthContext` (`organization_id`, `user_id`, organization role).
- access guards enforce that:
  - workspace belongs to caller organization
  - graph belongs to caller organization
- personal users are normalized to deterministic organization ids (`personal:{user_id}`).
- graph/workspace mutations and run endpoints operate under organization-scoped checks.

### 12.3 Identity Persistence Model

Implemented:

- relational identity tables:
  - `users`
  - `organizations`
  - `organization_memberships`
- `workspaces.organization_id` is required and FK-backed.
- graph metadata includes `organization_id` and `created_by_user_id`.
- store bootstrap ensures user/org/membership rows exist for authenticated callers.

### 12.4 Auditability (Current + Next)

Current:

- graph revisions preserve actor identity (`created_by`) for branch/mutation history.
- AI mutation confirmations are linked to authenticated users.

Next:

- expanded immutable audit event streams for promotion/run/AI policy actions.

## 13. Optional Additional Encryption (Placeholder)

Baseline:

- TLS for transport
- database credentials and secrets managed via environment

Planned optional controls:

- envelope encryption for selected artifact classes
- encrypted-at-rest columns for sensitive configuration metadata
- key management abstraction with rotation support

## 14. AI Credits and Organizational Allocation (Placeholder)

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

## 15. Scalability and Evolution

Near-term evolution priorities:

- move from mixed polling to more reactive event delivery where practical
- strengthen port compatibility matrix and semantic contract validation
- continue registry-driven node metadata and backend-owned viewer contracts
- keep `kinds` domain modules small and focused; avoid re-accumulating a single monolithic node implementation file
- continue hardening the Three.js path through scene contracts while preserving legacy Cesium compatibility where required

## 16. Non-Goals (Current Phase)

- strict backward compatibility for old demo/example workspaces
- perfect final contract freeze before iterative usability delivery
- embedding core business logic inside any single frontend

## 17. Guiding Rule

If a behavior must be consistent across web, iOS, and desktop, it belongs in contracts and services, not in a single UI implementation.
