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

### 3.1 Node Implementation Structure

`crates/mine-eye-nodes/src/kinds/mod.rs` is the taxonomy entrypoint.

Domain modules own public node execution APIs:

| Module | Responsibility |
|---|---|
| `acquisition` | Ingest primitives (collar, survey, assay, observation, magnetic) |
| `data_model` | Table/dataset transforms |
| `spatial` | AOI contracts and CRS helpers |
| `surface` | Heatmap, iso, terrain, DEM, surface interpolation |
| `imagery_raster` | Imagery provider and tilebroker contracts |
| `trajectory` | Desurvey |
| `drillhole` | Merge and model |
| `resource_model` | Block grade modeling + resource summaries (k-d tree IDW, rayon parallel) |
| `magnetic_model` | Airborne magnetics cleanup, gridding, derivatives |
| `magnetic_depth` | Euler deconvolution: 3D source depth/susceptibility voxels from a magnetic grid |
| `scene_contract` | Scene layer composition |
| `visualization` | Viewer payload nodes |
| `node_group` | Wrapper node execution over an internal DAG with explicit input/output exposure |
| `stubs` | Alpha/placeholder node kinds |

`crates/mine-eye-nodes/src/kinds/runtime.rs` is internal helper/runtime logic and is not a domain API surface.

Rule:

- New node behavior should be implemented in the relevant domain module, not by adding new public `run_*` entrypoints in `runtime.rs`.

### 3.2 Shared Utility Modules

To eliminate cross-module duplication, two canonical utility modules live in `kinds/`:

- **`parse_util.rs`**: `parse_numeric_value()` (accepts Number or decimal string with comma/period),
  `lookup_numeric_ci()` (case-insensitive key lookup), `percentile_value()` (Nth percentile on a copy).
  Used by `resource_model`, `magnetic_depth`, `heatmap_raster_tile_cache`, and others.

- **`colour.rs`**: `interpolate_palette(name, t)` — named colour ramps (inferno, viridis, terrain,
  grayscale, default). Backend ramp counterpart to `apps/web/src/palettes.ts`.

Design constraint: ramp definitions in `colour.rs` and `palettes.ts` must stay in sync.
Any new ramp added on one side should be added to the other.

### 3.3 `resource_model` Architecture

The block grade model node (`resource_model.rs`) uses:

- **`GradeIndex`** — 3D k-d tree (kiddo `KdTree<f64, 3>`) with anisotropic coordinate normalisation
  for O(k log N) IDW search instead of O(N) linear scan.
- **`TriangleMeshIndex`** — 2D uniform cell grid over triangle XY bounding boxes for O(cells)
  mesh containment testing instead of O(T) per block.
- **`ConfidenceClass` enum** — replaces heap-allocated `String` per block; serde `lowercase` for
  JSON compatibility.
- **`CompositeAccumulator`** — struct replacing the previous 11-parameter flush closure.
- **Rayon parallelism** — `block_coords.into_par_iter()` over the estimated block grid.
- **Variogram unbiased sampling** — strides on the overall pair-sequence enumeration order (not on
  j-i gap) to avoid biasing toward large-separation pairs.

### 3.4 `magnetic_depth` Architecture (Euler Deconvolution)

The magnetic depth model node implements Reid et al. (1990) sliding-window Euler deconvolution.

Algorithm:

1. Parse `magnetic_grid.json` from the upstream `magnetic_model` output.
2. Compute spatial derivatives: ∂M/∂x, ∂M/∂y via central differences;
   ∂M/∂z approximated as `−FVD × flight_height × 0.5` (first vertical derivative from the second
   vertical derivative available in the magnetic grid). Falls back to Laplace estimation when FVD
   is absent.
3. For each sliding window: build the 4-unknown normal equations `[x₀, y₀, h, B]` with Tikhonov
   regularisation (λ = 1e-6 × max diagonal), solve via `gauss4()` (partial-pivot Gaussian elimination).
4. **Multi-N mode** (`structural_index_mode: "multi"`): tries structural indices N ∈ {0, 1, 2, 3}
   (contact plane, thin dyke, pipe/cylinder, sphere) and selects the best fit by normalised RMS residual.
   **Single-pass mode** (`structural_index_mode: "fixed"`, `structural_index: N`): solves for one
   user-specified N only.
5. Filter solutions by depth bounds, offset factor, and minimum confidence threshold.
6. Output schema: `block_grade_model_voxels.v1` with `display_pointer: "scene3d.block_voxels"`.
   Renders immediately in the existing 3D viewer block voxels renderer.

Key output attributes per voxel:

- `susceptibility_proxy` — `mean_anomaly_nt × depth_m^(N+1)`, normalised to p95 across all solutions
- `depth_m` — estimated source depth below flight level
- `structural_index` — best-fit N (0–3)
- `confidence` — `1 / (1 + norm_residual × 2)`
- `anomaly_nt` — mean |M| in the solution window

Voxel sizing: `max(depth_m × voxel_scale, resolution_m)` — deeper (more positionally uncertain)
sources are represented with proportionally larger blocks.

Execution: parallel rayon `into_par_iter()` over window centre grid positions.

Design constraints:

- avoid FFT dependency; the ∂M/∂z approximation is noted as exploratory-grade
- keep output schema consistent with `block_grade_model_voxels.v1` so no new renderer is needed

### 3.5 `node_group` Architecture

`node_group` is the first composition primitive for reducing canvas clutter without weakening contract discipline.

Model:

- the wrapper node persists a `group_definition` in node config
- the definition contains:
  - wrapper inputs/outputs
  - internal nodes
  - internal edges
  - explicit input bindings from wrapper inputs to internal node ports
  - explicit output bindings from internal node outputs to wrapper outputs
- wrapper-facing ports remain semantically typed and are used by the orchestrator during edge validation

Execution:

- the scheduler still plans the wrapper as a normal node
- the worker resolves upstream artifacts by destination port and passes them as `input_artifact_bindings` in the `JobEnvelope`
- the `node_group` executor topologically sorts the internal DAG and runs each internal node through the normal registry executor
- internal outputs are routed by explicit output index, then selected artifacts are re-exposed as the wrapper outputs

Design intent:

- keep composition data-driven and persistable so later plugin/group-template systems can extend it coherently
- avoid frontend-only grouping that would bypass the hardened middleware layer
- preserve observability by allowing intermediary internal outputs to be surfaced intentionally
- keep authoring explicit: the current web drill-in editor edits internal nodes, wrapper-input bindings, internal edges, exposed outputs, and internal layout as persisted definition data
- allow recursive group composition deliberately, with breadcrumb drill-in in the editor and a hard maximum depth enforced in middleware/runtime
- keep graph-open resilient by mounting the recursive group editor only when a concrete `node_group` is being edited, not as always-on canvas state

Current limit:

- nested groups are allowed up to a maximum depth of `3`; saves and execution both reject deeper compositions

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

### 4.1 Tile Cache

`dem_fetch` and `heatmap_raster_tile_cache` use a shared filesystem tile cache
(`data/tile-cache/`) keyed by content/provider/bbox hash.

Cache integrity rules:

- **Never cache error responses.** If an API returns an error payload (e.g. rate-limit JSON with no
  `"elevation"` key), the empty batch must not be written to cache. An all-null batch stored with a
  30-day TTL would poison every subsequent run for that bbox until manual eviction.
- Only cache batches that contain at least one real data value.
- OpenTopography AAI grid text is cached on successful parse only (not on HTTP/parse failure).
- DEM fetch default timeout is 60 s (`node_ui.timeout_ms`). Large AOIs from OpenTopography can take
  30+ s; the old 7 s default caused silent fallback for every large-area request.

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
- resolve input artifacts by destination port when wrapper/group execution requires it
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
- avoid duplicate "guessing" logic across web/iOS/desktop

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
- no client-owned "source of truth"
- all clients consume same graph/manifest/CRS contracts

### 8.1 Shared Palette Module (`apps/web/src/palettes.ts`)

Single source of truth for colour ramp definitions in the web client. Exports:

- `interpolatePalette(name, t)` → `[r, g, b]`
- `interpolatePaletteRgba(name, t, alpha)` → CSS `rgba(...)`
- `interpolatePaletteHex(name, t)` → `#rrggbb`
- `resolvePaletteName(name)` → normalized `PaletteName`

Named ramps: `mineeye`, `inferno`, `viridis`, `terrain`, `grayscale`.
Both the 2D heatmap (`Map2DPanel`) and 3D viewer (`Map3DThreePanel`) import from this module.

### 8.2 LRU Artifact Text Cache (`LruTextCache`)

Immutable artifact JSON text is cached in a bounded 200-entry Map-based LRU (`LruTextCache` class
inside `Map3DThreePanel.tsx`). Evicts the oldest entry on overflow. Replaces the previous unbounded
`Map` that grew without limit across scene loads.

## 9. Viewer Architecture

Viewer nodes are graph-native visualization nodes, not global singleton panels.

Current viewer node kinds:

- `plan_view_2d`
- `threejs_display_node` (active 3D scene workflow)
- `cesium_display_node` (supported legacy/parallel 3D path)
- `plan_view_3d` may still appear as compatibility alias in older graphs

A viewer renders only connected upstream artifacts by edge semantics.

### 9.1 Viewer Manifest as Stable Broker Contract

Manifest produced by orchestrator must contain sufficient layer metadata so each client can render consistently:

- artifact identity/provenance
- display contract and measure candidates
- heatmap/contour/terrain/traces hints
- renderer-neutral layer intent

This is the anti-drift mechanism for web/iOS/desktop parity.

### 9.2 Three.js Viewer Performance Architecture

The Three.js viewer (`Map3DThreePanel.tsx`) has three interlocking optimizations to prevent
unnecessary GPU resource churn:

**Canvas key stability**

The R3F `<Canvas>` is keyed only on `viewerNodeId` (the node identity), not on camera reset tokens.
Previously, `key={viewerNodeId + ":" + cameraResetToken}` caused React to destroy and recreate the
entire WebGL context — and all GPU textures — on every data load. Now the Canvas lifecycle is
decoupled from camera state.

**`DrapeTextureCache` (module-level, 20-entry LRU)**

```
DrapeTextureCache: url → THREE.Texture (GPU-resident)
  ├── persists across Canvas mount/unmount (tab switches)
  ├── persists across camera resets
  └── evicts LRU entry on overflow; eviction disposes GPU texture
```

Only a genuinely different imagery URL triggers a new network fetch and texture upload.

**`CameraAutoFit` component**

Imperative camera reset using `useThree()`. The component watches a `resetToken` prop and fires a
`useEffect` to reposition `camera` when the token changes — without touching the Canvas key and
without destroying any GPU state.

**Drape depth buffer** (`depthWrite={false}`)

The imagery drape `MeshBasicMaterial` does not write to the depth buffer. This is required for
correct semi-transparent rendering of the drape over underground geometry (e.g. block voxels from
`magnetic_depth_model`). With the default `depthWrite: true`, underground geometry fails the depth
test even at partial drape opacity and is discarded entirely.

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
- expand geophysics node family (gravity, EM, IP) following the `magnetic_depth` pattern:
  output `block_grade_model_voxels.v1` to reuse the existing block renderer without new viewer code

## 16. Non-Goals (Current Phase)

- strict backward compatibility for old demo/example workspaces
- perfect final contract freeze before iterative usability delivery
- embedding core business logic inside any single frontend

## 17. Guiding Rule

If a behavior must be consistent across web, iOS, and desktop, it belongs in contracts and services, not in a single UI implementation.
