# Activities Log (AI Context)

## 2026-04-17
- Added `sample-data/kimberlina_borehole_dataset`:
  - compact subset of the public `kim_ready.csv` borehole fixture used in the GemPy Kimberlina example.
  - reproducible generator script using Python stdlib only.
  - normalized `collar.csv`, `lithology_intervals.csv`, and `interface_points.csv` derivatives for future Rust middleware tests.
- Documented the next middleware direction:
  - treat borehole stratigraphy/contact handling as a distinct path from assay/grade modelling.
  - prepare for Rust-native interface extraction and formation/surface modelling nodes.

## 2026-04-08
- Added `block_grade_model` node (model domain) with three outputs:
  - voxel/mesh-ready block model with grade + cutoff flags.
  - block-center grade points for overlays/downstream transforms.
  - resource report JSON (tonnage/contained/grade stats and histogram).
- Added topography clipping (block-center vs terrain surface), SG constant support, configurable
  block sizing, grade cutoff/clamps, and nearest/IDW estimation controls.
- Extended Three.js viewer for `scene3d.block_voxels`:
  - instanced voxel rendering for performance.
  - measure-based color ramps and transparent below-cutoff display.
  - dynamic source-layer styling alongside existing trajectory/segment/point layers.
- Established Clerk-backed authentication and organization isolation across web + orchestrator:
  - web shell now requires sign-in (`ClerkProvider`, signed-out screen, user menu).
  - orchestrator verifies Clerk RS256 session tokens with JWKS caching and authorized-party checks.
  - request-scoped `AuthContext` is enforced on API routes with workspace/graph organization checks.
  - added identity persistence migration (`users`, `organizations`, `organization_memberships`, `workspaces.organization_id`).
- Improved viewer/workspace UX in web:
  - refreshed sidebar/inspector/3D panel interactions.
  - tightened project-local storage scoping per authenticated org/user context.

## 2026-04-07
- AOI map initialization bug investigated and patched:
  - fixed artifact fetch path to use `api(artifact.url)` in web app.
  - added AOI bbox extraction/reprojection helper module.
  - added AOI editor prop-sync so late-arriving bbox updates recenter map.
- Added `.env.dev` to `.gitignore`.
- Started AI chat implementation:
  - backend AI broker endpoint (OpenRouter integration) with tool-calling loop.
  - initial graph tools for inspect/read/top-tail/patch/wire/unwire.
  - web chat wired to backend endpoint and tool summary output.
  - added `apply_mutations` mode:
    - default safe `plan-only` (dry-run for mutations)
    - optional apply mode for real config/wiring changes.
  - AI-applied mutations now create graph revisions with explicit AI events.
  - AI chat request now supports branch targeting (`branch_id`) for branch-safe edits.
  - added artifact path traversal guard in top/tail tool.
  - added registry-backed `wire_nodes` validation (kind/port existence + semantic match).
  - added artifact utility tools:
    - `json_path_extract` for JSON contract probing
    - `csv_profile` for delimiter/header/type sampling.
    - `suggest_measure_fields` for ranked likely assay/measure columns.
    - `list_node_artifacts` and `read_registry_kind` for smarter candidate discovery.
  - chat UI now renders structured tool action cards for planned/applied actions.
  - tuned system prompt for natural teammate tone and to avoid repetitive context/file-list dumping.
  - added operating matrix: `docs/node-operating-matrix.md`.
  - marked Cesium nodes as historic/archive in node registry labels/roles/submenu.
  - chat now forwards attachment filenames/mime metadata into AI request context.
  - added `add_node` tool so AI can build graphs from empty state (in plan/apply modes).

## Next Updates
- Record major workflow and node-registry changes here.
- Add geology area notes (deposit style, alteration, controls) as they are confirmed.
