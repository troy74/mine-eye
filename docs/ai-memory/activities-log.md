# Activities Log (AI Context)

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
