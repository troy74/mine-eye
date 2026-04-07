# Plugin / Analytic Implementation Guide

This is the repeatable path for adding a new analytical capability ("plugin" in product terms, usually one or more node kinds plus UI and AI support).

Use this as a delivery checklist from concept to runnable workflow.

## 1. Define the Capability First

Write a short spec before coding:

- Name: analytic/plugin name
- Objective: what exploration question it answers
- Inputs: semantic port types + required fields
- Outputs: semantic port types + contract/schema ids
- Runtime assumptions: CRS, units, resolution, domain constraints
- Failure modes: validation errors and fallback behavior
- "Definition of done": what must render/run in web UI

Keep this in PR description or in `docs/` while building.

## 2. Data and Contract Design

Start in contracts/types before executors/UI:

1. Define/confirm output schema (for example `spatial.*`, `scene3d.*`, `table.*`).
2. Ensure artifact payload is deterministic and self-describing:
   - schema id/version
   - CRS/units metadata
   - provenance info
3. Define minimal required input fields and explicit validation messages.

Rule: If two clients must interpret it the same way, it belongs in contract metadata, not client logic.

## 3. Node and Graph Prep

Add or update node definition in orchestrator registry:

- `services/orchestrator/src/node-registry.json`
- category, label, role
- input/output ports with correct semantic types
- UI metadata fields that should be configurable

Then align runtime:

- node kind wiring in `crates/mine-eye-nodes/src/kinds/mod.rs`
- implementation module in correct domain folder
- avoid putting public analytic logic into `runtime.rs`

If ports/semantics are wrong, stop and fix registry first.

## 4. Rust Implementation Path

Typical files:

- `crates/mine-eye-nodes/src/kinds/<domain>.rs` (executor logic)
- `crates/mine-eye-nodes/src/executor.rs` (dispatch registration)
- optional helper module in `kinds/` for shared parsing/validation

Implementation checklist:

1. Parse and validate input artifacts/payloads early.
2. Return clear `invalid config` messages with actionable detail.
3. Preserve lineage and stable artifact naming.
4. Emit typed artifact outputs (include schema metadata).
5. Keep algorithm deterministic for same inputs.

Run:

- `cargo check`
- targeted run through orchestrator + worker to confirm outputs

## 5. Orchestrator Integration

Update orchestration surfaces as needed:

- run behavior (`/graphs/{id}/run` path already available)
- viewer manifest support if output should render in 2D/3D
- AI chat tools if the capability needs discovery/patch/wire/run automation

For AI tool additions in `services/orchestrator/src/ai_chat.rs`:

1. Add tool spec in `openai_tools_spec()`.
2. Add handler branch in `execute_tool(...)`.
3. Implement tool function with compact, structured output.
4. Update summarizers (`summarize_tool_payload`, `tool_output_preview`).

## 6. Web UI Integration

Typical areas:

- `apps/web/src/nodeRegistry.ts` and inspector/editor surfaces
- map/3D panels if renderer-specific handling is needed
- `graphApi.ts` for new endpoint payload typing
- chat panel UX if tool output/response format changes

UI checklist:

1. Expose only key config fields first.
2. Persist config in node params (no hidden client-only state).
3. Make run/error states visible and understandable.
4. Keep heavy diagnostics collapsible (not chat-spam).

Build:

- `npm --prefix apps/web run build`

## 7. Prompt and Skill Updates

When a new analytic lands, update AI behavior deliberately:

1. Add concise node/workflow fragment to:
   - `docs/ai-skills/node-workflow-fragments.md`
2. Add common file-combination playbook if relevant:
   - `docs/ai-skills/upload-combo-playbooks.md`
3. Update memory context docs if workflow conventions changed:
   - `docs/ai-memory/*`
4. Adjust system prompt/tool guidance in `ai_chat.rs` only where needed.

Prompt rule: prefer "discover -> patch -> validate -> run" over asking users for IDs/ports manually.

## 8. Execution and Validation Scenario

Before shipping, run one realistic end-to-end graph:

1. Ingest or attach representative data.
2. Configure/mutate nodes through intended UX (including chat if applicable).
3. Run ingest -> transform/model -> visualisation chain.
4. Verify artifact schemas and viewer behavior.
5. Verify failure messaging by intentionally breaking one required input.

Capture one "happy path" and one "failure path" in PR notes.

## 9. Documentation and Release Hygiene

Always update:

- `README.md` for user-facing capability additions
- `ARCHITECTURE.md` for component/contract changes
- node inventory/operating matrix docs if node semantics changed

If AI behavior changed, note:

- new tools
- prompt changes
- any response format changes (`<plain>` / `<system>`)

## 10. Quick Delivery Checklist

- [ ] Contract/schema defined and versioned
- [ ] Node registry entry updated (ports/semantics/category)
- [ ] Rust executor implemented and wired
- [ ] Orchestrator APIs/tools updated
- [ ] Web inspector/rendering updated
- [ ] AI skills/prompts updated
- [ ] End-to-end run verified
- [ ] Docs updated (`README`, `ARCHITECTURE`, relevant `docs/*`)

