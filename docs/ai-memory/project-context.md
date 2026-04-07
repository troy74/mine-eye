# Mine-Eye Project Context

## Product
- `mine-eye` is a graph-native exploration workflow platform for geoscience and mining workflows.
- Backend is source of truth for graph state, execution state, contracts, CRS behavior, and artifacts.
- Frontend (web) is primarily a renderer/editor of backend-owned contracts and manifests.

## Architecture Rules
- Keep logic middleware-first: orchestrator + worker own behavior, clients consume stable contracts.
- Avoid client-only inference that cannot be replayed by other clients.
- Respect typed semantic ports for deterministic wiring.
- Preserve reproducibility: node config and viewer settings are part of persisted graph state.

## Runtime Components
- Orchestrator: graph API, branch/revision workflows, viewer manifests, CRS APIs, AI broker endpoints.
- Worker: executes node kinds, writes artifacts, updates execution/cache state.
- Nodes crate: domain-specific node implementations for acquisition, spatial, surface, imagery, etc.

## Identity and Access
- Web app uses Clerk sign-in; authenticated state gates the main app shell.
- Orchestrator verifies Clerk tokens and derives request `AuthContext` (`organization_id`, `user_id`, role).
- Workspace and graph access are organization-scoped; personal accounts map to `personal:{user_id}` org ids.
- Persistence includes users, organizations, and organization memberships; workspaces are organization-owned.

## AI Assistant Scope
- Assist with geoscience workflow planning and graph authoring.
- Use backend tools for inspecting/updating graph definitions.
- Prefer explicit, auditable changes over hidden magic.
- Recommend validation steps after any mutation.
