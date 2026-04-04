# Scene3D Contract Stubs

These are draft middleware contract stubs for drift-resistant 2D/3D behavior.

Status:
- `v1` draft
- schema stubs only (non-enforcing until validators are wired)

Contracts:
- `spatial.aoi.v1.schema.json`
- `terrain.surface_grid.v1.schema.json`
- `terrain.material_stack.v1.schema.json`
- `scene3d.imagery_drape.v1.schema.json`
- `scene3d.tilebroker_request.v1.schema.json`
- `scene3d.tilebroker_response.v1.schema.json`
- `scene3d.layer_stack.v1.schema.json`
- `project.settings.v1.schema.json`

Notes:
- Keep frontend consumers reading these contracts instead of provider-specific fields.
- UI should only override fields explicitly exposed via `ui_capabilities` in `scene3d.layer_stack.v1`.
