# GemPy-Inspired Underground Modelling Roadmap

This document is the implementation checklist for bringing GemPy-style underground modelling into `mine-eye` in a way that fits our architecture.

Companion spec:
[GemPy-Inspired Underground Modelling Milestone 1 Spec](/Users/troytravlos/mine-eye/docs/gempy-underground-m1-spec.md)

The goal is not to clone GemPy internals one-for-one. The goal is to support the same class of workflows:

- structural geology inputs
- stratigraphic ordering
- contact points and orientations
- model domain definition
- interpolation into surfaces and volumes
- extraction of horizons and enclosed solids
- visualization driven by upstream geological metadata

We are explicitly not implementing the TensorFlow backend yet. Phase 1 should be deterministic, CPU-friendly, Rust-first middleware.

## Scope

This roadmap covers:

- contracts and data model
- node decomposition
- sequencing and milestone order
- what should be bundled together vs split into individual nodes
- a checklist we can track against

This roadmap does not yet cover:

- probabilistic modelling
- uncertainty propagation
- TensorFlow/XLA execution
- geostatistical optimization or automatic parameter fitting

## Design Principles

1. Rust middleware owns geological truth.
   Viewer state should not define formation semantics, order, fault relations, or palette truth.

2. Contracts come before interpolation.
   We should be able to inspect every modelling input as a graph artifact before compute begins.

3. Surfaces and volumes are both first-class outputs.
   The architecture should not stop at pretty meshes.

4. Structural semantics must be explicit.
   Series, formations, faults, unconformities, topography masks, and inferred-vs-observed provenance all need contract-level representation.

5. Node outputs should be explainable.
   Every phase should be debuggable from artifacts and node boundaries, not hidden inside a monolithic modelling node.

## Target Capability

At the end of the first serious implementation pass, we want to be able to:

1. Ingest collars, trajectories, lithology intervals, orientations, optional faults, and topography.
2. Build a structural frame with ordered formations and group relations.
3. Derive contact/interface points and orientation constraints.
4. Define a 3D modelling domain and resolution strategy.
5. Compute a conformable stratigraphic model over that domain.
6. Extract horizon meshes and labelled lithology/block volumes.
7. Close or clip those surfaces into enclosed formation solids against topography and domain bounds.
8. Render the result with formation-aware discrete palettes and upstream metadata.

## Node Strategy

Some work belongs in grouped workstreams, but most runtime execution should remain modular. The rule of thumb:

- Use individual nodes when the output is useful on its own, inspectable, or reusable by multiple downstream paths.
- Group work into a milestone when several nodes only make sense together as one capability slice.

### Node Groups

These are the major groups that should ship together.

#### Group A: Structural Inputs

These nodes normalize raw geology inputs into modelling primitives.

- `orientation_ingest`
- `fault_ingest`
- `formation_catalog_build`
- `stratigraphic_order_define`

These belong together because they define the semantic framework the rest of the model depends on.

#### Group B: Constraint Extraction

These nodes derive the actual interpolation constraints.

- `formation_contact_extract`
- `formation_orientation_extract`
- `fault_surface_extract`
- `constraint_merge`

These belong together because they transform raw geology records into contact/orientation/fault constraints for interpolation.

#### Group C: Domain Definition

These nodes define where and how the model is solved.

- `model_domain_define`
- `topography_clip_define`
- `resolution_strategy_define`

These belong together because interpolation, meshing, and clipping all depend on the same domain contract.

#### Group D: Structural Frame Assembly

These nodes turn semantics plus constraints into an executable modelling dataset.

- `structural_frame_builder`
- `structural_frame_validate`

These belong together because we need one canonical structural-frame artifact plus a diagnostics pass before compute.

#### Group E: Core Modelling

These nodes compute the actual geological model.

- `stratigraphic_interpolator`
- `fault_displacement_model` later
- `lith_block_model_build`
- `scalar_field_pack`

This group should begin simple. Only the conformable stratigraphy slice is phase 1.

#### Group F: Extraction and Solid Generation

These nodes turn model fields into useful geometry.

- `horizon_mesh_extract`
- `formation_volume_extract`
- `topography_constrained_closure`
- `volume_mesh_pack`

These belong together because users need both surfaces and enclosed solids.

#### Group G: Viewer Semantics

These are not geological compute nodes. They package display-ready semantics from upstream truth.

- `formation_palette_assign`
- `scene3d_geology_layer_pack`

These belong together because they keep the viewer thin and metadata-driven.

## Individual Nodes We Should Add

The list below is the practical candidate set. Some are phase 1, some are later.

### Contracts / Inputs

- `orientation_ingest`
- `fault_ingest`
- `formation_catalog_build`
- `stratigraphic_order_define`

### Constraint Prep

- `formation_contact_extract`
- `formation_orientation_extract`
- `fault_surface_extract`
- `constraint_merge`

### Domain / Framing

- `model_domain_define`
- `resolution_strategy_define`
- `topography_clip_define`
- `structural_frame_builder`
- `structural_frame_validate`

### Compute

- `stratigraphic_interpolator`
- `lith_block_model_build`
- `scalar_field_pack`

### Geometry Extraction

- `horizon_mesh_extract`
- `formation_volume_extract`
- `topography_constrained_closure`
- `volume_mesh_pack`

### Viewer / Packaging

- `formation_palette_assign`
- `scene3d_geology_layer_pack`

## Contracts We Need

These should be introduced before or alongside the first compute milestone.

### Must-Have Phase 1 Contracts

- `geology.formation_catalog.v1`
  Purpose: canonical list of formations, ids, names, order, optional aliases.

- `geology.stratigraphic_order.v1`
  Purpose: explicit ordering of formations within a stratigraphic group/series.

- `geology.formation_orientations.v1`
  Purpose: dip/azimuth or pole-vector observations, source, confidence, inferred flag.

- `geology.model_domain.v1`
  Purpose: CRS, extent, z-range, resolution strategy, clipping rules, topography binding.

- `geology.structural_frame.v1`
  Purpose: groups, formations, group relations, faults, chronology, active flags.

- `geology.interpolation_constraints.v1`
  Purpose: merged contacts, orientations, fault constraints, provenance, diagnostics.

- `geology.scalar_field.v1`
  Purpose: regular-grid or octree scalar field output for one structural group or formation boundary.

- `geology.lith_block_model.v1`
  Purpose: labelled 3D occupancy / block model output.

### Strongly Recommended Early Phase 2 Contracts

- `geology.fault_set.v1`
- `geology.horizon_mesh_set.v1`
- `geology.formation_volume_mesh_set.v1`
- `geology.palette_binding.v1`
- `geology.model_diagnostics.v1`

## What Goes Together

This is the most important implementation guidance.

### Bundle 1: Semantic Foundation

Implement together:

- `geology.formation_catalog.v1`
- `geology.stratigraphic_order.v1`
- `formation_catalog_build`
- `stratigraphic_order_define`

Reason:
Without explicit formation identity and order, every later node becomes brittle and viewer-driven.

### Bundle 2: Constraint Foundation

Implement together:

- `geology.formation_orientations.v1`
- `geology.interpolation_constraints.v1`
- `orientation_ingest`
- `formation_contact_extract`
- `formation_orientation_extract`
- `constraint_merge`

Reason:
This is the minimum viable modelling input set analogous to GemPy surface points + orientations.

### Bundle 3: Domain and Structural Frame

Implement together:

- `geology.model_domain.v1`
- `geology.structural_frame.v1`
- `model_domain_define`
- `topography_clip_define`
- `structural_frame_builder`
- `structural_frame_validate`

Reason:
Interpolation should not run until both the domain and the structural frame are explicit and validated.

### Bundle 4: First Compute Slice

Implement together:

- `geology.scalar_field.v1`
- `geology.lith_block_model.v1`
- `stratigraphic_interpolator`
- `lith_block_model_build`

Reason:
Surface-only output is too limited. The first compute slice should already produce both boundary information and block occupancy.

### Bundle 5: Extraction and Enclosed Solids

Implement together:

- `horizon_mesh_extract`
- `formation_volume_extract`
- `topography_constrained_closure`
- `geology.horizon_mesh_set.v1`
- `geology.formation_volume_mesh_set.v1`

Reason:
This is where the workflow starts to feel like a proper underground modeller rather than a horizon display tool.

### Bundle 6: Palette and Viewer Semantics

Implement together:

- `geology.palette_binding.v1`
- `formation_palette_assign`
- `scene3d_geology_layer_pack`

Reason:
We want colors and visual grouping to come from geology metadata upstream, not ad hoc viewer logic.

## Suggested Milestones

## Milestone 1: Modelling Inputs Are Formalized

Deliverable:
We can ingest all core data and inspect contracts for formations, order, orientations, contacts, and domain.

Checklist:

- [ ] Add `geology.formation_catalog.v1`
- [ ] Add `geology.stratigraphic_order.v1`
- [ ] Add `geology.formation_orientations.v1`
- [ ] Add `geology.model_domain.v1`
- [ ] Implement `formation_catalog_build`
- [ ] Implement `stratigraphic_order_define`
- [ ] Implement `orientation_ingest`
- [ ] Implement `model_domain_define`
- [ ] Add node registry entries and AI/node inspector support
- [ ] Add fixture datasets covering lithology + orientation + topography inputs

## Milestone 2: Structural Constraints Are Computable

Deliverable:
We can derive contact and orientation constraints from drillholes/surfaces and package them into one modelling input artifact.

Checklist:

- [ ] Implement `formation_contact_extract`
- [ ] Implement `formation_orientation_extract`
- [ ] Add `geology.interpolation_constraints.v1`
- [ ] Implement `constraint_merge`
- [ ] Include provenance per constraint: observed, inferred, derived-from-surface, derived-from-interval
- [ ] Add diagnostics for missing orientations and ambiguous formation ids

## Milestone 3: Structural Frame Exists

Deliverable:
We can construct a structural frame analogous to GemPy’s series/elements/group relations.

Checklist:

- [ ] Add `geology.structural_frame.v1`
- [ ] Implement `structural_frame_builder`
- [ ] Implement `structural_frame_validate`
- [ ] Support ordered formations within a group
- [ ] Support group relation types: conformable, unconformity, faulted
- [ ] Mark active/inactive elements
- [ ] Emit diagnostics for ordering gaps and orphan formations

## Milestone 4: First Deterministic Stratigraphic Model

Deliverable:
We can compute a conformable stratigraphic model over a domain without TensorFlow.

Checklist:

- [ ] Add `geology.scalar_field.v1`
- [ ] Add `geology.lith_block_model.v1`
- [ ] Implement `stratigraphic_interpolator`
- [ ] Start with conformable stratigraphy only
- [ ] Support regular grid first
- [ ] Design API so octree refinement can come later
- [ ] Emit scalar field(s) per structural group or boundary
- [ ] Emit lithology/block assignment artifact
- [ ] Add basic compute diagnostics: input count, unresolved cells, clipping summary

## Milestone 5: Horizons and Volumes

Deliverable:
We can extract watertight horizons and enclosed formation solids.

Checklist:

- [ ] Implement `horizon_mesh_extract`
- [ ] Implement `formation_volume_extract`
- [ ] Implement `topography_constrained_closure`
- [ ] Add `geology.horizon_mesh_set.v1`
- [ ] Add `geology.formation_volume_mesh_set.v1`
- [ ] Clip volumes against topography and model bbox
- [ ] Preserve touching relationships between stacked volumes
- [ ] Support export-friendly mesh packaging

## Milestone 6: Fault-Aware Modelling

Deliverable:
We can begin modelling faulted stratigraphy.

Checklist:

- [ ] Add `geology.fault_set.v1`
- [ ] Implement `fault_ingest`
- [ ] Implement `fault_surface_extract`
- [ ] Extend `structural_frame_builder` for fault chronology
- [ ] Add `fault_displacement_model`
- [ ] Update `stratigraphic_interpolator` to respect displaced domains

## Milestone 7: Metadata-Driven Viewer Integration

Deliverable:
The viewer consumes geology semantics and palette hints directly from upstream artifacts.

Checklist:

- [ ] Add `geology.palette_binding.v1`
- [ ] Implement `formation_palette_assign`
- [ ] Implement `scene3d_geology_layer_pack`
- [ ] Honor formation palette bindings in the viewer
- [ ] Honor inferred-vs-observed styling
- [ ] Surface groupings should map to structural groups/series
- [ ] Viewer should show model diagnostics without inventing geology semantics

## Milestone 8: Advanced Compute Later

Deliverable:
The architecture is ready for backends beyond the first deterministic CPU implementation.

Checklist:

- [ ] Keep interpolation engine abstraction backend-neutral
- [ ] Separate solver inputs from solver implementation details
- [ ] Document future TensorFlow backend insertion point
- [ ] Document probabilistic/ensemble extension point
- [ ] Do not hard-wire current scalar-field representation to one solver strategy

## Recommended Execution Order

This is the order I would actually build in:

1. Formation catalog + strat order + domain contracts
2. Orientation ingest + contact/orientation extraction + merged constraints
3. Structural frame builder + validator
4. First conformable interpolator + lith block output
5. Horizon extraction + enclosed solids
6. Palette binding + viewer packaging
7. Fault support
8. Backend abstraction for future TensorFlow/probabilistic work

## Implementation Notes

### Why We Should Not Jump Straight To Faults

Fault-aware chronology complicates everything:

- group relations
- scalar field continuity
- clipping
- mesh extraction
- diagnostics

We will move faster and more cleanly by proving conformable stratigraphy first.

### Why Block Models Should Arrive Early

GemPy-style workflows are not just about surfaces. A labelled 3D occupancy model unlocks:

- enclosed volume generation
- section slices
- topography clipping
- future reserve-style queries
- better debugging than surface-only outputs

### Why Palette Binding Should Be Upstream

We already see this in the viewer:

- categorical geology should use discrete palettes
- formations should keep consistent colors across layers
- drillholes, interface points, surfaces, and volumes should share the same formation color mapping

That belongs in upstream metadata, not isolated viewer state.

## Open Questions

- [ ] Do we want one scalar field per boundary or one grouped field per structural series?
- [ ] Do we want regular-grid only in v1, or regular-grid plus octree-ready contract fields?
- [ ] Should orientation inference from surfaces be a separate node or part of `formation_orientation_extract`?
- [ ] Should closure against bbox and closure against topography be one node or two nodes?
- [ ] Do we want the structural frame to carry palette hints directly, or keep palette binding as a separate contract?

## Definition Of “First Good Version”

We should consider the first genuinely useful GemPy-inspired implementation complete when:

- formations and ordering are explicit contracts
- contact points and orientations are inspectable artifacts
- a structural frame artifact exists and validates cleanly
- the model produces both horizons and a lithology/block volume
- volumes can be clipped and enclosed against domain/topography
- the viewer renders all outputs with consistent formation metadata and discrete palettes

At that point, we have a proper underground modelling base. TensorFlow, faults, uncertainty, and advanced interpolation can layer on top without forcing a redesign.
