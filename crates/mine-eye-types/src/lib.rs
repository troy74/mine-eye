//! Shared contracts: semantic port types, canonical primitives, CRS, job envelopes.

mod branching;
mod crs;
mod domain;
mod envelope;
mod graph_meta;
mod identity;
mod lineage;
mod node;
mod policy;
mod ports;

pub use branching::{
    BranchPromotionRecord, BranchPromotionStatus, BranchStatus, GraphBranch, GraphRevision,
};
pub use crs::{CrsRecord, ProjectCrs};
pub use domain::{
    BlockModelMeta, CollarRecord, DemRecord, DesurveyedInterval, IntervalSampleRecord,
    PointSampleRecord, ScalarFieldMeta, SurfacePointRecord, SurveyStationRecord, TrajectorySegment,
};
pub use envelope::{ArtifactRef, JobEnvelope, JobResult, JobStatus};
pub use graph_meta::{ApprovalRecord, GraphMeta, LockState, OwnerRef, WorkspaceStatus};
pub use identity::{
    personal_organization_id, AuthContextRef, OrganizationMembership, OrganizationRecord,
    OrganizationRole, UserRecord,
};
pub use lineage::LineageMeta;
pub use node::{CacheState, ExecutionState, NodeCategory, NodeConfig, NodeRecord, PortBinding};
pub use policy::{NodeExecutionPolicy, PropagationPolicy, QualityPolicy, RecomputePolicy};
pub use ports::{PortDirection, PortSpec, SemanticPortType};
