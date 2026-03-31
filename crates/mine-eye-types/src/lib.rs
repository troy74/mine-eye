//! Shared contracts: semantic port types, canonical primitives, CRS, job envelopes.

mod crs;
mod domain;
mod envelope;
mod graph_meta;
mod lineage;
mod node;
mod ports;
mod policy;

pub use crs::{CrsRecord, ProjectCrs};
pub use domain::{
    BlockModelMeta, CollarRecord, DemRecord, DesurveyedInterval, IntervalSampleRecord,
    PointSampleRecord, ScalarFieldMeta, SurfacePointRecord, SurveyStationRecord, TrajectorySegment,
};
pub use envelope::{ArtifactRef, JobEnvelope, JobResult, JobStatus};
pub use graph_meta::{ApprovalRecord, GraphMeta, LockState, OwnerRef, WorkspaceStatus};
pub use lineage::LineageMeta;
pub use node::{CacheState, ExecutionState, NodeCategory, NodeConfig, NodeRecord, PortBinding};
pub use ports::{PortDirection, PortSpec, SemanticPortType};
pub use policy::{
    NodeExecutionPolicy, PropagationPolicy, QualityPolicy, RecomputePolicy,
};
