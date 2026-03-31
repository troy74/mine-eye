use serde::{Deserialize, Serialize};

/// Semantic types for node ports ([V1SPEC §1](V1SPEC.md)).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SemanticPortType {
    PointSet,
    IntervalSet,
    TrajectorySet,
    Surface,
    Raster,
    Mesh,
    BlockModel,
    Table,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PortDirection {
    In,
    Out,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PortSpec {
    pub name: String,
    pub direction: PortDirection,
    pub semantic_type: SemanticPortType,
}
