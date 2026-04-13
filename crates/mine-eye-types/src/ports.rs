use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Semantic types for node ports (see architecture and README docs).
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
    DataTable,
    SemanticJson,
    Any,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticCompatibility {
    Exact,
    WildcardSink,
}

impl SemanticPortType {
    pub fn as_str(self) -> &'static str {
        match self {
            SemanticPortType::PointSet => "point_set",
            SemanticPortType::IntervalSet => "interval_set",
            SemanticPortType::TrajectorySet => "trajectory_set",
            SemanticPortType::Surface => "surface",
            SemanticPortType::Raster => "raster",
            SemanticPortType::Mesh => "mesh",
            SemanticPortType::BlockModel => "block_model",
            SemanticPortType::DataTable => "data_table",
            SemanticPortType::SemanticJson => "semantic_json",
            SemanticPortType::Any => "any",
        }
    }

    pub fn can_emit(self) -> bool {
        self != SemanticPortType::Any
    }

    pub fn compatibility_to(self, input: SemanticPortType) -> Option<SemanticCompatibility> {
        if !self.can_emit() {
            return None;
        }
        if self == input {
            return Some(SemanticCompatibility::Exact);
        }
        if input == SemanticPortType::Any {
            return Some(SemanticCompatibility::WildcardSink);
        }
        None
    }
}

impl FromStr for SemanticPortType {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "point_set" | "pointset" => Ok(SemanticPortType::PointSet),
            "interval_set" | "intervalset" => Ok(SemanticPortType::IntervalSet),
            "trajectory_set" | "trajectoryset" => Ok(SemanticPortType::TrajectorySet),
            "surface" => Ok(SemanticPortType::Surface),
            "raster" => Ok(SemanticPortType::Raster),
            "mesh" => Ok(SemanticPortType::Mesh),
            "block_model" | "blockmodel" => Ok(SemanticPortType::BlockModel),
            "data_table" | "datatable" | "table" => Ok(SemanticPortType::DataTable),
            "semantic_json" | "semanticjson" => Ok(SemanticPortType::SemanticJson),
            "any" => Ok(SemanticPortType::Any),
            other => Err(format!("unsupported semantic_type '{}'", other)),
        }
    }
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
