use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::policy::NodeExecutionPolicy;
use crate::{LineageMeta, SemanticPortType};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum NodeCategory {
    #[default]
    Input,
    Transform,
    Model,
    Qa,
    Visualisation,
    Export,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionState {
    #[default]
    Idle,
    Pending,
    Running,
    Failed,
    Succeeded,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum CacheState {
    #[default]
    Miss,
    Hit,
    Stale,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PortBinding {
    pub port_name: String,
    pub semantic_type: SemanticPortType,
    pub connected_node_id: Option<Uuid>,
    pub connected_port_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeConfig {
    pub version: u32,
    pub kind: String,
    pub params: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeRecord {
    pub id: Uuid,
    pub graph_id: Uuid,
    pub category: NodeCategory,
    pub config: NodeConfig,
    pub execution: ExecutionState,
    pub cache: CacheState,
    pub policy: NodeExecutionPolicy,
    pub ports: Vec<PortBinding>,
    pub lineage: LineageMeta,
    pub content_hash: Option<String>,
    /// Last worker/orchestrator failure message (cleared on success / new run).
    pub last_error: Option<String>,
}
