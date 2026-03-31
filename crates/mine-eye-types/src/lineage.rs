use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LineageMeta {
    pub parent_node_ids: Vec<Uuid>,
    pub source_artifact_ids: Vec<Uuid>,
    pub transform_version: String,
}

impl Default for LineageMeta {
    fn default() -> Self {
        Self {
            parent_node_ids: Vec::new(),
            source_artifact_ids: Vec::new(),
            transform_version: "0.1.0".to_string(),
        }
    }
}
