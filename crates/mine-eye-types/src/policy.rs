use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RecomputePolicy {
    #[default]
    Auto,
    Manual,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum PropagationPolicy {
    Eager,
    #[default]
    Debounce,
    Hold,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum QualityPolicy {
    #[default]
    Preview,
    Final,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeExecutionPolicy {
    pub recompute: RecomputePolicy,
    pub propagation: PropagationPolicy,
    pub quality: QualityPolicy,
}

impl Default for NodeExecutionPolicy {
    fn default() -> Self {
        Self {
            recompute: RecomputePolicy::Auto,
            propagation: PropagationPolicy::Debounce,
            quality: QualityPolicy::Preview,
        }
    }
}
