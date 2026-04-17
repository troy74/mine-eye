use serde::{Deserialize, Serialize};

use crate::{NodeCategory, SemanticPortType};

pub const NODE_GROUP_MAX_DEPTH: usize = 3;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeGroupPort {
    pub id: String,
    pub label: String,
    pub semantic: SemanticPortType,
    #[serde(default)]
    pub optional: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeGroupNode {
    pub id: String,
    pub kind: String,
    pub category: NodeCategory,
    #[serde(default)]
    pub params: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeGroupEdge {
    pub from_node_id: String,
    pub from_port_id: String,
    #[serde(default)]
    pub from_output_index: Option<usize>,
    pub to_node_id: String,
    pub to_port_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeGroupInputBinding {
    pub group_input_id: String,
    pub internal_node_id: String,
    pub internal_port_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeGroupOutputBinding {
    pub group_output_id: String,
    pub internal_node_id: String,
    pub internal_port_id: String,
    #[serde(default)]
    pub internal_output_index: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeGroupDefinition {
    pub version: u32,
    pub label: String,
    #[serde(default)]
    pub inputs: Vec<NodeGroupPort>,
    #[serde(default)]
    pub outputs: Vec<NodeGroupPort>,
    #[serde(default)]
    pub internal_nodes: Vec<NodeGroupNode>,
    #[serde(default)]
    pub internal_edges: Vec<NodeGroupEdge>,
    #[serde(default)]
    pub input_bindings: Vec<NodeGroupInputBinding>,
    #[serde(default)]
    pub output_bindings: Vec<NodeGroupOutputBinding>,
}

fn nested_group_definition(node: &NodeGroupNode) -> Result<Option<NodeGroupDefinition>, String> {
    if node.kind != "node_group" {
        return Ok(None);
    }
    let raw = node.params.get("group_definition").cloned().or_else(|| {
        node.params
            .get("ui")
            .and_then(|v| v.get("group_definition"))
            .cloned()
    });
    let Some(raw) = raw else {
        return Ok(None);
    };
    serde_json::from_value(raw)
        .map(Some)
        .map_err(|e| format!("invalid nested node_group definition: {}", e))
}

pub fn node_group_depth(def: &NodeGroupDefinition) -> Result<usize, String> {
    let mut max_depth = 1usize;
    for node in &def.internal_nodes {
        if let Some(nested) = nested_group_definition(node)? {
            max_depth = max_depth.max(1 + node_group_depth(&nested)?);
        }
    }
    Ok(max_depth)
}
