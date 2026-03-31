use std::collections::{HashMap, HashSet, VecDeque};

use mine_eye_types::{NodeRecord, SemanticPortType};
use petgraph::graph::{DiGraph, NodeIndex};
use uuid::Uuid;

use crate::GraphError;

/// Directed edge: upstream output -> downstream input.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EdgeRef {
    pub id: Uuid,
    pub from_node: Uuid,
    pub from_port: String,
    pub to_node: Uuid,
    pub to_port: String,
    pub semantic_type: SemanticPortType,
}

/// In-memory graph view for scheduling.
#[derive(Debug, Clone)]
pub struct GraphSnapshot {
    pub graph_id: Uuid,
    pub nodes: HashMap<Uuid, NodeRecord>,
    pub edges: Vec<EdgeRef>,
}

impl GraphSnapshot {
    pub fn downstream(&self, node_id: Uuid) -> Vec<Uuid> {
        let mut out = Vec::new();
        for e in &self.edges {
            if e.from_node == node_id && !out.contains(&e.to_node) {
                out.push(e.to_node);
            }
        }
        out
    }

    pub fn upstream(&self, node_id: Uuid) -> Vec<Uuid> {
        let mut out = Vec::new();
        for e in &self.edges {
            if e.to_node == node_id && !out.contains(&e.from_node) {
                out.push(e.from_node);
            }
        }
        out
    }

    /// Topological order; returns error on cycle.
    pub fn topological_order(&self) -> Result<Vec<Uuid>, GraphError> {
        let mut g: DiGraph<Uuid, ()> = DiGraph::new();
        let mut idx: HashMap<Uuid, NodeIndex> = HashMap::new();
        for id in self.nodes.keys() {
            let n = g.add_node(*id);
            idx.insert(*id, n);
        }
        for e in &self.edges {
            let a = *idx.get(&e.from_node).ok_or(GraphError::InvalidEdge(
                "missing from_node".into(),
            ))?;
            let b = *idx
                .get(&e.to_node)
                .ok_or(GraphError::InvalidEdge("missing to_node".into()))?;
            g.add_edge(a, b, ());
        }
        let sorted = petgraph::algo::toposort(&g, None).map_err(|_| GraphError::Cycle)?;
        Ok(sorted.into_iter().map(|i| g[i]).collect())
    }
}

/// Mark nodes stale downstream from `changed` using BFS.
pub fn propagate_stale(
    snapshot: &GraphSnapshot,
    changed: &[Uuid],
) -> HashSet<Uuid> {
    let mut seen = HashSet::new();
    let mut q: VecDeque<Uuid> = changed.iter().copied().collect();
    while let Some(n) = q.pop_front() {
        if !seen.insert(n) {
            continue;
        }
        for d in snapshot.downstream(n) {
            q.push_back(d);
        }
    }
    seen
}
