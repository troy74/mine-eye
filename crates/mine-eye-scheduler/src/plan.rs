use std::collections::{HashMap, HashSet};

use mine_eye_graph::{hash_node_config, propagate_stale, GraphSnapshot};
use mine_eye_types::{ArtifactRef, JobEnvelope, PropagationPolicy, RecomputePolicy};
use sha2::{Digest, Sha256};

use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct SchedulePlan {
    pub jobs: Vec<JobEnvelope>,
    pub skipped_manual: Vec<Uuid>,
}

pub struct Scheduler {
    pub protocol_version: u32,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self {
            protocol_version: 1,
        }
    }
}

impl Scheduler {
    /// `dirty_nodes`: ids whose inputs or config changed and need recompute.
    /// `input_artifacts`: per-node map of resolved input artifact refs (from last known edges).
    pub fn plan(
        &self,
        snapshot: &GraphSnapshot,
        dirty_nodes: &HashSet<Uuid>,
        run_roots: &HashSet<Uuid>,
        input_artifacts: &HashMap<Uuid, Vec<ArtifactRef>>,
        run_id: Uuid,
        project_crs: Option<mine_eye_types::CrsRecord>,
        include_manual: bool,
    ) -> SchedulePlan {
        let order = match snapshot.topological_order() {
            Ok(o) => o,
            Err(_) => return SchedulePlan::default(),
        };

        let mut jobs = Vec::new();
        let mut skipped_manual = Vec::new();

        for node_id in order {
            if !dirty_nodes.contains(&node_id) {
                continue;
            }
            let Some(node) = snapshot.nodes.get(&node_id) else {
                continue;
            };
            if !include_manual && matches!(node.policy.recompute, RecomputePolicy::Manual) {
                skipped_manual.push(node_id);
                continue;
            }
            if matches!(node.policy.propagation, PropagationPolicy::Hold)
                && !(include_manual && run_roots.contains(&node_id))
            {
                continue;
            }

            let config_hash = hash_node_config(&node.config);
            let inputs = input_artifacts.get(&node_id).cloned().unwrap_or_default();
            let input_fingerprint = hash_input_artifacts(&inputs);
            let node_ui = node
                .config
                .params
                .get("ui")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            let job = JobEnvelope {
                protocol_version: self.protocol_version,
                job_id: Uuid::new_v4(),
                run_id,
                graph_id: snapshot.graph_id,
                node_id,
                node_kind: node.config.kind.clone(),
                config_hash,
                input_fingerprint,
                project_crs: project_crs.clone(),
                input_artifact_refs: inputs,
                input_payload: None,
                output_spec: serde_json::json!({
                    "quality": format!("{:?}", node.policy.quality),
                    "node_ui": node_ui,
                }),
            };
            jobs.push(job);
        }

        SchedulePlan {
            jobs,
            skipped_manual,
        }
    }
}

fn hash_input_artifacts(inputs: &[ArtifactRef]) -> String {
    let mut rows: Vec<(String, String, String)> = inputs
        .iter()
        .map(|a| {
            (
                a.key.clone(),
                a.content_hash.clone(),
                a.media_type.clone().unwrap_or_default(),
            )
        })
        .collect();
    rows.sort_unstable_by(|a, b| a.cmp(b));
    let mut h = Sha256::new();
    for (k, c, m) in rows {
        h.update(k.as_bytes());
        h.update([0x1f]);
        h.update(c.as_bytes());
        h.update([0x1f]);
        h.update(m.as_bytes());
        h.update([0x1e]);
    }
    hex::encode(h.finalize())
}

/// Expand dirty set using propagation policy (eager marks all downstream; debounce same BFS).
pub fn expand_dirty(snapshot: &GraphSnapshot, roots: &[Uuid]) -> HashSet<Uuid> {
    propagate_stale(snapshot, roots)
}

/// After a node succeeds, downstream nodes may need invalidation based on output hash change.
pub fn mark_dirty_from_hash_change(
    snapshot: &GraphSnapshot,
    node_id: Uuid,
    _old_hash: Option<&str>,
    _new_hash: &str,
) -> Vec<Uuid> {
    snapshot.downstream(node_id)
}

pub fn collect_dirty_nodes(snapshot: &GraphSnapshot, roots: &[Uuid]) -> HashSet<Uuid> {
    expand_dirty(snapshot, roots)
}
