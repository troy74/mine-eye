use std::collections::{HashMap, VecDeque};

use mine_eye_types::{
    node_group_depth, ArtifactRef, InputArtifactBinding, JobEnvelope, JobResult, JobStatus,
    NodeGroupDefinition, NODE_GROUP_MAX_DEPTH,
};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::executor::{ExecutionContext, NodeExecutor, RegistryExecutor};
use crate::NodeError;

fn group_definition(job: &JobEnvelope) -> Result<NodeGroupDefinition, NodeError> {
    let raw = job
        .output_spec
        .get("node_params")
        .and_then(|v| {
            v.get("group_definition")
                .or_else(|| v.get("ui").and_then(|u| u.get("group_definition")))
        })
        .cloned()
        .ok_or_else(|| {
            NodeError::InvalidConfig("node_group requires params.group_definition".into())
        })?;
    serde_json::from_value(raw)
        .map_err(|e| NodeError::InvalidConfig(format!("invalid node_group definition: {}", e)))
}

fn topo(def: &NodeGroupDefinition) -> Result<Vec<String>, NodeError> {
    let mut indegree = HashMap::<String, usize>::new();
    let mut outgoing = HashMap::<String, Vec<String>>::new();
    for n in &def.internal_nodes {
        indegree.insert(n.id.clone(), 0);
        outgoing.entry(n.id.clone()).or_default();
    }
    for e in &def.internal_edges {
        *indegree.entry(e.to_node_id.clone()).or_default() += 1;
        outgoing
            .entry(e.from_node_id.clone())
            .or_default()
            .push(e.to_node_id.clone());
    }
    let mut q = indegree
        .iter()
        .filter_map(|(k, v)| (*v == 0).then_some(k.clone()))
        .collect::<VecDeque<_>>();
    let mut out = Vec::new();
    while let Some(id) = q.pop_front() {
        out.push(id.clone());
        for nxt in outgoing.get(&id).cloned().unwrap_or_default() {
            if let Some(d) = indegree.get_mut(&nxt) {
                *d = d.saturating_sub(1);
                if *d == 0 {
                    q.push_back(nxt);
                }
            }
        }
    }
    if out.len() != def.internal_nodes.len() {
        return Err(NodeError::InvalidConfig(
            "node_group internal graph contains a cycle".into(),
        ));
    }
    Ok(out)
}

pub async fn run_node_group(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let def = group_definition(job)?;
    let static_depth = node_group_depth(&def)
        .map_err(|e| NodeError::InvalidConfig(format!("invalid node_group depth: {}", e)))?;
    if static_depth > NODE_GROUP_MAX_DEPTH {
        return Err(NodeError::InvalidConfig(format!(
            "node_group nesting depth {} exceeds max supported depth {}",
            static_depth, NODE_GROUP_MAX_DEPTH
        )));
    }
    let current_depth = job
        .output_spec
        .get("group_depth")
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as usize;
    if current_depth > NODE_GROUP_MAX_DEPTH {
        return Err(NodeError::InvalidConfig(format!(
            "node_group execution depth {} exceeds max supported depth {}",
            current_depth, NODE_GROUP_MAX_DEPTH
        )));
    }
    let order = topo(&def)?;
    let node_by_id = def
        .internal_nodes
        .iter()
        .map(|n| (n.id.clone(), n))
        .collect::<HashMap<_, _>>();
    let mut wrapper_inputs = HashMap::<String, Vec<ArtifactRef>>::new();
    for InputArtifactBinding {
        to_port,
        artifact_ref,
    } in &job.input_artifact_bindings
    {
        wrapper_inputs
            .entry(to_port.clone())
            .or_default()
            .push(artifact_ref.clone());
    }

    let executor = RegistryExecutor::new();
    let mut internal_outputs = HashMap::<String, Vec<ArtifactRef>>::new();
    let mut published = Vec::<ArtifactRef>::new();

    for internal_id in order {
        let node = node_by_id.get(&internal_id).ok_or_else(|| {
            NodeError::InvalidConfig(format!(
                "internal node '{}' missing from definition",
                internal_id
            ))
        })?;
        if node.kind == "node_group" {
            return Err(NodeError::InvalidConfig(
                "nested node_group is not supported in V1".into(),
            ));
        }
        let mut refs = Vec::<ArtifactRef>::new();
        let mut bindings = Vec::<InputArtifactBinding>::new();
        for b in def
            .input_bindings
            .iter()
            .filter(|b| b.internal_node_id == internal_id)
        {
            for artifact_ref in wrapper_inputs
                .get(&b.group_input_id)
                .cloned()
                .unwrap_or_default()
            {
                refs.push(artifact_ref.clone());
                bindings.push(InputArtifactBinding {
                    to_port: b.internal_port_id.clone(),
                    artifact_ref,
                });
            }
        }
        for e in def
            .internal_edges
            .iter()
            .filter(|e| e.to_node_id == internal_id)
        {
            let _src = node_by_id.get(&e.from_node_id).ok_or_else(|| {
                NodeError::InvalidConfig(format!(
                    "internal edge from unknown node '{}'",
                    e.from_node_id
                ))
            })?;
            let out_refs = internal_outputs.get(&e.from_node_id).ok_or_else(|| {
                NodeError::InvalidConfig(format!(
                    "internal node '{}' produced no outputs",
                    e.from_node_id
                ))
            })?;
            let idx = e.from_output_index.ok_or_else(|| {
                NodeError::InvalidConfig(format!(
                    "internal edge '{}.{} -> {}.{}' is missing from_output_index",
                    e.from_node_id, e.from_port_id, e.to_node_id, e.to_port_id
                ))
            })?;
            let Some(r) = out_refs.get(idx).cloned() else {
                return Err(NodeError::InvalidConfig(format!(
                    "internal node '{}' missing artifact for output port '{}'",
                    e.from_node_id, e.from_port_id
                )));
            };
            refs.push(r.clone());
            bindings.push(InputArtifactBinding {
                to_port: e.to_port_id.clone(),
                artifact_ref: r,
            });
        }
        let internal_job = JobEnvelope {
            protocol_version: job.protocol_version,
            job_id: Uuid::new_v4(),
            run_id: job.run_id,
            graph_id: job.graph_id,
            node_id: Uuid::new_v4(),
            node_kind: node.kind.clone(),
            config_hash: String::new(),
            input_fingerprint: String::new(),
            project_crs: job.project_crs.clone(),
            input_artifact_refs: refs.clone(),
            input_artifact_bindings: bindings,
            input_payload: None,
            output_spec: json!({
                "quality": job.output_spec.get("quality").cloned().unwrap_or(Value::String("Preview".into())),
                "node_ui": node.params,
                "node_params": node.params,
                "group_depth": current_depth + 1
            }),
        };
        let result = executor.execute(ctx, &internal_job).await?;
        if result.status != JobStatus::Succeeded {
            return Err(NodeError::InvalidConfig(format!(
                "internal group node '{}' failed",
                internal_id
            )));
        }
        internal_outputs.insert(internal_id.clone(), result.output_artifact_refs);
    }

    for b in &def.output_bindings {
        let _src = node_by_id.get(&b.internal_node_id).ok_or_else(|| {
            NodeError::InvalidConfig(format!(
                "group output '{}' references unknown node '{}'",
                b.group_output_id, b.internal_node_id
            ))
        })?;
        let refs = internal_outputs.get(&b.internal_node_id).ok_or_else(|| {
            NodeError::InvalidConfig(format!(
                "group output '{}' references node '{}' with no outputs",
                b.group_output_id, b.internal_node_id
            ))
        })?;
        let idx = b.internal_output_index.ok_or_else(|| {
            NodeError::InvalidConfig(format!(
                "group output '{}' is missing internal_output_index for '{}.{}'",
                b.group_output_id, b.internal_node_id, b.internal_port_id
            ))
        })?;
        let Some(r) = refs.get(idx).cloned() else {
            return Err(NodeError::InvalidConfig(format!(
                "group output '{}' missing internal artifact",
                b.group_output_id
            )));
        };
        published.push(r);
    }

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        content_hashes: published.iter().map(|r| r.content_hash.clone()).collect(),
        output_artifact_refs: published,
        error_message: None,
    })
}
