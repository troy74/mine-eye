use mine_eye_types::{JobEnvelope, JobResult, JobStatus};

use crate::executor::ExecutionContext;
use crate::NodeError;

pub async fn run_scene3d_layer_stack(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let mut layers: Vec<serde_json::Value> = Vec::new();
    let mut priority = 10i64;
    for ar in &job.input_artifact_refs {
        let v = super::runtime::read_json_artifact(ctx, &ar.key).await?;
        let schema_id = v.get("schema_id").and_then(|x| x.as_str()).unwrap_or("");
        let display_pointer = v
            .pointer("/display_contract/display_pointer")
            .and_then(|x| x.as_str())
            .unwrap_or("scene3d.generic");
        let kind = if schema_id == "scene3d.imagery_drape.v1"
            || schema_id == "scene3d.tilebroker_response.v1"
            || display_pointer == "scene3d.imagery_drape"
        {
            "imagery_drape"
        } else if v.get("surface_grid").is_some() || display_pointer == "scene3d.terrain" {
            "terrain"
        } else if display_pointer == "scene3d.contour_lines" {
            "contours"
        } else if display_pointer == "scene3d.trace_polyline" {
            "drill_segments"
        } else if display_pointer == "scene3d.block_voxels" {
            "block_voxels"
        } else if display_pointer == "scene3d.sample_points" {
            "assay_points"
        } else {
            "mesh"
        };
        let ui_caps = match kind {
            "imagery_drape" => serde_json::json!(["visible", "opacity", "provider"]),
            "terrain" => serde_json::json!(["visible", "opacity"]),
            "contours" => serde_json::json!(["visible", "opacity", "color", "width", "interval_step"]),
            "drill_segments" => serde_json::json!(["visible", "opacity", "palette", "radius_scale", "measure"]),
            "block_voxels" => serde_json::json!(["visible", "opacity", "palette", "measure", "cutoff"]),
            "assay_points" => serde_json::json!(["visible", "opacity", "palette", "size_scale", "measure"]),
            _ => serde_json::json!(["visible", "opacity"]),
        };
        layers.push(serde_json::json!({
            "layer_id": format!("layer_{}", layers.len() + 1),
            "kind": kind,
            "source_artifact_ref": {
                "key": ar.key,
                "content_hash": ar.content_hash
            },
            "style_defaults": {
                "opacity": 1.0
            },
            "ui_capabilities": ui_caps,
            "priority": priority,
            "visibility_default": true
        }));
        priority += 10;
    }

    let out = serde_json::json!({
        "schema_id":"scene3d.layer_stack.v1",
        "schema_version":1,
        "layers":layers,
        "provenance":{
            "node_kind":"scene3d_layer_stack",
            "node_id": job.node_id.to_string()
        }
    });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!("graphs/{}/nodes/{}/scene3d_layer_stack.json", job.graph_id, job.node_id);
    let artifact =
        super::runtime::write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}
