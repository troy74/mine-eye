use mine_eye_types::{CrsRecord, JobEnvelope, JobResult, JobStatus};

use crate::executor::ExecutionContext;
use crate::NodeError;

pub async fn run_aoi(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let mode = job
        .output_spec
        .pointer("/node_ui/mode")
        .and_then(|v| v.as_str())
        .unwrap_or("inferred");
    let margin_raw = job
        .output_spec
        .pointer("/node_ui/margin_pct")
        .and_then(|v| v.as_f64())
        .unwrap_or(25.0);
    let margin_pct = if margin_raw > 1.0 {
        margin_raw / 100.0
    } else {
        margin_raw
    }
    .max(0.0);
    let bbox_cfg = job
        .output_spec
        .pointer("/node_ui/bbox")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let mut xyz_points: Vec<super::runtime::XYZ> = Vec::new();
    let mut xy_points: Vec<(f64, f64)> = Vec::new();
    for ar in &job.input_artifact_refs {
        let v = super::runtime::read_json_artifact(ctx, &ar.key).await?;
        xyz_points.extend(super::runtime::collect_xyz_points(&v));
        xy_points.extend(super::runtime::collect_xy_points(&v));
    }

    let inferred_bbox = if bbox_cfg.len() >= 4 {
        let x0 = bbox_cfg[0].as_f64().unwrap_or(0.0);
        let y0 = bbox_cfg[1].as_f64().unwrap_or(0.0);
        let x1 = bbox_cfg[2].as_f64().unwrap_or(1.0);
        let y1 = bbox_cfg[3].as_f64().unwrap_or(1.0);
        Some((x0.min(x1), x0.max(x1), y0.min(y1), y0.max(y1)))
    } else {
        super::runtime::merge_extents(
            super::runtime::infer_extent(&xyz_points, margin_pct),
            super::runtime::infer_extent_xy(&xy_points, margin_pct),
        )
    };
    let (xmin, xmax, ymin, ymax) = inferred_bbox.unwrap_or((-0.5, 0.5, -0.5, 0.5));

    let geometry = serde_json::json!({
        "type":"Polygon",
        "coordinates":[[
            [xmin, ymin],
            [xmax, ymin],
            [xmax, ymax],
            [xmin, ymax],
            [xmin, ymin]
        ]]
    });
    let out = serde_json::json!({
        "schema_id":"spatial.aoi.v1",
        "schema_version":1,
        "crs": job.project_crs.clone().unwrap_or_else(|| CrsRecord::epsg(4326)),
        "geometry": geometry,
        "bounds":{"xmin":xmin,"xmax":xmax,"ymin":ymin,"ymax":ymax},
        "meta":{
            "mode":mode,
            "locked": job.output_spec.pointer("/node_ui/locked").and_then(|v| v.as_bool()).unwrap_or(false),
            "margin_pct": margin_raw,
            "inferred_from_points": xyz_points.len(),
            "inferred_from_xy_points": xy_points.len(),
            "warning": if inferred_bbox.is_none() { serde_json::Value::String("fallback default AOI used".into()) } else { serde_json::Value::Null }
        },
        "provenance":{
            "node_kind":"aoi",
            "node_id": job.node_id.to_string()
        }
    });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!("graphs/{}/nodes/{}/aoi.json", job.graph_id, job.node_id);
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
