use mine_eye_types::{IntervalSampleRecord, JobEnvelope, JobResult, JobStatus, TrajectorySegment};

use crate::executor::ExecutionContext;
use crate::NodeError;

/// Merge collar / survey / assay JSON shards into one package for desurvey.
pub async fn run_drillhole_merge(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let (collars, surveys, assays) = super::runtime::collect_drillhole_inputs(ctx, job).await?;

    let merged = serde_json::json!({
        "collars": collars,
        "surveys": surveys,
        "assays": assays,
    });
    let bytes = serde_json::to_vec(&merged)?;
    let key = format!("graphs/{}/nodes/{}/ingest.json", job.graph_id, job.node_id);
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

fn position_at_depth(segments: &[TrajectorySegment], depth_m: f64) -> Option<(f64, f64, f64)> {
    for s in segments {
        if depth_m < s.depth_from_m || depth_m > s.depth_to_m {
            continue;
        }
        let len = (s.depth_to_m - s.depth_from_m).abs();
        let t = if len <= f64::EPSILON {
            0.0
        } else {
            (depth_m - s.depth_from_m) / (s.depth_to_m - s.depth_from_m)
        };
        let x = s.x_from + (s.x_to - s.x_from) * t;
        let y = s.y_from + (s.y_to - s.y_from) * t;
        let z = s.z_from + (s.z_to - s.z_from) * t;
        return Some((x, y, z));
    }
    None
}

/// Build drillhole geometry payloads from trajectory + assays:
/// - `drillhole_meshes.json`: segment-wise cylinders with assay overlap metadata
/// - `assay_points.json`: midpoint assay points for later interpolation/kriging
pub async fn run_drillhole_model(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let mut trajectory: Vec<TrajectorySegment> = Vec::new();
    let mut assays: Vec<IntervalSampleRecord> = Vec::new();

    for ar in &job.input_artifact_refs {
        let v = super::runtime::read_json_artifact(ctx, &ar.key).await?;
        if let Ok(mut segs) = serde_json::from_value::<Vec<TrajectorySegment>>(v.clone()) {
            trajectory.append(&mut segs);
        }
        if let Some(a) = v.get("assays") {
            if let Ok(mut part) = serde_json::from_value::<Vec<IntervalSampleRecord>>(a.clone()) {
                assays.append(&mut part);
            }
        }
    }

    if trajectory.is_empty() {
        return Err(NodeError::InvalidConfig(
            "drillhole_model requires trajectory input".into(),
        ));
    }

    let mut hole_segments: std::collections::HashMap<String, Vec<TrajectorySegment>> =
        std::collections::HashMap::new();
    for s in trajectory {
        hole_segments
            .entry(s.hole_id.clone())
            .or_default()
            .push(s);
    }
    for segs in hole_segments.values_mut() {
        segs.sort_by(|a, b| a.depth_from_m.partial_cmp(&b.depth_from_m).unwrap());
    }

    let radius_m = job
        .output_spec
        .pointer("/node_ui/hole_radius_m")
        .and_then(|v| v.as_f64())
        .filter(|r| *r > 0.0)
        .unwrap_or(0.6);

    let mut mesh_segments: Vec<serde_json::Value> = Vec::new();
    for (hole_id, segs) in &hole_segments {
        for (idx, s) in segs.iter().enumerate() {
            let overlapping_assays: Vec<serde_json::Value> = assays
                .iter()
                .filter(|a| {
                    a.hole_id == *hole_id && a.to_m > s.depth_from_m && a.from_m < s.depth_to_m
                })
                .map(|a| {
                    serde_json::json!({
                        "from_m": a.from_m,
                        "to_m": a.to_m,
                        "attributes": a.attributes,
                        "qa_flags": a.qa_flags,
                    })
                })
                .collect();
            mesh_segments.push(serde_json::json!({
                "hole_id": hole_id,
                "segment_index": idx,
                "from_depth_m": s.depth_from_m,
                "to_depth_m": s.depth_to_m,
                "from_xyz": [s.x_from, s.y_from, s.z_from],
                "to_xyz": [s.x_to, s.y_to, s.z_to],
                "crs": s.crs.clone(),
                "radius_m": radius_m,
                "assays": overlapping_assays,
            }));
        }
    }

    let mut assay_points: Vec<serde_json::Value> = Vec::new();
    for a in &assays {
        let Some(segs) = hole_segments.get(&a.hole_id) else {
            continue;
        };
        let mid = (a.from_m + a.to_m) * 0.5;
        if let Some((x, y, z)) = position_at_depth(segs, mid) {
            let crs = segs
                .iter()
                .find(|s| mid >= s.depth_from_m && mid <= s.depth_to_m)
                .map(|s| s.crs.clone())
                .or_else(|| segs.first().map(|s| s.crs.clone()));
            assay_points.push(serde_json::json!({
                "hole_id": a.hole_id,
                "from_m": a.from_m,
                "to_m": a.to_m,
                "depth_m": mid,
                "x": x,
                "y": y,
                "z": z,
                "crs": crs,
                "attributes": a.attributes,
                "qa_flags": a.qa_flags,
            }));
        }
    }

    let mesh_payload = serde_json::json!({
        "kind": "drillhole_cylinder_mesh_segments",
        "crs": hole_segments
            .values()
            .next()
            .and_then(|segs| segs.first().map(|s| s.crs.clone())),
        "segments": mesh_segments,
    });
    let mesh_bytes = serde_json::to_vec(&mesh_payload)?;
    let mesh_key = format!(
        "graphs/{}/nodes/{}/drillhole_meshes.json",
        job.graph_id, job.node_id
    );
    let mesh_ref = super::runtime::write_artifact(
        ctx,
        &mesh_key,
        &mesh_bytes,
        Some("application/json"),
    )
    .await?;

    let points_payload = serde_json::json!({ "assay_points": assay_points });
    let points_bytes = serde_json::to_vec(&points_payload)?;
    let points_key = format!(
        "graphs/{}/nodes/{}/assay_points.json",
        job.graph_id, job.node_id
    );
    let points_ref = super::runtime::write_artifact(
        ctx,
        &points_key,
        &points_bytes,
        Some("application/json"),
    )
    .await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![mesh_ref.clone(), points_ref.clone()],
        content_hashes: vec![mesh_ref.content_hash, points_ref.content_hash],
        error_message: None,
    })
}
