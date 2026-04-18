use mine_eye_types::{JobEnvelope, JobResult, JobStatus, SurveyStationRecord, TrajectorySegment};

use crate::executor::ExecutionContext;
use crate::NodeError;

/// Minimal straight-hole desurvey from collars + surveys; writes trajectory JSON.
pub async fn run_desurvey_trajectory(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let dip_positive_down = job
        .output_spec
        .pointer("/node_ui/dip_positive_down")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let (collars, surveys, _assays) = super::runtime::collect_drillhole_inputs(ctx, job).await?;
    if collars.is_empty() {
        return Err(NodeError::InvalidConfig(
            "desurvey_trajectory requires collars input".into(),
        ));
    }
    if surveys.is_empty() {
        return Err(NodeError::InvalidConfig(
            "desurvey_trajectory requires surveys input".into(),
        ));
    }

    let mut segments = Vec::new();
    for c in &collars {
        let mut stations: Vec<&SurveyStationRecord> =
            surveys.iter().filter(|s| s.hole_id == c.hole_id).collect();
        stations.sort_by(|a, b| a.depth_m.partial_cmp(&b.depth_m).unwrap());
        if stations.is_empty() {
            segments.push(TrajectorySegment {
                hole_id: c.hole_id.clone(),
                depth_from_m: 0.0,
                depth_to_m: 1.0,
                x_from: c.x,
                y_from: c.y,
                z_from: c.z,
                x_to: c.x,
                y_to: c.y,
                z_to: c.z - 1.0,
                crs: c.crs.clone(),
            });
            continue;
        }
        let mut prev_d = 0.0f64;
        let mut prev_x = c.x;
        let mut prev_y = c.y;
        let mut prev_z = c.z;
        for st in stations {
            let dz = st.depth_m - prev_d;
            let dip_rad = st.dip_deg.to_radians();
            let az_rad = st.azimuth_deg.to_radians();
            let run = dz * dip_rad.cos();
            let dx = run * az_rad.sin();
            let dy = run * az_rad.cos();
            let dz_vert = dz * dip_rad.sin();
            let x_to = prev_x + dx;
            let y_to = prev_y + dy;
            let z_to = if dip_positive_down {
                prev_z - dz_vert
            } else {
                prev_z + dz_vert
            };
            segments.push(TrajectorySegment {
                hole_id: c.hole_id.clone(),
                depth_from_m: prev_d,
                depth_to_m: st.depth_m,
                x_from: prev_x,
                y_from: prev_y,
                z_from: prev_z,
                x_to,
                y_to,
                z_to,
                crs: c.crs.clone(),
            });
            prev_d = st.depth_m;
            prev_x = x_to;
            prev_y = y_to;
            prev_z = z_to;
        }
    }

    let bytes = serde_json::to_vec(&segments)?;
    let key = format!(
        "graphs/{}/nodes/{}/trajectory.json",
        job.graph_id, job.node_id
    );
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

/// Build straight vertical trajectories from collars alone.
pub async fn run_vertical_trajectory(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let (collars, _surveys, _assays) = super::runtime::collect_drillhole_inputs(ctx, job).await?;
    if collars.is_empty() {
        return Err(NodeError::InvalidConfig(
            "vertical_trajectory requires collars input".into(),
        ));
    }

    let depth_override = job
        .output_spec
        .pointer("/node_ui/default_depth_m")
        .and_then(|v| v.as_f64())
        .filter(|v| *v > 0.0);

    let mut depth_by_hole = std::collections::HashMap::<String, f64>::new();
    for ar in &job.input_artifact_refs {
        let root = super::runtime::read_json_artifact(ctx, &ar.key).await?;
        let intervals = root
            .get("intervals")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        for row in intervals {
            let Some(obj) = row.as_object() else { continue };
            let Some(hole_id) = obj.get("hole_id").and_then(|v| v.as_str()) else {
                continue;
            };
            let Some(to_m) = obj.get("to_m").and_then(|v| v.as_f64()) else {
                continue;
            };
            depth_by_hole
                .entry(hole_id.to_string())
                .and_modify(|d| *d = d.max(to_m))
                .or_insert(to_m);
        }
    }

    let mut segments = Vec::new();
    for c in &collars {
        let depth = depth_by_hole
            .get(&c.hole_id)
            .copied()
            .or(depth_override)
            .unwrap_or(100.0)
            .max(0.1);
        segments.push(TrajectorySegment {
            hole_id: c.hole_id.clone(),
            depth_from_m: 0.0,
            depth_to_m: depth,
            x_from: c.x,
            y_from: c.y,
            z_from: c.z,
            x_to: c.x,
            y_to: c.y,
            z_to: c.z - depth,
            crs: c.crs.clone(),
        });
    }

    let bytes = serde_json::to_vec(&segments)?;
    let key = format!(
        "graphs/{}/nodes/{}/trajectory.json",
        job.graph_id, job.node_id
    );
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
