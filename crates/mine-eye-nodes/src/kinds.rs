use mine_eye_types::{
    ArtifactRef, CollarRecord, CrsRecord, IntervalSampleRecord, JobEnvelope, JobResult, JobStatus,
    SurveyStationRecord, TrajectorySegment,
};
use sha2::{Digest, Sha256};
use tokio::fs;

use crate::crs_transform::transform_xy;
use crate::executor::ExecutionContext;
use crate::NodeError;

fn collar_output_crs_mode(job: &JobEnvelope) -> &str {
    job.output_spec
        .pointer("/node_ui/output_crs_mode")
        .and_then(|v| v.as_str())
        .unwrap_or("project")
}

/// Target CRS for written collar coordinates, or `None` when output should stay in source CRS.
fn collar_output_target_crs(job: &JobEnvelope) -> Result<Option<CrsRecord>, NodeError> {
    match collar_output_crs_mode(job) {
        "source" => Ok(None),
        "wgs84" => Ok(Some(CrsRecord::epsg(4326))),
        "custom" => {
            let e = job
                .output_spec
                .pointer("/node_ui/output_crs_epsg")
                .and_then(|v| v.as_u64())
                .unwrap_or(4326) as i32;
            Ok(Some(CrsRecord::epsg(e)))
        }
        "project" | _ => Ok(Some(
            job.project_crs
                .clone()
                .unwrap_or_else(|| CrsRecord::epsg(4326)),
        )),
    }
}

fn hash_bytes(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

async fn write_artifact(
    ctx: &ExecutionContext<'_>,
    relative_key: &str,
    bytes: &[u8],
    media_type: Option<&str>,
) -> Result<ArtifactRef, NodeError> {
    let path = ctx.artifact_root.join(relative_key);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    fs::write(&path, bytes).await?;
    let content_hash = hash_bytes(bytes);
    Ok(ArtifactRef {
        key: relative_key.to_string(),
        content_hash,
        media_type: media_type.map(String::from),
    })
}

/// Parses collars/surveys/assays from job config params JSON and writes canonical JSON artifact.
pub async fn run_drillhole_ingest(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payload = job
        .input_payload
        .as_ref()
        .ok_or_else(|| NodeError::InvalidConfig("missing input_payload for ingest".into()))?;
    let collars: Vec<CollarRecord> = serde_json::from_value(
        payload
            .pointer("/collars")
            .cloned()
            .unwrap_or(serde_json::json!([])),
    )
    .unwrap_or_default();
    let surveys: Vec<SurveyStationRecord> = serde_json::from_value(
        payload
            .pointer("/surveys")
            .cloned()
            .unwrap_or(serde_json::json!([])),
    )
    .unwrap_or_default();
    let assays: Vec<IntervalSampleRecord> = serde_json::from_value(
        payload
            .pointer("/assays")
            .cloned()
            .unwrap_or(serde_json::json!([])),
    )
    .unwrap_or_default();

    let payload = serde_json::json!({
        "collars": collars,
        "surveys": surveys,
        "assays": assays,
    });
    let bytes = serde_json::to_vec(&payload)?;
    let key = format!("graphs/{}/nodes/{}/ingest.json", job.graph_id, job.node_id);
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

/// Single primitive: collars only ([V1SPEC §2](V1SPEC.md) — Collar).
pub async fn run_collar_ingest(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payload = job
        .input_payload
        .as_ref()
        .ok_or_else(|| NodeError::InvalidConfig("missing input_payload for collar_ingest".into()))?;
    let mut collars: Vec<CollarRecord> = serde_json::from_value(
        payload
            .pointer("/collars")
            .cloned()
            .unwrap_or(serde_json::json!([])),
    )
    .unwrap_or_default();

    if let Some(target) = collar_output_target_crs(job)? {
        let project_missing = collar_output_crs_mode(job) == "project" && job.project_crs.is_none();
        for c in &mut collars {
            if project_missing {
                c.qa_flags
                    .push("project_crs_missing_output_epsg_4326".into());
            }
            if c.crs == target {
                continue;
            }
            let (nx, ny) = transform_xy(&c.crs, &target, c.x, c.y)?;
            c.x = nx;
            c.y = ny;
            c.crs = target.clone();
            c.qa_flags.push("reprojected_xy".into());
        }
    }

    let out = serde_json::json!({ "collars": collars });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!("graphs/{}/nodes/{}/collars.json", job.graph_id, job.node_id);
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

/// Single primitive: survey stations only ([V1SPEC §2](V1SPEC.md) — SurveyStation).
pub async fn run_survey_ingest(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payload = job
        .input_payload
        .as_ref()
        .ok_or_else(|| NodeError::InvalidConfig("missing input_payload for survey_ingest".into()))?;
    let surveys: Vec<SurveyStationRecord> = serde_json::from_value(
        payload
            .pointer("/surveys")
            .cloned()
            .unwrap_or(serde_json::json!([])),
    )
    .unwrap_or_default();
    let out = serde_json::json!({ "surveys": surveys });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!("graphs/{}/nodes/{}/surveys.json", job.graph_id, job.node_id);
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

/// Single primitive: interval assays only ([V1SPEC §2](V1SPEC.md) — IntervalSample).
pub async fn run_assay_ingest(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payload = job
        .input_payload
        .as_ref()
        .ok_or_else(|| NodeError::InvalidConfig("missing input_payload for assay_ingest".into()))?;
    let assays: Vec<IntervalSampleRecord> = serde_json::from_value(
        payload
            .pointer("/assays")
            .cloned()
            .unwrap_or(serde_json::json!([])),
    )
    .unwrap_or_default();
    let out = serde_json::json!({ "assays": assays });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!("graphs/{}/nodes/{}/assays.json", job.graph_id, job.node_id);
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

/// Merge collar / survey / assay JSON shards into one package for desurvey.
pub async fn run_drillhole_merge(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let mut collars: Vec<CollarRecord> = Vec::new();
    let mut surveys: Vec<SurveyStationRecord> = Vec::new();
    let mut assays: Vec<IntervalSampleRecord> = Vec::new();

    for ar in &job.input_artifact_refs {
        let path = ctx.artifact_root.join(&ar.key);
        let raw = fs::read(&path).await?;
        let v: serde_json::Value = serde_json::from_slice(&raw)?;
        if let Some(c) = v.get("collars") {
            if let Ok(mut part) = serde_json::from_value::<Vec<CollarRecord>>(c.clone()) {
                collars.append(&mut part);
            }
        }
        if let Some(s) = v.get("surveys") {
            if let Ok(mut part) = serde_json::from_value::<Vec<SurveyStationRecord>>(s.clone()) {
                surveys.append(&mut part);
            }
        }
        if let Some(a) = v.get("assays") {
            if let Ok(mut part) = serde_json::from_value::<Vec<IntervalSampleRecord>>(a.clone()) {
                assays.append(&mut part);
            }
        }
    }

    let merged = serde_json::json!({
        "collars": collars,
        "surveys": surveys,
        "assays": assays,
    });
    let bytes = serde_json::to_vec(&merged)?;
    let key = format!("graphs/{}/nodes/{}/ingest.json", job.graph_id, job.node_id);
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

/// Minimal straight-hole desurvey from collars + surveys; writes trajectory JSON.
pub async fn run_desurvey_trajectory(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let ingest = job
        .input_artifact_refs
        .iter()
        .find(|a| a.key.ends_with("ingest.json"))
        .or_else(|| job.input_artifact_refs.first())
        .ok_or_else(|| NodeError::InvalidConfig("missing ingest artifact".into()))?;
    let ingest_path = ctx.artifact_root.join(&ingest.key);
    let raw = fs::read(&ingest_path).await?;
    let v: serde_json::Value = serde_json::from_slice(&raw)?;
    let collars: Vec<CollarRecord> = serde_json::from_value(v["collars"].clone()).unwrap_or_default();
    let surveys: Vec<SurveyStationRecord> =
        serde_json::from_value(v["surveys"].clone()).unwrap_or_default();

    let mut segments = Vec::new();
    for c in &collars {
        let mut stations: Vec<&SurveyStationRecord> = surveys
            .iter()
            .filter(|s| s.hole_id == c.hole_id)
            .collect();
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
            let z_to = prev_z - dz_vert;
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
    let key = format!("graphs/{}/nodes/{}/trajectory.json", job.graph_id, job.node_id);
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

pub async fn run_dem_integrate_stub(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let stub = serde_json::json!({ "dem": "stub", "graph": job.graph_id });
    let bytes = serde_json::to_vec(&stub)?;
    let key = format!("graphs/{}/nodes/{}/dem_stub.json", job.graph_id, job.node_id);
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

pub async fn run_block_model_stub(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let nx = 4u32;
    let ny = 4u32;
    let nz = 4u32;
    let n = (nx * ny * nz) as usize;
    let mut grid = vec![0f32; n];
    for i in 0..n {
        grid[i] = i as f32 * 0.1;
    }
    let meta = serde_json::json!({
        "nx": nx, "ny": ny, "nz": nz,
        "origin_x": 0.0, "origin_y": 0.0, "origin_z": 0.0,
        "cell_x": 10.0, "cell_y": 10.0, "cell_z": 5.0,
    });
    let meta_bytes = serde_json::to_vec(&meta)?;
    let meta_key = format!("graphs/{}/nodes/{}/block_model_meta.json", job.graph_id, job.node_id);
    let meta_ref = write_artifact(ctx, &meta_key, &meta_bytes, Some("application/json")).await?;

    let bin: Vec<u8> = grid.iter().flat_map(|f| f.to_le_bytes()).collect();
    let bin_key = format!("graphs/{}/nodes/{}/block_model_f32.bin", job.graph_id, job.node_id);
    let bin_ref = write_artifact(ctx, &bin_key, &bin, Some("application/octet-stream")).await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![meta_ref.clone(), bin_ref.clone()],
        content_hashes: vec![meta_ref.content_hash, bin_ref.content_hash],
        error_message: None,
    })
}
