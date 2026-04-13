use mine_eye_types::{JobEnvelope, JobResult, JobStatus};

use crate::executor::ExecutionContext;
use crate::NodeError;

pub async fn run_dem_integrate_stub(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let stub = serde_json::json!({ "dem": "stub", "graph": job.graph_id });
    let bytes = serde_json::to_vec(&stub)?;
    let key = format!(
        "graphs/{}/nodes/{}/dem_stub.json",
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

pub async fn run_block_model_stub(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let nx = 4u32;
    let ny = 4u32;
    let nz = 4u32;
    let n = (nx * ny * nz) as usize;
    let mut grid = vec![0f32; n];
    for (i, val) in grid.iter_mut().enumerate() {
        *val = i as f32 * 0.1;
    }
    let meta = serde_json::json!({
        "nx": nx, "ny": ny, "nz": nz,
        "origin_x": 0.0, "origin_y": 0.0, "origin_z": 0.0,
        "cell_x": 10.0, "cell_y": 10.0, "cell_z": 5.0,
    });
    let meta_bytes = serde_json::to_vec(&meta)?;
    let meta_key = format!(
        "graphs/{}/nodes/{}/block_model_meta.json",
        job.graph_id, job.node_id
    );
    let meta_ref =
        super::runtime::write_artifact(ctx, &meta_key, &meta_bytes, Some("application/json"))
            .await?;

    let bin: Vec<u8> = grid.iter().flat_map(|f| f.to_le_bytes()).collect();
    let bin_key = format!(
        "graphs/{}/nodes/{}/block_model_f32.bin",
        job.graph_id, job.node_id
    );
    let bin_ref =
        super::runtime::write_artifact(ctx, &bin_key, &bin, Some("application/octet-stream"))
            .await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![meta_ref.clone(), bin_ref.clone()],
        content_hashes: vec![meta_ref.content_hash, bin_ref.content_hash],
        error_message: None,
    })
}
