use mine_eye_types::{JobEnvelope, JobResult};

use crate::executor::ExecutionContext;
use crate::NodeError;

pub async fn run_assay_heatmap(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    super::runtime::run_assay_heatmap_impl(ctx, job).await
}

pub async fn run_surface_iso_extract(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    super::runtime::run_surface_iso_extract_impl(ctx, job).await
}

pub async fn run_terrain_adjust(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    super::runtime::run_terrain_adjust_impl(ctx, job).await
}

pub async fn run_xyz_to_surface(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    super::runtime::run_xyz_to_surface_impl(ctx, job).await
}

pub async fn run_dem_fetch(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    super::runtime::run_dem_fetch_impl(ctx, job).await
}
