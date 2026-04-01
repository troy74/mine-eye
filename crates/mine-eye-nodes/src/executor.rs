use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use mine_eye_types::{JobEnvelope, JobResult};
use crate::kinds::{
    run_assay_heatmap, run_assay_ingest, run_block_model_stub, run_collar_ingest, run_dem_integrate_stub,
    run_desurvey_trajectory, run_drillhole_ingest, run_drillhole_merge, run_drillhole_model,
    run_plan_view_2d, run_plan_view_3d,
    run_surface_iso_extract, run_terrain_adjust,
    run_surface_sample_ingest, run_survey_ingest,
};
use crate::NodeError;

pub struct ExecutionContext<'a> {
    pub artifact_root: &'a Path,
}

#[async_trait]
pub trait NodeExecutor: Send + Sync {
    async fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        job: &JobEnvelope,
    ) -> Result<JobResult, NodeError>;
}

pub struct RegistryExecutor {
    inner: HashMap<String, Arc<dyn NodeExecutor>>,
}

impl RegistryExecutor {
    pub fn new() -> Self {
        let mut inner: HashMap<String, Arc<dyn NodeExecutor>> = HashMap::new();
        inner.insert(
            "drillhole_ingest".into(),
            Arc::new(DrillholeIngestExecutor),
        );
        inner.insert("collar_ingest".into(), Arc::new(CollarIngestExecutor));
        inner.insert("survey_ingest".into(), Arc::new(SurveyIngestExecutor));
        inner.insert(
            "surface_sample_ingest".into(),
            Arc::new(SurfaceSampleIngestExecutor),
        );
        inner.insert("assay_ingest".into(), Arc::new(AssayIngestExecutor));
        inner.insert("assay_heatmap".into(), Arc::new(AssayHeatmapExecutor));
        inner.insert("surface_iso_extract".into(), Arc::new(SurfaceIsoExtractExecutor));
        inner.insert("terrain_adjust".into(), Arc::new(TerrainAdjustExecutor));
        inner.insert("drillhole_merge".into(), Arc::new(DrillholeMergeExecutor));
        inner.insert("drillhole_model".into(), Arc::new(DrillholeModelExecutor));
        inner.insert(
            "desurvey_trajectory".into(),
            Arc::new(DesurveyExecutor),
        );
        inner.insert("dem_integrate".into(), Arc::new(DemExecutor));
        inner.insert("block_model_basic".into(), Arc::new(BlockModelExecutor));
        inner.insert("plan_view_2d".into(), Arc::new(PlanView2DExecutor));
        inner.insert("plan_view_3d".into(), Arc::new(PlanView3DExecutor));
        inner.insert("cesium_display_node".into(), Arc::new(PlanView3DExecutor));
        Self { inner }
    }
}

#[async_trait]
impl NodeExecutor for RegistryExecutor {
    async fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        job: &JobEnvelope,
    ) -> Result<JobResult, NodeError> {
        let Some(ex) = self.inner.get(&job.node_kind) else {
            return Err(NodeError::UnknownKind(job.node_kind.clone()));
        };
        ex.execute(ctx, job).await
    }
}

struct CollarIngestExecutor;

#[async_trait]
impl NodeExecutor for CollarIngestExecutor {
    async fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        job: &JobEnvelope,
    ) -> Result<JobResult, NodeError> {
        run_collar_ingest(ctx, job).await
    }
}

struct SurveyIngestExecutor;

#[async_trait]
impl NodeExecutor for SurveyIngestExecutor {
    async fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        job: &JobEnvelope,
    ) -> Result<JobResult, NodeError> {
        run_survey_ingest(ctx, job).await
    }
}

struct AssayIngestExecutor;

#[async_trait]
impl NodeExecutor for AssayIngestExecutor {
    async fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        job: &JobEnvelope,
    ) -> Result<JobResult, NodeError> {
        run_assay_ingest(ctx, job).await
    }
}

struct SurfaceSampleIngestExecutor;

#[async_trait]
impl NodeExecutor for SurfaceSampleIngestExecutor {
    async fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        job: &JobEnvelope,
    ) -> Result<JobResult, NodeError> {
        run_surface_sample_ingest(ctx, job).await
    }
}

struct AssayHeatmapExecutor;

#[async_trait]
impl NodeExecutor for AssayHeatmapExecutor {
    async fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        job: &JobEnvelope,
    ) -> Result<JobResult, NodeError> {
        run_assay_heatmap(ctx, job).await
    }
}

struct SurfaceIsoExtractExecutor;

#[async_trait]
impl NodeExecutor for SurfaceIsoExtractExecutor {
    async fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        job: &JobEnvelope,
    ) -> Result<JobResult, NodeError> {
        run_surface_iso_extract(ctx, job).await
    }
}

struct TerrainAdjustExecutor;

#[async_trait]
impl NodeExecutor for TerrainAdjustExecutor {
    async fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        job: &JobEnvelope,
    ) -> Result<JobResult, NodeError> {
        run_terrain_adjust(ctx, job).await
    }
}

struct DrillholeMergeExecutor;

#[async_trait]
impl NodeExecutor for DrillholeMergeExecutor {
    async fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        job: &JobEnvelope,
    ) -> Result<JobResult, NodeError> {
        run_drillhole_merge(ctx, job).await
    }
}

struct DrillholeIngestExecutor;

#[async_trait]
impl NodeExecutor for DrillholeIngestExecutor {
    async fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        job: &JobEnvelope,
    ) -> Result<JobResult, NodeError> {
        run_drillhole_ingest(ctx, job).await
    }
}

struct DrillholeModelExecutor;

#[async_trait]
impl NodeExecutor for DrillholeModelExecutor {
    async fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        job: &JobEnvelope,
    ) -> Result<JobResult, NodeError> {
        run_drillhole_model(ctx, job).await
    }
}

struct DesurveyExecutor;

#[async_trait]
impl NodeExecutor for DesurveyExecutor {
    async fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        job: &JobEnvelope,
    ) -> Result<JobResult, NodeError> {
        run_desurvey_trajectory(ctx, job).await
    }
}

struct DemExecutor;

#[async_trait]
impl NodeExecutor for DemExecutor {
    async fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        job: &JobEnvelope,
    ) -> Result<JobResult, NodeError> {
        run_dem_integrate_stub(ctx, job).await
    }
}

struct BlockModelExecutor;

#[async_trait]
impl NodeExecutor for BlockModelExecutor {
    async fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        job: &JobEnvelope,
    ) -> Result<JobResult, NodeError> {
        run_block_model_stub(ctx, job).await
    }
}

struct PlanView2DExecutor;

#[async_trait]
impl NodeExecutor for PlanView2DExecutor {
    async fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        job: &JobEnvelope,
    ) -> Result<JobResult, NodeError> {
        run_plan_view_2d(ctx, job).await
    }
}

struct PlanView3DExecutor;

#[async_trait]
impl NodeExecutor for PlanView3DExecutor {
    async fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        job: &JobEnvelope,
    ) -> Result<JobResult, NodeError> {
        run_plan_view_3d(ctx, job).await
    }
}

pub type NodeExecutorRegistry = RegistryExecutor;
