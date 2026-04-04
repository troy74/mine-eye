use mine_eye_types::{JobEnvelope, JobResult, JobStatus};
use sqlx::PgPool;
use uuid::Uuid;

use crate::StoreError;

#[async_trait::async_trait]
pub trait JobQueue: Send + Sync {
    async fn enqueue(&self, envelope: &JobEnvelope) -> Result<Uuid, StoreError>;
    async fn claim_next(&self) -> Result<Option<(Uuid, JobEnvelope)>, StoreError>;
    async fn complete(&self, job_row_id: Uuid, result: &JobResult) -> Result<(), StoreError>;
}

pub struct PgJobQueue {
    pool: PgPool,
}

impl PgJobQueue {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl JobQueue for PgJobQueue {
    async fn enqueue(&self, envelope: &JobEnvelope) -> Result<Uuid, StoreError> {
        // Coalesce duplicate work: if same node/config is already queued or running,
        // reuse that queue row instead of creating another duplicate job.
        let existing: Option<(Uuid,)> = sqlx::query_as(
            r#"
            SELECT id
            FROM job_queue
            WHERE graph_id = $1
              AND node_id = $2
              AND status IN ('queued','running')
              AND (payload->>'config_hash') = $3
              AND COALESCE(payload->>'input_fingerprint', '') = $4
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(envelope.graph_id)
        .bind(envelope.node_id)
        .bind(&envelope.config_hash)
        .bind(&envelope.input_fingerprint)
        .fetch_optional(&self.pool)
        .await?;
        if let Some((id,)) = existing {
            return Ok(id);
        }

        let id = Uuid::new_v4();
        let payload = serde_json::to_value(envelope)?;
        sqlx::query(
            r#"
            INSERT INTO job_queue (id, graph_id, node_id, payload, status)
            VALUES ($1, $2, $3, $4, 'queued')
            "#,
        )
        .bind(id)
        .bind(envelope.graph_id)
        .bind(envelope.node_id)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    async fn claim_next(&self) -> Result<Option<(Uuid, JobEnvelope)>, StoreError> {
        let mut tx = self.pool.begin().await?;
        let row: Option<(Uuid, serde_json::Value)> = sqlx::query_as(
            r#"
            SELECT id, payload FROM job_queue
            WHERE status = 'queued'
            ORDER BY created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .fetch_optional(&mut *tx)
        .await?;

        let Some((row_id, payload)) = row else {
            tx.commit().await?;
            return Ok(None);
        };

        sqlx::query(
            r#"UPDATE job_queue SET status = 'running', started_at = now() WHERE id = $1"#,
        )
        .bind(row_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        let envelope: JobEnvelope = serde_json::from_value(payload)?;
        Ok(Some((row_id, envelope)))
    }

    async fn complete(&self, job_row_id: Uuid, result: &JobResult) -> Result<(), StoreError> {
        let status_str = match result.status {
            JobStatus::Queued => "queued",
            JobStatus::Running => "running",
            JobStatus::Succeeded => "succeeded",
            JobStatus::Failed => "failed",
        };
        let result_json = serde_json::to_value(result)?;
        sqlx::query(
            r#"
            UPDATE job_queue
            SET status = $2,
                result = $3,
                error_message = $4,
                finished_at = now()
            WHERE id = $1
            "#,
        )
        .bind(job_row_id)
        .bind(status_str)
        .bind(result_json)
        .bind(&result.error_message)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
