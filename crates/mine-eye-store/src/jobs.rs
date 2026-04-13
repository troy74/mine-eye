use mine_eye_types::{JobEnvelope, JobResult, JobStatus};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use crate::StoreError;

#[derive(Debug, Clone)]
pub struct JobRuntimeStatus {
    pub job_id: Uuid,
    pub status: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub finished_at: Option<chrono::DateTime<chrono::Utc>>,
    pub progress: Option<Value>,
}

#[async_trait::async_trait]
pub trait JobQueue: Send + Sync {
    async fn enqueue(&self, envelope: &JobEnvelope) -> Result<Uuid, StoreError>;
    async fn claim_next(&self) -> Result<Option<(Uuid, JobEnvelope)>, StoreError>;
    async fn complete(&self, job_row_id: Uuid, result: &JobResult) -> Result<(), StoreError>;
    async fn update_progress(
        &self,
        job_row_id: Uuid,
        stage: &str,
        percent: Option<f64>,
        message: Option<&str>,
        stats: Option<Value>,
    ) -> Result<(), StoreError>;
    async fn touch_heartbeat(&self, job_row_id: Uuid) -> Result<(), StoreError>;
    async fn reap_stale_running(&self, stale_after_seconds: i64) -> Result<u64, StoreError>;
    async fn latest_for_node(
        &self,
        graph_id: Uuid,
        node_id: Uuid,
    ) -> Result<Option<JobRuntimeStatus>, StoreError>;
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
            r#"
            UPDATE job_queue
            SET status = 'running',
                started_at = now(),
                payload = jsonb_set(
                    payload,
                    '{runtime_progress}',
                    $2::jsonb,
                    true
                )
            WHERE id = $1
            "#,
        )
        .bind(row_id)
        .bind(json!({
            "stage": "queued_to_running",
            "percent": 0.01,
            "message": "Job claimed by worker",
            "updated_at": chrono::Utc::now(),
            "heartbeat_at": chrono::Utc::now(),
        }))
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
                payload = jsonb_set(
                    payload,
                    '{runtime_progress}',
                    $5::jsonb,
                    true
                ),
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
        .bind(json!({
            "stage": if result.status == JobStatus::Succeeded { "completed" } else { "failed" },
            "percent": if result.status == JobStatus::Succeeded { 1.0 } else { 0.0 },
            "message": result.error_message.clone().unwrap_or_else(|| "Job finished".to_string()),
            "updated_at": chrono::Utc::now(),
            "heartbeat_at": chrono::Utc::now(),
        }))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn update_progress(
        &self,
        job_row_id: Uuid,
        stage: &str,
        percent: Option<f64>,
        message: Option<&str>,
        stats: Option<Value>,
    ) -> Result<(), StoreError> {
        let progress = json!({
            "stage": stage,
            "percent": percent.map(|p| p.clamp(0.0, 1.0)),
            "message": message.unwrap_or(""),
            "stats": stats.unwrap_or(Value::Null),
            "updated_at": chrono::Utc::now(),
            "heartbeat_at": chrono::Utc::now(),
        });
        sqlx::query(
            r#"
            UPDATE job_queue
            SET payload = jsonb_set(payload, '{runtime_progress}', $2::jsonb, true)
            WHERE id = $1
            "#,
        )
        .bind(job_row_id)
        .bind(progress)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn touch_heartbeat(&self, job_row_id: Uuid) -> Result<(), StoreError> {
        sqlx::query(
            r#"
            UPDATE job_queue
            SET payload = jsonb_set(
                COALESCE(payload, '{}'::jsonb),
                '{runtime_progress,heartbeat_at}',
                to_jsonb(now()),
                true
            )
            WHERE id = $1
            "#,
        )
        .bind(job_row_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn reap_stale_running(&self, stale_after_seconds: i64) -> Result<u64, StoreError> {
        let n = stale_after_seconds.max(30);
        let res = sqlx::query(
            r#"
            UPDATE job_queue
            SET status = 'failed',
                error_message = COALESCE(
                    error_message,
                    format('stale running job reaped after %s seconds without heartbeat', $1)
                ),
                result = COALESCE(
                    result,
                    jsonb_build_object(
                        'status', 'failed',
                        'error_message', format('stale running job reaped after %s seconds without heartbeat', $1)
                    )
                ),
                finished_at = now()
            WHERE status = 'running'
              AND now() - COALESCE(
                    NULLIF(payload->'runtime_progress'->>'heartbeat_at', '')::timestamptz,
                    NULLIF(payload->'runtime_progress'->>'updated_at', '')::timestamptz,
                    started_at,
                    created_at
                  ) > make_interval(secs => $1)
            "#,
        )
        .bind(n)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
    }

    async fn latest_for_node(
        &self,
        graph_id: Uuid,
        node_id: Uuid,
    ) -> Result<Option<JobRuntimeStatus>, StoreError> {
        let row: Option<(
            Uuid,
            String,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
            Option<chrono::DateTime<chrono::Utc>>,
            Option<Value>,
        )> = sqlx::query_as(
            r#"
            SELECT
              id,
              status,
              created_at,
              started_at,
              finished_at,
              payload->'runtime_progress' as progress
            FROM job_queue
            WHERE graph_id = $1
              AND node_id = $2
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(graph_id)
        .bind(node_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(
            |(job_id, status, created_at, started_at, finished_at, progress)| JobRuntimeStatus {
                job_id,
                status,
                created_at,
                started_at,
                finished_at,
                progress,
            },
        ))
    }
}
