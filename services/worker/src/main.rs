use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use mine_eye_nodes::{ExecutionContext, NodeExecutor, NodeExecutorRegistry, ProgressUpdate};
use mine_eye_store::{JobQueue, PgJobQueue, PgStore};
use mine_eye_types::{CacheState, ExecutionState, JobStatus};
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Local dev convenience: load .env.dev first, then .env if present.
    let _ = dotenvy::from_filename(".env.dev");
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mine_eye_worker=info".into()),
        )
        .init();

    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@127.0.0.1:5433/mine_eye".into());
    let artifact_root = env::var("ARTIFACT_ROOT").unwrap_or_else(|_| "./data/artifacts".into());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
    let store = PgStore::new(pool.clone());
    let jobs = Arc::new(PgJobQueue::new(pool));
    let executor = NodeExecutorRegistry::new();
    let artifact_path = PathBuf::from(&artifact_root);

    tokio::fs::create_dir_all(&artifact_root).await?;

    tracing::info!("worker started, polling job_queue");

    loop {
        match jobs.claim_next().await {
            Ok(Some((row_id, mut envelope))) => {
                let jobs_for_progress = jobs.clone();
                let row_id_for_progress = row_id;
                let (hb_tx, mut hb_rx) = tokio::sync::oneshot::channel::<()>();
                let hb_jobs = jobs.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                                let _ = hb_jobs.touch_heartbeat(row_id_for_progress).await;
                            }
                            _ = &mut hb_rx => {
                                break;
                            }
                        }
                    }
                });
                let ctx = ExecutionContext {
                    artifact_root: artifact_path.as_path(),
                    progress: Some(Arc::new(move |u: ProgressUpdate| {
                        let jobs = jobs_for_progress.clone();
                        tokio::spawn(async move {
                            let _ = jobs
                                .update_progress(
                                    row_id_for_progress,
                                    &u.stage,
                                    u.percent,
                                    u.message.as_deref(),
                                    u.stats,
                                )
                                .await;
                        });
                    })),
                };
                tracing::info!(
                    job_id = %envelope.job_id,
                    run_id = %envelope.run_id,
                    node = %envelope.node_id,
                    "running job"
                );
                let _ = store
                    .update_node_execution(
                        envelope.node_id,
                        ExecutionState::Running,
                        mine_eye_types::CacheState::Miss,
                        None,
                        None,
                    )
                    .await;

                // Envelopes snapshot inputs at enqueue time; upstream jobs may not have written
                // artifacts yet. Re-bind from DB so desurvey/transform nodes see ingest output.
                match store
                    .resolve_input_artifact_refs(envelope.graph_id, envelope.node_id)
                    .await
                {
                    Ok(refs) => {
                        envelope.input_artifact_refs = refs;
                    }
                    Err(e) => tracing::warn!(error = %e, "resolve_input_artifact_refs"),
                }

                let result = executor.execute(&ctx, &envelope).await;
                let _ = hb_tx.send(());

                match result {
                    Ok(r) => {
                        if r.status == JobStatus::Succeeded {
                            let artifacts: Vec<(String, String, Option<String>)> = r
                                .output_artifact_refs
                                .iter()
                                .map(|a| {
                                    (
                                        a.key.clone(),
                                        a.content_hash.clone(),
                                        a.media_type.clone(),
                                    )
                                })
                                .collect();
                            if let Err(e) = store
                                .replace_node_artifacts(envelope.node_id, &artifacts)
                                .await
                            {
                                tracing::error!("persist artifacts: {e}");
                            }
                            let primary_hash = r.content_hashes.first().map(String::as_str);
                            let _ = store
                                .update_node_execution(
                                    envelope.node_id,
                                    ExecutionState::Succeeded,
                                    CacheState::Hit,
                                    primary_hash,
                                    None,
                                )
                                .await;
                        } else if r.status == JobStatus::Failed {
                            let msg = r
                                .error_message
                                .as_deref()
                                .unwrap_or("job returned failed status");
                            let _ = store
                                .update_node_execution(
                                    envelope.node_id,
                                    ExecutionState::Failed,
                                    CacheState::Stale,
                                    None,
                                    Some(msg),
                                )
                                .await;
                        }
                        let _ = jobs.complete(row_id, &r).await;
                    }
                    Err(e) => {
                        tracing::error!("job failed: {e}");
                        let err_txt = e.to_string();
                        let r = mine_eye_types::JobResult {
                            job_id: envelope.job_id,
                            status: JobStatus::Failed,
                            output_artifact_refs: vec![],
                            content_hashes: vec![],
                            error_message: Some(err_txt.clone()),
                        };
                        let _ = store
                            .update_node_execution(
                                envelope.node_id,
                                ExecutionState::Failed,
                                CacheState::Stale,
                                None,
                                Some(err_txt.as_str()),
                            )
                            .await;
                        let _ = jobs.complete(row_id, &r).await;
                    }
                }
            }
            Ok(None) => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => {
                tracing::error!("claim job: {e}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}
