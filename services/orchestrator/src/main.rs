use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
mod ingest_synth;

use mine_eye_graph::propagate_stale;
use mine_eye_scheduler::{collect_dirty_nodes, Scheduler};
use mine_eye_store::{JobQueue, PgJobQueue, PgStore, StoreError};
use mine_eye_types::{
    ArtifactRef, CacheState, CrsRecord, ExecutionState, GraphMeta, LineageMeta, LockState,
    NodeCategory, NodeConfig, NodeExecutionPolicy, NodeRecord, OwnerRef, PropagationPolicy,
    QualityPolicy, RecomputePolicy, SemanticPortType, WorkspaceStatus,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::ServeDir;
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    store: Arc<PgStore>,
    jobs: Arc<PgJobQueue>,
    scheduler: Arc<Scheduler>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mine_eye_orchestrator=info,tower_http=info".into()),
        )
        .init();

    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@127.0.0.1:5433/mine_eye".into());
    let artifact_root = env::var("ARTIFACT_ROOT").unwrap_or_else(|_| "./data/artifacts".into());
    let listen = env::var("LISTEN").unwrap_or_else(|_| "0.0.0.0:3000".into());

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;
    let store = Arc::new(PgStore::new(pool.clone()));
    store.migrate().await?;

    let jobs = Arc::new(PgJobQueue::new(pool));
    tokio::fs::create_dir_all(&artifact_root).await?;

    let state = AppState {
        store,
        jobs,
        scheduler: Arc::new(Scheduler::default()),
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let artifact_dir = PathBuf::from(&artifact_root);
    let app = Router::new()
        .route("/health", get(health))
        .route("/workspaces", post(create_workspace))
        .route("/workspaces/{ws_id}/graphs", post(create_graph))
        .route("/graphs/{graph_id}", get(get_graph))
        .route("/graphs/{graph_id}/nodes", post(add_node))
        .route(
            "/graphs/{graph_id}/nodes/{node_id}",
            patch(patch_node_params).delete(delete_node),
        )
        .route("/graphs/{graph_id}/edges", post(add_edge))
        .route("/graphs/{graph_id}/edges/{edge_id}", delete(delete_edge))
        .route("/graphs/{graph_id}/run", post(run_graph))
        .route("/graphs/{graph_id}/artifacts", get(list_artifacts))
        .route("/graphs/{graph_id}/ai/suggest", post(ai_suggest))
        .route("/graphs/{graph_id}/ai/suggestions", get(list_ai_suggestions))
        .route("/ai/suggestions/{id}/confirm", post(ai_confirm))
        .route("/demo/seed", post(demo_seed))
        .nest_service(
            "/files",
            ServeDir::new(artifact_dir.clone()).append_index_html_on_directories(false),
        )
        .layer(cors)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&listen).await?;
    tracing::info!("orchestrator listening on http://{}", listen);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

#[derive(Deserialize)]
struct CreateWorkspaceReq {
    name: String,
    owner_user_id: String,
    project_crs: Option<CrsRecord>,
}

#[derive(Serialize)]
struct IdResp {
    id: Uuid,
}

async fn create_workspace(
    State(s): State<AppState>,
    Json(body): Json<CreateWorkspaceReq>,
) -> Result<Json<IdResp>, (StatusCode, String)> {
    let owner = OwnerRef {
        user_id: body.owner_user_id,
    };
    let crs = body
        .project_crs
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let id = s
        .store
        .create_workspace(&body.name, owner, crs)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(IdResp { id }))
}

#[derive(Deserialize)]
struct CreateGraphReq {
    name: String,
    workspace_id: Uuid,
    owner_user_id: String,
}

async fn create_graph(
    State(s): State<AppState>,
    Path(ws_id): Path<Uuid>,
    Json(body): Json<CreateGraphReq>,
) -> Result<Json<IdResp>, (StatusCode, String)> {
    if body.workspace_id != ws_id {
        return Err((StatusCode::BAD_REQUEST, "workspace mismatch".into()));
    }
    let graph_id = Uuid::new_v4();
    let meta = GraphMeta {
        graph_id,
        workspace_id: ws_id,
        name: body.name.clone(),
        owner: OwnerRef {
            user_id: body.owner_user_id,
        },
        status: WorkspaceStatus::Draft,
        lock: LockState::Unlocked,
        approval: None,
    };
    s.store
        .create_graph(ws_id, &body.name, &meta)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(IdResp { id: graph_id }))
}

#[derive(Serialize)]
struct GraphView {
    graph_id: Uuid,
    workspace_id: Option<Uuid>,
    project_crs: Option<CrsRecord>,
    nodes: Vec<NodeRecord>,
    edges: Vec<mine_eye_graph::EdgeRef>,
}

async fn get_graph(
    State(s): State<AppState>,
    Path(graph_id): Path<Uuid>,
) -> Result<Json<GraphView>, (StatusCode, String)> {
    let snap = s
        .store
        .load_graph(graph_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let ws = s
        .store
        .graph_workspace_meta(graph_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let (workspace_id, project_crs) = match ws {
        Some((id, crs)) => (Some(id), crs),
        None => (None, None),
    };
    Ok(Json(GraphView {
        graph_id,
        workspace_id,
        project_crs,
        nodes: snap.nodes.into_values().collect(),
        edges: snap.edges,
    }))
}

#[derive(Deserialize)]
struct AddNodeReq {
    category: String,
    kind: String,
    params: serde_json::Value,
    policy: Option<NodeExecutionPolicy>,
}

async fn add_node(
    State(s): State<AppState>,
    Path(graph_id): Path<Uuid>,
    Json(body): Json<AddNodeReq>,
) -> Result<Json<IdResp>, (StatusCode, String)> {
    let category = match body.category.as_str() {
        "input" => NodeCategory::Input,
        "transform" => NodeCategory::Transform,
        "model" => NodeCategory::Model,
        "qa" => NodeCategory::Qa,
        "visualisation" => NodeCategory::Visualisation,
        "export" => NodeCategory::Export,
        _ => NodeCategory::Transform,
    };
    let id = Uuid::new_v4();
    let node = NodeRecord {
        id,
        graph_id,
        category,
        config: NodeConfig {
            version: 1,
            kind: body.kind,
            params: body.params,
        },
        execution: ExecutionState::Idle,
        cache: CacheState::Stale,
        policy: body.policy.unwrap_or_default(),
        ports: Vec::new(),
        lineage: LineageMeta::default(),
        content_hash: None,
        last_error: None,
    };
    s.store
        .upsert_node(&node)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(IdResp { id }))
}

#[derive(Deserialize)]
struct PatchNodeParamsReq {
    params: serde_json::Value,
}

async fn patch_node_params(
    State(s): State<AppState>,
    Path((graph_id, node_id)): Path<(Uuid, Uuid)>,
    Json(body): Json<PatchNodeParamsReq>,
) -> Result<Json<NodeRecord>, (StatusCode, String)> {
    let node = s
        .store
        .patch_node_params(graph_id, node_id, body.params)
        .await
        .map_err(|e| match e {
            StoreError::NotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        })?;
    Ok(Json(node))
}

#[derive(Deserialize)]
struct AddEdgeReq {
    from_node: Uuid,
    from_port: String,
    to_node: Uuid,
    to_port: String,
    semantic_type: SemanticPortType,
}

async fn add_edge(
    State(s): State<AppState>,
    Path(graph_id): Path<Uuid>,
    Json(body): Json<AddEdgeReq>,
) -> Result<Json<IdResp>, (StatusCode, String)> {
    let eid = s
        .store
        .add_edge(
            graph_id,
            body.from_node,
            &body.from_port,
            body.to_node,
            &body.to_port,
            body.semantic_type,
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(IdResp { id: eid }))
}

async fn delete_node(
    State(s): State<AppState>,
    Path((graph_id, node_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, (StatusCode, String)> {
    s.store
        .delete_node(graph_id, node_id)
        .await
        .map_err(|e| match e {
            StoreError::NotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_edge(
    State(s): State<AppState>,
    Path((graph_id, edge_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, (StatusCode, String)> {
    s.store
        .delete_edge(graph_id, edge_id)
        .await
        .map_err(|e| match e {
            StoreError::NotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        })?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct RunGraphReq {
    /// If empty, all stale nodes are roots (full graph dirty).
    dirty_roots: Option<Vec<Uuid>>,
    /// Per-node inline payloads for workers (e.g. ingest JSON).
    input_payloads: Option<HashMap<Uuid, serde_json::Value>>,
}

async fn run_graph(
    State(s): State<AppState>,
    Path(graph_id): Path<Uuid>,
    Json(body): Json<RunGraphReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let snapshot = s
        .store
        .load_graph(graph_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let roots = body.dirty_roots.unwrap_or_else(|| snapshot.nodes.keys().copied().collect());
    let dirty = collect_dirty_nodes(&snapshot, &roots);
    let input_map = build_input_artifacts(&s.store, &snapshot)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let project_crs: Option<CrsRecord> = s
        .store
        .graph_workspace_meta(graph_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .and_then(|(_, crs)| crs);

    let plan = s.scheduler.plan(
        &snapshot,
        &dirty,
        &input_map,
        project_crs.clone(),
    );

    let mut queued = Vec::new();
    for mut job in plan.jobs {
        if let Some(ref payloads) = body.input_payloads {
            if let Some(p) = payloads.get(&job.node_id) {
                job.input_payload = Some(p.clone());
            }
        }
        if job.input_payload.is_none() {
            if let Some(node) = snapshot.nodes.get(&job.node_id) {
                if let Some(p) =
                    ingest_synth::synthesize_input_payload(node, project_crs.as_ref())
                {
                    job.input_payload = Some(p);
                }
            }
        }
        let row_id = s
            .jobs
            .enqueue(&job)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        queued.push(serde_json::json!({ "queue_row": row_id, "job_id": job.job_id, "node_id": job.node_id }));
    }

    Ok(Json(serde_json::json!({
        "queued": queued,
        "skipped_manual": plan.skipped_manual,
    })))
}

async fn build_input_artifacts(
    store: &PgStore,
    snapshot: &mine_eye_graph::GraphSnapshot,
) -> Result<HashMap<Uuid, Vec<ArtifactRef>>, mine_eye_store::StoreError> {
    let mut m: HashMap<Uuid, Vec<ArtifactRef>> = HashMap::new();
    for edge in &snapshot.edges {
        let arts = store.list_artifacts_for_node(edge.from_node).await?;
        let refs: Vec<ArtifactRef> = arts
            .into_iter()
            .map(|(key, content_hash, media_type)| ArtifactRef {
                key,
                content_hash,
                media_type,
            })
            .collect();
        m.entry(edge.to_node).or_default().extend(refs);
    }
    Ok(m)
}

#[derive(Serialize)]
struct ArtifactEntry {
    node_id: Uuid,
    key: String,
    url: String,
    content_hash: String,
}

async fn list_artifacts(
    State(s): State<AppState>,
    Path(graph_id): Path<Uuid>,
) -> Result<Json<Vec<ArtifactEntry>>, (StatusCode, String)> {
    let snap = s
        .store
        .load_graph(graph_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let mut out = Vec::new();
    for nid in snap.nodes.keys() {
        let rows = s
            .store
            .list_artifacts_for_node(*nid)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        for (key, hash, _) in rows {
            out.push(ArtifactEntry {
                node_id: *nid,
                key: key.clone(),
                url: format!("/files/{}", key),
                content_hash: hash,
            });
        }
    }
    Ok(Json(out))
}

#[derive(Deserialize)]
struct AiSuggestReq {
    kind: String,
    payload: serde_json::Value,
}

async fn list_ai_suggestions(
    State(s): State<AppState>,
    Path(graph_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let rows = s
        .store
        .list_ai_suggestions(graph_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let v: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(id, kind, payload, status)| {
            serde_json::json!({
                "id": id,
                "kind": kind,
                "payload": payload,
                "status": status,
            })
        })
        .collect();
    Ok(Json(serde_json::Value::Array(v)))
}

async fn ai_suggest(
    State(s): State<AppState>,
    Path(graph_id): Path<Uuid>,
    Json(body): Json<AiSuggestReq>,
) -> Result<Json<IdResp>, (StatusCode, String)> {
    let id = s
        .store
        .insert_ai_suggestion(graph_id, &body.kind, body.payload)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(IdResp { id }))
}

#[derive(Deserialize)]
struct AiConfirmReq {
    user_id: String,
}

async fn ai_confirm(
    State(s): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<AiConfirmReq>,
) -> Result<StatusCode, (StatusCode, String)> {
    s.store
        .confirm_ai_suggestion(id, &body.user_id)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

/// Seeds demo: collar + survey + assay inputs → merge → desurvey → dem → block ([V1SPEC §2](V1SPEC.md)).
async fn demo_seed(State(s): State<AppState>) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let ws = s
        .store
        .create_workspace(
            "demo",
            OwnerRef {
                user_id: "demo".into(),
            },
            Some(serde_json::to_value(CrsRecord::epsg(4326)).unwrap()),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let graph_id = Uuid::new_v4();
    let meta = GraphMeta {
        graph_id,
        workspace_id: ws,
        name: "demo-graph".into(),
        owner: OwnerRef {
            user_id: "demo".into(),
        },
        status: WorkspaceStatus::Draft,
        lock: LockState::Unlocked,
        approval: None,
    };
    s.store
        .create_graph(ws, "demo-graph", &meta)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let n_collar = Uuid::new_v4();
    let n_survey = Uuid::new_v4();
    let n_assay = Uuid::new_v4();
    let n_merge = Uuid::new_v4();
    let n_desurvey = Uuid::new_v4();
    let n_dem = Uuid::new_v4();
    let n_block = Uuid::new_v4();

    for (id, kind, cat) in [
        (n_collar, "collar_ingest", NodeCategory::Input),
        (n_survey, "survey_ingest", NodeCategory::Input),
        (n_assay, "assay_ingest", NodeCategory::Input),
        (n_merge, "drillhole_merge", NodeCategory::Transform),
        (n_desurvey, "desurvey_trajectory", NodeCategory::Transform),
        (n_dem, "dem_integrate", NodeCategory::Transform),
        (n_block, "block_model_basic", NodeCategory::Model),
    ] {
        let node = NodeRecord {
            id,
            graph_id,
            category: cat,
            config: NodeConfig {
                version: 1,
                kind: kind.into(),
                params: serde_json::json!({}),
            },
            execution: ExecutionState::Idle,
            cache: CacheState::Stale,
            policy: NodeExecutionPolicy::default(),
            ports: Vec::new(),
            lineage: LineageMeta::default(),
            content_hash: None,
            last_error: None,
        };
        s.store
            .upsert_node(&node)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    use SemanticPortType as S;
    // Semantic port types per V1SPEC; collar/survey/assay links use distinct types for graph UI.
    let edges_spec: [(Uuid, &str, Uuid, &str, S); 6] = [
        (n_collar, "collars", n_merge, "collars_in", S::PointSet),
        (n_survey, "surveys", n_merge, "surveys_in", S::TrajectorySet),
        (n_assay, "assays", n_merge, "assays_in", S::IntervalSet),
        (n_merge, "package", n_desurvey, "in", S::Table),
        (n_desurvey, "trajectory", n_dem, "in", S::TrajectorySet),
        (n_dem, "dem", n_block, "in", S::Raster),
    ];
    for (from, fp, to, tp, sem) in edges_spec {
        s.store
            .add_edge(graph_id, from, fp, to, tp, sem)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    let n_viewer = Uuid::new_v4();
    let viewer_node = NodeRecord {
        id: n_viewer,
        graph_id,
        category: NodeCategory::Visualisation,
        config: NodeConfig {
            version: 1,
            kind: "plan_view_2d".into(),
            params: serde_json::json!({}),
        },
        execution: ExecutionState::Idle,
        cache: CacheState::Stale,
        policy: NodeExecutionPolicy {
            recompute: RecomputePolicy::Manual,
            propagation: PropagationPolicy::Hold,
            quality: QualityPolicy::Preview,
        },
        ports: Vec::new(),
        lineage: LineageMeta::default(),
        content_hash: None,
        last_error: None,
    };
    s.store
        .upsert_node(&viewer_node)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    s.store
        .add_edge(
            graph_id,
            n_merge,
            "package",
            n_viewer,
            "in",
            SemanticPortType::Table,
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let collar_payload = serde_json::json!({
        "collars": [{ "hole_id": "DH-1", "x": 100.0, "y": 200.0, "z": 50.0, "crs": { "epsg": 4326, "wkt": null }, "qa_flags": [] }],
    });
    let survey_payload = serde_json::json!({
        "surveys": [
            { "hole_id": "DH-1", "depth_m": 50.0, "azimuth_deg": 0.0, "dip_deg": -60.0, "qa_flags": [] }
        ],
    });
    let assay_payload = serde_json::json!({ "assays": [] });

    let snapshot = s
        .store
        .load_graph(graph_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let roots: Vec<Uuid> = snapshot.nodes.keys().copied().collect();
    let dirty = propagate_stale(&snapshot, &roots);
    let input_map = build_input_artifacts(&s.store, &snapshot)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let plan = s.scheduler.plan(&snapshot, &dirty, &input_map, Some(CrsRecord::epsg(4326)));

    for mut job in plan.jobs {
        if job.node_id == n_collar {
            job.input_payload = Some(collar_payload.clone());
        } else if job.node_id == n_survey {
            job.input_payload = Some(survey_payload.clone());
        } else if job.node_id == n_assay {
            job.input_payload = Some(assay_payload.clone());
        }
        s.jobs
            .enqueue(&job)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    Ok(Json(serde_json::json!({
        "workspace_id": ws,
        "graph_id": graph_id,
        "nodes": {
            "collar_ingest": n_collar,
            "survey_ingest": n_survey,
            "assay_ingest": n_assay,
            "drillhole_merge": n_merge,
            "desurvey": n_desurvey,
            "dem": n_dem,
            "block": n_block,
            "plan_view_2d": n_viewer
        }
    })))
}
