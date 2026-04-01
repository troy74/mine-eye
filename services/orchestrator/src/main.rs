use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use async_stream::stream;
mod ingest_synth;
mod viewer_manifest;
const NODE_REGISTRY_JSON: &str = include_str!("node-registry.json");

use mine_eye_graph::propagate_stale;
use mine_eye_scheduler::{collect_dirty_nodes, Scheduler};
use mine_eye_store::{JobQueue, PgJobQueue, PgStore, StoreError};
use mine_eye_types::{
    ArtifactRef, BranchPromotionStatus, BranchStatus, CacheState, CrsRecord, ExecutionState,
    GraphMeta, LineageMeta, LockState, NodeCategory, NodeConfig, NodeExecutionPolicy, NodeRecord,
    OwnerRef, PropagationPolicy, QualityPolicy, RecomputePolicy, SemanticPortType, WorkspaceStatus,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::ServeDir;
use uuid::Uuid;
use viewer_manifest::ViewerManifest;

#[derive(Clone)]
struct AppState {
    store: Arc<PgStore>,
    jobs: Arc<PgJobQueue>,
    scheduler: Arc<Scheduler>,
    artifact_root: PathBuf,
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
        artifact_root: PathBuf::from(&artifact_root),
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let artifact_dir = PathBuf::from(&artifact_root);
    let app = Router::new()
        .route("/health", get(health))
        .route("/registry/nodes", get(get_node_registry))
        .route("/epsg/search", get(search_epsg))
        .route("/workspaces", post(create_workspace))
        .route("/workspaces/{ws_id}/graphs", post(create_graph))
        .route("/graphs/{graph_id}", get(get_graph))
        .route("/graphs/{graph_id}/events", get(graph_events))
        .route(
            "/graphs/{graph_id}/branches",
            get(list_graph_branches).post(create_graph_branch),
        )
        .route(
            "/graphs/{graph_id}/branches/{branch_id}/commit-current",
            post(commit_current_to_branch),
        )
        .route(
            "/graphs/{graph_id}/branches/{branch_id}/checkout",
            post(checkout_graph_branch),
        )
        .route("/graphs/{graph_id}/revisions", get(list_graph_revisions))
        .route(
            "/graphs/{graph_id}/revisions/{from_revision_id}/diff/{to_revision_id}",
            get(diff_graph_revisions),
        )
        .route(
            "/graphs/{graph_id}/promotions",
            get(list_graph_promotions).post(record_graph_promotion),
        )
        .route(
            "/graphs/{graph_id}/promotions/execute",
            post(execute_graph_promotion),
        )
        .route("/graphs/{graph_id}/nodes", post(add_node))
        .route(
            "/graphs/{graph_id}/nodes/{node_id}",
            patch(patch_node_params).delete(delete_node),
        )
        .route("/graphs/{graph_id}/edges", post(add_edge))
        .route("/graphs/{graph_id}/edges/{edge_id}", delete(delete_edge))
        .route("/graphs/{graph_id}/run", post(run_graph))
        .route("/graphs/{graph_id}/artifacts", get(list_artifacts))
        .route(
            "/graphs/{graph_id}/viewers/{viewer_node_id}/manifest",
            get(get_viewer_manifest),
        )
        .route("/graphs/{graph_id}/ai/suggest", post(ai_suggest))
        .route(
            "/graphs/{graph_id}/ai/suggestions",
            get(list_ai_suggestions),
        )
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

async fn get_node_registry() -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let v: serde_json::Value = serde_json::from_str(NODE_REGISTRY_JSON)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(v))
}

#[derive(Deserialize)]
struct EpsgSearchQuery {
    q: String,
}

#[derive(Serialize)]
struct EpsgHit {
    code: String,
    name: String,
}

async fn search_epsg(
    Query(q): Query<EpsgSearchQuery>,
) -> Result<Json<Vec<EpsgHit>>, (StatusCode, String)> {
    let query = q.q.trim();
    if query.len() < 2 {
        return Ok(Json(Vec::new()));
    }

    let url = format!(
        "https://epsg.io/?q={}&format=json",
        urlencoding::encode(query)
    );
    let client = reqwest::Client::builder()
        .user_agent("mine-eye-orchestrator/0.1 (+https://localhost)")
        .build()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let resp = client
        .get(url)
        .header("accept", "application/json")
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("epsg upstream error: {}", e)))?;
    if !resp.status().is_success() {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("epsg upstream http {}", resp.status()),
        ));
    }
    let body = resp
        .text()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("epsg read error: {}", e)))?;
    let root: serde_json::Value = serde_json::from_str(&body).map_err(|e| {
        let sample = body.chars().take(120).collect::<String>();
        (
            StatusCode::BAD_GATEWAY,
            format!("epsg parse error: {}; body_start={}", e, sample),
        )
    })?;

    let arr = root
        .get("results")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let mut out: Vec<EpsgHit> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for item in arr {
        let code = item
            .get("code")
            .or_else(|| item.get("srid"))
            .or_else(|| item.get("auth_srid"))
            .and_then(|v| {
                v.as_str()
                    .map(|s| s.to_string())
                    .or_else(|| v.as_i64().map(|n| n.to_string()))
                    .or_else(|| v.as_u64().map(|n| n.to_string()))
            })
            .unwrap_or_default();
        let name = item
            .get("name")
            .or_else(|| item.get("title"))
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        if code.is_empty() || name.is_empty() || !code.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        if seen.insert(code.clone()) {
            out.push(EpsgHit { code, name });
        }
        if out.len() >= 50 {
            break;
        }
    }

    Ok(Json(out))
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
    let owner_user_id = body.owner_user_id.clone();
    let meta = GraphMeta {
        graph_id,
        workspace_id: ws_id,
        name: body.name.clone(),
        owner: OwnerRef {
            user_id: owner_user_id.clone(),
        },
        status: WorkspaceStatus::Draft,
        lock: LockState::Unlocked,
        approval: None,
    };
    s.store
        .create_graph(ws_id, &body.name, &meta)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    // Seed branch/revision baseline for newly created graphs.
    let main_branch_id = s
        .store
        .create_branch(
            graph_id,
            "main",
            None,
            &owner_user_id,
            BranchStatus::Promoted,
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    s.store
        .create_revision(
            graph_id,
            main_branch_id,
            None,
            &owner_user_id,
            serde_json::json!({ "event": "graph_created" }),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(IdResp { id: graph_id }))
}

async fn commit_graph_revision(
    store: &PgStore,
    graph_id: Uuid,
    actor: &str,
    event: &str,
    branch_id: Option<Uuid>,
    details: serde_json::Value,
) -> Result<Uuid, StoreError> {
    let snapshot = store.load_graph(graph_id).await?;
    let branches = store.list_branches(graph_id).await?;
    let branch = branch_id
        .and_then(|id| branches.iter().find(|b| b.id == id))
        .or_else(|| {
            branches
        .iter()
        .find(|b| b.name == "main")
        })
        .or_else(|| branches.first());

    let branch_id = if let Some(b) = branch {
        b.id
    } else {
        store
            .create_branch(graph_id, "main", None, actor, BranchStatus::Draft)
            .await?
    };
    let parent = branches
        .iter()
        .find(|b| b.id == branch_id)
        .and_then(|b| b.head_revision_id);
    store
        .create_revision(
            graph_id,
            branch_id,
            parent,
            actor,
            serde_json::json!({
                "event": event,
                "details": details,
                "snapshot": {
                    "nodes": snapshot.nodes.into_values().collect::<Vec<_>>(),
                    "edges": snapshot.edges,
                }
            }),
        )
        .await
}

#[derive(Debug, Clone)]
struct GraphState {
    nodes: Vec<NodeRecord>,
    edges: Vec<mine_eye_graph::EdgeRef>,
}

fn node_def(n: &NodeRecord) -> serde_json::Value {
    serde_json::json!({
        "category": n.category,
        "config": n.config,
        "policy": n.policy,
        "ports": n.ports,
    })
}

fn edge_key(e: &mine_eye_graph::EdgeRef) -> (Uuid, String, Uuid, String, SemanticPortType) {
    (
        e.from_node,
        e.from_port.clone(),
        e.to_node,
        e.to_port.clone(),
        e.semantic_type,
    )
}

fn state_from_snapshot(s: mine_eye_graph::GraphSnapshot) -> GraphState {
    GraphState {
        nodes: s.nodes.into_values().collect(),
        edges: s.edges,
    }
}

fn state_from_revision_meta(meta: &serde_json::Value) -> Option<GraphState> {
    let snap = meta.get("snapshot")?;
    let nodes: Vec<NodeRecord> = serde_json::from_value(snap.get("nodes")?.clone()).ok()?;
    let edges: Vec<mine_eye_graph::EdgeRef> = serde_json::from_value(snap.get("edges")?.clone()).ok()?;
    Some(GraphState { nodes, edges })
}

fn empty_state() -> GraphState {
    GraphState {
        nodes: Vec::new(),
        edges: Vec::new(),
    }
}

fn merge_graph_states_3way(
    base: &GraphState,
    source: &GraphState,
    target: &GraphState,
) -> Result<GraphState, serde_json::Value> {
    let base_nodes: HashMap<Uuid, NodeRecord> = base.nodes.iter().cloned().map(|n| (n.id, n)).collect();
    let source_nodes: HashMap<Uuid, NodeRecord> = source.nodes.iter().cloned().map(|n| (n.id, n)).collect();
    let target_nodes: HashMap<Uuid, NodeRecord> = target.nodes.iter().cloned().map(|n| (n.id, n)).collect();

    let mut all_node_ids = HashSet::new();
    all_node_ids.extend(base_nodes.keys().copied());
    all_node_ids.extend(source_nodes.keys().copied());
    all_node_ids.extend(target_nodes.keys().copied());

    let mut merged_nodes: HashMap<Uuid, NodeRecord> = HashMap::new();
    let mut conflicts: Vec<serde_json::Value> = Vec::new();

    for id in all_node_ids {
        let b = base_nodes.get(&id);
        let s = source_nodes.get(&id);
        let t = target_nodes.get(&id);
        match (b, s, t) {
            (None, None, None) => {}
            (None, Some(sn), None) => {
                merged_nodes.insert(id, sn.clone());
            }
            (None, None, Some(tn)) => {
                merged_nodes.insert(id, tn.clone());
            }
            (None, Some(sn), Some(tn)) => {
                if node_def(sn) == node_def(tn) {
                    merged_nodes.insert(id, sn.clone());
                } else {
                    conflicts.push(serde_json::json!({"type":"same_node_added_differently","node_id":id}));
                }
            }
            (Some(_), None, None) => {}
            (Some(bn), Some(sn), None) => {
                if node_def(sn) != node_def(bn) {
                    conflicts.push(serde_json::json!({"type":"node_edited_vs_deleted","node_id":id,"edited_side":"source"}));
                }
            }
            (Some(bn), None, Some(tn)) => {
                if node_def(tn) != node_def(bn) {
                    conflicts.push(serde_json::json!({"type":"node_edited_vs_deleted","node_id":id,"edited_side":"target"}));
                }
            }
            (Some(bn), Some(sn), Some(tn)) => {
                let bdef = node_def(bn);
                let sdef = node_def(sn);
                let tdef = node_def(tn);
                if sdef == tdef {
                    merged_nodes.insert(id, sn.clone());
                } else if sdef == bdef {
                    merged_nodes.insert(id, tn.clone());
                } else if tdef == bdef {
                    merged_nodes.insert(id, sn.clone());
                } else {
                    conflicts.push(serde_json::json!({"type":"node_edited_differently","node_id":id}));
                }
            }
        }
    }

    let base_edges: HashMap<(Uuid, String, Uuid, String, SemanticPortType), mine_eye_graph::EdgeRef> =
        base.edges.iter().cloned().map(|e| (edge_key(&e), e)).collect();
    let source_edges: HashMap<(Uuid, String, Uuid, String, SemanticPortType), mine_eye_graph::EdgeRef> =
        source.edges.iter().cloned().map(|e| (edge_key(&e), e)).collect();
    let target_edges: HashMap<(Uuid, String, Uuid, String, SemanticPortType), mine_eye_graph::EdgeRef> =
        target.edges.iter().cloned().map(|e| (edge_key(&e), e)).collect();

    let mut all_edge_keys = HashSet::new();
    all_edge_keys.extend(base_edges.keys().cloned());
    all_edge_keys.extend(source_edges.keys().cloned());
    all_edge_keys.extend(target_edges.keys().cloned());

    let mut merged_edges: HashMap<(Uuid, String, Uuid, String, SemanticPortType), mine_eye_graph::EdgeRef> =
        HashMap::new();
    for key in all_edge_keys {
        match (
            base_edges.get(&key),
            source_edges.get(&key),
            target_edges.get(&key),
        ) {
            (None, None, None) => {}
            (None, Some(se), None) => {
                merged_edges.insert(key.clone(), se.clone());
            }
            (None, None, Some(te)) => {
                merged_edges.insert(key.clone(), te.clone());
            }
            (None, Some(se), Some(_)) => {
                merged_edges.insert(key.clone(), se.clone());
            }
            (Some(_), None, None) => {}
            (Some(_), Some(se), None) => {
                merged_edges.insert(key.clone(), se.clone());
            }
            (Some(_), None, Some(te)) => {
                merged_edges.insert(key.clone(), te.clone());
            }
            (Some(_), Some(se), Some(_)) => {
                merged_edges.insert(key.clone(), se.clone());
            }
        }
    }

    if !conflicts.is_empty() {
        return Err(serde_json::json!({ "conflicts": conflicts }));
    }

    let valid_node_ids: HashSet<Uuid> = merged_nodes.keys().copied().collect();
    let mut edge_out = Vec::new();
    for e in merged_edges.into_values() {
        if valid_node_ids.contains(&e.from_node) && valid_node_ids.contains(&e.to_node) {
            edge_out.push(e);
        } else {
            conflicts.push(serde_json::json!({
                "type": "edge_endpoint_missing_after_merge",
                "edge_id": e.id
            }));
        }
    }
    if !conflicts.is_empty() {
        return Err(serde_json::json!({ "conflicts": conflicts }));
    }

    Ok(GraphState {
        nodes: merged_nodes.into_values().collect(),
        edges: edge_out,
    })
}

fn diff_graph_states(from: &GraphState, to: &GraphState) -> serde_json::Value {
    let from_nodes: HashMap<Uuid, NodeRecord> =
        from.nodes.iter().cloned().map(|n| (n.id, n)).collect();
    let to_nodes: HashMap<Uuid, NodeRecord> = to.nodes.iter().cloned().map(|n| (n.id, n)).collect();

    let mut node_added = Vec::new();
    let mut node_removed = Vec::new();
    let mut node_changed = Vec::new();

    for id in to_nodes.keys() {
        if !from_nodes.contains_key(id) {
            node_added.push(*id);
        }
    }
    for id in from_nodes.keys() {
        if !to_nodes.contains_key(id) {
            node_removed.push(*id);
        }
    }
    for (id, f) in &from_nodes {
        if let Some(t) = to_nodes.get(id) {
            if node_def(f) != node_def(t) {
                node_changed.push(*id);
            }
        }
    }

    let from_edges: HashSet<(Uuid, String, Uuid, String, SemanticPortType)> =
        from.edges.iter().map(edge_key).collect();
    let to_edges: HashSet<(Uuid, String, Uuid, String, SemanticPortType)> =
        to.edges.iter().map(edge_key).collect();

    let edge_added: Vec<_> = to_edges.difference(&from_edges).cloned().collect();
    let edge_removed: Vec<_> = from_edges.difference(&to_edges).cloned().collect();

    serde_json::json!({
        "summary": {
            "nodes_added": node_added.len(),
            "nodes_removed": node_removed.len(),
            "nodes_changed": node_changed.len(),
            "edges_added": edge_added.len(),
            "edges_removed": edge_removed.len(),
        },
        "nodes": {
            "added": node_added,
            "removed": node_removed,
            "changed": node_changed,
        },
        "edges": {
            "added": edge_added,
            "removed": edge_removed,
        }
    })
}

#[derive(Deserialize)]
struct CreateBranchReq {
    name: String,
    base_revision_id: Option<Uuid>,
    created_by: String,
    status: Option<BranchStatus>,
}

async fn create_graph_branch(
    State(s): State<AppState>,
    Path(graph_id): Path<Uuid>,
    Json(body): Json<CreateBranchReq>,
) -> Result<Json<IdResp>, (StatusCode, String)> {
    let id = s
        .store
        .create_branch(
            graph_id,
            &body.name,
            body.base_revision_id,
            &body.created_by,
            body.status.unwrap_or(BranchStatus::Draft),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(IdResp { id }))
}

async fn list_graph_branches(
    State(s): State<AppState>,
    Path(graph_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let rows = s
        .store
        .list_branches(graph_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let v: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| serde_json::to_value(r).unwrap_or(serde_json::Value::Null))
        .collect();
    Ok(Json(serde_json::Value::Array(v)))
}

#[derive(Deserialize)]
struct ListRevisionsQuery {
    branch_id: Option<Uuid>,
}

async fn list_graph_revisions(
    State(s): State<AppState>,
    Path(graph_id): Path<Uuid>,
    axum::extract::Query(q): axum::extract::Query<ListRevisionsQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let rows = s
        .store
        .list_revisions(graph_id, q.branch_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let v: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| serde_json::to_value(r).unwrap_or(serde_json::Value::Null))
        .collect();
    Ok(Json(serde_json::Value::Array(v)))
}

async fn diff_graph_revisions(
    State(s): State<AppState>,
    Path((graph_id, from_revision_id, to_revision_id)): Path<(Uuid, Uuid, Uuid)>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let revs = s
        .store
        .list_revisions(graph_id, None)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let from = revs
        .iter()
        .find(|r| r.id == from_revision_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "from revision not found".to_string()))?;
    let to = revs
        .iter()
        .find(|r| r.id == to_revision_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "to revision not found".to_string()))?;

    let from_state = state_from_revision_meta(&from.meta)
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "from revision missing snapshot".to_string()))?;
    let to_state = state_from_revision_meta(&to.meta)
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "to revision missing snapshot".to_string()))?;

    Ok(Json(serde_json::json!({
        "from_revision_id": from_revision_id,
        "to_revision_id": to_revision_id,
        "diff": diff_graph_states(&from_state, &to_state),
    })))
}

#[derive(Deserialize)]
struct CommitCurrentReq {
    created_by: String,
    event: Option<String>,
    details: Option<serde_json::Value>,
}

async fn commit_current_to_branch(
    State(s): State<AppState>,
    Path((graph_id, branch_id)): Path<(Uuid, Uuid)>,
    Json(body): Json<CommitCurrentReq>,
) -> Result<Json<IdResp>, (StatusCode, String)> {
    let rev = commit_graph_revision(
        &s.store,
        graph_id,
        &body.created_by,
        body.event
            .as_deref()
            .unwrap_or("manual_branch_commit"),
        Some(branch_id),
        body.details.unwrap_or_else(|| serde_json::json!({})),
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(IdResp { id: rev }))
}

#[derive(Deserialize)]
struct CheckoutBranchReq {
    created_by: Option<String>,
}

async fn checkout_graph_branch(
    State(s): State<AppState>,
    Path((graph_id, branch_id)): Path<(Uuid, Uuid)>,
    Json(body): Json<CheckoutBranchReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let branches = s
        .store
        .list_branches(graph_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let branch = branches
        .iter()
        .find(|b| b.id == branch_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "branch not found".to_string()))?;
    let head_id = branch
        .head_revision_id
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "branch has no head revision".to_string()))?;

    let revs = s
        .store
        .list_revisions(graph_id, Some(branch_id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let head = revs
        .into_iter()
        .find(|r| r.id == head_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "head revision not found".to_string()))?;
    let state = state_from_revision_meta(&head.meta)
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "head revision missing snapshot".to_string()))?;

    s.store
        .replace_graph_definition(graph_id, &state.nodes, &state.edges)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let actor = body.created_by.unwrap_or_else(|| "system".to_string());
    let _ = commit_graph_revision(
        &s.store,
        graph_id,
        &actor,
        "checkout_branch",
        Some(branch_id),
        serde_json::json!({ "branch_id": branch_id, "head_revision_id": head_id }),
    )
    .await;

    Ok(Json(serde_json::json!({
        "status": "ok",
        "branch_id": branch_id,
        "head_revision_id": head_id
    })))
}

#[derive(Deserialize)]
struct BranchMutationQuery {
    branch_id: Option<Uuid>,
}

#[derive(Deserialize)]
struct RecordPromotionReq {
    source_branch_id: Uuid,
    target_branch_id: Uuid,
    source_head_revision_id: Option<Uuid>,
    promoted_revision_id: Option<Uuid>,
    status: BranchPromotionStatus,
    conflict_report: Option<serde_json::Value>,
    created_by: String,
}

async fn record_graph_promotion(
    State(s): State<AppState>,
    Path(_graph_id): Path<Uuid>,
    Json(body): Json<RecordPromotionReq>,
) -> Result<Json<IdResp>, (StatusCode, String)> {
    let id = s
        .store
        .record_branch_promotion(
            body.source_branch_id,
            body.target_branch_id,
            body.source_head_revision_id,
            body.promoted_revision_id,
            body.status,
            body.conflict_report,
            &body.created_by,
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(IdResp { id }))
}

async fn list_graph_promotions(
    State(s): State<AppState>,
    Path(graph_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let rows = s
        .store
        .list_branch_promotions(graph_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let v: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| serde_json::to_value(r).unwrap_or(serde_json::Value::Null))
        .collect();
    Ok(Json(serde_json::Value::Array(v)))
}

#[derive(Deserialize)]
struct ExecutePromotionReq {
    source_branch_id: Uuid,
    target_branch_id: Uuid,
    created_by: String,
    apply_to_graph: Option<bool>,
}

async fn execute_graph_promotion(
    State(s): State<AppState>,
    Path(graph_id): Path<Uuid>,
    Json(body): Json<ExecutePromotionReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let branches = s
        .store
        .list_branches(graph_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let source = branches
        .iter()
        .find(|b| b.id == body.source_branch_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "source branch not found".to_string()))?;
    let target = branches
        .iter()
        .find(|b| b.id == body.target_branch_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "target branch not found".to_string()))?;

    let source_head = source
        .head_revision_id
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "source branch has no head revision".to_string()))?;
    let target_head = target.head_revision_id;
    let source_base = source.base_revision_id;

    let all_revs = s
        .store
        .list_revisions(graph_id, None)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let rev_map: HashMap<Uuid, mine_eye_types::GraphRevision> =
        all_revs.into_iter().map(|r| (r.id, r)).collect();

    let source_state = rev_map
        .get(&source_head)
        .and_then(|r| state_from_revision_meta(&r.meta))
        .unwrap_or_else(empty_state);
    let live_state = state_from_snapshot(
        s.store
            .load_graph(graph_id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?,
    );
    let target_state = target_head
        .and_then(|id| rev_map.get(&id))
        .and_then(|r| state_from_revision_meta(&r.meta))
        .unwrap_or_else(|| live_state.clone());
    let base_state = source_base
        .and_then(|id| rev_map.get(&id))
        .and_then(|r| state_from_revision_meta(&r.meta))
        .unwrap_or_else(empty_state);

    let is_fast_forward = target_head.is_some() && source_base == target_head;
    let merge_result = if is_fast_forward {
        Ok(source_state.clone())
    } else {
        merge_graph_states_3way(&base_state, &source_state, &target_state)
    };

    let merged = match merge_result {
        Ok(m) => m,
        Err(conflict_report) => {
            let pid = s
                .store
                .record_branch_promotion(
                    source.id,
                    target.id,
                    Some(source_head),
                    None,
                    BranchPromotionStatus::Conflict,
                    Some(conflict_report.clone()),
                    &body.created_by,
                )
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            return Ok(Json(serde_json::json!({
                "promotion_id": pid,
                "status": "conflict",
                "mode": if is_fast_forward { "fast_forward" } else { "three_way" },
                "conflict_report": conflict_report
            })));
        }
    };

    let promoted_rev = s
        .store
        .create_revision(
            graph_id,
            target.id,
            target_head,
            &body.created_by,
            serde_json::json!({
                "event": if is_fast_forward { "branch_promote_fast_forward" } else { "branch_promote_three_way" },
                "details": {
                    "source_branch_id": source.id,
                    "target_branch_id": target.id,
                    "source_head_revision_id": source_head,
                    "target_head_revision_id": target_head,
                },
                "snapshot": {
                    "nodes": merged.nodes.clone(),
                    "edges": merged.edges.clone()
                }
            }),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if body.apply_to_graph.unwrap_or(true) {
        s.store
            .replace_graph_definition(graph_id, &merged.nodes, &merged.edges)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    let pid = s
        .store
        .record_branch_promotion(
            source.id,
            target.id,
            Some(source_head),
            Some(promoted_rev),
            BranchPromotionStatus::Succeeded,
            None,
            &body.created_by,
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({
        "promotion_id": pid,
        "promoted_revision_id": promoted_rev,
        "status": "succeeded",
        "mode": if is_fast_forward { "fast_forward" } else { "three_way" }
    })))
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
    branch_id: Option<Uuid>,
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
    commit_graph_revision(
        &s.store,
        graph_id,
        "system",
        "add_node",
        body.branch_id,
        serde_json::json!({ "node_id": id, "kind": node.config.kind }),
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(IdResp { id }))
}

#[derive(Deserialize)]
struct PatchNodeParamsReq {
    params: serde_json::Value,
    policy: Option<NodeExecutionPolicy>,
    branch_id: Option<Uuid>,
}

async fn patch_node_params(
    State(s): State<AppState>,
    Path((graph_id, node_id)): Path<(Uuid, Uuid)>,
    Json(body): Json<PatchNodeParamsReq>,
) -> Result<Json<NodeRecord>, (StatusCode, String)> {
    let node = s
        .store
        .patch_node_config(graph_id, node_id, body.params, body.policy)
        .await
        .map_err(|e| match e {
            StoreError::NotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        })?;
    commit_graph_revision(
        &s.store,
        graph_id,
        "system",
        "patch_node_params",
        body.branch_id,
        serde_json::json!({ "node_id": node_id }),
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(node))
}

fn stable_graph_sig(snap: &mine_eye_graph::GraphSnapshot, arts: &[ArtifactEntry]) -> String {
    let mut nodes = snap.nodes.values().cloned().collect::<Vec<_>>();
    nodes.sort_by_key(|n| n.id);
    let mut edges = snap.edges.clone();
    edges.sort_by_key(|e| (e.from_node, e.to_node, e.from_port.clone(), e.to_port.clone()));
    let mut art_map: BTreeMap<String, String> = BTreeMap::new();
    for a in arts {
        art_map.insert(format!("{}:{}", a.node_id, a.key), a.content_hash.clone());
    }
    serde_json::json!({
        "nodes": nodes,
        "edges": edges,
        "artifacts": art_map
    })
    .to_string()
}

async fn graph_events(
    State(s): State<AppState>,
    Path(graph_id): Path<Uuid>,
) -> Sse<impl futures_core::Stream<Item = Result<Event, std::convert::Infallible>>> {
    let stream = stream! {
        let mut last_sig = String::new();
        loop {
            let mut out_event: Option<Event> = None;
            match s.store.load_graph(graph_id).await {
                Ok(snap) => {
                    let mut out = Vec::new();
                    for nid in snap.nodes.keys() {
                        if let Ok(rows) = s.store.list_artifacts_for_node(*nid).await {
                            for (key, hash, _) in rows {
                                out.push(ArtifactEntry {
                                    node_id: *nid,
                                    key: key.clone(),
                                    url: format!("/files/{}", key),
                                    content_hash: hash,
                                });
                            }
                        }
                    }
                    let sig = stable_graph_sig(&snap, &out);
                    if sig != last_sig {
                        last_sig = sig.clone();
                        out_event = Some(Event::default().event("changed").data(sig));
                    }
                }
                Err(e) => {
                    out_event = Some(Event::default().event("error").data(e.to_string()));
                }
            }
            if let Some(ev) = out_event {
                yield Ok(ev);
            }
            tokio::time::sleep(Duration::from_millis(900)).await;
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(10)))
}

#[derive(Deserialize)]
struct AddEdgeReq {
    from_node: Uuid,
    from_port: String,
    to_node: Uuid,
    to_port: String,
    semantic_type: SemanticPortType,
    branch_id: Option<Uuid>,
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
    commit_graph_revision(
        &s.store,
        graph_id,
        "system",
        "add_edge",
        body.branch_id,
        serde_json::json!({
            "edge_id": eid,
            "from_node": body.from_node,
            "to_node": body.to_node,
            "semantic_type": body.semantic_type
        }),
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(IdResp { id: eid }))
}

async fn delete_node(
    State(s): State<AppState>,
    Path((graph_id, node_id)): Path<(Uuid, Uuid)>,
    axum::extract::Query(q): axum::extract::Query<BranchMutationQuery>,
) -> Result<StatusCode, (StatusCode, String)> {
    s.store
        .delete_node(graph_id, node_id)
        .await
        .map_err(|e| match e {
            StoreError::NotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        })?;
    commit_graph_revision(
        &s.store,
        graph_id,
        "system",
        "delete_node",
        q.branch_id,
        serde_json::json!({ "node_id": node_id }),
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_edge(
    State(s): State<AppState>,
    Path((graph_id, edge_id)): Path<(Uuid, Uuid)>,
    axum::extract::Query(q): axum::extract::Query<BranchMutationQuery>,
) -> Result<StatusCode, (StatusCode, String)> {
    s.store
        .delete_edge(graph_id, edge_id)
        .await
        .map_err(|e| match e {
            StoreError::NotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        })?;
    commit_graph_revision(
        &s.store,
        graph_id,
        "system",
        "delete_edge",
        q.branch_id,
        serde_json::json!({ "edge_id": edge_id }),
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct RunGraphReq {
    /// If empty, all stale nodes are roots (full graph dirty).
    dirty_roots: Option<Vec<Uuid>>,
    /// Per-node inline payloads for workers (e.g. ingest JSON).
    input_payloads: Option<HashMap<Uuid, serde_json::Value>>,
    /// Include nodes with recompute=manual in this run request.
    include_manual: Option<bool>,
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

    let roots = body
        .dirty_roots
        .unwrap_or_else(|| snapshot.nodes.keys().copied().collect());
    let root_set: HashSet<Uuid> = roots.iter().copied().collect();
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

    let plan = s
        .scheduler
        .plan(
            &snapshot,
            &dirty,
            &root_set,
            &input_map,
            project_crs.clone(),
            body.include_manual.unwrap_or(false),
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
                if let Some(p) = ingest_synth::synthesize_input_payload(node, project_crs.as_ref())
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

async fn get_viewer_manifest(
    State(s): State<AppState>,
    Path((graph_id, viewer_node_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<ViewerManifest>, (StatusCode, String)> {
    let snap = s
        .store
        .load_graph(graph_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let manifest = viewer_manifest::build_viewer_manifest(
        &s.store,
        &s.artifact_root,
        &snap,
        graph_id,
        viewer_node_id,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(manifest))
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

/// Seeds demo: collars + surveys -> desurvey; desurvey + assays -> drillhole model.
async fn demo_seed(
    State(s): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
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
    let n_desurvey = Uuid::new_v4();
    let n_drillhole = Uuid::new_v4();

    for (id, kind, cat) in [
        (n_collar, "collar_ingest", NodeCategory::Input),
        (n_survey, "survey_ingest", NodeCategory::Input),
        (n_assay, "assay_ingest", NodeCategory::Input),
        (n_desurvey, "desurvey_trajectory", NodeCategory::Transform),
        (n_drillhole, "drillhole_model", NodeCategory::Model),
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
    let edges_spec: [(Uuid, &str, Uuid, &str, S); 4] = [
        (n_collar, "collars", n_desurvey, "collars_in", S::PointSet),
        (n_survey, "surveys", n_desurvey, "surveys_in", S::TrajectorySet),
        (n_desurvey, "trajectory", n_drillhole, "trajectory_in", S::TrajectorySet),
        (n_assay, "assays", n_drillhole, "assays_in", S::IntervalSet),
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
            n_drillhole,
            "assay_points",
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
    let assay_payload = serde_json::json!({
        "assays": [
            { "hole_id":"DH-1", "from_m": 0.0, "to_m": 25.0, "attributes": {"au_ppm": 0.8}, "qa_flags":[] },
            { "hole_id":"DH-1", "from_m": 25.0, "to_m": 50.0, "attributes": {"au_ppm": 1.6}, "qa_flags":[] }
        ]
    });

    let snapshot = s
        .store
        .load_graph(graph_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let roots: Vec<Uuid> = snapshot.nodes.keys().copied().collect();
    let root_set: HashSet<Uuid> = roots.iter().copied().collect();
    let dirty = propagate_stale(&snapshot, &roots);
    let input_map = build_input_artifacts(&s.store, &snapshot)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let plan = s
        .scheduler
        .plan(
            &snapshot,
            &dirty,
            &root_set,
            &input_map,
            Some(CrsRecord::epsg(4326)),
            false,
        );

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
            "desurvey": n_desurvey,
            "drillhole_model": n_drillhole,
            "plan_view_2d": n_viewer
        }
    })))
}
