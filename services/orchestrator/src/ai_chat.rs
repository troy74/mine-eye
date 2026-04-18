use std::collections::{HashMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::commit_graph_revision;
use crate::registry::NodeRegistry;
use axum::http::StatusCode;
use mine_eye_scheduler::{collect_dirty_nodes, Scheduler};
use mine_eye_store::{JobQueue, PgJobQueue, PgStore};
use mine_eye_types::{
    ArtifactRef, CacheState, CrsRecord, ExecutionState, LineageMeta, NodeCategory, NodeConfig,
    NodeExecutionPolicy, NodeRecord, SemanticPortType,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

const DEFAULT_MODEL: &str = "openai/gpt-5.4";
const DEFAULT_BASE: &str = "https://openrouter.ai/api/v1";
const MAX_TOOL_STEPS: usize = 8;
const MAX_CONTEXT_MESSAGES: usize = 24;
const MAX_TOOL_OUTPUT_CHARS: usize = 40_000;
const MAX_UPLOAD_TEXT_CHARS: usize = 120_000;

#[derive(Debug, Deserialize)]
pub struct AiChatRequest {
    pub messages: Vec<AiChatMessage>,
    pub model: Option<String>,
    pub user_id: Option<String>,
    pub apply_mutations: Option<bool>,
    pub branch_id: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AiChatMessage {
    pub role: String,
    pub text: String,
    #[serde(default)]
    pub attachments: Vec<AiChatAttachment>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AiChatAttachment {
    pub name: String,
    #[serde(default)]
    pub mime: String,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub artifact_key: Option<String>,
    #[serde(default)]
    pub content_hash: Option<String>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub preview_text: Option<String>,
}

#[derive(Debug, Clone)]
struct UploadedDoc {
    name: String,
    mime: String,
    size: u64,
    text: String,
}

#[derive(Debug, Serialize)]
pub struct AiChatResponse {
    pub model: String,
    pub assistant_text: String,
    pub tool_events: Vec<ToolEvent>,
    pub memory_files_used: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ToolEvent {
    pub name: String,
    pub arguments: serde_json::Value,
    pub ok: bool,
    pub summary: String,
    pub output_preview: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OrToolCall {
    id: String,
    #[serde(default)]
    function: OrFunctionCall,
}

#[derive(Debug, Deserialize, Default)]
struct OrFunctionCall {
    name: String,
    arguments: String,
}

pub async fn run_ai_chat(
    store: Arc<PgStore>,
    jobs: Arc<PgJobQueue>,
    scheduler: Arc<Scheduler>,
    artifact_root: &Path,
    graph_id: Uuid,
    req: AiChatRequest,
) -> Result<AiChatResponse, (StatusCode, String)> {
    let key = env::var("OPENROUTER_API_KEY").unwrap_or_default();
    if key.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "OPENROUTER_API_KEY is not set in orchestrator environment".to_string(),
        ));
    }

    let model = req
        .model
        .filter(|m| !m.trim().is_empty())
        .or_else(|| env::var("OPENROUTER_MODEL").ok())
        .unwrap_or_else(|| DEFAULT_MODEL.to_string());
    let user_label = req
        .user_id
        .clone()
        .unwrap_or_else(|| "unknown-user".to_string());
    let apply_mutations = req.apply_mutations.unwrap_or(false);
    let branch_id = req
        .branch_id
        .as_deref()
        .and_then(|s| Uuid::parse_str(s).ok());
    let base = env::var("OPENROUTER_BASE").unwrap_or_else(|_| DEFAULT_BASE.to_string());

    let (memory_text, memory_files_used) = load_memory_context();
    let system_prompt = build_system_prompt(&memory_text);

    let mut convo: Vec<serde_json::Value> = vec![json!({
        "role": "system",
        "content": format!(
            "{}\nSession user_id: {}\nMutation mode: {}",
            system_prompt,
            user_label,
            if apply_mutations { "apply" } else { "plan-only" }
        )
    })];

    let mut clipped = req.messages;
    if clipped.len() > MAX_CONTEXT_MESSAGES {
        clipped = clipped[clipped.len() - MAX_CONTEXT_MESSAGES..].to_vec();
    }
    let uploaded_docs = collect_uploaded_docs(&clipped, artifact_root).await;
    for m in clipped {
        let role = match m.role.as_str() {
            "assistant" => "assistant",
            _ => "user",
        };
        let content = render_chat_message_content(&m);
        if content.trim().is_empty() {
            continue;
        }
        convo.push(json!({
            "role": role,
            "content": content
        }));
    }
    if convo.len() == 1 {
        convo.push(json!({
            "role": "user",
            "content": "A project is now open. Ask me 3 concise onboarding questions to set up the workflow (objective, data source node, and desired output), then propose the next best action."
        }));
    }

    let tools = openai_tools_spec();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(90))
        .build()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut tool_events: Vec<ToolEvent> = Vec::new();
    let mut final_text = String::new();

    for _ in 0..MAX_TOOL_STEPS {
        let body = json!({
            "model": model,
            "messages": convo,
            "tools": tools,
            "tool_choice": "auto",
            "temperature": 0.2
        });

        let resp = client
            .post(format!("{}/chat/completions", base.trim_end_matches('/')))
            .header("authorization", format!("Bearer {}", key))
            .header("content-type", "application/json")
            .header("http-referer", "https://mine-eye.local")
            .header("x-title", "mine-eye")
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                (
                    StatusCode::BAD_GATEWAY,
                    format!("openrouter request failed: {}", e),
                )
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let detail = resp.text().await.unwrap_or_default();
            return Err((
                StatusCode::BAD_GATEWAY,
                format!("openrouter http {}: {}", status, truncate(&detail, 600)),
            ));
        }

        let raw: serde_json::Value = resp.json().await.map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("openrouter decode failed: {}", e),
            )
        })?;

        let message = raw
            .get("choices")
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|v| v.get("message"))
            .cloned()
            .unwrap_or_else(|| json!({}));

        let tool_calls: Vec<OrToolCall> = serde_json::from_value(
            message
                .get("tool_calls")
                .cloned()
                .unwrap_or_else(|| json!([])),
        )
        .unwrap_or_default();

        if !tool_calls.is_empty() {
            convo.push(json!({
                "role": "assistant",
                "content": message.get("content").cloned().unwrap_or(json!(null)),
                "tool_calls": message.get("tool_calls").cloned().unwrap_or(json!([]))
            }));

            for tc in tool_calls {
                let args: serde_json::Value =
                    serde_json::from_str(&tc.function.arguments).unwrap_or_else(|_| json!({}));
                let r = execute_tool(
                    &store,
                    &jobs,
                    &scheduler,
                    artifact_root,
                    graph_id,
                    &uploaded_docs,
                    &tc.function.name,
                    &args,
                    apply_mutations,
                    &user_label,
                    branch_id,
                )
                .await;
                let (ok, summary, payload) = match r {
                    Ok(v) => (true, summarize_tool_payload(&v), v),
                    Err(e) => (false, truncate(&e, 180), json!({ "error": e })),
                };
                tool_events.push(ToolEvent {
                    name: tc.function.name.clone(),
                    arguments: args.clone(),
                    ok,
                    summary,
                    output_preview: Some(tool_output_preview(&payload)),
                });
                convo.push(json!({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": truncate(&payload.to_string(), MAX_TOOL_OUTPUT_CHARS)
                }));
            }
            continue;
        }

        final_text = extract_content_text(message.get("content"));
        if final_text.trim().is_empty() {
            final_text = "I completed the tool pass but did not receive a final assistant message."
                .to_string();
        }
        break;
    }

    if final_text.trim().is_empty() {
        final_text = "Tool loop reached max steps before final response.".to_string();
    }

    Ok(AiChatResponse {
        model,
        assistant_text: final_text,
        tool_events,
        memory_files_used,
    })
}

fn openai_tools_spec() -> serde_json::Value {
    json!([
        {
            "type": "function",
            "function": {
                "name": "run_graph",
                "description": "Queue execution for dirty graph nodes (or selected roots). Use after mapping/wiring changes.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "dirty_roots": { "type": "array", "items": { "type": "string" } },
                        "include_manual": { "type": "boolean" }
                    }
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "run_node",
                "description": "Queue execution for one node and downstream dependencies.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "node_id": { "type": "string" },
                        "include_manual": { "type": "boolean" }
                    },
                    "required": ["node_id"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "graph_audit_bundle",
                "description": "One-shot compact audit for a graph: node health, wiring, ingest mapping status, compact registry capabilities, and optional artifact top/tail snippets.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "include_artifact_samples": { "type": "boolean" },
                        "head_lines": { "type": "integer" },
                        "tail_lines": { "type": "integer" }
                    }
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "registry_capability_matrix",
                "description": "Return compact capability matrix for all node kinds (category + port semantics + archived marker).",
                "parameters": { "type": "object", "properties": {} }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "list_uploaded_files",
                "description": "List uploaded files available in current chat context with compact schema hints.",
                "parameters": { "type": "object", "properties": {} }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "uploaded_file_top_tail",
                "description": "Inspect uploaded file text using head/tail lines.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                        "head_lines": { "type": "integer" },
                        "tail_lines": { "type": "integer" }
                    },
                    "required": ["name"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "uploaded_csv_profile",
                "description": "Profile a CSV-like uploaded file (delimiter, headers, inferred types, sample values).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                        "sample_rows": { "type": "integer" }
                    },
                    "required": ["name"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "suggest_ingest_mapping_from_upload",
                "description": "Suggest ingest UI mapping keys for collar/survey/assay files from uploaded data.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                        "node_kind": { "type": "string" }
                    },
                    "required": ["name", "node_kind"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "apply_upload_to_ingest_node",
                "description": "Patch ingest node UI config from uploaded CSV (headers, rows, mapping). Works for collar_ingest, survey_ingest, assay_ingest, lithology_ingest, orientation_ingest, and surface_sample_ingest.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                        "node_id": { "type": "string" },
                        "source_crs_epsg": { "type": "integer" },
                        "use_project_crs": { "type": "boolean" }
                    },
                    "required": ["name", "node_id"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "list_nodes",
                "description": "List graph nodes with kind, alias, execution/cache state.",
                "parameters": { "type": "object", "properties": {} }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "list_edges",
                "description": "List graph edges and semantic wire types.",
                "parameters": { "type": "object", "properties": {} }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "read_node",
                "description": "Read one node config in detail.",
                "parameters": {
                    "type": "object",
                    "properties": { "node_id": { "type": "string" } },
                    "required": ["node_id"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "list_node_artifacts",
                "description": "List artifacts for a node with likely tabular hints.",
                "parameters": {
                    "type": "object",
                    "properties": { "node_id": { "type": "string" } },
                    "required": ["node_id"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "read_registry_kind",
                "description": "Inspect node registry metadata for one node kind.",
                "parameters": {
                    "type": "object",
                    "properties": { "kind": { "type": "string" } },
                    "required": ["kind"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "artifact_top_tail",
                "description": "Read head/tail text of an artifact file for format inspection and conversion planning.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "node_id": { "type": "string" },
                        "artifact_key": { "type": "string" },
                        "head_lines": { "type": "integer" },
                        "tail_lines": { "type": "integer" }
                    },
                    "required": ["node_id"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "json_path_extract",
                "description": "Extract a value from a JSON artifact using a dotted path (e.g. bounds.xmin or geometry.coordinates.0).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "node_id": { "type": "string" },
                        "artifact_key": { "type": "string" },
                        "path": { "type": "string" }
                    },
                    "required": ["node_id", "path"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "csv_profile",
                "description": "Profile CSV-like artifact columns (delimiter, headers, inferred types, sample values).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "node_id": { "type": "string" },
                        "artifact_key": { "type": "string" },
                        "sample_rows": { "type": "integer" }
                    },
                    "required": ["node_id"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "suggest_measure_fields",
                "description": "Rank likely geochemical measure columns from tabular artifacts.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "node_id": { "type": "string" },
                        "artifact_key": { "type": "string" },
                        "top_k": { "type": "integer" }
                    },
                    "required": ["node_id"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "trace_upstream_tabular_sources",
                "description": "Trace upstream nodes/artifacts likely to contain tabular data for a target node.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "target_node_id": { "type": "string" },
                        "max_depth": { "type": "integer" }
                    },
                    "required": ["target_node_id"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "profile_numeric_distribution",
                "description": "Profile numeric distribution for a CSV column (min/max/mean/quantiles/missing).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "node_id": { "type": "string" },
                        "artifact_key": { "type": "string" },
                        "column": { "type": "string" }
                    },
                    "required": ["node_id", "column"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "infer_transform_patch",
                "description": "Infer a practical data_model_transform params patch from source artifact schema for goals like heatmap prep.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "source_node_id": { "type": "string" },
                        "goal": { "type": "string" },
                        "preferred_measure": { "type": "string" }
                    },
                    "required": ["source_node_id"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "preview_graph_diff_for_plan",
                "description": "Validate and preview a proposed graph change plan without applying.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "operations": {
                            "type": "array",
                            "items": { "type": "object" }
                        }
                    },
                    "required": ["operations"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "validate_pipeline_for_goal",
                "description": "Validate graph readiness for a goal workflow (e.g. heatmap, dem_imagery_3d).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "goal": { "type": "string" },
                        "target_node_id": { "type": "string" }
                    },
                    "required": ["goal"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "add_node",
                "description": "Add a node by kind (optionally with alias and params).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "kind": { "type": "string" },
                        "alias": { "type": "string" },
                        "params": { "type": "object" }
                    },
                    "required": ["kind"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "patch_node_config",
                "description": "Merge a config patch into node.params (for node config updates).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "node_id": { "type": "string" },
                        "params_patch": { "type": "object" }
                    },
                    "required": ["node_id", "params_patch"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "wire_nodes",
                "description": "Create graph edge between node ports.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "from_node": { "type": "string" },
                        "from_port": { "type": "string" },
                        "to_node": { "type": "string" },
                        "to_port": { "type": "string" },
                        "semantic_type": { "type": "string" }
                    },
                    "required": ["from_node", "from_port", "to_node", "to_port", "semantic_type"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "unwire_edge",
                "description": "Delete an edge by edge_id.",
                "parameters": {
                    "type": "object",
                    "properties": { "edge_id": { "type": "string" } },
                    "required": ["edge_id"]
                }
            }
        }
    ])
}

async fn execute_tool(
    store: &Arc<PgStore>,
    jobs: &Arc<PgJobQueue>,
    scheduler: &Arc<Scheduler>,
    artifact_root: &Path,
    graph_id: Uuid,
    uploaded_docs: &[UploadedDoc],
    name: &str,
    args: &serde_json::Value,
    apply_mutations: bool,
    actor: &str,
    branch_id: Option<Uuid>,
) -> Result<serde_json::Value, String> {
    match name {
        "run_graph" => {
            let roots = args
                .get("dirty_roots")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|x| x.as_str())
                        .filter_map(|s| Uuid::parse_str(s).ok())
                        .collect::<Vec<_>>()
                });
            let include_manual = args
                .get("include_manual")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            tool_run_graph(store, jobs, scheduler, graph_id, roots, include_manual).await
        }
        "run_node" => {
            let node_id =
                parse_uuid(args.get("node_id")).ok_or_else(|| "node_id is required".to_string())?;
            let include_manual = args
                .get("include_manual")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            tool_run_graph(
                store,
                jobs,
                scheduler,
                graph_id,
                Some(vec![node_id]),
                include_manual,
            )
            .await
        }
        "list_uploaded_files" => tool_list_uploaded_files(uploaded_docs),
        "uploaded_file_top_tail" => {
            let name = args
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "name is required".to_string())?;
            let head_lines = args
                .get("head_lines")
                .and_then(|v| v.as_u64())
                .unwrap_or(25) as usize;
            let tail_lines = args
                .get("tail_lines")
                .and_then(|v| v.as_u64())
                .unwrap_or(25) as usize;
            tool_uploaded_file_top_tail(uploaded_docs, name, head_lines, tail_lines)
        }
        "uploaded_csv_profile" => {
            let name = args
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "name is required".to_string())?;
            let sample_rows = args
                .get("sample_rows")
                .and_then(|v| v.as_u64())
                .unwrap_or(200) as usize;
            tool_uploaded_csv_profile(uploaded_docs, name, sample_rows)
        }
        "suggest_ingest_mapping_from_upload" => {
            let name = args
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "name is required".to_string())?;
            let node_kind = args
                .get("node_kind")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "node_kind is required".to_string())?;
            tool_suggest_ingest_mapping_from_upload(uploaded_docs, name, node_kind)
        }
        "apply_upload_to_ingest_node" => {
            let name = args
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "name is required".to_string())?;
            let node_id =
                parse_uuid(args.get("node_id")).ok_or_else(|| "node_id is required".to_string())?;
            let source_crs_epsg = args
                .get("source_crs_epsg")
                .and_then(|v| v.as_i64())
                .map(|v| v as i32);
            let use_project_crs = args
                .get("use_project_crs")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            tool_apply_upload_to_ingest_node(
                store,
                graph_id,
                uploaded_docs,
                name,
                node_id,
                source_crs_epsg,
                use_project_crs,
                apply_mutations,
                actor,
                branch_id,
            )
            .await
        }
        "graph_audit_bundle" => {
            let include_artifact_samples = args
                .get("include_artifact_samples")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            let head_lines = args
                .get("head_lines")
                .and_then(|v| v.as_u64())
                .unwrap_or(20) as usize;
            let tail_lines = args
                .get("tail_lines")
                .and_then(|v| v.as_u64())
                .unwrap_or(20) as usize;
            tool_graph_audit_bundle(
                store,
                artifact_root,
                graph_id,
                include_artifact_samples,
                head_lines,
                tail_lines,
            )
            .await
        }
        "registry_capability_matrix" => tool_registry_capability_matrix(),
        "list_nodes" => tool_list_nodes(store, graph_id).await,
        "list_edges" => tool_list_edges(store, graph_id).await,
        "list_node_artifacts" => {
            let node_id =
                parse_uuid(args.get("node_id")).ok_or_else(|| "node_id is required".to_string())?;
            tool_list_node_artifacts(store, node_id).await
        }
        "read_registry_kind" => {
            let kind = args
                .get("kind")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "kind is required".to_string())?;
            tool_read_registry_kind(kind)
        }
        "read_node" => {
            let node_id =
                parse_uuid(args.get("node_id")).ok_or_else(|| "node_id is required".to_string())?;
            tool_read_node(store, graph_id, node_id).await
        }
        "artifact_top_tail" => {
            let node_id =
                parse_uuid(args.get("node_id")).ok_or_else(|| "node_id is required".to_string())?;
            let artifact_key = args
                .get("artifact_key")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let head_lines = args
                .get("head_lines")
                .and_then(|v| v.as_u64())
                .unwrap_or(40) as usize;
            let tail_lines = args
                .get("tail_lines")
                .and_then(|v| v.as_u64())
                .unwrap_or(40) as usize;
            tool_artifact_top_tail(
                store,
                artifact_root,
                node_id,
                artifact_key,
                head_lines,
                tail_lines,
            )
            .await
        }
        "json_path_extract" => {
            let node_id =
                parse_uuid(args.get("node_id")).ok_or_else(|| "node_id is required".to_string())?;
            let artifact_key = args
                .get("artifact_key")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "path is required".to_string())?;
            tool_json_path_extract(store, artifact_root, node_id, artifact_key, path).await
        }
        "csv_profile" => {
            let node_id =
                parse_uuid(args.get("node_id")).ok_or_else(|| "node_id is required".to_string())?;
            let artifact_key = args
                .get("artifact_key")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let sample_rows = args
                .get("sample_rows")
                .and_then(|v| v.as_u64())
                .unwrap_or(200) as usize;
            tool_csv_profile(store, artifact_root, node_id, artifact_key, sample_rows).await
        }
        "suggest_measure_fields" => {
            let node_id =
                parse_uuid(args.get("node_id")).ok_or_else(|| "node_id is required".to_string())?;
            let artifact_key = args
                .get("artifact_key")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let top_k = args.get("top_k").and_then(|v| v.as_u64()).unwrap_or(6) as usize;
            tool_suggest_measure_fields(store, artifact_root, node_id, artifact_key, top_k).await
        }
        "trace_upstream_tabular_sources" => {
            let target = parse_uuid(args.get("target_node_id"))
                .ok_or_else(|| "target_node_id is required".to_string())?;
            let max_depth = args.get("max_depth").and_then(|v| v.as_u64()).unwrap_or(4) as usize;
            tool_trace_upstream_tabular_sources(store, graph_id, target, max_depth).await
        }
        "profile_numeric_distribution" => {
            let node_id =
                parse_uuid(args.get("node_id")).ok_or_else(|| "node_id is required".to_string())?;
            let artifact_key = args
                .get("artifact_key")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let column = args
                .get("column")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "column is required".to_string())?;
            tool_profile_numeric_distribution(store, artifact_root, node_id, artifact_key, column)
                .await
        }
        "infer_transform_patch" => {
            let source_node_id = parse_uuid(args.get("source_node_id"))
                .ok_or_else(|| "source_node_id is required".to_string())?;
            let goal = args
                .get("goal")
                .and_then(|v| v.as_str())
                .unwrap_or("heatmap");
            let preferred_measure = args
                .get("preferred_measure")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            tool_infer_transform_patch(
                store,
                artifact_root,
                source_node_id,
                goal,
                preferred_measure,
            )
            .await
        }
        "preview_graph_diff_for_plan" => {
            let ops = args
                .get("operations")
                .and_then(|v| v.as_array())
                .cloned()
                .ok_or_else(|| "operations array is required".to_string())?;
            tool_preview_graph_diff_for_plan(store, graph_id, &ops).await
        }
        "validate_pipeline_for_goal" => {
            let goal = args
                .get("goal")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "goal is required".to_string())?;
            let target_node_id = parse_uuid(args.get("target_node_id"));
            tool_validate_pipeline_for_goal(store, graph_id, goal, target_node_id).await
        }
        "add_node" => {
            let kind = args
                .get("kind")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "kind is required".to_string())?;
            let alias = args
                .get("alias")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let params = args.get("params").cloned().unwrap_or_else(|| json!({}));
            tool_add_node(
                store,
                graph_id,
                kind,
                alias,
                params,
                apply_mutations,
                actor,
                branch_id,
            )
            .await
        }
        "patch_node_config" => {
            let node_id =
                parse_uuid(args.get("node_id")).ok_or_else(|| "node_id is required".to_string())?;
            let patch = args
                .get("params_patch")
                .cloned()
                .ok_or_else(|| "params_patch is required".to_string())?;
            tool_patch_node_config(
                store,
                graph_id,
                node_id,
                patch,
                apply_mutations,
                actor,
                branch_id,
            )
            .await
        }
        "wire_nodes" => {
            let from_node = parse_uuid(args.get("from_node"))
                .ok_or_else(|| "from_node is required".to_string())?;
            let to_node =
                parse_uuid(args.get("to_node")).ok_or_else(|| "to_node is required".to_string())?;
            let from_port = args
                .get("from_port")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            let to_port = args
                .get("to_port")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            let semantic = args
                .get("semantic_type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            if from_port.is_empty() || to_port.is_empty() || semantic.is_empty() {
                return Err("from_port, to_port and semantic_type are required".to_string());
            }
            tool_wire_nodes(
                store,
                graph_id,
                from_node,
                &from_port,
                to_node,
                &to_port,
                &semantic,
                apply_mutations,
                actor,
                branch_id,
            )
            .await
        }
        "unwire_edge" => {
            let edge_id =
                parse_uuid(args.get("edge_id")).ok_or_else(|| "edge_id is required".to_string())?;
            tool_unwire_edge(store, graph_id, edge_id, apply_mutations, actor, branch_id).await
        }
        _ => Err(format!("unknown tool: {}", name)),
    }
}

async fn tool_list_nodes(
    store: &Arc<PgStore>,
    graph_id: Uuid,
) -> Result<serde_json::Value, String> {
    let snap = store
        .load_graph(graph_id)
        .await
        .map_err(|e| e.to_string())?;
    let mut out = snap
        .nodes
        .values()
        .map(|n| {
            let alias = n
                .config
                .params
                .get("_alias")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            json!({
                "node_id": n.id,
                "kind": n.config.kind,
                "alias": alias,
                "category": format!("{:?}", n.category).to_ascii_lowercase(),
                "execution": format!("{:?}", n.execution).to_ascii_lowercase(),
                "cache": format!("{:?}", n.cache).to_ascii_lowercase(),
            })
        })
        .collect::<Vec<_>>();
    out.sort_by_key(|v| {
        v.get("node_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string()
    });
    Ok(json!({ "nodes": out }))
}

fn tool_list_uploaded_files(uploaded_docs: &[UploadedDoc]) -> Result<serde_json::Value, String> {
    let files = uploaded_docs
        .iter()
        .map(|d| {
            let (headers, rows, delimiter) = parse_delimited_text(&d.text);
            json!({
                "name": d.name,
                "mime": d.mime,
                "size": d.size,
                "text_chars": d.text.chars().count(),
                "has_tabular_shape": !headers.is_empty() && rows > 0,
                "delimiter": delimiter.map(|c| c.to_string()),
                "header_count": headers.len(),
                "headers": headers.into_iter().take(24).collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({ "files": files }))
}

fn tool_uploaded_file_top_tail(
    uploaded_docs: &[UploadedDoc],
    name: &str,
    head_lines: usize,
    tail_lines: usize,
) -> Result<serde_json::Value, String> {
    let doc = find_uploaded_doc(uploaded_docs, name)?;
    let lines = doc.text.lines().collect::<Vec<_>>();
    let h = head_lines.clamp(1, 200);
    let t = tail_lines.clamp(1, 200);
    let head = lines.iter().take(h).cloned().collect::<Vec<_>>().join("\n");
    let tail = lines
        .iter()
        .rev()
        .take(t)
        .cloned()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n");
    Ok(json!({
        "name": doc.name,
        "mime": doc.mime,
        "size": doc.size,
        "line_count": lines.len(),
        "head": truncate(&head, MAX_TOOL_OUTPUT_CHARS / 2),
        "tail": truncate(&tail, MAX_TOOL_OUTPUT_CHARS / 2)
    }))
}

fn tool_uploaded_csv_profile(
    uploaded_docs: &[UploadedDoc],
    name: &str,
    sample_rows: usize,
) -> Result<serde_json::Value, String> {
    let doc = find_uploaded_doc(uploaded_docs, name)?;
    profile_csv_text(&doc.text, sample_rows).map(|mut v| {
        if let Some(obj) = v.as_object_mut() {
            obj.insert("name".to_string(), json!(doc.name.clone()));
            obj.insert("mime".to_string(), json!(doc.mime.clone()));
            obj.insert("size".to_string(), json!(doc.size));
        }
        v
    })
}

fn tool_suggest_ingest_mapping_from_upload(
    uploaded_docs: &[UploadedDoc],
    name: &str,
    node_kind: &str,
) -> Result<serde_json::Value, String> {
    let doc = find_uploaded_doc(uploaded_docs, name)?;
    let (headers, rows, delimiter) = parse_delimited_text(&doc.text);
    if headers.is_empty() {
        return Err(format!(
            "uploaded file '{}' has no tabular header row",
            name
        ));
    }
    let mapping = infer_ingest_mapping(node_kind, &headers);
    let required_missing = required_mapping_keys(node_kind)
        .into_iter()
        .filter(|k| {
            mapping
                .get(*k)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .is_empty()
        })
        .collect::<Vec<_>>();
    let confidence = mapping_confidence_score(node_kind, &mapping);
    Ok(json!({
        "name": doc.name,
        "node_kind": node_kind,
        "delimiter": delimiter.map(|d| d.to_string()),
        "row_count_estimate": rows,
        "mapping": mapping,
        "missing_required": required_missing,
        "confidence": confidence
    }))
}

async fn tool_apply_upload_to_ingest_node(
    store: &Arc<PgStore>,
    graph_id: Uuid,
    uploaded_docs: &[UploadedDoc],
    name: &str,
    node_id: Uuid,
    source_crs_epsg: Option<i32>,
    use_project_crs: bool,
    apply_mutations: bool,
    actor: &str,
    branch_id: Option<Uuid>,
) -> Result<serde_json::Value, String> {
    let doc = find_uploaded_doc(uploaded_docs, name)?;
    let snap = store
        .load_graph(graph_id)
        .await
        .map_err(|e| e.to_string())?;
    let node = snap
        .nodes
        .get(&node_id)
        .ok_or_else(|| format!("node {} not found in graph {}", node_id, graph_id))?;
    let kind = node.config.kind.as_str();
    if !matches!(
        kind,
        "collar_ingest"
            | "survey_ingest"
            | "assay_ingest"
            | "lithology_ingest"
            | "orientation_ingest"
            | "surface_sample_ingest"
    ) {
        return Err(format!(
            "node {} kind '{}' is not an ingest node supported by this tool",
            node_id, kind
        ));
    }

    let (headers, rows, _) = parse_delimited_rows(&doc.text, 600);
    if headers.is_empty() || rows.is_empty() {
        return Err(format!("uploaded file '{}' is missing tabular rows", name));
    }
    let mapping = infer_ingest_mapping(kind, &headers);
    let missing_required = required_mapping_keys(kind)
        .into_iter()
        .filter(|k| {
            mapping
                .get(*k)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .is_empty()
        })
        .collect::<Vec<_>>();
    let patch = json!({
        "ui": {
            "csv_headers": headers,
            "csv_rows": rows.into_iter().take(200).collect::<Vec<_>>(),
            "mapping": mapping,
            "use_project_crs": use_project_crs,
            "source_crs_epsg": source_crs_epsg.unwrap_or(4326)
        }
    });

    let preview = json!({
        "node_id": node_id,
        "node_kind": kind,
        "file": doc.name,
        "missing_required": missing_required,
        "params_patch": patch
    });

    if !apply_mutations {
        return Ok(json!({
            "dry_run": true,
            "note": "Mutation skipped because apply_mutations=false",
            "proposal": preview
        }));
    }

    let out = tool_patch_node_config(
        store,
        graph_id,
        node_id,
        patch,
        apply_mutations,
        actor,
        branch_id,
    )
    .await?;
    Ok(json!({
        "applied": out,
        "missing_required": missing_required
    }))
}

async fn tool_graph_audit_bundle(
    store: &Arc<PgStore>,
    artifact_root: &Path,
    graph_id: Uuid,
    include_artifact_samples: bool,
    head_lines: usize,
    tail_lines: usize,
) -> Result<serde_json::Value, String> {
    let snap = store
        .load_graph(graph_id)
        .await
        .map_err(|e| e.to_string())?;
    let registry = NodeRegistry::global()?;
    let capabilities = tool_registry_capability_matrix()?;

    let mut node_rows: Vec<serde_json::Value> = Vec::new();
    let mut failed_nodes: Vec<Uuid> = Vec::new();
    let mut ingest_missing_payload: Vec<Uuid> = Vec::new();

    let mut ids = snap.nodes.keys().copied().collect::<Vec<_>>();
    ids.sort();
    for node_id in ids {
        let Some(n) = snap.nodes.get(&node_id) else {
            continue;
        };
        let alias = n
            .config
            .params
            .get("_alias")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let exec = format!("{:?}", n.execution).to_ascii_lowercase();
        let cache = format!("{:?}", n.cache).to_ascii_lowercase();
        let last_error = n.last_error.clone().unwrap_or_default();
        if exec == "failed" {
            failed_nodes.push(node_id);
        }
        if matches!(
            n.config.kind.as_str(),
            "collar_ingest"
                | "survey_ingest"
                | "assay_ingest"
                | "lithology_ingest"
                | "orientation_ingest"
                | "surface_sample_ingest"
        ) && last_error
            .to_ascii_lowercase()
            .contains("missing input_payload")
        {
            ingest_missing_payload.push(node_id);
        }

        let mapping_keys = n
            .config
            .params
            .get("ui")
            .and_then(|ui| ui.get("mapping"))
            .and_then(|m| m.as_object())
            .map(|m| {
                let mut k = m.keys().cloned().collect::<Vec<_>>();
                k.sort();
                k
            })
            .unwrap_or_default();
        let csv_rows_count = n
            .config
            .params
            .get("ui")
            .and_then(|ui| ui.get("csv_rows"))
            .and_then(|r| r.as_array())
            .map(|r| r.len())
            .unwrap_or(0usize);

        let mut arts = store
            .list_artifacts_for_node(node_id)
            .await
            .map_err(|e| e.to_string())?;
        arts.sort_by(|a, b| a.0.cmp(&b.0));
        let mut artifact_rows: Vec<serde_json::Value> = Vec::new();
        for (artifact_key, _, media_type) in arts.iter().take(6) {
            artifact_rows.push(json!({
                "artifact_key": artifact_key,
                "media_type": media_type,
            }));
        }

        let sample = if include_artifact_samples {
            if let Some((first_key, _, _)) = arts.first() {
                let sample_json = tool_artifact_top_tail(
                    store,
                    artifact_root,
                    node_id,
                    Some(first_key.clone()),
                    head_lines,
                    tail_lines,
                )
                .await
                .unwrap_or_else(|e| json!({ "error": e }));
                Some(sample_json)
            } else {
                None
            }
        } else {
            None
        };

        let ports = registry
            .ports_for_kind(&n.config.kind)
            .cloned()
            .unwrap_or_default();
        node_rows.push(json!({
            "node_id": n.id,
            "kind": n.config.kind,
            "alias": alias,
            "execution": exec,
            "cache": cache,
            "last_error": last_error,
            "inputs": ports.inputs,
            "outputs": ports.outputs,
            "ui_mapping_keys": mapping_keys,
            "ui_csv_rows_count": csv_rows_count,
            "artifacts": artifact_rows,
            "artifact_sample": sample
        }));
    }

    let mut edges = snap
        .edges
        .iter()
        .map(|e| {
            let fk = snap
                .nodes
                .get(&e.from_node)
                .map(|n| n.config.kind.clone())
                .unwrap_or_else(|| "unknown".to_string());
            let tk = snap
                .nodes
                .get(&e.to_node)
                .map(|n| n.config.kind.clone())
                .unwrap_or_else(|| "unknown".to_string());
            json!({
                "edge_id": e.id,
                "from_node": e.from_node,
                "from_kind": fk,
                "from_port": e.from_port,
                "to_node": e.to_node,
                "to_kind": tk,
                "to_port": e.to_port,
                "semantic_type": e.semantic_type.as_str(),
            })
        })
        .collect::<Vec<_>>();
    edges.sort_by_key(|v| {
        v.get("edge_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string()
    });

    Ok(json!({
        "graph_id": graph_id,
        "summary": {
            "node_count": node_rows.len(),
            "edge_count": edges.len(),
            "failed_node_count": failed_nodes.len(),
            "failed_nodes": failed_nodes,
            "ingest_missing_payload_nodes": ingest_missing_payload
        },
        "nodes": node_rows,
        "edges": edges,
        "capability_matrix": capabilities.get("kinds").cloned().unwrap_or_else(|| json!([]))
    }))
}

async fn tool_run_graph(
    store: &Arc<PgStore>,
    jobs: &Arc<PgJobQueue>,
    scheduler: &Arc<Scheduler>,
    graph_id: Uuid,
    dirty_roots: Option<Vec<Uuid>>,
    include_manual: bool,
) -> Result<serde_json::Value, String> {
    let snapshot = store
        .load_graph(graph_id)
        .await
        .map_err(|e| e.to_string())?;
    let roots = dirty_roots.unwrap_or_else(|| snapshot.nodes.keys().copied().collect::<Vec<_>>());
    let root_set: HashSet<Uuid> = roots.iter().copied().collect();
    let dirty = collect_dirty_nodes(&snapshot, &roots);
    let input_map = build_input_artifacts_for_chat(store, &snapshot).await?;
    let project_crs: Option<CrsRecord> = store
        .graph_workspace_meta(graph_id)
        .await
        .map_err(|e| e.to_string())?
        .and_then(|(_, _, crs)| crs);

    let plan = scheduler.plan(
        &snapshot,
        &dirty,
        &root_set,
        &input_map,
        Uuid::new_v4(),
        project_crs.clone(),
        include_manual,
    );
    let mut queued = Vec::new();
    for mut job in plan.jobs {
        if job.input_payload.is_none() {
            if let Some(node) = snapshot.nodes.get(&job.node_id) {
                if let Some(p) =
                    crate::ingest_synth::synthesize_input_payload(node, project_crs.as_ref())
                {
                    job.input_payload = Some(p);
                }
            }
        }
        let row_id = jobs.enqueue(&job).await.map_err(|e| e.to_string())?;
        queued.push(json!({
            "queue_row": row_id,
            "job_id": job.job_id,
            "node_id": job.node_id
        }));
    }

    Ok(json!({
        "queued": queued,
        "queued_count": queued.len(),
        "skipped_manual": plan.skipped_manual,
        "roots": roots
    }))
}

async fn build_input_artifacts_for_chat(
    store: &PgStore,
    snapshot: &mine_eye_graph::GraphSnapshot,
) -> Result<HashMap<Uuid, Vec<ArtifactRef>>, String> {
    let mut m: HashMap<Uuid, Vec<ArtifactRef>> = HashMap::new();
    for edge in &snapshot.edges {
        let arts = store
            .list_artifacts_for_node(edge.from_node)
            .await
            .map_err(|e| e.to_string())?;
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

fn tool_registry_capability_matrix() -> Result<serde_json::Value, String> {
    let root = NodeRegistry::global()?.root().clone();
    let mut rows = root
        .nodes
        .into_iter()
        .map(|n| {
            let label = n.label.clone().unwrap_or_default();
            let role = n.role.clone().unwrap_or_default();
            let archived = {
                let l = label.to_ascii_lowercase();
                let r = role.to_ascii_lowercase();
                l.contains("historic")
                    || l.contains("archive")
                    || r.contains("historic")
                    || r.contains("archive")
            };
            json!({
                "kind": n.kind,
                "label": n.label,
                "category": n.category,
                "role": n.role,
                "archived_or_historic": archived,
                "inputs": n.ports.inputs,
                "outputs": n.ports.outputs,
                "assistant_hint": node_prompt_fragment(&n.kind)
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|v| {
        v.get("kind")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string()
    });
    Ok(json!({ "kinds": rows }))
}

async fn tool_list_edges(
    store: &Arc<PgStore>,
    graph_id: Uuid,
) -> Result<serde_json::Value, String> {
    let snap = store
        .load_graph(graph_id)
        .await
        .map_err(|e| e.to_string())?;
    let mut out = snap
        .edges
        .iter()
        .map(|e| {
            json!({
                "edge_id": e.id,
                "from_node": e.from_node,
                "from_port": e.from_port,
                "to_node": e.to_node,
                "to_port": e.to_port,
                "semantic_type": e.semantic_type.as_str(),
            })
        })
        .collect::<Vec<_>>();
    out.sort_by_key(|v| {
        v.get("edge_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string()
    });
    Ok(json!({ "edges": out }))
}

async fn tool_list_node_artifacts(
    store: &Arc<PgStore>,
    node_id: Uuid,
) -> Result<serde_json::Value, String> {
    let mut rows = store
        .list_artifacts_for_node(node_id)
        .await
        .map_err(|e| e.to_string())?;
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let artifacts = rows
        .into_iter()
        .map(|(key, hash, media)| {
            let lower = key.to_ascii_lowercase();
            let likely_tabular = lower.ends_with(".csv")
                || lower.ends_with(".tsv")
                || lower.ends_with(".json")
                || lower.ends_with(".geojson")
                || lower.contains("sample")
                || lower.contains("assay")
                || lower.contains("table");
            json!({
                "artifact_key": key,
                "content_hash": hash,
                "media_type": media,
                "likely_tabular": likely_tabular
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({ "node_id": node_id, "artifacts": artifacts }))
}

fn tool_read_registry_kind(kind: &str) -> Result<serde_json::Value, String> {
    let registry = NodeRegistry::global()?;
    let n = registry
        .kind(kind)
        .cloned()
        .ok_or_else(|| format!("kind '{}' not found in registry", kind))?;
    Ok(json!({
        "kind": n.kind,
        "label": n.label,
        "role": n.role,
        "category": n.category,
        "ports": n.ports,
        "assistant_hint": node_prompt_fragment(kind)
    }))
}

async fn tool_read_node(
    store: &Arc<PgStore>,
    graph_id: Uuid,
    node_id: Uuid,
) -> Result<serde_json::Value, String> {
    let snap = store
        .load_graph(graph_id)
        .await
        .map_err(|e| e.to_string())?;
    let node = snap
        .nodes
        .get(&node_id)
        .ok_or_else(|| format!("node {} not found in graph {}", node_id, graph_id))?;
    Ok(json!({
        "node_id": node.id,
        "kind": node.config.kind,
        "version": node.config.version,
        "params": node.config.params,
        "policy": node.policy
    }))
}

async fn tool_artifact_top_tail(
    store: &Arc<PgStore>,
    artifact_root: &Path,
    node_id: Uuid,
    artifact_key: Option<String>,
    head_lines: usize,
    tail_lines: usize,
) -> Result<serde_json::Value, String> {
    let (selected_key, bytes) =
        read_artifact_bytes(store, artifact_root, node_id, artifact_key).await?;

    let mut is_text = true;
    for b in bytes.iter().take(1024) {
        if *b == 0 {
            is_text = false;
            break;
        }
    }

    if !is_text {
        let head = bytes
            .iter()
            .take(128)
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join("");
        let tail = bytes
            .iter()
            .rev()
            .take(128)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join("");
        return Ok(json!({
            "artifact_key": selected_key,
            "size_bytes": bytes.len(),
            "mode": "binary",
            "head_hex": head,
            "tail_hex": tail
        }));
    }

    let text = String::from_utf8_lossy(&bytes).to_string();
    let lines = text.lines().collect::<Vec<_>>();
    let h = head_lines.clamp(1, 200);
    let t = tail_lines.clamp(1, 200);
    let head = lines.iter().take(h).cloned().collect::<Vec<_>>().join("\n");
    let tail = lines
        .iter()
        .rev()
        .take(t)
        .cloned()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n");

    Ok(json!({
        "artifact_key": selected_key,
        "size_bytes": bytes.len(),
        "line_count": lines.len(),
        "mode": "text",
        "head": truncate(&head, MAX_TOOL_OUTPUT_CHARS / 2),
        "tail": truncate(&tail, MAX_TOOL_OUTPUT_CHARS / 2),
    }))
}

async fn tool_json_path_extract(
    store: &Arc<PgStore>,
    artifact_root: &Path,
    node_id: Uuid,
    artifact_key: Option<String>,
    path: &str,
) -> Result<serde_json::Value, String> {
    let (selected_key, bytes) =
        read_artifact_bytes(store, artifact_root, node_id, artifact_key).await?;
    let root: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|e| format!("artifact is not valid JSON: {}", e))?;
    let found =
        json_lookup_path(&root, path).ok_or_else(|| format!("path '{}' not found", path))?;
    Ok(json!({
        "artifact_key": selected_key,
        "path": path,
        "value": found
    }))
}

async fn tool_csv_profile(
    store: &Arc<PgStore>,
    artifact_root: &Path,
    node_id: Uuid,
    artifact_key: Option<String>,
    sample_rows: usize,
) -> Result<serde_json::Value, String> {
    let (selected_key, bytes) =
        read_artifact_bytes(store, artifact_root, node_id, artifact_key).await?;
    let text = String::from_utf8(bytes).map_err(|_| "artifact is not UTF-8 text".to_string())?;
    let lines = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .take(1200)
        .collect::<Vec<_>>();
    if lines.is_empty() {
        return Err("artifact has no non-empty lines".to_string());
    }
    let delimiter = detect_delimiter(lines[0]);
    let split = |line: &str| {
        line.split(delimiter)
            .map(|s| s.trim().to_string())
            .collect::<Vec<_>>()
    };

    let headers = split(lines[0]);
    if headers.is_empty() {
        return Err("could not parse headers".to_string());
    }
    let mut types = vec![TypeCounts::default(); headers.len()];
    let max_rows = sample_rows.clamp(10, 2000);
    for line in lines.iter().skip(1).take(max_rows) {
        let cols = split(line);
        for (i, tc) in types.iter_mut().enumerate() {
            let val = cols.get(i).map(|s| s.as_str()).unwrap_or("");
            tc.observe(val);
        }
    }
    let columns = headers
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let inferred = types[i].inferred();
            json!({
                "name": h,
                "inferred_type": inferred,
                "samples": types[i].samples.clone(),
                "null_like": types[i].null_like,
                "numeric_like": types[i].numeric_like,
                "bool_like": types[i].bool_like
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "artifact_key": selected_key,
        "delimiter": delimiter.to_string(),
        "header_count": headers.len(),
        "sampled_rows": lines.iter().skip(1).take(max_rows).count(),
        "columns": columns
    }))
}

async fn tool_suggest_measure_fields(
    store: &Arc<PgStore>,
    artifact_root: &Path,
    node_id: Uuid,
    artifact_key: Option<String>,
    top_k: usize,
) -> Result<serde_json::Value, String> {
    let (selected_key, bytes) =
        read_artifact_bytes(store, artifact_root, node_id, artifact_key).await?;
    let text = String::from_utf8(bytes).map_err(|_| "artifact is not UTF-8 text".to_string())?;
    let lines = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .take(1200)
        .collect::<Vec<_>>();
    if lines.len() < 2 {
        return Err("artifact needs header + data rows for field suggestion".to_string());
    }
    let delimiter = detect_delimiter(lines[0]);
    let split = |line: &str| {
        line.split(delimiter)
            .map(|s| s.trim().to_string())
            .collect::<Vec<_>>()
    };
    let headers = split(lines[0]);
    if headers.is_empty() {
        return Err("could not parse headers".to_string());
    }

    let mut numeric_hits = vec![0usize; headers.len()];
    let mut non_empty = vec![0usize; headers.len()];
    for row in lines.iter().skip(1).take(600) {
        let cols = split(row);
        for i in 0..headers.len() {
            let v = cols.get(i).map(|s| s.trim()).unwrap_or("");
            if v.is_empty() {
                continue;
            }
            non_empty[i] += 1;
            if v.parse::<f64>().is_ok() {
                numeric_hits[i] += 1;
            }
        }
    }

    let mut ranked = headers
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let ratio = if non_empty[i] == 0 {
                0.0
            } else {
                numeric_hits[i] as f64 / non_empty[i] as f64
            };
            let name_score = measure_name_score(h);
            let score = (ratio * 0.7) + (name_score * 0.3);
            json!({
                "column": h,
                "score": (score * 1000.0).round() / 1000.0,
                "numeric_ratio": (ratio * 1000.0).round() / 1000.0,
                "name_score": (name_score * 1000.0).round() / 1000.0,
                "non_empty": non_empty[i]
            })
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|a, b| {
        b.get("score")
            .and_then(|v| v.as_f64())
            .partial_cmp(&a.get("score").and_then(|v| v.as_f64()))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let k = top_k.clamp(1, 20);
    ranked.truncate(k);
    Ok(json!({
        "artifact_key": selected_key,
        "delimiter": delimiter.to_string(),
        "top_candidates": ranked
    }))
}

async fn tool_trace_upstream_tabular_sources(
    store: &Arc<PgStore>,
    graph_id: Uuid,
    target_node_id: Uuid,
    max_depth: usize,
) -> Result<serde_json::Value, String> {
    let snap = store
        .load_graph(graph_id)
        .await
        .map_err(|e| e.to_string())?;
    if !snap.nodes.contains_key(&target_node_id) {
        return Err(format!("target_node_id {} not found", target_node_id));
    }
    let depth_cap = max_depth.clamp(1, 12);
    let mut frontier = vec![target_node_id];
    let mut seen = HashSet::new();
    seen.insert(target_node_id);
    let mut found = Vec::new();

    for depth in 1..=depth_cap {
        let mut next = Vec::new();
        for node in frontier {
            for e in snap.edges.iter().filter(|e| e.to_node == node) {
                if seen.insert(e.from_node) {
                    next.push(e.from_node);
                }
                let Some(src) = snap.nodes.get(&e.from_node) else {
                    continue;
                };
                let rows = store
                    .list_artifacts_for_node(e.from_node)
                    .await
                    .map_err(|er| er.to_string())?;
                for (key, _, media) in rows {
                    let lower = key.to_ascii_lowercase();
                    let likely = lower.ends_with(".csv")
                        || lower.ends_with(".tsv")
                        || lower.ends_with(".json")
                        || lower.ends_with(".geojson")
                        || matches!(media.as_deref(), Some("text/csv" | "application/json"));
                    if !likely {
                        continue;
                    }
                    found.push(json!({
                        "depth": depth,
                        "node_id": src.id,
                        "node_kind": src.config.kind,
                        "artifact_key": key,
                        "via_edge": e.id
                    }));
                }
            }
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
    }
    Ok(json!({
        "target_node_id": target_node_id,
        "max_depth": depth_cap,
        "candidates": found
    }))
}

async fn tool_profile_numeric_distribution(
    store: &Arc<PgStore>,
    artifact_root: &Path,
    node_id: Uuid,
    artifact_key: Option<String>,
    column: &str,
) -> Result<serde_json::Value, String> {
    let (selected_key, bytes) =
        read_artifact_bytes(store, artifact_root, node_id, artifact_key).await?;
    let text = String::from_utf8(bytes).map_err(|_| "artifact is not UTF-8 text".to_string())?;
    let lines = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .take(5000)
        .collect::<Vec<_>>();
    if lines.len() < 2 {
        return Err("artifact needs header + rows".to_string());
    }
    let delimiter = detect_delimiter(lines[0]);
    let headers = lines[0]
        .split(delimiter)
        .map(|s| s.trim().to_string())
        .collect::<Vec<_>>();
    let idx = headers
        .iter()
        .position(|h| h.eq_ignore_ascii_case(column))
        .ok_or_else(|| format!("column '{}' not found", column))?;

    let mut vals: Vec<f64> = Vec::new();
    let mut missing = 0usize;
    for row in lines.iter().skip(1) {
        let cols = row.split(delimiter).map(|s| s.trim()).collect::<Vec<_>>();
        let v = cols.get(idx).copied().unwrap_or("");
        if v.is_empty() || matches!(v.to_ascii_lowercase().as_str(), "null" | "na" | "n/a") {
            missing += 1;
            continue;
        }
        if let Ok(n) = v.parse::<f64>() {
            if n.is_finite() {
                vals.push(n);
            }
        } else {
            missing += 1;
        }
    }
    if vals.is_empty() {
        return Err(format!("column '{}' has no numeric values", column));
    }
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = vals.len();
    let min = vals[0];
    let max = vals[n - 1];
    let mean = vals.iter().sum::<f64>() / n as f64;
    let p = |q: f64| -> f64 {
        let pos = ((n - 1) as f64 * q).round() as usize;
        vals[pos]
    };
    Ok(json!({
        "artifact_key": selected_key,
        "column": headers[idx],
        "count_numeric": n,
        "count_missing_or_non_numeric": missing,
        "min": min,
        "p05": p(0.05),
        "p25": p(0.25),
        "p50": p(0.50),
        "p75": p(0.75),
        "p95": p(0.95),
        "max": max,
        "mean": mean
    }))
}

async fn tool_infer_transform_patch(
    store: &Arc<PgStore>,
    artifact_root: &Path,
    source_node_id: Uuid,
    goal: &str,
    preferred_measure: Option<String>,
) -> Result<serde_json::Value, String> {
    let suggested =
        tool_suggest_measure_fields(store, artifact_root, source_node_id, None, 8).await?;
    let top_candidates = suggested
        .get("top_candidates")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let chosen = if let Some(pref) = preferred_measure.as_deref() {
        top_candidates
            .iter()
            .find(|v| {
                v.get("column")
                    .and_then(|x| x.as_str())
                    .map(|s| s.eq_ignore_ascii_case(pref))
                    .unwrap_or(false)
            })
            .and_then(|v| {
                v.get("column")
                    .and_then(|x| x.as_str())
                    .map(|s| s.to_string())
            })
            .unwrap_or_else(|| pref.to_string())
    } else {
        top_candidates
            .first()
            .and_then(|v| v.get("column").and_then(|x| x.as_str()))
            .unwrap_or("value")
            .to_string()
    };

    let goal_norm = goal.trim().to_ascii_lowercase();
    let patch = if goal_norm.contains("heat") {
        json!({
            "mode": "column_select_and_normalize",
            "keep_columns": ["x", "y", "z", chosen.as_str()],
            "measure_column": chosen,
            "null_policy": "drop_rows_with_missing_measure",
            "unit_hint": "preserve"
        })
    } else {
        json!({
            "mode": "column_select",
            "measure_column": chosen
        })
    };
    Ok(json!({
        "source_node_id": source_node_id,
        "goal": goal,
        "suggested_params_patch": patch,
        "candidate_columns": top_candidates
    }))
}

async fn tool_preview_graph_diff_for_plan(
    store: &Arc<PgStore>,
    graph_id: Uuid,
    operations: &[serde_json::Value],
) -> Result<serde_json::Value, String> {
    let snap = store
        .load_graph(graph_id)
        .await
        .map_err(|e| e.to_string())?;
    let registry = NodeRegistry::global()?;
    let mut results = Vec::new();
    for (i, op) in operations.iter().enumerate() {
        let t = op.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let result = match t {
            "patch_node_config" => {
                let node_id = parse_uuid(op.get("node_id"));
                match node_id {
                    Some(id) if snap.nodes.contains_key(&id) => {
                        json!({"index": i, "type": t, "ok": true, "summary": "node exists"})
                    }
                    Some(id) => {
                        json!({"index": i, "type": t, "ok": false, "summary": format!("node {} not found", id)})
                    }
                    None => {
                        json!({"index": i, "type": t, "ok": false, "summary": "node_id missing/invalid"})
                    }
                }
            }
            "wire_nodes" => {
                let from = parse_uuid(op.get("from_node"));
                let to = parse_uuid(op.get("to_node"));
                let fp = op.get("from_port").and_then(|v| v.as_str()).unwrap_or("");
                let tp = op.get("to_port").and_then(|v| v.as_str()).unwrap_or("");
                let ok = if let (Some(f), Some(tn)) = (from, to) {
                    if let (Some(fn_rec), Some(tn_rec)) = (snap.nodes.get(&f), snap.nodes.get(&tn))
                    {
                        registry
                            .resolve_edge_semantic(&fn_rec.config.kind, fp, &tn_rec.config.kind, tp)
                            .is_ok()
                    } else {
                        false
                    }
                } else {
                    false
                };
                json!({"index": i, "type": t, "ok": ok, "summary": if ok { "valid wire plan" } else { "invalid wire plan" }})
            }
            "unwire_edge" => {
                let edge_id = parse_uuid(op.get("edge_id"));
                let ok = edge_id
                    .map(|id| snap.edges.iter().any(|e| e.id == id))
                    .unwrap_or(false);
                json!({"index": i, "type": t, "ok": ok, "summary": if ok { "edge exists" } else { "edge missing/invalid" }})
            }
            _ => {
                json!({"index": i, "type": t, "ok": false, "summary": "unsupported operation type"})
            }
        };
        results.push(result);
    }
    let ok_count = results
        .iter()
        .filter(|r| r.get("ok").and_then(|v| v.as_bool()).unwrap_or(false))
        .count();
    Ok(json!({
        "operation_count": operations.len(),
        "ok_count": ok_count,
        "results": results
    }))
}

async fn tool_validate_pipeline_for_goal(
    store: &Arc<PgStore>,
    graph_id: Uuid,
    goal: &str,
    target_node_id: Option<Uuid>,
) -> Result<serde_json::Value, String> {
    let snap = store
        .load_graph(graph_id)
        .await
        .map_err(|e| e.to_string())?;
    let goal_norm = goal.trim().to_ascii_lowercase();
    let mut findings: Vec<serde_json::Value> = Vec::new();
    let kinds = snap
        .nodes
        .values()
        .map(|n| n.config.kind.clone())
        .collect::<Vec<_>>();
    let has_kind = |k: &str| kinds.iter().any(|x| x == k);

    if goal_norm.contains("heat") {
        if !has_kind("assay_heatmap") {
            findings.push(json!({"severity":"error","message":"Missing assay_heatmap node"}));
        }
        if !has_kind("aoi") {
            findings.push(json!({"severity":"warn","message":"No AOI node found; extent constraints may be weak"}));
        }
    } else if goal_norm.contains("dem") || goal_norm.contains("3d") {
        if !has_kind("dem_fetch") {
            findings.push(json!({"severity":"error","message":"Missing dem_fetch node"}));
        }
        if !has_kind("tilebroker") && !has_kind("imagery_provider") {
            findings.push(json!({"severity":"warn","message":"No imagery provider/tilebroker for draped terrain"}));
        }
    }

    if let Some(target) = target_node_id {
        let inbound = snap.edges.iter().filter(|e| e.to_node == target).count();
        if inbound == 0 {
            findings.push(json!({"severity":"warn","message":"Target node has no inbound edges"}));
        }
    }
    if findings.is_empty() {
        findings.push(
            json!({"severity":"ok","message":"No obvious structural issues for stated goal"}),
        );
    }
    Ok(json!({
        "goal": goal,
        "target_node_id": target_node_id,
        "findings": findings
    }))
}

async fn tool_add_node(
    store: &Arc<PgStore>,
    graph_id: Uuid,
    kind: &str,
    alias: Option<String>,
    params: serde_json::Value,
    apply_mutations: bool,
    actor: &str,
    branch_id: Option<Uuid>,
) -> Result<serde_json::Value, String> {
    let category = node_category_for_kind(kind)?;
    let mut params_obj = if params.is_object() {
        params
    } else {
        json!({})
    };
    if let Some(a) = alias {
        if let Some(obj) = params_obj.as_object_mut() {
            obj.insert("_alias".to_string(), json!(a));
        }
    }
    if !apply_mutations {
        return Ok(json!({
            "dry_run": true,
            "would_add": {
                "kind": kind,
                "category": format!("{:?}", category).to_ascii_lowercase(),
                "params": params_obj
            },
            "note": "Mutation skipped because apply_mutations=false"
        }));
    }
    let id = Uuid::new_v4();
    let node = NodeRecord {
        id,
        graph_id,
        category,
        config: NodeConfig {
            version: 1,
            kind: kind.to_string(),
            params: params_obj.clone(),
        },
        execution: ExecutionState::Idle,
        cache: CacheState::Stale,
        policy: NodeExecutionPolicy::default(),
        ports: Vec::new(),
        lineage: LineageMeta::default(),
        content_hash: None,
        last_error: None,
    };
    store.upsert_node(&node).await.map_err(|e| e.to_string())?;
    let _ = commit_graph_revision(
        store.as_ref(),
        graph_id,
        actor,
        "ai_add_node",
        branch_id,
        json!({
            "node_id": id,
            "kind": kind,
            "params": params_obj
        }),
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(json!({
        "added_node_id": id,
        "kind": kind
    }))
}

async fn tool_patch_node_config(
    store: &Arc<PgStore>,
    graph_id: Uuid,
    node_id: Uuid,
    params_patch: serde_json::Value,
    apply_mutations: bool,
    actor: &str,
    branch_id: Option<Uuid>,
) -> Result<serde_json::Value, String> {
    if !params_patch.is_object() {
        return Err("params_patch must be a JSON object".to_string());
    }
    if !apply_mutations {
        return Ok(json!({
            "dry_run": true,
            "would_update_node_id": node_id,
            "params_patch": params_patch,
            "note": "Mutation skipped because apply_mutations=false"
        }));
    }
    let updated = store
        .patch_node_config(
            graph_id,
            node_id,
            params_patch.clone(),
            Option::<NodeExecutionPolicy>::None,
        )
        .await
        .map_err(|e| e.to_string())?;
    let _ = commit_graph_revision(
        store.as_ref(),
        graph_id,
        actor,
        "ai_patch_node_config",
        branch_id,
        json!({
            "node_id": node_id,
            "params_patch": params_patch
        }),
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(json!({
        "updated_node_id": updated.id,
        "kind": updated.config.kind,
        "applied_params_patch": params_patch
    }))
}

async fn tool_wire_nodes(
    store: &Arc<PgStore>,
    graph_id: Uuid,
    from_node: Uuid,
    from_port: &str,
    to_node: Uuid,
    to_port: &str,
    semantic_type: &str,
    apply_mutations: bool,
    actor: &str,
    branch_id: Option<Uuid>,
) -> Result<serde_json::Value, String> {
    let snap = store
        .load_graph(graph_id)
        .await
        .map_err(|e| e.to_string())?;
    let from_node_ref = snap
        .nodes
        .get(&from_node)
        .ok_or_else(|| format!("from_node {} not found", from_node))?;
    let to_node_ref = snap
        .nodes
        .get(&to_node)
        .ok_or_else(|| format!("to_node {} not found", to_node))?;

    let registry = NodeRegistry::global()?;
    let resolved_semantic = registry.resolve_edge_semantic(
        &from_node_ref.config.kind,
        from_port,
        &to_node_ref.config.kind,
        to_port,
    )?;
    let requested_semantic: SemanticPortType = semantic_type.parse()?;
    if requested_semantic != resolved_semantic {
        return Err(format!(
            "semantic_type '{}' does not match resolved wire semantic '{}'",
            semantic_type,
            resolved_semantic.as_str()
        ));
    }

    if snap.edges.iter().any(|e| {
        e.from_node == from_node
            && e.from_port == from_port
            && e.to_node == to_node
            && e.to_port == to_port
    }) {
        return Err("edge already exists".to_string());
    }
    if !apply_mutations {
        return Ok(json!({
            "dry_run": true,
            "would_create_edge": {
                "from_node": from_node,
                "from_port": from_port,
                "to_node": to_node,
                "to_port": to_port,
                "semantic_type": resolved_semantic.as_str()
            },
            "note": "Mutation skipped because apply_mutations=false"
        }));
    }
    let edge_id = store
        .add_edge(
            graph_id,
            from_node,
            from_port,
            to_node,
            to_port,
            resolved_semantic,
        )
        .await
        .map_err(|e| e.to_string())?;
    let _ = commit_graph_revision(
        store.as_ref(),
        graph_id,
        actor,
        "ai_wire_nodes",
        branch_id,
        json!({
            "edge_id": edge_id,
            "from_node": from_node,
            "from_port": from_port,
            "to_node": to_node,
            "to_port": to_port,
            "semantic_type": resolved_semantic.as_str()
        }),
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(json!({
        "edge_id": edge_id,
        "from_node": from_node,
        "from_port": from_port,
        "to_node": to_node,
        "to_port": to_port,
        "semantic_type": resolved_semantic.as_str()
    }))
}

async fn tool_unwire_edge(
    store: &Arc<PgStore>,
    graph_id: Uuid,
    edge_id: Uuid,
    apply_mutations: bool,
    actor: &str,
    branch_id: Option<Uuid>,
) -> Result<serde_json::Value, String> {
    if !apply_mutations {
        return Ok(json!({
            "dry_run": true,
            "would_delete_edge_id": edge_id,
            "note": "Mutation skipped because apply_mutations=false"
        }));
    }
    store
        .delete_edge(graph_id, edge_id)
        .await
        .map_err(|e| e.to_string())?;
    let _ = commit_graph_revision(
        store.as_ref(),
        graph_id,
        actor,
        "ai_unwire_edge",
        branch_id,
        json!({
            "edge_id": edge_id
        }),
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(json!({
        "deleted_edge_id": edge_id
    }))
}

fn node_category_for_kind(kind: &str) -> Result<NodeCategory, String> {
    let root = NodeRegistry::global()?.root().clone();
    let cat = root
        .nodes
        .into_iter()
        .find(|n| n.kind == kind)
        .and_then(|n| n.category)
        .ok_or_else(|| format!("kind '{}' missing category in registry", kind))?;
    match cat.to_ascii_lowercase().as_str() {
        "input" => Ok(NodeCategory::Input),
        "transform" => Ok(NodeCategory::Transform),
        "model" => Ok(NodeCategory::Model),
        "qa" => Ok(NodeCategory::Qa),
        "visualisation" | "visualization" => Ok(NodeCategory::Visualisation),
        "export" => Ok(NodeCategory::Export),
        other => Err(format!("unsupported node category '{}'", other)),
    }
}

fn parse_uuid(v: Option<&serde_json::Value>) -> Option<Uuid> {
    v.and_then(|x| x.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
}

async fn read_artifact_bytes(
    store: &Arc<PgStore>,
    artifact_root: &Path,
    node_id: Uuid,
    artifact_key: Option<String>,
) -> Result<(String, Vec<u8>), String> {
    let mut rows = store
        .list_artifacts_for_node(node_id)
        .await
        .map_err(|e| e.to_string())?;
    if rows.is_empty() {
        return Err(format!("node {} has no artifacts", node_id));
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let selected_key = if let Some(key) = artifact_key {
        if rows.iter().any(|r| r.0 == key) {
            key
        } else {
            return Err(format!(
                "artifact_key '{}' not found for node {}",
                key, node_id
            ));
        }
    } else {
        rows[0].0.clone()
    };
    let path = safe_artifact_path(artifact_root, &selected_key)?;
    let bytes = tokio::fs::read(&path)
        .await
        .map_err(|e| format!("read {} failed: {}", selected_key, e))?;
    Ok((selected_key, bytes))
}

fn json_lookup_path<'a>(root: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut cur = root;
    for seg in path.split('.') {
        if seg.is_empty() {
            continue;
        }
        if let Ok(i) = seg.parse::<usize>() {
            cur = cur.as_array()?.get(i)?;
            continue;
        }
        cur = cur.get(seg)?;
    }
    Some(cur)
}

fn detect_delimiter(header: &str) -> char {
    let candidates = [',', ';', '\t', '|'];
    let mut best = (',', 0usize);
    for c in candidates {
        let score = header.matches(c).count();
        if score > best.1 {
            best = (c, score);
        }
    }
    best.0
}

fn find_uploaded_doc<'a>(
    uploaded_docs: &'a [UploadedDoc],
    name: &str,
) -> Result<&'a UploadedDoc, String> {
    uploaded_docs
        .iter()
        .find(|d| d.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| format!("uploaded file '{}' not found in current chat context", name))
}

fn parse_delimited_text(text: &str) -> (Vec<String>, usize, Option<char>) {
    let lines = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .take(5000)
        .collect::<Vec<_>>();
    if lines.is_empty() {
        return (Vec::new(), 0, None);
    }
    let delimiter = detect_delimiter(lines[0]);
    let headers = lines[0]
        .split(delimiter)
        .map(|s| s.trim().to_string())
        .collect::<Vec<_>>();
    let rows = lines.len().saturating_sub(1);
    (headers, rows, Some(delimiter))
}

fn parse_delimited_rows(
    text: &str,
    max_rows: usize,
) -> (Vec<String>, Vec<Vec<String>>, Option<char>) {
    let lines = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .take(max_rows.saturating_add(1))
        .collect::<Vec<_>>();
    if lines.is_empty() {
        return (Vec::new(), Vec::new(), None);
    }
    let delimiter = detect_delimiter(lines[0]);
    let split = |line: &str| {
        line.split(delimiter)
            .map(|s| s.trim().to_string())
            .collect::<Vec<_>>()
    };
    let headers = split(lines[0]);
    let rows = lines
        .iter()
        .skip(1)
        .map(|line| split(line))
        .collect::<Vec<_>>();
    (headers, rows, Some(delimiter))
}

fn profile_csv_text(text: &str, sample_rows: usize) -> Result<serde_json::Value, String> {
    let lines = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .take(1200)
        .collect::<Vec<_>>();
    if lines.is_empty() {
        return Err("file has no non-empty lines".to_string());
    }
    let delimiter = detect_delimiter(lines[0]);
    let split = |line: &str| {
        line.split(delimiter)
            .map(|s| s.trim().to_string())
            .collect::<Vec<_>>()
    };
    let headers = split(lines[0]);
    if headers.is_empty() {
        return Err("could not parse headers".to_string());
    }
    let mut types = vec![TypeCounts::default(); headers.len()];
    let max_rows = sample_rows.clamp(10, 2000);
    for line in lines.iter().skip(1).take(max_rows) {
        let cols = split(line);
        for (i, tc) in types.iter_mut().enumerate() {
            let val = cols.get(i).map(|s| s.as_str()).unwrap_or("");
            tc.observe(val);
        }
    }
    let columns = headers
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let inferred = types[i].inferred();
            json!({
                "name": h,
                "inferred_type": inferred,
                "samples": types[i].samples.clone(),
                "null_like": types[i].null_like,
                "numeric_like": types[i].numeric_like,
                "bool_like": types[i].bool_like
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "delimiter": delimiter.to_string(),
        "header_count": headers.len(),
        "sampled_rows": lines.iter().skip(1).take(max_rows).count(),
        "columns": columns
    }))
}

fn pick_header(headers: &[String], aliases: &[&str]) -> String {
    let canon = |s: &str| {
        s.to_ascii_lowercase()
            .replace(' ', "")
            .replace('_', "")
            .replace('-', "")
            .replace('.', "")
    };
    for h in headers {
        let hc = canon(h);
        if aliases.iter().any(|a| canon(a) == hc) {
            return h.clone();
        }
    }
    for h in headers {
        let hc = canon(h);
        if aliases.iter().any(|a| hc.contains(&canon(a))) {
            return h.clone();
        }
    }
    String::new()
}

fn infer_ingest_mapping(kind: &str, headers: &[String]) -> serde_json::Value {
    match kind {
        "collar_ingest" => json!({
            "hole_id": pick_header(headers, &["hole_id", "holeid", "hole", "bhid", "id"]),
            "x": pick_header(headers, &["x", "easting", "east", "lon", "longitude"]),
            "y": pick_header(headers, &["y", "northing", "north", "lat", "latitude"]),
            "z": pick_header(headers, &["z", "rl", "rl_m", "elev", "elevation"]),
            "azimuth_deg": pick_header(headers, &["azimuth", "azi", "azimuth_deg"]),
            "dip_deg": pick_header(headers, &["dip", "inclination", "dip_deg"])
        }),
        "survey_ingest" => json!({
            "hole_id": pick_header(headers, &["hole_id", "holeid", "hole", "bhid", "id"]),
            "depth_or_length_m": pick_header(headers, &["depth", "depth_m", "md", "measured_depth", "length", "at"]),
            "azimuth_deg": pick_header(headers, &["azimuth", "azi", "azimuth_deg", "bearing"]),
            "dip_deg": pick_header(headers, &["dip", "inclination", "dip_deg"]),
            "segment_id": pick_header(headers, &["segment", "segment_id", "run", "station"])
        }),
        "assay_ingest" => json!({
            "hole_id": pick_header(headers, &["hole_id", "holeid", "hole", "bhid", "id"]),
            "from_m": pick_header(headers, &["from", "from_m", "depth_from", "start"]),
            "to_m": pick_header(headers, &["to", "to_m", "depth_to", "end"]),
            "value": pick_header(headers, &["au", "au_ppm", "grade", "value", "cu", "zn", "pb", "ag"])
        }),
        "lithology_ingest" => json!({
            "hole_id": pick_header(headers, &["hole_id", "holeid", "hole", "bhid", "id", "name"]),
            "from_m": pick_header(headers, &["from", "from_m", "top", "depth_from", "start", "md"]),
            "to_m": pick_header(headers, &["to", "to_m", "base", "depth_to", "end"]),
            "formation": pick_header(headers, &["formation", "lithology", "unit", "strat", "rock"]),
            "group": pick_header(headers, &["group", "group_name", "series"]),
            "lithology_code": pick_header(headers, &["code", "lithology_code", "unit_code"])
        }),
        "orientation_ingest" => json!({
            "formation": pick_header(headers, &["formation", "unit", "strat", "lithology"]),
            "x": pick_header(headers, &["x", "easting", "east", "lon", "longitude"]),
            "y": pick_header(headers, &["y", "northing", "north", "lat", "latitude"]),
            "z": pick_header(headers, &["z", "elev", "elevation", "rl"]),
            "dip_deg": pick_header(headers, &["dip", "dip_deg", "inclination"]),
            "azimuth_deg": pick_header(headers, &["azimuth", "azimuth_deg", "azi", "bearing"]),
            "pole_x": pick_header(headers, &["pole_x", "nx", "normal_x"]),
            "pole_y": pick_header(headers, &["pole_y", "ny", "normal_y"]),
            "pole_z": pick_header(headers, &["pole_z", "nz", "normal_z"])
        }),
        "surface_sample_ingest" => json!({
            "sample_id": pick_header(headers, &["sample_id", "id", "sample"]),
            "x": pick_header(headers, &["x", "easting", "east", "lon", "longitude"]),
            "y": pick_header(headers, &["y", "northing", "north", "lat", "latitude"]),
            "z": pick_header(headers, &["z", "elev", "elevation", "rl"])
        }),
        _ => json!({}),
    }
}

fn required_mapping_keys(kind: &str) -> Vec<&'static str> {
    match kind {
        "collar_ingest" => vec!["hole_id", "x", "y"],
        "survey_ingest" => vec!["hole_id", "depth_or_length_m", "azimuth_deg", "dip_deg"],
        "assay_ingest" => vec!["hole_id", "from_m", "to_m"],
        "lithology_ingest" => vec!["hole_id", "from_m", "to_m", "formation"],
        "orientation_ingest" => vec!["formation", "x", "y", "z"],
        "surface_sample_ingest" => vec!["x", "y"],
        _ => vec![],
    }
}

fn mapping_confidence_score(kind: &str, mapping: &serde_json::Value) -> f64 {
    let required = required_mapping_keys(kind);
    if required.is_empty() {
        return 0.0;
    }
    let hit = required
        .iter()
        .filter(|k| {
            mapping
                .get(**k)
                .and_then(|v| v.as_str())
                .map(|s| !s.is_empty())
                .unwrap_or(false)
        })
        .count();
    (hit as f64 / required.len() as f64 * 1000.0).round() / 1000.0
}

fn node_prompt_fragment(kind: &str) -> &'static str {
    match kind {
        "collar_ingest" => "Load collar table with hole_id + x/y (+optional z). Set ui.csv_headers/csv_rows and ui.mapping to enable ingest payload synthesis.",
        "survey_ingest" => "Load survey stations with hole_id + depth_or_length_m + azimuth_deg + dip_deg. Missing any required mapping blocks desurvey.",
        "assay_ingest" => "Load interval assays with hole_id + from_m + to_m and preserve geochem columns in attributes for downstream model/visualisation.",
        "lithology_ingest" => "Load lithology/stratigraphic intervals with hole_id + from_m + to_m + formation so downstream interface extraction can generate 3D contacts.",
        "orientation_ingest" => "Load structural orientations with formation + x/y/z and either dip_deg+azimuth_deg or pole_x/pole_y/pole_z so later GemPy-style modelling can consume explicit orientation constraints.",
        "ip_survey_ingest" => "Load TDIP/DCIP quadrupole rows with inline A/B/M/N electrode geometry or structured payload rows. Produces a canonical IP observations contract plus electrode points.",
        "ip_qc_normalize" => "Run conservative IP QC before modelling. Rejects malformed quadrupoles, high-reciprocity-error rows, and out-of-bounds chargeability values while preserving a hardened observations contract.",
        "ip_pseudosection" => "Convert cleaned IP observation rows into pseudo-depth points and rows for fast section-style QC in the existing 3D/table tooling.",
        "ip_corridor_model" => "Inflate pseudosection rows into a stitched pseudo-volume that reuses the existing block voxel renderer for immediate 3D IP context.",
        "ip_inversion_mesh" => "Build a regular IP mesh/domain from pseudosection rows so later TDIP solvers can target a stable middle-layer contract.",
        "ip_inversion_input" => "Prepare a hardened IP modelling payload from cleaned observations, pseudosection rows, and the IP mesh so downstream solvers consume one explicit contract.",
        "ip_invert" => "Run a first-pass regularized IP inversion surrogate in Rust from the prepared inversion-input contract and emit inversion result plus diagnostics.",
        "ip_inversion_preview" => "Consume the prepared inversion-input contract and interpolate onto the IP mesh to create a testable inversion-result preview plus explicit diagnostics and confidence metadata.",
        "ip_section_slice" => "Render a vertical coloured section plane from pseudosection or inversion-result data so line-oriented IP interpretation is clear in 3D.",
        "surface_sample_ingest" => "Load surface samples with x/y (+optional z/sample_id). Use project CRS when available.",
        "desurvey_trajectory" => "Requires collars_in (point_set) and surveys_in (trajectory_set). Produces 3D trajectory segments.",
        "vertical_trajectory" => "Requires collars_in and creates straight downhole trajectories, optionally sized from lithology interval extents when no surveys exist.",
        "formation_interface_extract" => "Requires trajectory_in + lithology_in to generate 3D formation contact points for viewer and future stratigraphic surface modelling.",
        "formation_catalog_build" => "Build a canonical formation catalog from lithology intervals so later nodes can normalize names, ids, and basement/group metadata consistently.",
        "stratigraphic_order_define" => "Build an explicit top-to-bottom formation order from the catalog and any interval evidence so structural modelling semantics are no longer implicit.",
        "model_domain_define" => "Define the modelling bounds and grid strategy from AOI, terrain, interface points, or orientations so later interpolation works against one canonical domain.",
        "constraint_merge" => "Merge interface points and orientations into one compute-facing interpolation constraints artifact with normalized formation names and diagnostics.",
        "structural_frame_builder" => "Assemble formation catalog, stratigraphic order, constraints, and model domain into one structural-frame artifact that downstream underground modelling can consume directly.",
        "stratigraphic_interpolator" => "Run a first-pass deterministic underground stratigraphic interpolation from the structural frame, constraints, and domain to emit a scalar-field style geology artifact.",
        "lith_block_model_build" => "Convert the scalar-field stratigraphy result into categorical lithology block voxels and centers so the underground model can be inspected immediately in 3D.",
        "stratigraphic_surface_model" => "Requires interface_points_in and emits one gridded contact surface artifact per formation for immediate 3D surface visualisation.",
        "drillhole_model" => "Requires trajectory_in + assays_in to generate assay points/meshes for 3D display.",
        "threejs_display_node" => "Primary active 3D viewer. Feed mesh/surface/raster/table-compatible layers via typed ports.",
        "aoi" => "Canonical AOI extent. Seed from collars/samples or manual bbox; use for DEM/imagery constraints.",
        "dem_fetch" => "Terrain fetch for AOI. Can consume tileserver imagery for drape context.",
        "tilebroker" => "Imagery provider broker. Common pattern: aoi_out -> tilebroker -> dem_fetch tileserver_in.",
        "plan_view_3d" | "cesium_display_node" => "Historic/archived Cesium path. Prefer threejs_display_node unless user asks explicitly.",
        _ => "Validate semantic ports and required params before wiring or running."
    }
}

fn measure_name_score(name: &str) -> f64 {
    let n = name.to_ascii_lowercase();
    let mut s: f64 = 0.0;
    for kw in [
        "au", "gold", "cu", "copper", "zn", "zinc", "pb", "lead", "ag", "silver", "grade", "ppm",
        "ppb", "pct", "%", "value", "assay",
    ] {
        if n.contains(kw) {
            s += 0.12;
        }
    }
    s.clamp(0.0, 1.0)
}

#[derive(Debug, Clone, Default)]
struct TypeCounts {
    null_like: usize,
    numeric_like: usize,
    bool_like: usize,
    text_like: usize,
    samples: Vec<String>,
}

impl TypeCounts {
    fn observe(&mut self, raw: &str) {
        let v = raw.trim();
        if self.samples.len() < 3 && !v.is_empty() {
            self.samples.push(v.to_string());
        }
        if v.is_empty() || matches!(v.to_ascii_lowercase().as_str(), "null" | "na" | "n/a") {
            self.null_like += 1;
            return;
        }
        if v.parse::<f64>().is_ok() {
            self.numeric_like += 1;
            return;
        }
        if matches!(
            v.to_ascii_lowercase().as_str(),
            "true" | "false" | "yes" | "no" | "0" | "1"
        ) {
            self.bool_like += 1;
            return;
        }
        self.text_like += 1;
    }

    fn inferred(&self) -> &'static str {
        if self.numeric_like > self.bool_like && self.numeric_like > self.text_like {
            return "number";
        }
        if self.bool_like > self.text_like {
            return "boolean";
        }
        if self.text_like == 0 && self.null_like > 0 {
            return "null_or_empty";
        }
        "string"
    }
}

fn summarize_tool_payload(v: &serde_json::Value) -> String {
    if v.get("dry_run").and_then(|x| x.as_bool()).unwrap_or(false) {
        return "planned (dry-run)".to_string();
    }
    if let Some(err) = v.get("error").and_then(|x| x.as_str()) {
        return truncate(err, 180);
    }
    if let Some(n) = v.get("nodes").and_then(|x| x.as_array()) {
        return format!("returned {} nodes", n.len());
    }
    if let Some(n) = v.get("queued_count").and_then(|x| x.as_u64()) {
        return format!("queued {} jobs", n);
    }
    if let Some(s) = v.get("summary") {
        let node_count = s.get("node_count").and_then(|x| x.as_u64()).unwrap_or(0);
        let failed = s
            .get("failed_node_count")
            .and_then(|x| x.as_u64())
            .unwrap_or(0);
        return format!("audit {} nodes ({} failed)", node_count, failed);
    }
    if let Some(k) = v.get("kinds").and_then(|x| x.as_array()) {
        return format!("returned {} node capabilities", k.len());
    }
    if let Some(n) = v.get("edges").and_then(|x| x.as_array()) {
        return format!("returned {} edges", n.len());
    }
    if let Some(n) = v.get("artifacts").and_then(|x| x.as_array()) {
        return format!("returned {} artifacts", n.len());
    }
    if let Some(n) = v.get("top_candidates").and_then(|x| x.as_array()) {
        return format!("ranked {} measure candidates", n.len());
    }
    if let Some(n) = v.get("files").and_then(|x| x.as_array()) {
        return format!("found {} uploaded files", n.len());
    }
    if let Some(n) = v.get("missing_required").and_then(|x| x.as_array()) {
        if n.is_empty() {
            return "mapping complete".to_string();
        }
        return format!("missing {} required mapping fields", n.len());
    }
    if let Some(id) = v.get("edge_id").and_then(|x| x.as_str()) {
        return format!("created edge {}", &id[..8]);
    }
    if let Some(id) = v.get("deleted_edge_id").and_then(|x| x.as_str()) {
        return format!("deleted edge {}", &id[..8]);
    }
    if let Some(id) = v.get("updated_node_id").and_then(|x| x.as_str()) {
        return format!("updated node {}", &id[..8]);
    }
    if let Some(id) = v.get("added_node_id").and_then(|x| x.as_str()) {
        return format!("added node {}", &id[..8]);
    }
    "ok".to_string()
}

fn tool_output_preview(v: &serde_json::Value) -> String {
    if let Some(s) = v.get("summary") {
        let failed = s
            .get("failed_node_count")
            .and_then(|x| x.as_u64())
            .unwrap_or(0);
        let missing = s
            .get("ingest_missing_payload_nodes")
            .and_then(|x| x.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        return format!(
            "audit: failed_nodes={}, ingest_missing_payload={}",
            failed, missing
        );
    }
    if let Some(cols) = v.get("columns").and_then(|x| x.as_array()) {
        let names = cols
            .iter()
            .take(8)
            .filter_map(|c| c.get("name").and_then(|n| n.as_str()))
            .collect::<Vec<_>>();
        if !names.is_empty() {
            return format!("columns: {}", names.join(", "));
        }
    }
    if let Some(q) = v.get("queued").and_then(|x| x.as_array()) {
        let ids = q
            .iter()
            .take(6)
            .filter_map(|r| r.get("node_id").and_then(|n| n.as_str()))
            .map(|s| s.chars().take(8).collect::<String>())
            .collect::<Vec<_>>();
        if !ids.is_empty() {
            return format!("queued nodes: {}", ids.join(", "));
        }
    }
    if let Some(files) = v.get("files").and_then(|x| x.as_array()) {
        let names = files
            .iter()
            .take(6)
            .filter_map(|f| f.get("name").and_then(|n| n.as_str()))
            .collect::<Vec<_>>();
        if !names.is_empty() {
            return format!("uploaded: {}", names.join(", "));
        }
    }
    if let Some(cands) = v.get("top_candidates").and_then(|x| x.as_array()) {
        let names = cands
            .iter()
            .take(6)
            .filter_map(|c| c.get("column").and_then(|n| n.as_str()))
            .collect::<Vec<_>>();
        if !names.is_empty() {
            return format!("candidates: {}", names.join(", "));
        }
    }
    if let Some(arts) = v.get("artifacts").and_then(|x| x.as_array()) {
        let names = arts
            .iter()
            .take(6)
            .filter_map(|a| a.get("artifact_key").and_then(|n| n.as_str()))
            .collect::<Vec<_>>();
        if !names.is_empty() {
            return format!("artifacts: {}", names.join(", "));
        }
    }
    truncate(&v.to_string(), 260)
}

fn safe_artifact_path(artifact_root: &Path, key: &str) -> Result<PathBuf, String> {
    let rel = Path::new(key);
    if rel.is_absolute() {
        return Err("artifact key must be relative".to_string());
    }
    if rel
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err("artifact key cannot contain parent traversal".to_string());
    }
    Ok(artifact_root.join(rel))
}

async fn collect_uploaded_docs(
    messages: &[AiChatMessage],
    artifact_root: &Path,
) -> Vec<UploadedDoc> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for m in messages {
        for a in &m.attachments {
            let key = format!("{}::{}", a.name.to_ascii_lowercase(), a.size);
            if !seen.insert(key) {
                continue;
            }
            let text = if let Some(t) = a.text.as_deref() {
                truncate(t, MAX_UPLOAD_TEXT_CHARS)
            } else if let Some(t) = a.preview_text.as_deref() {
                truncate(t, MAX_UPLOAD_TEXT_CHARS)
            } else if let Some(k) = a.artifact_key.as_deref() {
                read_artifact_preview_text(artifact_root, k, MAX_UPLOAD_TEXT_CHARS)
                    .await
                    .unwrap_or_default()
            } else {
                String::new()
            };
            out.push(UploadedDoc {
                name: a.name.clone(),
                mime: a.mime.clone(),
                size: a.size,
                text,
            });
        }
    }
    out
}

fn render_chat_message_content(m: &AiChatMessage) -> String {
    let mut out = m.text.trim().to_string();
    if m.attachments.is_empty() {
        return out;
    }
    let mut lines = vec!["[uploaded_files]".to_string()];
    for a in m.attachments.iter().take(6) {
        let chars = a.text.as_ref().map(|t| t.chars().count()).unwrap_or(0);
        lines.push(format!(
            "- {} (mime={}, size_kb={}, text_chars={}, artifact_key={}, content_hash={}, format={})",
            a.name,
            if a.mime.is_empty() { "unknown" } else { &a.mime },
            a.size / 1024,
            chars,
            a.artifact_key.as_deref().unwrap_or("-"),
            a.content_hash.as_deref().map(|s| &s[..s.len().min(12)]).unwrap_or("-"),
            a.format.as_deref().unwrap_or("-")
        ));
        let txt = a.text.as_deref().or(a.preview_text.as_deref());
        if let Some(t) = txt {
            let preview = t.lines().take(6).collect::<Vec<_>>().join("\n");
            if !preview.trim().is_empty() {
                lines.push(format!("  preview:\n{}", truncate(&preview, 900)));
            }
        }
    }
    if !out.is_empty() {
        out.push_str("\n\n");
    }
    out.push_str(&lines.join("\n"));
    out
}

async fn read_artifact_preview_text(
    artifact_root: &Path,
    key: &str,
    max_chars: usize,
) -> Option<String> {
    let path = safe_artifact_path(artifact_root, key).ok()?;
    let raw = tokio::fs::read(&path).await.ok()?;
    let text = String::from_utf8_lossy(&raw);
    let mut out = String::new();
    for line in text.lines().take(20) {
        out.push_str(line);
        out.push('\n');
    }
    if text.lines().count() > 24 {
        out.push_str("...\n");
        let tail = text.lines().rev().take(6).collect::<Vec<_>>();
        for line in tail.into_iter().rev() {
            out.push_str(line);
            out.push('\n');
        }
    }
    Some(truncate(&out, max_chars))
}

fn extract_content_text(content: Option<&serde_json::Value>) -> String {
    let Some(content) = content else {
        return String::new();
    };
    if let Some(s) = content.as_str() {
        return s.to_string();
    }
    if let Some(arr) = content.as_array() {
        let mut out = String::new();
        for item in arr {
            if let Some(t) = item.get("text").and_then(|x| x.as_str()) {
                if !out.is_empty() {
                    out.push('\n');
                }
                out.push_str(t);
            }
        }
        return out;
    }
    String::new()
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out = String::new();
    for ch in s.chars().take(max) {
        out.push(ch);
    }
    out.push_str("…");
    out
}

fn build_system_prompt(memory_context: &str) -> String {
    format!(
        "You are Mine-Eye Field Assistant: an experienced, globally travelled exploration geologist with an Oxbridge-style analytical communication style.\n\
Voice: warm, grounded, practical, occasionally adventurous in tone, always professional and safety-conscious.\n\
\n\
Conversation style:\n\
- Sound like a senior field geologist helping a colleague in real time.\n\
- Keep replies short by default: 3-8 lines unless user asks for detail.\n\
- Put outcome first, then exact actions taken, then optional caveats.\n\
- Ask at most one clarifying question, and only if blocked.\n\
- Never ask for node IDs/ports/file headers until after tool discovery.\n\
- Response format is mandatory:\n\
  `<plain>...</plain>` = concise plain-meaning answer.\n\
  `<system>...</system>` = detailed diagnostics/actions.\n\
\n\
Tool use behavior:\n\
- Start each technical diagnosis by calling `graph_audit_bundle` once (include artifact samples) so decisions are grounded in current node errors, mappings, wiring, and registry capabilities.\n\
- If files are uploaded, call `list_uploaded_files` first, then `uploaded_csv_profile`/`suggest_ingest_mapping_from_upload`, then `apply_upload_to_ingest_node`.\n\
- When user asks to run/execute, call `run_node` or `run_graph` directly.\n\
- For data-column questions, inspect uploaded files/artifacts first before asking user for exact column names.\n\
- If ingest nodes fail with missing payload/mapping, propose and apply explicit mapping candidates (hole_id, from/to, azimuth/dip, assay value) from profiling.\n\
- Prefer decisive suggestions with confidence and fallback options; do not be stilted.\n\
- Use read_registry_kind before wiring/config edits when node semantics are unclear.\n\
- Mutations must happen only through tools and respect current mutation mode.\n\
\n\
Workflow output quality bar:\n\
- Always include: objective, exact changes, and validation/run checks.\n\
- For transform recommendations, include configurable params that should be exposed in node UI.\n\
- Keep CRS and semantic-port compatibility explicit.\n\
\n\
Exploration defaults:\n\
- If user provides or mentions likely `collar`, `survey`, and `assay` files, default pipeline should be:\n\
  `collar_ingest + survey_ingest -> desurvey_trajectory -> drillhole_model -> threejs_display_node`.\n\
- Include `aoi -> dem_fetch -> tilebroker` when a terrain/imagery 3D context is requested.\n\
- Treat Cesium viewers (`plan_view_3d`, `cesium_display_node`) as historic/archived unless user explicitly asks for Cesium.\n\
- For file-to-workflow requests, prefer creating needed nodes + wiring plan instead of asking for node IDs first.\n\
- Standard end-to-end drillhole pattern: ingest files -> map columns -> patch ingest ui -> run ingest/desurvey/model -> wire 3D viewer.\n\
\n\
Project memory context (internal reference):\n\
{}\n",
        memory_context
    )
}

fn load_memory_context() -> (String, Vec<String>) {
    let mut used: Vec<String> = Vec::new();
    let mut seen = HashSet::new();
    let candidates = [
        "docs/ai-memory/project-context.md",
        "docs/ai-memory/geology-summary.md",
        "docs/ai-memory/activities-log.md",
        "docs/ai-memory/system-prompt-notes.md",
        "docs/ai-skills/upload-combo-playbooks.md",
        "docs/ai-skills/node-workflow-fragments.md",
        "docs/node-operating-matrix.md",
        "README.md",
        "ARCHITECTURE.md",
    ];

    let mut chunks: Vec<String> = Vec::new();
    for rel in candidates {
        let p = PathBuf::from(rel);
        let Ok(content) = std::fs::read_to_string(&p) else {
            continue;
        };
        if !seen.insert(rel.to_string()) {
            continue;
        }
        used.push(rel.to_string());
        chunks.push(format!("\n# {}\n{}", rel, truncate(&content, 6_000)));
    }
    (chunks.join("\n"), used)
}
