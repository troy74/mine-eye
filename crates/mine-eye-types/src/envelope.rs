use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

fn default_run_id() -> Uuid {
    Uuid::nil()
}

fn default_input_fingerprint() -> String {
    String::new()
}

/// Pointer to a blob in object storage or local artifact root.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactRef {
    pub key: String,
    pub content_hash: String,
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InputArtifactBinding {
    pub to_port: String,
    pub artifact_ref: ArtifactRef,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    #[default]
    Queued,
    Running,
    Succeeded,
    Failed,
}

/// Worker job envelope — stable contract for streaming/chunked extensions later.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobEnvelope {
    pub protocol_version: u32,
    pub job_id: Uuid,
    #[serde(default = "default_run_id")]
    pub run_id: Uuid,
    pub graph_id: Uuid,
    pub node_id: Uuid,
    pub node_kind: String,
    pub config_hash: String,
    #[serde(default = "default_input_fingerprint")]
    pub input_fingerprint: String,
    pub project_crs: Option<crate::CrsRecord>,
    pub input_artifact_refs: Vec<ArtifactRef>,
    #[serde(default)]
    pub input_artifact_bindings: Vec<InputArtifactBinding>,
    /// Optional inline inputs (e.g. ingest JSON) when no artifact exists yet.
    pub input_payload: Option<JsonValue>,
    pub output_spec: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobResult {
    pub job_id: Uuid,
    pub status: JobStatus,
    pub output_artifact_refs: Vec<ArtifactRef>,
    pub content_hashes: Vec<String>,
    pub error_message: Option<String>,
}
