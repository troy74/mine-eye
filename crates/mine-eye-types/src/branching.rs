use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum BranchStatus {
    #[default]
    Draft,
    Qa,
    Approved,
    Promoted,
    Archived,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphBranch {
    pub id: Uuid,
    pub graph_id: Uuid,
    pub name: String,
    pub base_revision_id: Option<Uuid>,
    pub head_revision_id: Option<Uuid>,
    pub status: BranchStatus,
    pub created_by: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphRevision {
    pub id: Uuid,
    pub graph_id: Uuid,
    pub branch_id: Option<Uuid>,
    pub parent_revision_id: Option<Uuid>,
    pub created_by: String,
    pub meta: serde_json::Value,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum BranchPromotionStatus {
    #[default]
    Pending,
    Succeeded,
    Conflict,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BranchPromotionRecord {
    pub id: Uuid,
    pub source_branch_id: Uuid,
    pub target_branch_id: Uuid,
    pub source_head_revision_id: Option<Uuid>,
    pub promoted_revision_id: Option<Uuid>,
    pub status: BranchPromotionStatus,
    pub conflict_report: Option<serde_json::Value>,
    pub created_by: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}
