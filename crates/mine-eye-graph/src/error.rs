use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum GraphError {
    #[error("node not found: {0}")]
    NodeNotFound(Uuid),
    #[error("cycle detected")]
    Cycle,
    #[error("invalid edge: {0}")]
    InvalidEdge(String),
}
