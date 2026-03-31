use thiserror::Error;

#[derive(Debug, Error)]
pub enum NodeError {
    #[error("unknown node kind: {0}")]
    UnknownKind(String),
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("CRS transform: {0}")]
    Proj(String),
}
