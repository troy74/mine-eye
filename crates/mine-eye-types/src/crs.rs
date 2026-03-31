use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Source CRS preserved from acquisition; use EPSG code or WKT.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrsRecord {
    pub epsg: Option<i32>,
    pub wkt: Option<String>,
}

impl CrsRecord {
    pub fn epsg(code: i32) -> Self {
        Self {
            epsg: Some(code),
            wkt: None,
        }
    }
}

/// Project working CRS for a workspace/graph.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectCrs {
    pub workspace_id: Uuid,
    pub crs: CrsRecord,
}
