//! Horizontal reprojection for ingest outputs (PROJ). Z is passed through unchanged.

use mine_eye_types::CrsRecord;
use proj::Proj;

use crate::NodeError;

fn crs_to_proj_id(c: &CrsRecord) -> Result<String, NodeError> {
    if let Some(e) = c.epsg {
        return Ok(format!("EPSG:{}", e));
    }
    if let Some(ref wkt) = c.wkt {
        if !wkt.trim().is_empty() {
            return Ok(wkt.clone());
        }
    }
    Err(NodeError::InvalidConfig(
        "CRS must have epsg or non-empty wkt for reprojection".into(),
    ))
}

/// Transform (x, y) from `from` to `to`. Creates one PROJ pipeline per call — reuse at call site for batches.
pub fn transform_xy(
    from: &CrsRecord,
    to: &CrsRecord,
    x: f64,
    y: f64,
) -> Result<(f64, f64), NodeError> {
    let from_id = crs_to_proj_id(from)?;
    let to_id = crs_to_proj_id(to)?;
    if from_id == to_id {
        return Ok((x, y));
    }
    let proj =
        Proj::new_known_crs(&from_id, &to_id, None).map_err(|e| NodeError::Proj(e.to_string()))?;
    proj.convert((x, y))
        .map_err(|e| NodeError::Proj(e.to_string()))
}
