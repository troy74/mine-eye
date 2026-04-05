//! Canonical acquisition and derived primitives (see architecture and README docs).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::CrsRecord;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollarRecord {
    pub hole_id: String,
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub crs: CrsRecord,
    pub qa_flags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurveyStationRecord {
    pub hole_id: String,
    pub depth_m: f64,
    pub azimuth_deg: f64,
    pub dip_deg: f64,
    pub qa_flags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IntervalSampleRecord {
    pub hole_id: String,
    pub from_m: f64,
    pub to_m: f64,
    pub attributes: serde_json::Value,
    pub qa_flags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PointSampleRecord {
    pub id: String,
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub crs: CrsRecord,
    pub attributes: serde_json::Value,
    pub qa_flags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfacePointRecord {
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub crs: CrsRecord,
    pub qa_flags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DemRecord {
    pub artifact_id: Uuid,
    pub crs: CrsRecord,
    pub resolution_m: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TrajectorySegment {
    pub hole_id: String,
    pub depth_from_m: f64,
    pub depth_to_m: f64,
    pub x_from: f64,
    pub y_from: f64,
    pub z_from: f64,
    pub x_to: f64,
    pub y_to: f64,
    pub z_to: f64,
    pub crs: CrsRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DesurveyedInterval {
    pub hole_id: String,
    pub from_m: f64,
    pub to_m: f64,
    pub mid_x: f64,
    pub mid_y: f64,
    pub mid_z: f64,
    pub crs: CrsRecord,
    pub qa_flags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BlockModelMeta {
    pub nx: u32,
    pub ny: u32,
    pub nz: u32,
    pub origin_x: f64,
    pub origin_y: f64,
    pub origin_z: f64,
    pub cell_x: f64,
    pub cell_y: f64,
    pub cell_z: f64,
    pub crs: CrsRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScalarFieldMeta {
    pub block_model: BlockModelMeta,
    pub field_name: String,
    pub artifact_id: Uuid,
}
