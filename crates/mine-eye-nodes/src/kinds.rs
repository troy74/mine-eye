use mine_eye_types::{
    ArtifactRef, CollarRecord, CrsRecord, IntervalSampleRecord, JobEnvelope, JobResult, JobStatus,
    SurveyStationRecord, TrajectorySegment,
};
use sha2::{Digest, Sha256};
use tokio::fs;

use crate::crs_transform::transform_xy;
use crate::executor::ExecutionContext;
use crate::NodeError;

fn collar_output_crs_mode(job: &JobEnvelope) -> &str {
    job.output_spec
        .pointer("/node_ui/output_crs_mode")
        .and_then(|v| v.as_str())
        .unwrap_or("project")
}

/// Target CRS for written collar coordinates, or `None` when output should stay in source CRS.
fn collar_output_target_crs(job: &JobEnvelope) -> Result<Option<CrsRecord>, NodeError> {
    match collar_output_crs_mode(job) {
        "source" => Ok(None),
        "wgs84" => Ok(Some(CrsRecord::epsg(4326))),
        "custom" => {
            let e = job
                .output_spec
                .pointer("/node_ui/output_crs_epsg")
                .and_then(|v| v.as_u64())
                .unwrap_or(4326) as i32;
            Ok(Some(CrsRecord::epsg(e)))
        }
        "project" | _ => Ok(Some(
            job.project_crs
                .clone()
                .unwrap_or_else(|| CrsRecord::epsg(4326)),
        )),
    }
}

fn hash_bytes(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

async fn write_artifact(
    ctx: &ExecutionContext<'_>,
    relative_key: &str,
    bytes: &[u8],
    media_type: Option<&str>,
) -> Result<ArtifactRef, NodeError> {
    let path = ctx.artifact_root.join(relative_key);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    fs::write(&path, bytes).await?;
    let content_hash = hash_bytes(bytes);
    Ok(ArtifactRef {
        key: relative_key.to_string(),
        content_hash,
        media_type: media_type.map(String::from),
    })
}

async fn read_json_artifact(
    ctx: &ExecutionContext<'_>,
    key: &str,
) -> Result<serde_json::Value, NodeError> {
    let path = ctx.artifact_root.join(key);
    let raw = fs::read(&path).await?;
    let v: serde_json::Value = serde_json::from_slice(&raw)?;
    Ok(v)
}

async fn collect_drillhole_inputs(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<(Vec<CollarRecord>, Vec<SurveyStationRecord>, Vec<IntervalSampleRecord>), NodeError> {
    let mut collars: Vec<CollarRecord> = Vec::new();
    let mut surveys: Vec<SurveyStationRecord> = Vec::new();
    let mut assays: Vec<IntervalSampleRecord> = Vec::new();

    for ar in &job.input_artifact_refs {
        let v = read_json_artifact(ctx, &ar.key).await?;
        if let Some(c) = v.get("collars") {
            if let Ok(mut part) = serde_json::from_value::<Vec<CollarRecord>>(c.clone()) {
                collars.append(&mut part);
            }
        }
        if let Some(s) = v.get("surveys") {
            if let Ok(mut part) = serde_json::from_value::<Vec<SurveyStationRecord>>(s.clone()) {
                surveys.append(&mut part);
            }
        }
        if let Some(a) = v.get("assays") {
            if let Ok(mut part) = serde_json::from_value::<Vec<IntervalSampleRecord>>(a.clone()) {
                assays.append(&mut part);
            }
        }
    }

    Ok((collars, surveys, assays))
}

/// Parses collars/surveys/assays from job config params JSON and writes canonical JSON artifact.
pub async fn run_drillhole_ingest(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payload = job
        .input_payload
        .as_ref()
        .ok_or_else(|| NodeError::InvalidConfig("missing input_payload for ingest".into()))?;
    let collars: Vec<CollarRecord> = serde_json::from_value(
        payload
            .pointer("/collars")
            .cloned()
            .unwrap_or(serde_json::json!([])),
    )
    .unwrap_or_default();
    let surveys: Vec<SurveyStationRecord> = serde_json::from_value(
        payload
            .pointer("/surveys")
            .cloned()
            .unwrap_or(serde_json::json!([])),
    )
    .unwrap_or_default();
    let assays: Vec<IntervalSampleRecord> = serde_json::from_value(
        payload
            .pointer("/assays")
            .cloned()
            .unwrap_or(serde_json::json!([])),
    )
    .unwrap_or_default();

    let payload = serde_json::json!({
        "collars": collars,
        "surveys": surveys,
        "assays": assays,
    });
    let bytes = serde_json::to_vec(&payload)?;
    let key = format!("graphs/{}/nodes/{}/ingest.json", job.graph_id, job.node_id);
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

/// Single primitive: collars only ([V1SPEC §2](V1SPEC.md) — Collar).
pub async fn run_collar_ingest(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payload = job
        .input_payload
        .as_ref()
        .ok_or_else(|| NodeError::InvalidConfig("missing input_payload for collar_ingest".into()))?;
    let mut collars: Vec<CollarRecord> = serde_json::from_value(
        payload
            .pointer("/collars")
            .cloned()
            .unwrap_or(serde_json::json!([])),
    )
    .unwrap_or_default();

    if let Some(target) = collar_output_target_crs(job)? {
        let project_missing = collar_output_crs_mode(job) == "project" && job.project_crs.is_none();
        for c in &mut collars {
            if project_missing {
                c.qa_flags
                    .push("project_crs_missing_output_epsg_4326".into());
            }
            if c.crs == target {
                continue;
            }
            let (nx, ny) = transform_xy(&c.crs, &target, c.x, c.y)?;
            c.x = nx;
            c.y = ny;
            c.crs = target.clone();
            c.qa_flags.push("reprojected_xy".into());
        }
    }

    let out = serde_json::json!({ "collars": collars });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!("graphs/{}/nodes/{}/collars.json", job.graph_id, job.node_id);
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

/// Single primitive: survey stations only ([V1SPEC §2](V1SPEC.md) — SurveyStation).
pub async fn run_survey_ingest(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payload = job
        .input_payload
        .as_ref()
        .ok_or_else(|| NodeError::InvalidConfig("missing input_payload for survey_ingest".into()))?;
    let surveys: Vec<SurveyStationRecord> = serde_json::from_value(
        payload
            .pointer("/surveys")
            .cloned()
            .unwrap_or(serde_json::json!([])),
    )
    .unwrap_or_default();
    let out = serde_json::json!({ "surveys": surveys });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!("graphs/{}/nodes/{}/surveys.json", job.graph_id, job.node_id);
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

/// Single primitive: interval assays only ([V1SPEC §2](V1SPEC.md) — IntervalSample).
pub async fn run_assay_ingest(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payload = job
        .input_payload
        .as_ref()
        .ok_or_else(|| NodeError::InvalidConfig("missing input_payload for assay_ingest".into()))?;
    let assays: Vec<IntervalSampleRecord> = serde_json::from_value(
        payload
            .pointer("/assays")
            .cloned()
            .unwrap_or(serde_json::json!([])),
    )
    .unwrap_or_default();
    let out = serde_json::json!({ "assays": assays });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!("graphs/{}/nodes/{}/assays.json", job.graph_id, job.node_id);
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

/// Single primitive: surface samples as point rows for plan-view and downstream interpolation.
pub async fn run_surface_sample_ingest(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payload = job.input_payload.as_ref().ok_or_else(|| {
        NodeError::InvalidConfig("missing input_payload for surface_sample_ingest".into())
    })?;
    let mut points = payload
        .pointer("/points")
        .cloned()
        .unwrap_or(serde_json::json!([]));
    if let Some(target) = collar_output_target_crs(job)? {
        let project_missing = collar_output_crs_mode(job) == "project" && job.project_crs.is_none();
        if let Some(arr) = points.as_array_mut() {
            for p in arr.iter_mut() {
                let Some(obj) = p.as_object_mut() else {
                    continue;
                };
                let (Some(x), Some(y)) = (
                    obj.get("x").and_then(|v| v.as_f64()),
                    obj.get("y").and_then(|v| v.as_f64()),
                ) else {
                    continue;
                };

                let src_crs = obj
                    .get("crs")
                    .cloned()
                    .and_then(|v| serde_json::from_value::<CrsRecord>(v).ok())
                    .unwrap_or_else(|| target.clone());
                let (nx, ny) = if src_crs == target {
                    (x, y)
                } else {
                    transform_xy(&src_crs, &target, x, y)?
                };
                obj.insert("x".into(), serde_json::json!(nx));
                obj.insert("y".into(), serde_json::json!(ny));
                obj.insert("crs".into(), serde_json::to_value(&target)?);

                let qa = obj
                    .entry("qa_flags")
                    .or_insert_with(|| serde_json::json!([]));
                let mut qa_vals: Vec<String> = qa
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default();
                if project_missing {
                    qa_vals.push("project_crs_missing_output_epsg_4326".into());
                }
                if src_crs != target {
                    qa_vals.push("reprojected_xy".into());
                }
                *qa = serde_json::json!(qa_vals);
            }
        }
    }
    let out = serde_json::json!({ "points": points });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!(
        "graphs/{}/nodes/{}/surface_samples.json",
        job.graph_id, job.node_id
    );
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

#[derive(Clone, Copy)]
struct HeatPt {
    x: f64,
    y: f64,
    v: f64,
}

fn interpolate_value(
    samples: &[HeatPt],
    x: f64,
    y: f64,
    method: &str,
    power: f64,
    search_radius_m: f64,
    min_points: usize,
    max_points: usize,
) -> Option<f64> {
    let mut near: Vec<(f64, f64)> = samples
        .iter()
        .filter_map(|s| {
            let dx = x - s.x;
            let dy = y - s.y;
            let d2 = dx * dx + dy * dy;
            if d2 < 1e-12 {
                return Some((0.0, s.v));
            }
            if search_radius_m > 0.0 && d2.sqrt() > search_radius_m {
                return None;
            }
            Some((d2, s.v))
        })
        .collect();
    if near.is_empty() {
        return None;
    }
    near.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    if near.len() > max_points {
        near.truncate(max_points);
    }
    if near.len() < min_points {
        return None;
    }
    if near[0].0 <= 0.0 {
        return Some(near[0].1);
    }

    let mut num = 0.0;
    let mut den = 0.0;
    match method {
        "nearest" => Some(near[0].1),
        "rbf" => {
            let scale = near.last().map(|x| x.0.sqrt()).unwrap_or(1.0).max(1e-9);
            for (d2, v) in near {
                let d = d2.sqrt();
                let w = (-(d / scale).powi(2)).exp();
                num += w * v;
                den += w;
            }
            (den > 0.0).then_some(num / den)
        }
        "kriging" => {
            let range = near.last().map(|x| x.0.sqrt()).unwrap_or(1.0).max(1e-9);
            let nugget = 0.05;
            for (d2, v) in near {
                let d = d2.sqrt();
                let gamma = 1.0 - (-(d / range)).exp();
                let w = 1.0 / (nugget + gamma.max(1e-6));
                num += w * v;
                den += w;
            }
            (den > 0.0).then_some(num / den)
        }
        _ => {
            let p = power.clamp(1.0, 6.0);
            for (d2, v) in near {
                let w = 1.0 / d2.powf(0.5 * p);
                num += w * v;
                den += w;
            }
            (den > 0.0).then_some(num / den)
        }
    }
}

fn marching_segments(
    grid: &[Option<f64>],
    nx: usize,
    ny: usize,
    xmin: f64,
    xmax: f64,
    ymin: f64,
    ymax: f64,
    level: f64,
) -> Vec<[[f64; 2]; 2]> {
    let mut out = Vec::new();
    if nx < 2 || ny < 2 {
        return out;
    }
    let dx = (xmax - xmin) / (nx as f64 - 1.0);
    let dy = (ymax - ymin) / (ny as f64 - 1.0);
    let idx = |ix: usize, iy: usize| -> usize { iy * nx + ix };
    let interp = |a: (f64, f64, f64), b: (f64, f64, f64)| -> [f64; 2] {
        let (ax, ay, av) = a;
        let (bx, by, bv) = b;
        let t = if (bv - av).abs() < 1e-12 {
            0.5
        } else {
            ((level - av) / (bv - av)).clamp(0.0, 1.0)
        };
        [ax + (bx - ax) * t, ay + (by - ay) * t]
    };

    for iy in 0..(ny - 1) {
        for ix in 0..(nx - 1) {
            let p00 = (xmin + ix as f64 * dx, ymin + iy as f64 * dy);
            let p10 = (xmin + (ix + 1) as f64 * dx, ymin + iy as f64 * dy);
            let p11 = (xmin + (ix + 1) as f64 * dx, ymin + (iy + 1) as f64 * dy);
            let p01 = (xmin + ix as f64 * dx, ymin + (iy + 1) as f64 * dy);

            let Some(v00) = grid[idx(ix, iy)] else {
                continue;
            };
            let Some(v10) = grid[idx(ix + 1, iy)] else {
                continue;
            };
            let Some(v11) = grid[idx(ix + 1, iy + 1)] else {
                continue;
            };
            let Some(v01) = grid[idx(ix, iy + 1)] else {
                continue;
            };

            let mut crosses: Vec<[f64; 2]> = Vec::new();
            let s00 = v00 >= level;
            let s10 = v10 >= level;
            let s11 = v11 >= level;
            let s01 = v01 >= level;
            if s00 != s10 {
                crosses.push(interp((p00.0, p00.1, v00), (p10.0, p10.1, v10)));
            }
            if s10 != s11 {
                crosses.push(interp((p10.0, p10.1, v10), (p11.0, p11.1, v11)));
            }
            if s11 != s01 {
                crosses.push(interp((p11.0, p11.1, v11), (p01.0, p01.1, v01)));
            }
            if s01 != s00 {
                crosses.push(interp((p01.0, p01.1, v01), (p00.0, p00.1, v00)));
            }

            if crosses.len() == 2 {
                out.push([crosses[0], crosses[1]]);
            } else if crosses.len() == 4 {
                out.push([crosses[0], crosses[1]]);
                out.push([crosses[2], crosses[3]]);
            }
        }
    }
    out
}

/// Phase-2 heatmap node: configurable interpolation + contour and diagnostics artifacts.
pub async fn run_assay_heatmap(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let mut points: Vec<serde_json::Value> = Vec::new();
    for ar in &job.input_artifact_refs {
        let v = read_json_artifact(ctx, &ar.key).await?;
        if let Some(arr) = v.get("assay_points").and_then(|x| x.as_array()) {
            points.extend(arr.iter().cloned());
        } else if let Some(arr) = v.get("points").and_then(|x| x.as_array()) {
            points.extend(arr.iter().cloned());
        }
    }

    let measure = job
        .output_spec
        .pointer("/node_ui/measure")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let method = job
        .output_spec
        .pointer("/node_ui/method")
        .and_then(|v| v.as_str())
        .unwrap_or("idw")
        .to_string();
    let scale = job
        .output_spec
        .pointer("/node_ui/scale")
        .and_then(|v| v.as_str())
        .unwrap_or("linear")
        .to_string();
    let palette = job
        .output_spec
        .pointer("/node_ui/palette")
        .and_then(|v| v.as_str())
        .unwrap_or("rainbow")
        .to_string();
    let clamp_low = job
        .output_spec
        .pointer("/node_ui/clamp_low_pct")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let clamp_high = job
        .output_spec
        .pointer("/node_ui/clamp_high_pct")
        .and_then(|v| v.as_f64())
        .unwrap_or(100.0);
    let idw_power = job
        .output_spec
        .pointer("/node_ui/idw_power")
        .and_then(|v| v.as_f64())
        .unwrap_or(2.0)
        .clamp(1.0, 4.0);
    let smoothness = job
        .output_spec
        .pointer("/node_ui/smoothness")
        .and_then(|v| v.as_u64())
        .unwrap_or(256)
        .clamp(128, 512);
    let search_radius_m = job
        .output_spec
        .pointer("/node_ui/search_radius_m")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0)
        .max(0.0);
    let min_points = job
        .output_spec
        .pointer("/node_ui/min_points")
        .and_then(|v| v.as_u64())
        .unwrap_or(3)
        .max(1) as usize;
    let max_points = job
        .output_spec
        .pointer("/node_ui/max_points")
        .and_then(|v| v.as_u64())
        .unwrap_or(32)
        .max(min_points as u64) as usize;
    let contours_enabled = job
        .output_spec
        .pointer("/node_ui/contours_enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let contour_mode = job
        .output_spec
        .pointer("/node_ui/contour_mode")
        .and_then(|v| v.as_str())
        .unwrap_or("fixed_interval")
        .to_string();
    let contour_interval = job
        .output_spec
        .pointer("/node_ui/contour_interval")
        .and_then(|v| v.as_f64())
        .unwrap_or(1.0)
        .max(0.0001);
    let contour_levels = job
        .output_spec
        .pointer("/node_ui/contour_levels")
        .and_then(|v| v.as_u64())
        .unwrap_or(10)
        .max(2) as usize;
    let gradient_enabled = job
        .output_spec
        .pointer("/node_ui/gradient_enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let gradient_mode = job
        .output_spec
        .pointer("/node_ui/gradient_mode")
        .and_then(|v| v.as_str())
        .unwrap_or("magnitude")
        .to_string();

    let mut measure_candidates: Vec<String> = Vec::new();
    for p in &points {
        let Some(obj) = p.as_object() else {
            continue;
        };
        let attrs = obj
            .get("attributes")
            .and_then(|a| a.as_object())
            .cloned()
            .unwrap_or_default();
        for (k, v) in attrs {
            if v.is_number() && !measure_candidates.contains(&k) {
                measure_candidates.push(k);
            }
        }
    }
    measure_candidates.sort();
    let selected_measure = if !measure.is_empty() {
        measure.clone()
    } else {
        measure_candidates.first().cloned().unwrap_or_default()
    };

    let mut raw_values: Vec<f64> = Vec::new();
    for p in &points {
        let Some(obj) = p.as_object() else {
            continue;
        };
        let Some(attrs) = obj.get("attributes").and_then(|a| a.as_object()) else {
            continue;
        };
        let Some(v) = attrs.get(&selected_measure).and_then(|x| x.as_f64()) else {
            continue;
        };
        if v.is_finite() {
            raw_values.push(v);
        }
    }
    raw_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let pctl = |pct: f64| -> f64 {
        if raw_values.is_empty() {
            return 0.0;
        }
        let t = pct.clamp(0.0, 100.0) / 100.0;
        let idx = ((raw_values.len().saturating_sub(1)) as f64 * t).round() as usize;
        raw_values[idx.min(raw_values.len().saturating_sub(1))]
    };
    let lo = pctl(clamp_low);
    let hi = pctl(clamp_high).max(lo);
    let transform = |v0: f64| -> Option<f64> {
        let mut v = v0.clamp(lo, hi);
        match scale.as_str() {
            "log10" => {
                if v <= 0.0 {
                    return None;
                }
                v = v.log10();
            }
            "ln" => {
                if v <= 0.0 {
                    return None;
                }
                v = v.ln();
            }
            "sqrt" => {
                if v < 0.0 {
                    return None;
                }
                v = v.sqrt();
            }
            _ => {}
        }
        Some(v)
    };

    let mut transformed_values: Vec<f64> = Vec::new();
    let mut enriched_points: Vec<serde_json::Value> = Vec::new();
    for mut p in points {
        let Some(obj) = p.as_object_mut() else {
            continue;
        };
        let attrs = obj
            .entry("attributes")
            .or_insert_with(|| serde_json::json!({}));
        let Some(attrs_obj) = attrs.as_object_mut() else {
            continue;
        };
        if let Some(raw) = attrs_obj.get(&selected_measure).and_then(|x| x.as_f64()) {
            if let Some(tv) = transform(raw) {
                transformed_values.push(tv);
                attrs_obj.insert("__heatmap_value".into(), serde_json::json!(tv));
            }
        }
        enriched_points.push(serde_json::Value::Object(obj.clone()));
    }
    transformed_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let (vmin, vmax) = match (
        transformed_values.first().copied(),
        transformed_values.last().copied(),
    ) {
        (Some(a), Some(b)) => (a, b),
        _ => (0.0, 0.0),
    };

    let mut contour_breaks: Vec<f64> = Vec::new();
    if contours_enabled && vmax > vmin {
        if contour_mode == "quantile" {
            for i in 1..contour_levels {
                let t = (i as f64) / (contour_levels as f64);
                let idx = ((transformed_values.len().saturating_sub(1)) as f64 * t).round() as usize;
                contour_breaks
                    .push(transformed_values[idx.min(transformed_values.len().saturating_sub(1))]);
            }
        } else {
            let mut x = vmin + contour_interval;
            while x < vmax {
                contour_breaks.push(x);
                x += contour_interval;
            }
        }
    }

    let heat_samples: Vec<HeatPt> = enriched_points
        .iter()
        .filter_map(|p| {
            let obj = p.as_object()?;
            let x = obj.get("x")?.as_f64()?;
            let y = obj.get("y")?.as_f64()?;
            let attrs = obj.get("attributes")?.as_object()?;
            let v = attrs.get("__heatmap_value")?.as_f64()?;
            Some(HeatPt { x, y, v })
        })
        .collect();

    let (xmin, xmax, ymin, ymax) = if heat_samples.is_empty() {
        (0.0, 1.0, 0.0, 1.0)
    } else {
        let xs: Vec<f64> = heat_samples.iter().map(|s| s.x).collect();
        let ys: Vec<f64> = heat_samples.iter().map(|s| s.y).collect();
        (
            xs.iter().copied().fold(f64::INFINITY, f64::min),
            xs.iter().copied().fold(f64::NEG_INFINITY, f64::max),
            ys.iter().copied().fold(f64::INFINITY, f64::min),
            ys.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        )
    };

    let grid_n = (smoothness as usize / 4).clamp(48, 128);
    let nx = grid_n;
    let ny = grid_n;
    let gx = (xmax - xmin).max(1e-9);
    let gy = (ymax - ymin).max(1e-9);
    let mut grid_values: Vec<Option<f64>> = vec![None; nx * ny];
    for iy in 0..ny {
        for ix in 0..nx {
            let x = xmin + (ix as f64 + 0.5) / (nx as f64) * gx;
            let y = ymin + (iy as f64 + 0.5) / (ny as f64) * gy;
            grid_values[iy * nx + ix] = interpolate_value(
                &heat_samples,
                x,
                y,
                &method,
                idw_power,
                search_radius_m,
                min_points,
                max_points,
            );
        }
    }

    let mut residual_features: Vec<serde_json::Value> = Vec::new();
    let mut residuals: Vec<f64> = Vec::new();
    for (i, s) in heat_samples.iter().enumerate() {
        let mut others = Vec::with_capacity(heat_samples.len().saturating_sub(1));
        for (j, o) in heat_samples.iter().enumerate() {
            if i != j {
                others.push(*o);
            }
        }
        let pred = interpolate_value(
            &others,
            s.x,
            s.y,
            &method,
            idw_power,
            search_radius_m,
            min_points,
            max_points,
        );
        if let Some(pv) = pred {
            let r = s.v - pv;
            residuals.push(r);
            residual_features.push(serde_json::json!({
                "type":"Feature",
                "geometry":{"type":"Point","coordinates":[s.x,s.y]},
                "properties":{"observed":s.v,"predicted":pv,"residual":r}
            }));
        }
    }
    let mae = if residuals.is_empty() {
        0.0
    } else {
        residuals.iter().map(|r| r.abs()).sum::<f64>() / residuals.len() as f64
    };
    let rmse = if residuals.is_empty() {
        0.0
    } else {
        (residuals.iter().map(|r| r * r).sum::<f64>() / residuals.len() as f64).sqrt()
    };
    let bias = if residuals.is_empty() {
        0.0
    } else {
        residuals.iter().sum::<f64>() / residuals.len() as f64
    };

    let mut contour_features: Vec<serde_json::Value> = Vec::new();
    if contours_enabled {
        for br in &contour_breaks {
            let segs = marching_segments(&grid_values, nx, ny, xmin, xmax, ymin, ymax, *br);
            for seg in segs {
                contour_features.push(serde_json::json!({
                    "type":"Feature",
                    "geometry":{"type":"LineString","coordinates":[[seg[0][0],seg[0][1]],[seg[1][0],seg[1][1]]]},
                    "properties":{"level":br}
                }));
            }
        }
    }

    let mut gradient_values: Vec<Option<f64>> = Vec::new();
    if gradient_enabled {
        gradient_values = vec![None; nx * ny];
        for iy in 1..(ny.saturating_sub(1)) {
            for ix in 1..(nx.saturating_sub(1)) {
                let c = grid_values[iy * nx + ix];
                let l = grid_values[iy * nx + (ix - 1)];
                let r = grid_values[iy * nx + (ix + 1)];
                let b = grid_values[(iy - 1) * nx + ix];
                let t = grid_values[(iy + 1) * nx + ix];
                let (Some(_cv), Some(lv), Some(rv), Some(bv), Some(tv)) = (c, l, r, b, t) else {
                    continue;
                };
                let gx = (rv - lv) * 0.5;
                let gy = (tv - bv) * 0.5;
                let g = if gradient_mode == "directional" { gy.atan2(gx) } else { (gx * gx + gy * gy).sqrt() };
                gradient_values[iy * nx + ix] = Some(g);
            }
        }
    }

    let out = serde_json::json!({
        "type": "assay_heatmap_surface",
        "measure": selected_measure,
        "measure_candidates": measure_candidates,
        "points": enriched_points,
        "stats": {
            "raw_count": raw_values.len(),
            "value_min": vmin,
            "value_max": vmax
        },
        "heatmap_config": {
            "measure": selected_measure,
            "method": method,
            "scale": scale,
            "palette": palette,
            "clamp_low_pct": clamp_low,
            "clamp_high_pct": clamp_high,
            "idw_power": idw_power,
            "smoothness": smoothness,
            "search_radius_m": search_radius_m,
            "min_points": min_points,
            "max_points": max_points,
            "contours_enabled": contours_enabled,
            "contour_mode": contour_mode,
            "contour_interval": contour_interval,
            "contour_levels": contour_levels,
            "contour_breaks": contour_breaks,
            "gradient_enabled": gradient_enabled,
            "gradient_mode": gradient_mode,
            "opacity": 0.52
        },
        "surface_grid": {
            "nx": nx,
            "ny": ny,
            "xmin": xmin,
            "xmax": xmax,
            "ymin": ymin,
            "ymax": ymax,
            "values": grid_values
        }
    });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!("graphs/{}/nodes/{}/heatmap.json", job.graph_id, job.node_id);
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;

    let mut outputs = vec![artifact.clone()];
    let mut hashes = vec![artifact.content_hash.clone()];

    let residual_json = serde_json::json!({
        "type":"FeatureCollection",
        "features": residual_features,
        "metrics": {"n": residuals.len(), "mae": mae, "rmse": rmse, "bias": bias}
    });
    let residual_bytes = serde_json::to_vec(&residual_json)?;
    let residual_key = format!("graphs/{}/nodes/{}/residuals.json", job.graph_id, job.node_id);
    let residual_ref = write_artifact(ctx, &residual_key, &residual_bytes, Some("application/json")).await?;
    outputs.push(residual_ref.clone());
    hashes.push(residual_ref.content_hash.clone());

    if contours_enabled {
        let contours_json = serde_json::json!({
            "type":"FeatureCollection",
            "features": contour_features
        });
        let contour_bytes = serde_json::to_vec(&contours_json)?;
        let contour_key = format!("graphs/{}/nodes/{}/contours.geojson", job.graph_id, job.node_id);
        let contour_ref = write_artifact(ctx, &contour_key, &contour_bytes, Some("application/geo+json")).await?;
        outputs.push(contour_ref.clone());
        hashes.push(contour_ref.content_hash.clone());
    }

    if gradient_enabled {
        let gradient_json = serde_json::json!({
            "type":"gradient_grid",
            "mode": gradient_mode,
            "nx": nx,
            "ny": ny,
            "xmin": xmin,
            "xmax": xmax,
            "ymin": ymin,
            "ymax": ymax,
            "values": gradient_values
        });
        let gradient_bytes = serde_json::to_vec(&gradient_json)?;
        let gradient_key = format!("graphs/{}/nodes/{}/gradient.json", job.graph_id, job.node_id);
        let gradient_ref = write_artifact(ctx, &gradient_key, &gradient_bytes, Some("application/json")).await?;
        outputs.push(gradient_ref.clone());
        hashes.push(gradient_ref.content_hash.clone());
    }

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: outputs,
        content_hashes: hashes,
        error_message: None,
    })
}

/// Merge collar / survey / assay JSON shards into one package for desurvey.
pub async fn run_drillhole_merge(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let (collars, surveys, assays) = collect_drillhole_inputs(ctx, job).await?;

    let merged = serde_json::json!({
        "collars": collars,
        "surveys": surveys,
        "assays": assays,
    });
    let bytes = serde_json::to_vec(&merged)?;
    let key = format!("graphs/{}/nodes/{}/ingest.json", job.graph_id, job.node_id);
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

/// Minimal straight-hole desurvey from collars + surveys; writes trajectory JSON.
pub async fn run_desurvey_trajectory(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let (collars, surveys, _assays) = collect_drillhole_inputs(ctx, job).await?;
    if collars.is_empty() {
        return Err(NodeError::InvalidConfig(
            "desurvey_trajectory requires collars input".into(),
        ));
    }
    if surveys.is_empty() {
        return Err(NodeError::InvalidConfig(
            "desurvey_trajectory requires surveys input".into(),
        ));
    }

    let mut segments = Vec::new();
    for c in &collars {
        let mut stations: Vec<&SurveyStationRecord> = surveys
            .iter()
            .filter(|s| s.hole_id == c.hole_id)
            .collect();
        stations.sort_by(|a, b| a.depth_m.partial_cmp(&b.depth_m).unwrap());
        if stations.is_empty() {
            segments.push(TrajectorySegment {
                hole_id: c.hole_id.clone(),
                depth_from_m: 0.0,
                depth_to_m: 1.0,
                x_from: c.x,
                y_from: c.y,
                z_from: c.z,
                x_to: c.x,
                y_to: c.y,
                z_to: c.z - 1.0,
                crs: c.crs.clone(),
            });
            continue;
        }
        let mut prev_d = 0.0f64;
        let mut prev_x = c.x;
        let mut prev_y = c.y;
        let mut prev_z = c.z;
        for st in stations {
            let dz = st.depth_m - prev_d;
            let dip_rad = st.dip_deg.to_radians();
            let az_rad = st.azimuth_deg.to_radians();
            let run = dz * dip_rad.cos();
            let dx = run * az_rad.sin();
            let dy = run * az_rad.cos();
            let dz_vert = dz * dip_rad.sin();
            let x_to = prev_x + dx;
            let y_to = prev_y + dy;
            let z_to = prev_z - dz_vert;
            segments.push(TrajectorySegment {
                hole_id: c.hole_id.clone(),
                depth_from_m: prev_d,
                depth_to_m: st.depth_m,
                x_from: prev_x,
                y_from: prev_y,
                z_from: prev_z,
                x_to,
                y_to,
                z_to,
                crs: c.crs.clone(),
            });
            prev_d = st.depth_m;
            prev_x = x_to;
            prev_y = y_to;
            prev_z = z_to;
        }
    }

    let bytes = serde_json::to_vec(&segments)?;
    let key = format!("graphs/{}/nodes/{}/trajectory.json", job.graph_id, job.node_id);
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

fn position_at_depth(segments: &[TrajectorySegment], depth_m: f64) -> Option<(f64, f64, f64)> {
    for s in segments {
        if depth_m < s.depth_from_m || depth_m > s.depth_to_m {
            continue;
        }
        let len = (s.depth_to_m - s.depth_from_m).abs();
        let t = if len <= f64::EPSILON {
            0.0
        } else {
            (depth_m - s.depth_from_m) / (s.depth_to_m - s.depth_from_m)
        };
        let x = s.x_from + (s.x_to - s.x_from) * t;
        let y = s.y_from + (s.y_to - s.y_from) * t;
        let z = s.z_from + (s.z_to - s.z_from) * t;
        return Some((x, y, z));
    }
    None
}

/// Build drillhole geometry payloads from trajectory + assays:
/// - `drillhole_meshes.json`: segment-wise cylinders with assay overlap metadata
/// - `assay_points.json`: midpoint assay points for later interpolation/kriging
pub async fn run_drillhole_model(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let mut trajectory: Vec<TrajectorySegment> = Vec::new();
    let mut assays: Vec<IntervalSampleRecord> = Vec::new();

    for ar in &job.input_artifact_refs {
        let v = read_json_artifact(ctx, &ar.key).await?;
        if let Ok(mut segs) = serde_json::from_value::<Vec<TrajectorySegment>>(v.clone()) {
            trajectory.append(&mut segs);
        }
        if let Some(a) = v.get("assays") {
            if let Ok(mut part) = serde_json::from_value::<Vec<IntervalSampleRecord>>(a.clone()) {
                assays.append(&mut part);
            }
        }
    }

    if trajectory.is_empty() {
        return Err(NodeError::InvalidConfig(
            "drillhole_model requires trajectory input".into(),
        ));
    }

    let mut hole_segments: std::collections::HashMap<String, Vec<TrajectorySegment>> =
        std::collections::HashMap::new();
    for s in trajectory {
        hole_segments
            .entry(s.hole_id.clone())
            .or_default()
            .push(s);
    }
    for segs in hole_segments.values_mut() {
        segs.sort_by(|a, b| a.depth_from_m.partial_cmp(&b.depth_from_m).unwrap());
    }

    let radius_m = job
        .output_spec
        .pointer("/node_ui/hole_radius_m")
        .and_then(|v| v.as_f64())
        .filter(|r| *r > 0.0)
        .unwrap_or(0.6);

    let mut mesh_segments: Vec<serde_json::Value> = Vec::new();
    for (hole_id, segs) in &hole_segments {
        for (idx, s) in segs.iter().enumerate() {
            let overlapping_assays: Vec<serde_json::Value> = assays
                .iter()
                .filter(|a| {
                    a.hole_id == *hole_id && a.to_m > s.depth_from_m && a.from_m < s.depth_to_m
                })
                .map(|a| {
                    serde_json::json!({
                        "from_m": a.from_m,
                        "to_m": a.to_m,
                        "attributes": a.attributes,
                        "qa_flags": a.qa_flags,
                    })
                })
                .collect();
            mesh_segments.push(serde_json::json!({
                "hole_id": hole_id,
                "segment_index": idx,
                "from_depth_m": s.depth_from_m,
                "to_depth_m": s.depth_to_m,
                "from_xyz": [s.x_from, s.y_from, s.z_from],
                "to_xyz": [s.x_to, s.y_to, s.z_to],
                "radius_m": radius_m,
                "assays": overlapping_assays,
            }));
        }
    }

    let mut assay_points: Vec<serde_json::Value> = Vec::new();
    for a in &assays {
        let Some(segs) = hole_segments.get(&a.hole_id) else {
            continue;
        };
        let mid = (a.from_m + a.to_m) * 0.5;
        if let Some((x, y, z)) = position_at_depth(segs, mid) {
            assay_points.push(serde_json::json!({
                "hole_id": a.hole_id,
                "from_m": a.from_m,
                "to_m": a.to_m,
                "depth_m": mid,
                "x": x,
                "y": y,
                "z": z,
                "attributes": a.attributes,
                "qa_flags": a.qa_flags,
            }));
        }
    }

    let mesh_payload = serde_json::json!({
        "kind": "drillhole_cylinder_mesh_segments",
        "segments": mesh_segments,
    });
    let mesh_bytes = serde_json::to_vec(&mesh_payload)?;
    let mesh_key = format!(
        "graphs/{}/nodes/{}/drillhole_meshes.json",
        job.graph_id, job.node_id
    );
    let mesh_ref = write_artifact(ctx, &mesh_key, &mesh_bytes, Some("application/json")).await?;

    let points_payload = serde_json::json!({ "assay_points": assay_points });
    let points_bytes = serde_json::to_vec(&points_payload)?;
    let points_key = format!(
        "graphs/{}/nodes/{}/assay_points.json",
        job.graph_id, job.node_id
    );
    let points_ref =
        write_artifact(ctx, &points_key, &points_bytes, Some("application/json")).await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![mesh_ref.clone(), points_ref.clone()],
        content_hashes: vec![mesh_ref.content_hash, points_ref.content_hash],
        error_message: None,
    })
}

pub async fn run_dem_integrate_stub(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let stub = serde_json::json!({ "dem": "stub", "graph": job.graph_id });
    let bytes = serde_json::to_vec(&stub)?;
    let key = format!("graphs/{}/nodes/{}/dem_stub.json", job.graph_id, job.node_id);
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

pub async fn run_block_model_stub(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let nx = 4u32;
    let ny = 4u32;
    let nz = 4u32;
    let n = (nx * ny * nz) as usize;
    let mut grid = vec![0f32; n];
    for i in 0..n {
        grid[i] = i as f32 * 0.1;
    }
    let meta = serde_json::json!({
        "nx": nx, "ny": ny, "nz": nz,
        "origin_x": 0.0, "origin_y": 0.0, "origin_z": 0.0,
        "cell_x": 10.0, "cell_y": 10.0, "cell_z": 5.0,
    });
    let meta_bytes = serde_json::to_vec(&meta)?;
    let meta_key = format!("graphs/{}/nodes/{}/block_model_meta.json", job.graph_id, job.node_id);
    let meta_ref = write_artifact(ctx, &meta_key, &meta_bytes, Some("application/json")).await?;

    let bin: Vec<u8> = grid.iter().flat_map(|f| f.to_le_bytes()).collect();
    let bin_key = format!("graphs/{}/nodes/{}/block_model_f32.bin", job.graph_id, job.node_id);
    let bin_ref = write_artifact(ctx, &bin_key, &bin, Some("application/octet-stream")).await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![meta_ref.clone(), bin_ref.clone()],
        content_hashes: vec![meta_ref.content_hash, bin_ref.content_hash],
        error_message: None,
    })
}

pub async fn run_plan_view_2d(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let out = serde_json::json!({
        "viewer": "plan_view_2d",
        "graph": job.graph_id,
        "node": job.node_id,
        "input_artifact_count": job.input_artifact_refs.len(),
        "inputs": job.input_artifact_refs,
    });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!("graphs/{}/nodes/{}/plan_view.json", job.graph_id, job.node_id);
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}
