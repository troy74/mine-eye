use std::collections::HashMap;
use std::env;

use mine_eye_types::{CrsRecord, JobEnvelope, JobResult, JobStatus};
use rayon::prelude::*;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde_json::{json, Map, Value};

use crate::crs_transform::transform_xy;
use crate::executor::ExecutionContext;
use crate::NodeError;

fn rows_from_csv_bytes(bytes: &[u8], delimiter: u8) -> Option<Vec<Value>> {
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(true)
        .flexible(true)
        .from_reader(std::io::Cursor::new(bytes));
    let headers = rdr
        .headers()
        .ok()
        .map(|h| h.iter().map(|s| s.to_string()).collect::<Vec<_>>())?;
    let mut rows = Vec::new();
    for rec in rdr.records().flatten() {
        let mut obj = Map::new();
        for (i, h) in headers.iter().enumerate() {
            if let Some(cell) = rec.get(i) {
                let raw = cell.trim();
                if raw.is_empty() {
                    continue;
                }
                if let Ok(v) = raw.replace(',', ".").parse::<f64>() {
                    obj.insert(h.clone(), json!(v));
                } else {
                    obj.insert(h.clone(), json!(raw));
                }
            }
        }
        if !obj.is_empty() {
            rows.push(Value::Object(obj));
        }
    }
    if rows.is_empty() {
        None
    } else {
        Some(rows)
    }
}

async fn rows_from_upstream(ctx: &ExecutionContext<'_>, job: &JobEnvelope) -> Option<Value> {
    for ar in &job.input_artifact_refs {
        let Ok(raw) = super::runtime::read_artifact_bytes(ctx, &ar.key).await else {
            continue;
        };
        let ext = ar.key.to_ascii_lowercase();
        if ext.ends_with(".json") {
            let Ok(v) = serde_json::from_slice::<Value>(&raw) else {
                continue;
            };
            if let Some(points) = v.pointer("/points").and_then(|x| x.as_array()) {
                let mut rows = Vec::<Value>::new();
                for p in points {
                    let Some(obj) = p.as_object() else {
                        continue;
                    };
                    let mut out = Map::new();
                    if let Some(x) = obj.get("x") {
                        out.insert("x".into(), x.clone());
                    }
                    if let Some(y) = obj.get("y") {
                        out.insert("y".into(), y.clone());
                    }
                    if let Some(z) = obj.get("z") {
                        out.insert("z".into(), z.clone());
                    }
                    if let Some(line_id) = obj.get("line_id").or_else(|| obj.get("segment_id")) {
                        out.insert("line_id".into(), line_id.clone());
                    }
                    if let Some(ts) = obj.get("timestamp") {
                        out.insert("timestamp".into(), ts.clone());
                    }
                    if let Some(fid) = obj.get("fid") {
                        out.insert("fid".into(), fid.clone());
                    }
                    if let Some(attrs) = obj.get("attributes").and_then(|a| a.as_object()) {
                        for (k, v) in attrs {
                            out.entry(k.clone()).or_insert_with(|| v.clone());
                        }
                    }
                    if !out.is_empty() {
                        rows.push(Value::Object(out));
                    }
                }
                if !rows.is_empty() {
                    return Some(json!({
                        "rows": rows,
                        "source_crs": v.get("crs").cloned().unwrap_or_else(|| v.get("source_crs").cloned().unwrap_or(Value::Null))
                    }));
                }
            }
            if let Some(rows) = v.pointer("/rows").and_then(|x| x.as_array()) {
                return Some(json!({
                    "rows": rows,
                    "source_crs": v.get("source_crs").cloned().unwrap_or(Value::Null)
                }));
            }
            if let Some(points) = v.pointer("/points").and_then(|x| x.as_array()) {
                return Some(json!({
                    "rows": points,
                    "source_crs": v.get("source_crs").cloned().unwrap_or(Value::Null)
                }));
            }
            let src_key = v
                .pointer("/source/artifact_key")
                .and_then(|x| x.as_str())
                .or_else(|| v.get("artifact_key").and_then(|x| x.as_str()));
            if let Some(k) = src_key {
                let src_raw = super::runtime::read_artifact_bytes(ctx, k).await.ok()?;
                let delimiter = v
                    .pointer("/source/delimiter")
                    .and_then(|x| x.as_str())
                    .and_then(|s| s.as_bytes().first().copied())
                    .unwrap_or(b',');
                if let Some(rows) = rows_from_csv_bytes(&src_raw, delimiter) {
                    return Some(json!({
                        "rows": rows,
                        "source_crs": v.get("source_crs").cloned().unwrap_or(Value::Null)
                    }));
                }
            }
        } else if ext.ends_with(".csv") || ext.ends_with(".tsv") || ext.ends_with(".txt") {
            let delimiter = if ext.ends_with(".tsv") { b'\t' } else { b',' };
            if let Some(rows) = rows_from_csv_bytes(&raw, delimiter) {
                return Some(json!({
                    "rows": rows,
                    "source_crs": job.project_crs
                }));
            }
        }
    }
    None
}

#[derive(Clone)]
struct Params {
    mapping: Map<String, Value>,
    grid_method: String,
    grid_resolution_m: f64,
    max_grid_cells: usize,
    idw_power: f64,
    search_radius_m: f64,
    max_points: usize,
    despike_sigma: f64,
    smooth_window_m: f64,
    resample_spacing_m: f64,
    decimate_pct: f64,
    llm_enabled: bool,
}

#[derive(Clone)]
struct Pt {
    x: f64,
    y: f64,
    z: f64,
    m: f64,
    line_id: String,
    fid: Option<f64>,
    timestamp: Option<String>,
    along_m: f64,
    attrs: Map<String, Value>,
}

fn parse_num(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64().filter(|x| x.is_finite()),
        Value::String(s) => s
            .trim()
            .replace(',', ".")
            .parse::<f64>()
            .ok()
            .filter(|x| x.is_finite()),
        _ => None,
    }
}

fn parse_params(job: &JobEnvelope) -> Params {
    let ui = |p: &str| job.output_spec.pointer(p);
    let parse_f64 = |p: &str, d: f64| ui(p).and_then(parse_num).unwrap_or(d);
    let parse_usize = |p: &str, d: usize| {
        ui(p)
            .and_then(parse_num)
            .map(|x| x.round() as i64)
            .filter(|x| *x > 0)
            .map(|x| x as usize)
            .unwrap_or(d)
    };
    Params {
        mapping: ui("/node_ui/mapping")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default(),
        grid_method: ui("/node_ui/grid_method")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "idw".to_string()),
        grid_resolution_m: parse_f64("/node_ui/grid_resolution_m", 25.0).max(1.0),
        max_grid_cells: parse_usize("/node_ui/max_grid_cells", 250_000).clamp(10_000, 2_000_000),
        idw_power: parse_f64("/node_ui/idw_power", 2.0).clamp(1.0, 6.0),
        search_radius_m: parse_f64("/node_ui/search_radius_m", 0.0).max(0.0),
        max_points: parse_usize("/node_ui/max_points", 32).clamp(4, 256),
        despike_sigma: parse_f64("/node_ui/despike_sigma", 6.0).clamp(2.0, 20.0),
        smooth_window_m: parse_f64("/node_ui/smooth_window_m", 0.0).max(0.0),
        resample_spacing_m: parse_f64("/node_ui/resample_spacing_m", 0.0).max(0.0),
        decimate_pct: parse_f64("/node_ui/decimate_pct", 100.0).clamp(1.0, 100.0),
        llm_enabled: ui("/node_ui/llm_enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
    }
}

fn lookup_ci(obj: &Map<String, Value>, key: &str) -> Option<Value> {
    if let Some(v) = obj.get(key) {
        return Some(v.clone());
    }
    let lk = key.to_ascii_lowercase();
    for (k, v) in obj {
        if k.to_ascii_lowercase() == lk {
            return Some(v.clone());
        }
    }
    None
}

fn mapped_col(mapping: &Map<String, Value>, key: &str) -> Option<String> {
    mapping
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn pick_col(headers_lc: &[String], candidates: &[&str]) -> Option<String> {
    for c in candidates {
        let cl = c.to_ascii_lowercase();
        if let Some(h) = headers_lc.iter().find(|h| h.as_str() == cl) {
            return Some(h.clone());
        }
    }
    for c in candidates {
        let cl = c.to_ascii_lowercase();
        if let Some(h) = headers_lc.iter().find(|h| h.contains(&cl)) {
            return Some(h.clone());
        }
    }
    None
}

fn median(vals: &mut [f64]) -> Option<f64> {
    if vals.is_empty() {
        return None;
    }
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = vals.len();
    if n % 2 == 1 {
        Some(vals[n / 2])
    } else {
        Some(0.5 * (vals[n / 2 - 1] + vals[n / 2]))
    }
}

fn mad(vals: &[f64], med: f64) -> f64 {
    let mut d = vals.iter().map(|x| (x - med).abs()).collect::<Vec<_>>();
    median(&mut d).unwrap_or(0.0).max(1e-9)
}

fn infer_utm_epsg(lon: f64, lat: f64) -> i32 {
    let mut zone = ((lon + 180.0) / 6.0).floor() as i32 + 1;
    zone = zone.clamp(1, 60);
    if lat >= 0.0 {
        32600 + zone
    } else {
        32700 + zone
    }
}

fn is_utm_epsg(epsg: i32) -> bool {
    (32601..=32660).contains(&epsg) || (32701..=32760).contains(&epsg)
}

fn projected_xy_plausible_for_epsg(epsg: i32, x: f64, y: f64) -> bool {
    if !is_utm_epsg(epsg) {
        return true;
    }
    (100_000.0..=900_000.0).contains(&x) && (0.0..=10_000_000.0).contains(&y)
}

fn interp_idw(x: f64, y: f64, pts: &[Pt], pwr: f64, radius: f64, max_points: usize) -> Option<f64> {
    let mut near = Vec::<(f64, f64)>::new();
    for pt in pts {
        let dx = x - pt.x;
        let dy = y - pt.y;
        let d2 = dx * dx + dy * dy;
        if d2 <= 1e-12 {
            return Some(pt.m);
        }
        if radius > 0.0 && d2.sqrt() > radius {
            continue;
        }
        near.push((d2, pt.m));
    }
    if near.is_empty() {
        return None;
    }
    if near.len() > max_points {
        let k = max_points.min(near.len().saturating_sub(1));
        near.select_nth_unstable_by(k, |a, b| {
            a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)
        });
        near.truncate(max_points);
    }
    let mut num = 0.0;
    let mut den = 0.0;
    for (d2, v) in near {
        let w = 1.0 / d2.powf(0.5 * pwr);
        num += w * v;
        den += w;
    }
    (den > 0.0).then_some(num / den)
}

fn preview_point_rows(rows: &[Value], max_rows: usize) -> Vec<Value> {
    if rows.len() <= max_rows {
        return rows.to_vec();
    }
    let stride = (rows.len() as f64 / max_rows as f64).ceil().max(1.0) as usize;
    rows.iter()
        .step_by(stride)
        .take(max_rows)
        .cloned()
        .collect()
}

fn preview_grid_rows(rows: &[Value], max_rows: usize) -> Vec<Value> {
    if rows.len() <= max_rows {
        return rows.to_vec();
    }
    let stride = (rows.len() as f64 / max_rows as f64).ceil().max(1.0) as usize;
    rows.iter()
        .step_by(stride)
        .take(max_rows)
        .cloned()
        .collect()
}

fn bilinear(
    nx: usize,
    ny: usize,
    xmin: f64,
    xmax: f64,
    ymin: f64,
    ymax: f64,
    vals: &[Option<f64>],
    x: f64,
    y: f64,
) -> Option<f64> {
    if nx < 2 || ny < 2 {
        return None;
    }
    let tx = ((x - xmin) / (xmax - xmin)).clamp(0.0, 1.0) * (nx as f64 - 1.0);
    let ty = ((y - ymin) / (ymax - ymin)).clamp(0.0, 1.0) * (ny as f64 - 1.0);
    let ix = tx.floor() as usize;
    let iy = ty.floor() as usize;
    let fx = tx - ix as f64;
    let fy = ty - iy as f64;
    let ix1 = (ix + 1).min(nx - 1);
    let iy1 = (iy + 1).min(ny - 1);
    let idx = |i: usize, j: usize| -> usize { j * nx + i };
    let v00 = vals.get(idx(ix, iy)).and_then(|v| *v)?;
    let v10 = vals.get(idx(ix1, iy)).and_then(|v| *v)?;
    let v01 = vals.get(idx(ix, iy1)).and_then(|v| *v)?;
    let v11 = vals.get(idx(ix1, iy1)).and_then(|v| *v)?;
    let v0 = v00 * (1.0 - fx) + v10 * fx;
    let v1 = v01 * (1.0 - fx) + v11 * fx;
    Some(v0 * (1.0 - fy) + v1 * fy)
}

async fn llm_commentary(context: &Value) -> Option<Value> {
    let api_key = env::var("OPENROUTER_API_KEY")
        .or_else(|_| env::var("OPENROUTER_KEY"))
        .ok()?;
    let sys = "You are an airborne magnetics QA assistant. Return STRICT JSON with keys: summary, risks, mutation_suggestions.";
    let usr = format!(
        "Review top/tail and processing audit. Keep concise and practical.\nContext:\n{}",
        serde_json::to_string_pretty(context).ok()?
    );
    let body = json!({
        "model": "openai/gpt-5-mini",
        "messages": [
            {"role":"system","content":sys},
            {"role":"user","content":usr}
        ],
        "temperature": 0.1
    });
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(24))
        .build()
        .ok()?;
    let resp = client
        .post("https://openrouter.ai/api/v1/chat/completions")
        .header(AUTHORIZATION, format!("Bearer {}", api_key))
        .header(CONTENT_TYPE, "application/json")
        .json(&body)
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let v: Value = resp.json().await.ok()?;
    let content = v
        .pointer("/choices/0/message/content")
        .and_then(|x| x.as_str())?;
    serde_json::from_str::<Value>(content).ok().or_else(|| {
        let s = content.find('{')?;
        let e = content.rfind('}')?;
        serde_json::from_str::<Value>(&content[s..=e]).ok()
    })
}

pub async fn run_magnetic_model(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let params = parse_params(job);
    ctx.report_progress(
        "load_input",
        Some(0.02),
        Some("Loading magnetic rows".to_string()),
        None,
    );
    let payload_derived;
    let payload = if let Some(p) = job.input_payload.as_ref() {
        p
    } else if let Some(p) = rows_from_upstream(ctx, job).await {
        payload_derived = p;
        &payload_derived
    } else {
        return Err(NodeError::InvalidConfig(
            "missing input_payload for magnetic_model".into(),
        ));
    };
    let rows = payload
        .pointer("/rows")
        .and_then(|v| v.as_array())
        .ok_or_else(|| NodeError::InvalidConfig("magnetic_model requires payload.rows[]".into()))?;

    let mut raw = Vec::<Map<String, Value>>::new();
    for r in rows {
        if let Some(obj) = r.as_object() {
            raw.push(obj.clone());
        }
    }
    if raw.is_empty() {
        return Err(NodeError::InvalidConfig(
            "magnetic_model received no tabular rows".into(),
        ));
    }

    let headers_lc = raw[0]
        .keys()
        .map(|k| k.to_ascii_lowercase())
        .collect::<Vec<_>>();

    let x_key = mapped_col(&params.mapping, "x")
        .or_else(|| pick_col(&headers_lc, &["x", "easting", "east", "utm_e", "x_m"]));
    let y_key = mapped_col(&params.mapping, "y")
        .or_else(|| pick_col(&headers_lc, &["y", "northing", "north", "utm_n", "y_m"]));
    let lat_key =
        mapped_col(&params.mapping, "lat").or_else(|| pick_col(&headers_lc, &["lat", "latitude"]));
    let lon_key = mapped_col(&params.mapping, "lon")
        .or_else(|| pick_col(&headers_lc, &["lon", "longitude", "long"]));
    let line_key = mapped_col(&params.mapping, "line_id").or_else(|| {
        pick_col(
            &headers_lc,
            &["line_id", "line", "flightline", "flight_line"],
        )
    });
    let time_key = mapped_col(&params.mapping, "utc")
        .or_else(|| pick_col(&headers_lc, &["utc", "timestamp", "datetime", "time"]));
    let fid_key = mapped_col(&params.mapping, "fid")
        .or_else(|| pick_col(&headers_lc, &["fid", "seq", "sample_no", "point_id"]));
    let tmf_key = mapped_col(&params.mapping, "tmf").or_else(|| {
        pick_col(
            &headers_lc,
            &["tmf", "tmi", "total_magnetic_field", "mag_total"],
        )
    });
    let mag_lev_key = mapped_col(&params.mapping, "mag_lev")
        .or_else(|| pick_col(&headers_lc, &["mag_lev", "mag", "magnetic", "mag_lvl"]));
    let igrf_key = mapped_col(&params.mapping, "igrf")
        .or_else(|| pick_col(&headers_lc, &["igrf", "regional_field"]));
    let radar_key = mapped_col(&params.mapping, "radar").or_else(|| {
        pick_col(
            &headers_lc,
            &["radar", "radar_alt", "clearance", "terrain_clearance"],
        )
    });
    let gps_alt_key = mapped_col(&params.mapping, "gps_alt")
        .or_else(|| pick_col(&headers_lc, &["gps_alt", "altitude", "elevation", "z"]));

    let mut parsed = Vec::<Map<String, Value>>::new();
    let mut malformed = 0usize;
    for r in raw {
        // Keep raw row but require at least one coordinate pair + one mag signal.
        let has_xy = x_key
            .as_ref()
            .and_then(|k| lookup_ci(&r, k))
            .and_then(|v| parse_num(&v))
            .zip(
                y_key
                    .as_ref()
                    .and_then(|k| lookup_ci(&r, k))
                    .and_then(|v| parse_num(&v)),
            )
            .is_some();
        let has_ll = lon_key
            .as_ref()
            .and_then(|k| lookup_ci(&r, k))
            .and_then(|v| parse_num(&v))
            .zip(
                lat_key
                    .as_ref()
                    .and_then(|k| lookup_ci(&r, k))
                    .and_then(|v| parse_num(&v)),
            )
            .is_some();
        let has_mag = tmf_key
            .as_ref()
            .and_then(|k| lookup_ci(&r, k))
            .and_then(|v| parse_num(&v))
            .or_else(|| {
                mag_lev_key
                    .as_ref()
                    .and_then(|k| lookup_ci(&r, k))
                    .and_then(|v| parse_num(&v))
            })
            .is_some();
        if (has_xy || has_ll) && has_mag {
            parsed.push(r);
        } else {
            malformed += 1;
        }
    }
    if parsed.is_empty() {
        return Err(NodeError::InvalidConfig(
            "magnetic_model found no valid rows (coords + magnetic signal)".into(),
        ));
    }

    let mut use_projected = false;
    let mut projected_valid = 0usize;
    let mut projected_total = 0usize;
    let mut projected_plausible = 0usize;
    let source_epsg_hint = payload
        .pointer("/source_crs/epsg")
        .and_then(parse_num)
        .map(|v| v as i32)
        .or(job.project_crs.as_ref().and_then(|c| c.epsg))
        .unwrap_or(4326);
    if let (Some(xk), Some(yk)) = (x_key.as_ref(), y_key.as_ref()) {
        for r in &parsed {
            if let (Some(x), Some(y)) = (
                lookup_ci(r, xk).and_then(|v| parse_num(&v)),
                lookup_ci(r, yk).and_then(|v| parse_num(&v)),
            ) {
                projected_total += 1;
                let looks_geo = x.abs() <= 180.0 && y.abs() <= 90.0;
                if !looks_geo && x.abs() > 500.0 && y.abs() > 500.0 {
                    projected_valid += 1;
                }
                if projected_xy_plausible_for_epsg(source_epsg_hint, x, y) {
                    projected_plausible += 1;
                }
            }
        }
    }
    if projected_total > 0 && projected_valid * 2 >= projected_total {
        use_projected = true;
    }

    let mut lon_lat = Vec::<(f64, f64)>::new();
    if let (Some(lok), Some(lak)) = (lon_key.as_ref(), lat_key.as_ref()) {
        for r in &parsed {
            if let (Some(lon), Some(lat)) = (
                lookup_ci(r, lok).and_then(|v| parse_num(&v)),
                lookup_ci(r, lak).and_then(|v| parse_num(&v)),
            ) {
                if lon.abs() <= 180.0 && lat.abs() <= 90.0 {
                    lon_lat.push((lon, lat));
                }
            }
        }
    }
    let forced_fallback_lonlat = use_projected
        && !lon_lat.is_empty()
        && projected_total > 0
        && is_utm_epsg(source_epsg_hint)
        && projected_plausible * 2 < projected_total;
    if forced_fallback_lonlat {
        use_projected = false;
    }

    let mut target_crs = payload
        .pointer("/source_crs/epsg")
        .and_then(parse_num)
        .map(|v| v as i32)
        .or(job.project_crs.as_ref().and_then(|c| c.epsg))
        .unwrap_or(4326);
    if !use_projected {
        if lon_lat.is_empty() {
            return Err(NodeError::InvalidConfig(
                "magnetic_model could not resolve usable projected XY or lat/lon".into(),
            ));
        }
        let mean_lon = lon_lat.iter().map(|x| x.0).sum::<f64>() / lon_lat.len() as f64;
        let mean_lat = lon_lat.iter().map(|x| x.1).sum::<f64>() / lon_lat.len() as f64;
        target_crs = infer_utm_epsg(mean_lon, mean_lat);
    }
    let target = CrsRecord::epsg(target_crs);
    let from_wgs84 = CrsRecord::epsg(4326);

    let mut pts = Vec::<Pt>::new();
    for (idx, r) in parsed.into_iter().enumerate() {
        let line_id = line_key
            .as_ref()
            .and_then(|k| lookup_ci(&r, k))
            .map(|v| match v {
                Value::String(s) => s.trim().to_string(),
                Value::Number(n) => n.to_string(),
                _ => String::new(),
            })
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "LINE_000".to_string());
        let timestamp = time_key
            .as_ref()
            .and_then(|k| lookup_ci(&r, k))
            .and_then(|v| match v {
                Value::String(s) => {
                    let t = s.trim();
                    if t.is_empty() {
                        None
                    } else {
                        Some(t.to_string())
                    }
                }
                Value::Number(n) => Some(n.to_string()),
                _ => None,
            });
        let fid = fid_key
            .as_ref()
            .and_then(|k| lookup_ci(&r, k))
            .and_then(|v| parse_num(&v));

        let (x, y) = if use_projected {
            let (Some(x), Some(y)) = (
                x_key
                    .as_ref()
                    .and_then(|k| lookup_ci(&r, k))
                    .and_then(|v| parse_num(&v)),
                y_key
                    .as_ref()
                    .and_then(|k| lookup_ci(&r, k))
                    .and_then(|v| parse_num(&v)),
            ) else {
                continue;
            };
            (x, y)
        } else {
            let (Some(lon), Some(lat)) = (
                lon_key
                    .as_ref()
                    .and_then(|k| lookup_ci(&r, k))
                    .and_then(|v| parse_num(&v)),
                lat_key
                    .as_ref()
                    .and_then(|k| lookup_ci(&r, k))
                    .and_then(|v| parse_num(&v)),
            ) else {
                continue;
            };
            match transform_xy(&from_wgs84, &target, lon, lat) {
                Ok(v) => v,
                Err(_) => continue,
            }
        };

        let z = radar_key
            .as_ref()
            .and_then(|k| lookup_ci(&r, k))
            .and_then(|v| parse_num(&v))
            .or_else(|| {
                gps_alt_key
                    .as_ref()
                    .and_then(|k| lookup_ci(&r, k))
                    .and_then(|v| parse_num(&v))
            })
            .unwrap_or(0.0);

        let tmf = tmf_key
            .as_ref()
            .and_then(|k| lookup_ci(&r, k))
            .and_then(|v| parse_num(&v));
        let mag_lev = mag_lev_key
            .as_ref()
            .and_then(|k| lookup_ci(&r, k))
            .and_then(|v| parse_num(&v));
        let igrf = igrf_key
            .as_ref()
            .and_then(|k| lookup_ci(&r, k))
            .and_then(|v| parse_num(&v));
        let signal = tmf.or(mag_lev);
        let Some(signal) = signal else { continue };
        let m = signal;

        let mut attrs = r.clone();
        attrs.insert("row_index".into(), json!(idx + 1));
        pts.push(Pt {
            x,
            y,
            z,
            m,
            line_id,
            fid,
            timestamp,
            along_m: 0.0,
            attrs,
        });
        if let Some(last) = pts.last_mut() {
            if let Some(v) = igrf {
                last.attrs.insert("__igrf__".into(), json!(v));
            }
            if tmf.is_some() {
                last.attrs.insert("__signal_col__".into(), json!("tmf"));
            } else if mag_lev.is_some() {
                last.attrs.insert("__signal_col__".into(), json!("mag_lev"));
            }
        }
    }
    if pts.is_empty() {
        return Err(NodeError::InvalidConfig(
            "magnetic_model retained no parseable points after coordinate normalization".into(),
        ));
    }
    ctx.report_progress(
        "normalize_points",
        Some(0.16),
        Some("Coordinate normalization complete".to_string()),
        Some(json!({ "points": pts.len() })),
    );

    let mut mvals = pts.iter().map(|p| p.m.abs()).collect::<Vec<_>>();
    let med_abs = median(&mut mvals).unwrap_or(0.0);
    let has_igrf = pts
        .iter()
        .any(|p| p.attrs.get("__igrf__").and_then(parse_num).is_some());
    let require_igrf_sub = has_igrf && med_abs > 5000.0;
    if require_igrf_sub {
        for p in &mut pts {
            if let Some(igrf) = p.attrs.get("__igrf__").and_then(parse_num) {
                p.m -= igrf;
            }
        }
    }

    let mut groups = HashMap::<String, Vec<Pt>>::new();
    for p in pts {
        groups.entry(p.line_id.clone()).or_default().push(p);
    }
    let mut cleaned = Vec::<Pt>::new();
    let mut non_monotonic_drop = 0usize;
    let mut despike_drop = 0usize;
    let mut total_lines = 0usize;
    for (_line, mut g) in groups {
        total_lines += 1;
        g.sort_by(|a, b| {
            a.timestamp.cmp(&b.timestamp).then_with(|| {
                a.fid
                    .unwrap_or(f64::INFINITY)
                    .partial_cmp(&b.fid.unwrap_or(f64::INFINITY))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
        });
        let mut mono = Vec::<Pt>::new();
        let mut prev_ts: Option<String> = None;
        for p in g {
            if let (Some(prev), Some(cur)) = (&prev_ts, &p.timestamp) {
                if cur < prev {
                    non_monotonic_drop += 1;
                    continue;
                }
            }
            if let Some(ts) = &p.timestamp {
                prev_ts = Some(ts.clone());
            }
            mono.push(p);
        }
        if mono.len() < 2 {
            cleaned.extend(mono);
            continue;
        }
        // Along-track distance
        let mut acc = 0.0;
        for i in 0..mono.len() {
            if i > 0 {
                let dx = mono[i].x - mono[i - 1].x;
                let dy = mono[i].y - mono[i - 1].y;
                acc += (dx * dx + dy * dy).sqrt();
            }
            mono[i].along_m = acc;
        }
        // Despike via local gradient MAD
        let mut grads = Vec::<f64>::new();
        for i in 1..mono.len() {
            let ds = (mono[i].along_m - mono[i - 1].along_m).max(1e-6);
            grads.push((mono[i].m - mono[i - 1].m) / ds);
        }
        let mut gcopy = grads.clone();
        let gmed = median(&mut gcopy).unwrap_or(0.0);
        let gmad = mad(&grads, gmed);
        let thr = gmed.abs() + params.despike_sigma * 1.4826 * gmad;
        let mut keep = vec![true; mono.len()];
        for i in 1..mono.len() {
            if grads[i - 1].abs() > thr {
                keep[i] = false;
                despike_drop += 1;
            }
        }
        let mut f = mono
            .into_iter()
            .enumerate()
            .filter_map(|(i, p)| keep[i].then_some(p))
            .collect::<Vec<_>>();
        if f.len() < 2 {
            cleaned.extend(f);
            continue;
        }
        // Optional smoothing (window in m)
        if params.smooth_window_m > 0.0 {
            let hw = 0.5 * params.smooth_window_m;
            let src = f.clone();
            for i in 0..f.len() {
                let d0 = src[i].along_m - hw;
                let d1 = src[i].along_m + hw;
                let mut num = 0.0;
                let mut den = 0.0;
                for q in &src {
                    if q.along_m >= d0 && q.along_m <= d1 {
                        num += q.m;
                        den += 1.0;
                    }
                }
                if den > 0.0 {
                    f[i].m = num / den;
                }
            }
        }
        // Optional resampling
        if params.resample_spacing_m > 0.0 && f.len() >= 2 {
            let spacing = params.resample_spacing_m;
            let max_d = f.last().map(|p| p.along_m).unwrap_or(0.0);
            let mut out = Vec::<Pt>::new();
            let mut t = 0.0;
            let mut j = 0usize;
            while t <= max_d && j + 1 < f.len() {
                while j + 1 < f.len() && f[j + 1].along_m < t {
                    j += 1;
                }
                if j + 1 >= f.len() {
                    break;
                }
                let a = &f[j];
                let b = &f[j + 1];
                let span = (b.along_m - a.along_m).max(1e-6);
                let r = ((t - a.along_m) / span).clamp(0.0, 1.0);
                let mut p = a.clone();
                p.x = a.x + (b.x - a.x) * r;
                p.y = a.y + (b.y - a.y) * r;
                p.z = a.z + (b.z - a.z) * r;
                p.m = a.m + (b.m - a.m) * r;
                p.along_m = t;
                out.push(p);
                t += spacing;
            }
            cleaned.extend(out);
        } else {
            cleaned.extend(f);
        }
    }
    if cleaned.len() < 4 {
        return Err(NodeError::InvalidConfig(
            "magnetic_model produced too few points after QA/despike".into(),
        ));
    }
    ctx.report_progress(
        "qa_cleanup",
        Some(0.32),
        Some("QA / despike / resample complete".to_string()),
        Some(json!({ "kept_points": cleaned.len() })),
    );

    let before_decimate = cleaned.len();
    if params.decimate_pct < 99.999 {
        let mut by_line = HashMap::<String, Vec<Pt>>::new();
        for p in cleaned {
            by_line.entry(p.line_id.clone()).or_default().push(p);
        }
        let mut reduced = Vec::<Pt>::new();
        let stride = (100.0 / params.decimate_pct).round().max(1.0) as usize;
        for (_line, mut lp) in by_line {
            lp.sort_by(|a, b| {
                a.along_m
                    .partial_cmp(&b.along_m)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            if lp.len() <= 2 || stride <= 1 {
                reduced.extend(lp);
                continue;
            }
            let llen = lp.len();
            for (i, p) in lp.into_iter().enumerate() {
                if i == 0 || i + 1 == llen || i % stride == 0 {
                    reduced.push(p);
                }
            }
        }
        cleaned = reduced;
    }

    let mut xmin = f64::INFINITY;
    let mut xmax = f64::NEG_INFINITY;
    let mut ymin = f64::INFINITY;
    let mut ymax = f64::NEG_INFINITY;
    for p in &cleaned {
        xmin = xmin.min(p.x);
        xmax = xmax.max(p.x);
        ymin = ymin.min(p.y);
        ymax = ymax.max(p.y);
    }
    let pad = params.grid_resolution_m;
    xmin -= pad;
    xmax += pad;
    ymin -= pad;
    ymax += pad;
    let mut effective_res_m = params.grid_resolution_m;
    let mut nx = (((xmax - xmin) / effective_res_m).ceil() as usize).max(4);
    let mut ny = (((ymax - ymin) / effective_res_m).ceil() as usize).max(4);
    let mut cells = nx.saturating_mul(ny);
    if cells > params.max_grid_cells {
        let scale = (cells as f64 / params.max_grid_cells as f64)
            .sqrt()
            .max(1.0);
        effective_res_m = (effective_res_m * scale).max(params.grid_resolution_m);
        nx = (((xmax - xmin) / effective_res_m).ceil() as usize).max(4);
        ny = (((ymax - ymin) / effective_res_m).ceil() as usize).max(4);
        cells = nx.saturating_mul(ny);
    }
    let mut grid_m = vec![None; nx * ny];
    ctx.report_progress(
        "grid_interpolation",
        Some(0.38),
        Some("Interpolating grid".to_string()),
        Some(json!({ "nx": nx, "ny": ny, "cells": cells, "resolution_m": effective_res_m })),
    );
    let row_block = (ny / 24).max(8);
    for y0 in (0..ny).step_by(row_block) {
        let y1 = (y0 + row_block).min(ny);
        let block: Vec<(usize, Vec<Option<f64>>)> = (y0..y1)
            .into_par_iter()
            .map(|iy| {
                let y = ymin + iy as f64 * effective_res_m;
                let mut row = vec![None; nx];
                for (ix, cell) in row.iter_mut().enumerate() {
                    let x = xmin + ix as f64 * effective_res_m;
                    *cell = interp_idw(
                        x,
                        y,
                        &cleaned,
                        params.idw_power,
                        params.search_radius_m,
                        params.max_points,
                    );
                }
                (iy, row)
            })
            .collect();
        for (iy, row) in block {
            let start = iy * nx;
            let end = start + nx;
            grid_m[start..end].copy_from_slice(&row);
        }
        let frac = y1 as f64 / ny as f64;
        ctx.report_progress(
            "grid_interpolation",
            Some(0.38 + 0.40 * frac),
            Some("Interpolating grid".to_string()),
            Some(json!({ "row": y1, "rows": ny })),
        );
    }
    if params.grid_method.eq_ignore_ascii_case("minimum_curvature") {
        for _ in 0..8 {
            let old = grid_m.clone();
            for iy in 1..(ny - 1) {
                for ix in 1..(nx - 1) {
                    let i = iy * nx + ix;
                    let n = old[(iy - 1) * nx + ix];
                    let s = old[(iy + 1) * nx + ix];
                    let w = old[iy * nx + ix - 1];
                    let e = old[iy * nx + ix + 1];
                    if let (Some(n), Some(s), Some(w), Some(e), Some(c)) = (n, s, w, e, old[i]) {
                        grid_m[i] = Some(0.5 * c + 0.125 * (n + s + w + e));
                    }
                }
            }
        }
    }

    let mut grid_fvd = vec![None; nx * ny];
    let mut grid_grad = vec![None; nx * ny];
    let mut grid_tilt = vec![None; nx * ny];
    let h = effective_res_m.max(1e-6);
    for iy in 1..(ny - 1) {
        for ix in 1..(nx - 1) {
            let idx = iy * nx + ix;
            let c = grid_m[idx];
            let xp = grid_m[iy * nx + ix + 1];
            let xm = grid_m[iy * nx + ix - 1];
            let yp = grid_m[(iy + 1) * nx + ix];
            let ym = grid_m[(iy - 1) * nx + ix];
            if let (Some(c), Some(xp), Some(xm), Some(yp), Some(ym)) = (c, xp, xm, yp, ym) {
                let gx = (xp - xm) / (2.0 * h);
                let gy = (yp - ym) / (2.0 * h);
                let d2x = (xp - 2.0 * c + xm) / (h * h);
                let d2y = (yp - 2.0 * c + ym) / (h * h);
                let fvd = -(d2x + d2y);
                let grad = (gx * gx + gy * gy).sqrt();
                let tilt = fvd.atan2(grad.max(1e-9));
                grid_fvd[idx] = Some(fvd);
                grid_grad[idx] = Some(grad);
                grid_tilt[idx] = Some(tilt);
            }
        }
    }
    ctx.report_progress(
        "derivatives",
        Some(0.84),
        Some("Derivatives computed".to_string()),
        None,
    );

    for p in &mut cleaned {
        if let Some(v) = bilinear(nx, ny, xmin, xmax, ymin, ymax, &grid_fvd, p.x, p.y) {
            p.attrs.insert("fvd".into(), json!(v));
        }
        if let Some(v) = bilinear(nx, ny, xmin, xmax, ymin, ymax, &grid_grad, p.x, p.y) {
            p.attrs.insert("grad_mag".into(), json!(v));
        }
        if let Some(v) = bilinear(nx, ny, xmin, xmax, ymin, ymax, &grid_tilt, p.x, p.y) {
            p.attrs.insert("tilt".into(), json!(v));
        }
        p.attrs.insert("M".into(), json!(p.m));
    }

    let mut mm = cleaned.iter().map(|p| p.m).collect::<Vec<_>>();
    let m_min = mm.iter().copied().fold(f64::INFINITY, |a, b| a.min(b));
    let m_max = mm.iter().copied().fold(f64::NEG_INFINITY, |a, b| a.max(b));
    let m_mean = if mm.is_empty() {
        0.0
    } else {
        mm.iter().sum::<f64>() / mm.len() as f64
    };
    let m_med = median(&mut mm).unwrap_or(0.0);

    let points_rows = cleaned
        .iter()
        .map(|p| {
            json!({
                "x": p.x,
                "y": p.y,
                "z": p.z,
                "line_id": p.line_id,
                "timestamp": p.timestamp,
                "fid": p.fid,
                "along_line_m": p.along_m,
                "attributes": p.attrs
            })
        })
        .collect::<Vec<_>>();

    let mut grid_rows = Vec::<Value>::new();
    for iy in 0..ny {
        let y = ymin + iy as f64 * effective_res_m;
        for ix in 0..nx {
            let x = xmin + ix as f64 * effective_res_m;
            let i = iy * nx + ix;
            if let Some(mv) = grid_m[i] {
                grid_rows.push(json!({
                    "x": x, "y": y, "M": mv,
                    "fvd": grid_fvd[i],
                    "grad_mag": grid_grad[i],
                    "tilt": grid_tilt[i]
                }));
            }
        }
    }

    let context = json!({
        "raw_rows": rows.len(),
        "kept_points": cleaned.len(),
        "malformed_dropped": malformed,
        "non_monotonic_dropped": non_monotonic_drop,
        "despike_dropped": despike_drop,
        "decimate_pct": params.decimate_pct,
        "decimated_dropped": before_decimate.saturating_sub(cleaned.len()),
        "line_count": total_lines,
        "coord_mode": if use_projected { "projected_xy" } else { "latlon_to_utm" },
        "projected_rows_seen": projected_total,
        "projected_rows_plausible": projected_plausible,
        "projected_fallback_to_lonlat": forced_fallback_lonlat,
        "target_epsg": target_crs,
        "signal_selected": if tmf_key.is_some() { "tmf_preferred" } else { "mag_lev" },
        "igrf_subtracted": require_igrf_sub,
        "M_stats": { "min": m_min, "max": m_max, "mean": m_mean, "median": m_med },
        "top_rows": rows.iter().take(8).cloned().collect::<Vec<_>>(),
        "tail_rows": rows.iter().rev().take(8).cloned().collect::<Vec<_>>(),
    });
    let llm = if params.llm_enabled {
        llm_commentary(&context).await
    } else {
        None
    };

    let points_payload = json!({
        "type": "magnetic_points",
        "crs": { "epsg": target_crs, "wkt": Value::Null },
        "measure_candidates": ["M", "fvd", "grad_mag", "tilt", "z"],
        "display_contract": {
            "renderer": "sample_points",
            "display_pointer": "scene3d.sample_points",
            "editable": ["visible","opacity","size","measure","palette"]
        },
        "points": points_rows
    });
    let points_preview_rows = preview_point_rows(&points_rows, 12_000);
    let points_preview_payload = json!({
        "type": "magnetic_points",
        "quality": "preview",
        "preview_capped": points_preview_rows.len() < points_rows.len(),
        "crs": { "epsg": target_crs, "wkt": Value::Null },
        "measure_candidates": ["M", "fvd", "grad_mag", "tilt", "z"],
        "display_contract": {
            "renderer": "sample_points",
            "display_pointer": "scene3d.sample_points",
            "editable": ["visible","opacity","size","measure","palette"]
        },
        "points": points_preview_rows
    });
    let grid_payload = json!({
        "schema_id": "grid.magnetic.v1",
        "type": "magnetic_grid_rows",
        "crs": { "epsg": target_crs, "wkt": Value::Null },
        "grid": {
            "nx": nx, "ny": ny,
            "xmin": xmin, "xmax": xmax,
            "ymin": ymin, "ymax": ymax,
            "resolution_m": effective_res_m,
        },
        "rows": grid_rows
    });
    let grid_preview_rows = preview_grid_rows(&grid_rows, 40_000);
    let grid_preview_payload = json!({
        "schema_id": "grid.magnetic.v1",
        "type": "magnetic_grid_rows",
        "quality": "preview",
        "preview_capped": grid_preview_rows.len() < grid_rows.len(),
        "crs": { "epsg": target_crs, "wkt": Value::Null },
        "grid": {
            "nx": nx, "ny": ny,
            "xmin": xmin, "xmax": xmax,
            "ymin": ymin, "ymax": ymax,
            "resolution_m": effective_res_m,
        },
        "rows": grid_preview_rows
    });
    let report_payload = json!({
        "schema_id": "report.magnetic_model.v1",
        "type": "magnetic_model_report",
        "summary": {
            "input_rows": rows.len(),
            "output_points": cleaned.len(),
            "grid_cells_with_values": grid_rows.len(),
            "grid_cells_total": cells,
            "effective_grid_resolution_m": effective_res_m,
            "malformed_dropped": malformed,
            "non_monotonic_dropped": non_monotonic_drop,
            "despike_dropped": despike_drop,
            "decimate_pct": params.decimate_pct,
            "decimated_dropped": before_decimate.saturating_sub(cleaned.len()),
            "line_count": total_lines,
            "coordinate_mode": if use_projected { "projected_xy" } else { "latlon_to_utm" },
            "target_epsg": target_crs,
            "igrf_subtracted": require_igrf_sub,
            "M_min": m_min,
            "M_max": m_max,
            "M_mean": m_mean,
            "M_median": m_med
        },
        "key_parameters": {
            "grid_method": params.grid_method,
            "grid_resolution_m": params.grid_resolution_m,
            "effective_grid_resolution_m": effective_res_m,
            "max_grid_cells": params.max_grid_cells,
            "idw_power": params.idw_power,
            "search_radius_m": params.search_radius_m,
            "max_points": params.max_points,
            "despike_sigma": params.despike_sigma,
            "smooth_window_m": params.smooth_window_m,
            "resample_spacing_m": params.resample_spacing_m
            ,"decimate_pct": params.decimate_pct
        },
        "field_selection": {
            "x": x_key, "y": y_key, "lat": lat_key, "lon": lon_key,
            "line_id": line_key, "utc": time_key, "fid": fid_key,
            "tmf": tmf_key, "mag_lev": mag_lev_key, "igrf": igrf_key,
            "radar": radar_key, "gps_alt": gps_alt_key
        },
        "audit": context,
        "llm_assist": {
            "enabled": params.llm_enabled,
            "provider": "openrouter",
            "model": "openai/gpt-5-mini",
            "result": llm
        }
    });

    let points_bytes = serde_json::to_vec(&points_payload)?;
    let points_preview_bytes = serde_json::to_vec(&points_preview_payload)?;
    let grid_bytes = serde_json::to_vec(&grid_payload)?;
    let grid_preview_bytes = serde_json::to_vec(&grid_preview_payload)?;
    let report_bytes = serde_json::to_vec(&report_payload)?;
    let points_preview_key = format!(
        "graphs/{}/nodes/{}/magnetic_points.preview.json",
        job.graph_id, job.node_id
    );
    let points_key = format!(
        "graphs/{}/nodes/{}/magnetic_points.json",
        job.graph_id, job.node_id
    );
    let grid_preview_key = format!(
        "graphs/{}/nodes/{}/magnetic_grid.preview.json",
        job.graph_id, job.node_id
    );
    let grid_key = format!(
        "graphs/{}/nodes/{}/magnetic_grid.json",
        job.graph_id, job.node_id
    );
    let report_key = format!(
        "graphs/{}/nodes/{}/magnetic_report.json",
        job.graph_id, job.node_id
    );
    let points_preview_ref = super::runtime::write_artifact(
        ctx,
        &points_preview_key,
        &points_preview_bytes,
        Some("application/json"),
    )
    .await?;
    let points_ref =
        super::runtime::write_artifact(ctx, &points_key, &points_bytes, Some("application/json"))
            .await?;
    let grid_preview_ref = super::runtime::write_artifact(
        ctx,
        &grid_preview_key,
        &grid_preview_bytes,
        Some("application/json"),
    )
    .await?;
    let grid_ref =
        super::runtime::write_artifact(ctx, &grid_key, &grid_bytes, Some("application/json"))
            .await?;
    let report_ref =
        super::runtime::write_artifact(ctx, &report_key, &report_bytes, Some("application/json"))
            .await?;
    ctx.report_progress(
        "write_outputs",
        Some(0.97),
        Some("Writing artifacts".to_string()),
        None,
    );

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![
            points_preview_ref.clone(),
            points_ref.clone(),
            grid_preview_ref.clone(),
            grid_ref.clone(),
            report_ref.clone(),
        ],
        content_hashes: vec![
            points_preview_ref.content_hash,
            points_ref.content_hash,
            grid_preview_ref.content_hash,
            grid_ref.content_hash,
            report_ref.content_hash,
        ],
        error_message: None,
    })
}
