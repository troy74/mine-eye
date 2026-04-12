use std::collections::{BTreeSet, HashMap};

use mine_eye_types::{CrsRecord, JobEnvelope, JobResult, JobStatus};
use serde_json::{json, Map, Value};

use crate::crs_transform::transform_xy;
use crate::executor::ExecutionContext;
use crate::NodeError;

fn parse_num(v: Option<&Value>) -> Option<f64> {
    match v {
        Some(Value::Number(n)) => n.as_f64().filter(|x| x.is_finite()),
        Some(Value::String(s)) => s
            .trim()
            .replace(',', ".")
            .parse::<f64>()
            .ok()
            .filter(|x| x.is_finite()),
        _ => None,
    }
}

fn detect_format(filename: &str, media_type: &str, sample: &str) -> String {
    let lower = filename.to_ascii_lowercase();
    if lower.ends_with(".geojson") || media_type.contains("geo+json") {
        return "geojson".to_string();
    }
    if lower.ends_with(".json") || media_type.contains("json") {
        return "json".to_string();
    }
    if lower.ends_with(".tsv") {
        return "tsv".to_string();
    }
    if lower.ends_with(".csv") {
        return "csv".to_string();
    }
    if sample.lines().take(20).all(|l| l.contains('\t')) {
        return "tsv".to_string();
    }
    if sample.lines().take(20).all(|l| l.contains(',')) {
        return "csv".to_string();
    }
    "fixed_width_or_text".to_string()
}

fn infer_delimiter(delim: &str, format: &str) -> u8 {
    if let Some(b) = delim.as_bytes().first().copied() {
        return b;
    }
    if format == "tsv" {
        b'\t'
    } else {
        b','
    }
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

fn pick_header_ci(headers: &[String], candidates: &[&str]) -> Option<String> {
    let lower = headers
        .iter()
        .map(|h| (h.to_ascii_lowercase(), h.clone()))
        .collect::<Vec<_>>();
    for c in candidates {
        let cl = c.to_ascii_lowercase();
        if let Some((_, orig)) = lower.iter().find(|(h, _)| h == &cl) {
            return Some(orig.clone());
        }
    }
    for c in candidates {
        let cl = c.to_ascii_lowercase();
        if let Some((_, orig)) = lower.iter().find(|(h, _)| h.contains(&cl)) {
            return Some(orig.clone());
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

#[derive(Clone)]
struct ObsPt {
    x: f64,
    y: f64,
    z: f64,
    t: Option<String>,
    segment_id: Option<String>,
    attrs: Map<String, Value>,
}

pub async fn run_observation_ingest(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let ui = job
        .output_spec
        .pointer("/node_ui")
        .and_then(|v| v.as_object())
        .ok_or_else(|| NodeError::InvalidConfig("observation_ingest missing node_ui".into()))?;

    let source_key = ui
        .get("csv_artifact_key")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or_else(|| {
            job.input_payload
                .as_ref()
                .and_then(|p| p.get("csv_artifact_key"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
        })
        .ok_or_else(|| {
            NodeError::InvalidConfig("observation_ingest missing csv_artifact_key".into())
        })?;

    let source_hash = ui
        .get("csv_artifact_hash")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| {
            job.input_payload
                .as_ref()
                .and_then(|p| p.get("csv_artifact_hash"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        });

    let filename = ui
        .get("csv_filename")
        .and_then(|v| v.as_str())
        .or_else(|| {
            job.input_payload
                .as_ref()
                .and_then(|p| p.get("csv_filename"))
                .and_then(|v| v.as_str())
        })
        .unwrap_or("uploaded_data")
        .to_string();

    let media_type = ui
        .get("csv_media_type")
        .and_then(|v| v.as_str())
        .or_else(|| {
            job.input_payload
                .as_ref()
                .and_then(|p| p.get("csv_media_type"))
                .and_then(|v| v.as_str())
        })
        .unwrap_or("application/octet-stream")
        .to_string();

    let raw = super::runtime::read_artifact_bytes(ctx, source_key).await?;
    let sample_text = String::from_utf8_lossy(&raw);
    let format = ui
        .get("csv_format")
        .and_then(|v| v.as_str())
        .or_else(|| {
            job.input_payload
                .as_ref()
                .and_then(|p| p.get("csv_format"))
                .and_then(|v| v.as_str())
        })
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| detect_format(&filename, &media_type, &sample_text));

    let delim = ui
        .get("csv_delimiter")
        .and_then(|v| v.as_str())
        .or_else(|| {
            job.input_payload
                .as_ref()
                .and_then(|p| p.get("csv_delimiter"))
                .and_then(|v| v.as_str())
        })
        .unwrap_or(",");
    let delimiter = infer_delimiter(delim, &format);

    let source_crs = if ui
        .get("use_project_crs")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        job.project_crs
            .clone()
            .unwrap_or_else(|| CrsRecord::epsg(4326))
    } else {
        let epsg = parse_num(ui.get("source_crs_epsg")).map(|v| v as i32).unwrap_or(4326);
        CrsRecord::epsg(epsg)
    };

    let mapping = ui
        .get("mapping")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();

    ctx.report_progress(
        "load_input",
        Some(0.08),
        Some("Loading artifact rows".to_string()),
        None,
    );

    let mut headers: Vec<String> = Vec::new();
    let mut rows: Vec<Map<String, Value>> = Vec::new();
    let mut preview_rows: Vec<Vec<String>> = Vec::new();
    let mut tail_rows: Vec<Vec<String>> = Vec::new();

    if format == "json" || format == "geojson" {
        if let Ok(v) = serde_json::from_slice::<Value>(&raw) {
            match v {
                Value::Array(arr) => {
                    for row in arr {
                        if let Some(obj) = row.as_object() {
                            for k in obj.keys() {
                                if !headers.iter().any(|h| h == k) {
                                    headers.push(k.clone());
                                }
                            }
                            rows.push(obj.clone());
                        }
                    }
                }
                Value::Object(obj) => {
                    if let Some(feats) = obj.get("features").and_then(|v| v.as_array()) {
                        for f in feats {
                            let mut r = f
                                .get("properties")
                                .and_then(|p| p.as_object())
                                .cloned()
                                .unwrap_or_default();
                            if let Some(coords) =
                                f.pointer("/geometry/coordinates").and_then(|v| v.as_array())
                            {
                                if coords.len() >= 2 {
                                    r.insert("lon".into(), coords[0].clone());
                                    r.insert("lat".into(), coords[1].clone());
                                }
                                if coords.len() >= 3 {
                                    r.insert("z".into(), coords[2].clone());
                                }
                            }
                            for k in r.keys() {
                                if !headers.iter().any(|h| h == k) {
                                    headers.push(k.clone());
                                }
                            }
                            rows.push(r);
                        }
                    }
                }
                _ => {}
            }
        }
    } else {
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(true)
            .flexible(true)
            .from_reader(std::io::Cursor::new(raw.clone()));
        if let Ok(h) = rdr.headers() {
            headers = h.iter().map(|s| s.to_string()).collect();
        }
        for rec in rdr.records().flatten() {
            let mut obj = Map::new();
            let row = rec.iter().map(|s| s.to_string()).collect::<Vec<_>>();
            if preview_rows.len() < 12 {
                preview_rows.push(row.clone());
            }
            tail_rows.push(row);
            if tail_rows.len() > 6 {
                tail_rows.remove(0);
            }
            for (i, h) in headers.iter().enumerate() {
                if let Some(cell) = rec.get(i) {
                    let raw_cell = cell.trim();
                    if raw_cell.is_empty() {
                        continue;
                    }
                    if let Ok(v) = raw_cell.replace(',', ".").parse::<f64>() {
                        obj.insert(h.clone(), json!(v));
                    } else {
                        obj.insert(h.clone(), json!(raw_cell));
                    }
                }
            }
            if !obj.is_empty() {
                rows.push(obj);
            }
        }
    }

    if headers.is_empty() {
        let mut uniq = BTreeSet::new();
        for line in sample_text.lines().take(30) {
            for token in line.split_whitespace().take(20) {
                if token
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
                {
                    uniq.insert(token.to_string());
                }
            }
        }
        headers = uniq.into_iter().take(30).collect();
    }

    if preview_rows.is_empty() {
        for r in rows.iter().take(10) {
            preview_rows.push(
                headers
                    .iter()
                    .map(|h| r.get(h).map(|v| v.to_string()).unwrap_or_default())
                    .collect::<Vec<_>>(),
            );
        }
        for r in rows.iter().rev().take(6).rev() {
            tail_rows.push(
                headers
                    .iter()
                    .map(|h| r.get(h).map(|v| v.to_string()).unwrap_or_default())
                    .collect::<Vec<_>>(),
            );
        }
    }

    let x_key = mapped_col(&mapping, "x")
        .or_else(|| pick_header_ci(&headers, &["x", "x_wgs84", "easting", "east", "utm_e", "x_m", "fx"]));
    let y_key = mapped_col(&mapping, "y")
        .or_else(|| pick_header_ci(&headers, &["y", "y_wgs84", "northing", "north", "utm_n", "y_m", "fy"]));
    let z_key = mapped_col(&mapping, "z")
        .or_else(|| pick_header_ci(&headers, &["z", "elevation", "alt", "gpsz", "fz"]));
    let lon_key = mapped_col(&mapping, "lon")
        .or_else(|| pick_header_ci(&headers, &["lon", "longitude", "long"]));
    let lat_key = mapped_col(&mapping, "lat")
        .or_else(|| pick_header_ci(&headers, &["lat", "latitude"]));
    let t_key = mapped_col(&mapping, "t")
        .or_else(|| pick_header_ci(&headers, &["utc", "time", "timestamp", "datetime"]));
    let line_key = mapped_col(&mapping, "line_id")
        .or_else(|| pick_header_ci(&headers, &["line", "line_id", "flightline", "group", "segment"]));

    let mut points = Vec::<ObsPt>::new();
    let mut malformed_rows = 0usize;

    let mut projected_ok = 0usize;
    let mut projected_total = 0usize;
    let mut projected_plausible = 0usize;
    let source_epsg = source_crs.epsg.unwrap_or(4326);
    if let (Some(xk), Some(yk)) = (x_key.as_ref(), y_key.as_ref()) {
        for r in &rows {
            if let (Some(x), Some(y)) = (parse_num(lookup_ci(r, xk).as_ref()), parse_num(lookup_ci(r, yk).as_ref())) {
                projected_total += 1;
                let looks_geo = x.abs() <= 180.0 && y.abs() <= 90.0;
                if !looks_geo && x.abs() > 500.0 && y.abs() > 500.0 {
                    projected_ok += 1;
                }
                if projected_xy_plausible_for_epsg(source_epsg, x, y) {
                    projected_plausible += 1;
                }
            }
        }
    }
    let mut use_projected = projected_total > 0 && projected_ok * 2 >= projected_total;

    let mut lon_lat = Vec::<(f64, f64)>::new();
    if let (Some(lok), Some(lak)) = (lon_key.as_ref(), lat_key.as_ref()) {
        for r in &rows {
            if let (Some(lon), Some(lat)) = (parse_num(lookup_ci(r, lok).as_ref()), parse_num(lookup_ci(r, lak).as_ref())) {
                if lon.abs() <= 180.0 && lat.abs() <= 90.0 {
                    lon_lat.push((lon, lat));
                }
            }
        }
    }

    let forced_fallback_lonlat = use_projected
        && !lon_lat.is_empty()
        && projected_total > 0
        && is_utm_epsg(source_epsg)
        && projected_plausible * 2 < projected_total;
    if forced_fallback_lonlat {
        use_projected = false;
    }

    let mut out_epsg = source_crs.epsg.unwrap_or(4326);
    if !use_projected && !lon_lat.is_empty() {
        let mean_lon = lon_lat.iter().map(|x| x.0).sum::<f64>() / lon_lat.len() as f64;
        let mean_lat = lon_lat.iter().map(|x| x.1).sum::<f64>() / lon_lat.len() as f64;
        out_epsg = infer_utm_epsg(mean_lon, mean_lat);
    }
    let to_crs = CrsRecord::epsg(out_epsg);
    let from_wgs84 = CrsRecord::epsg(4326);

    for r in rows.iter() {
        let (x, y) = if use_projected {
            let (Some(xk), Some(yk)) = (x_key.as_ref(), y_key.as_ref()) else {
                malformed_rows += 1;
                continue;
            };
            let (Some(x), Some(y)) = (
                parse_num(lookup_ci(r, xk).as_ref()),
                parse_num(lookup_ci(r, yk).as_ref()),
            ) else {
                malformed_rows += 1;
                continue;
            };
            (x, y)
        } else {
            let (Some(lok), Some(lak)) = (lon_key.as_ref(), lat_key.as_ref()) else {
                malformed_rows += 1;
                continue;
            };
            let (Some(lon), Some(lat)) = (
                parse_num(lookup_ci(r, lok).as_ref()),
                parse_num(lookup_ci(r, lak).as_ref()),
            ) else {
                malformed_rows += 1;
                continue;
            };
            match transform_xy(&from_wgs84, &to_crs, lon, lat) {
                Ok(v) => v,
                Err(_) => {
                    malformed_rows += 1;
                    continue;
                }
            }
        };

        let z = z_key
            .as_ref()
            .and_then(|k| parse_num(lookup_ci(r, k).as_ref()))
            .unwrap_or(0.0);

        let t = t_key
            .as_ref()
            .and_then(|k| lookup_ci(r, k))
            .and_then(|v| match v {
                Value::String(s) => {
                    let ss = s.trim();
                    (!ss.is_empty()).then_some(ss.to_string())
                }
                Value::Number(n) => Some(n.to_string()),
                _ => None,
            });

        let segment_id = line_key
            .as_ref()
            .and_then(|k| lookup_ci(r, k))
            .and_then(|v| match v {
                Value::String(s) => {
                    let ss = s.trim();
                    (!ss.is_empty()).then_some(ss.to_string())
                }
                Value::Number(n) => Some(n.to_string()),
                _ => None,
            });

        points.push(ObsPt {
            x,
            y,
            z,
            t,
            segment_id,
            attrs: r.clone(),
        });
    }

    let mut numeric_fields = BTreeSet::<String>::new();
    for p in &points {
        for (k, v) in &p.attrs {
            if parse_num(Some(v)).is_some() {
                numeric_fields.insert(k.clone());
            }
        }
    }

    ctx.report_progress(
        "build_points",
        Some(0.62),
        Some("Parsed rows and normalized coordinates".to_string()),
        Some(json!({
            "rows": rows.len(),
            "points": points.len(),
            "malformed": malformed_rows,
            "target_epsg": out_epsg,
        })),
    );

    let points_payload = json!({
        "schema_id": "point_set.observation.v1",
        "type": "observation_points",
        "crs": { "epsg": out_epsg, "wkt": Value::Null },
        "measure_candidates": numeric_fields.iter().cloned().collect::<Vec<_>>(),
        "display_contract": {
            "renderer": "sample_points",
            "display_pointer": "scene3d.sample_points",
            "editable": ["visible", "opacity", "size", "measure", "palette"]
        },
        "points": points.iter().map(|p| {
            json!({
                "x": p.x,
                "y": p.y,
                "z": p.z,
                "timestamp": p.t,
                "segment_id": p.segment_id,
                "attributes": p.attrs,
            })
        }).collect::<Vec<_>>()
    });

    let pointer_payload = json!({
        "schema_id": "artifact.tabular.pointer.v1",
        "type": "tabular_pointer",
        "source": {
            "artifact_key": source_key,
            "content_hash": source_hash,
            "filename": filename,
            "media_type": media_type,
            "format": format,
            "delimiter": (delimiter as char).to_string(),
            "size_bytes": raw.len(),
        },
        "source_crs": source_crs,
        "parsed_crs": { "epsg": out_epsg, "wkt": Value::Null },
        "headers": headers,
        "preview_rows": preview_rows,
        "tail_rows": tail_rows,
        "row_count_estimate": rows.len(),
    });

    let mut attrs_top = HashMap::<String, usize>::new();
    for p in &points {
        for k in p.attrs.keys() {
            *attrs_top.entry(k.clone()).or_insert(0) += 1;
        }
    }
    let mut attrs_rank = attrs_top.into_iter().collect::<Vec<_>>();
    attrs_rank.sort_by(|a, b| b.1.cmp(&a.1));

    let report_payload = json!({
        "schema_id": "report.observation_ingest.v1",
        "type": "observation_ingest_report",
        "summary": {
            "input_rows": rows.len(),
            "output_points": points.len(),
            "malformed_rows_dropped": malformed_rows,
            "coord_mode": if use_projected { "projected_xy" } else { "latlon_to_utm" },
            "source_epsg": source_crs.epsg,
            "target_epsg": out_epsg,
            "source_artifact": source_key,
            "projected_rows_seen": projected_total,
            "projected_rows_plausible": projected_plausible,
            "projected_fallback_to_lonlat": forced_fallback_lonlat,
        },
        "mapping": {
            "x": x_key,
            "y": y_key,
            "z": z_key,
            "lon": lon_key,
            "lat": lat_key,
            "t": t_key,
            "line_id": line_key,
        },
        "top_attributes": attrs_rank.into_iter().take(20).map(|(k, c)| json!({"field": k, "count": c})).collect::<Vec<_>>(),
        "notes": [
            "This node is artifact-first and retains source pointer/hash for stale detection.",
            "Use output point_set for spatial/model nodes and table pointer for tabular transform nodes."
        ]
    });

    let points_bytes = serde_json::to_vec(&points_payload)?;
    let pointer_bytes = serde_json::to_vec(&pointer_payload)?;
    let report_bytes = serde_json::to_vec(&report_payload)?;

    let points_key = format!(
        "graphs/{}/nodes/{}/observation_points.json",
        job.graph_id, job.node_id
    );
    let pointer_key = format!(
        "graphs/{}/nodes/{}/observation_table_pointer.json",
        job.graph_id, job.node_id
    );
    let report_key = format!(
        "graphs/{}/nodes/{}/observation_ingest_report.json",
        job.graph_id, job.node_id
    );

    let points_ref =
        super::runtime::write_artifact(ctx, &points_key, &points_bytes, Some("application/json"))
            .await?;
    let pointer_ref =
        super::runtime::write_artifact(ctx, &pointer_key, &pointer_bytes, Some("application/json"))
            .await?;
    let report_ref =
        super::runtime::write_artifact(ctx, &report_key, &report_bytes, Some("application/json"))
            .await?;

    ctx.report_progress(
        "write_outputs",
        Some(0.96),
        Some("Wrote observation artifacts".to_string()),
        Some(json!({
            "points": points.len(),
            "artifacts": 3,
        })),
    );

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![points_ref.clone(), pointer_ref.clone(), report_ref.clone()],
        content_hashes: vec![
            points_ref.content_hash,
            pointer_ref.content_hash,
            report_ref.content_hash,
        ],
        error_message: None,
    })
}
