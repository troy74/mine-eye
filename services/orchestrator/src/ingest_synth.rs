//! Build `input_payload` for ingest jobs from `node.config.params.ui` when the client
//! did not send `input_payloads` (web UI saves CSV mapping + csv_rows/csv_preview_rows).

use mine_eye_types::{
    CollarRecord, CrsRecord, IntervalSampleRecord, NodeRecord, SurveyStationRecord,
};
use serde_json::{json, Map, Value};
use std::path::Path;

pub fn synthesize_input_payload(
    node: &NodeRecord,
    project_crs: Option<&CrsRecord>,
) -> Option<Value> {
    match node.config.kind.as_str() {
        "collar_ingest" => collar_payload_from_ui(node, project_crs),
        "survey_ingest" => survey_payload_from_ui(node),
        "surface_sample_ingest" => surface_sample_payload_from_ui(node, project_crs),
        "assay_ingest" => assay_payload_from_ui(node),
        "lithology_ingest" => lithology_payload_from_ui(node, project_crs),
        "orientation_ingest" => orientation_payload_from_ui(node, project_crs),
        "magnetic_model" => magnetic_payload_from_ui(node, project_crs),
        _ => None,
    }
}

pub async fn synthesize_input_payload_from_artifact(
    node: &NodeRecord,
    project_crs: Option<&CrsRecord>,
    artifact_root: &Path,
) -> Option<Value> {
    if node.config.kind != "magnetic_model" {
        return None;
    }
    let ui = node.config.params.get("ui")?;
    let key = ui.get("csv_artifact_key")?.as_str()?.trim();
    if key.is_empty() {
        return None;
    }
    let raw = tokio::fs::read(artifact_root.join(key)).await.ok()?;
    let delim = ui
        .get("csv_delimiter")
        .and_then(|v| v.as_str())
        .and_then(|s| s.as_bytes().first().copied())
        .unwrap_or(b',');
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(delim)
        .has_headers(true)
        .flexible(true)
        .from_reader(std::io::Cursor::new(raw));
    let headers = rdr
        .headers()
        .ok()
        .map(|h| h.iter().map(|s| s.to_string()).collect::<Vec<_>>())?;

    let use_project = ui
        .get("use_project_crs")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let source_crs = if use_project {
        project_crs
            .cloned()
            .unwrap_or_else(|| CrsRecord::epsg(4326))
    } else {
        let epsg = ui
            .get("source_crs_epsg")
            .and_then(|v| v.as_u64())
            .map(|u| u as i32)
            .unwrap_or(4326);
        CrsRecord::epsg(epsg)
    };

    let mut rows = Vec::<Value>::new();
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
        Some(json!({ "rows": rows, "source_crs": source_crs }))
    }
}

fn rows_from_ui(ui: &serde_json::Value) -> Option<Vec<Vec<String>>> {
    if let Some(v) = ui.get("csv_rows") {
        serde_json::from_value(v.clone()).ok()
    } else {
        serde_json::from_value(ui.get("csv_preview_rows")?.clone()).ok()
    }
}

fn collar_payload_from_ui(node: &NodeRecord, project_crs: Option<&CrsRecord>) -> Option<Value> {
    let ui = node.config.params.get("ui")?;
    let headers: Vec<String> = serde_json::from_value(ui.get("csv_headers")?.clone()).ok()?;
    if headers.is_empty() {
        return None;
    }
    let rows: Vec<Vec<String>> = rows_from_ui(ui)?;
    let mapping = ui.get("mapping")?.as_object()?;
    let hole_col = mapping.get("hole_id")?.as_str()?;
    if hole_col.is_empty() {
        return None;
    }
    let x_col = mapping
        .get("x")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())?;
    let y_col = mapping
        .get("y")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())?;
    let z_col = mapping
        .get("z")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());

    let use_project = ui
        .get("use_project_crs")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let crs = if use_project {
        project_crs.cloned().unwrap_or_else(|| CrsRecord {
            epsg: Some(4326),
            wkt: None,
        })
    } else {
        let epsg = ui
            .get("source_crs_epsg")
            .and_then(|v| v.as_u64())
            .map(|u| u as i32)
            .unwrap_or(4326);
        CrsRecord {
            epsg: Some(epsg),
            wkt: None,
        }
    };

    let mut qa_base: Vec<String> = Vec::new();
    if use_project && project_crs.is_none() {
        qa_base.push("project_crs_missing_at_enqueue_assumed_epsg_4326".into());
    }
    if ui.get("z_is_relative").and_then(|v| v.as_bool()) == Some(true) {
        qa_base.push("z_is_relative".into());
    }
    qa_base.push("from_ui_preview_rows".into());

    let col_idx = |name: &str| headers.iter().position(|h| h == name);
    let hi = col_idx(hole_col)?;
    let xi = col_idx(x_col)?;
    let yi = col_idx(y_col)?;
    let zi = z_col.and_then(col_idx);

    let mut collars = Vec::new();
    for row in rows {
        if hi >= row.len() || xi >= row.len() || yi >= row.len() {
            continue;
        }
        let hole_id = row[hi].trim().to_string();
        if hole_id.is_empty() {
            continue;
        }
        let Some(x) = row[xi].trim().parse::<f64>().ok() else {
            continue;
        };
        let Some(y) = row[yi].trim().parse::<f64>().ok() else {
            continue;
        };
        let z = match zi {
            Some(i) if i < row.len() => row[i].trim().parse::<f64>().unwrap_or(0.0),
            _ => 0.0,
        };
        collars.push(CollarRecord {
            hole_id,
            x,
            y,
            z,
            crs: crs.clone(),
            qa_flags: qa_base.clone(),
        });
    }

    if collars.is_empty() {
        None
    } else {
        Some(json!({ "collars": collars }))
    }
}

fn survey_payload_from_ui(node: &NodeRecord) -> Option<Value> {
    let ui = node.config.params.get("ui")?;
    let headers: Vec<String> = serde_json::from_value(ui.get("csv_headers")?.clone()).ok()?;
    if headers.is_empty() {
        return None;
    }
    let rows: Vec<Vec<String>> = rows_from_ui(ui)?;
    let mapping = ui.get("mapping")?.as_object()?;
    let hole_col = mapping.get("hole_id")?.as_str()?;
    if hole_col.is_empty() {
        return None;
    }
    let az_col = mapping
        .get("azimuth_deg")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())?;
    let dip_col = mapping
        .get("dip_deg")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())?;
    let depth_col = mapping
        .get("depth_or_length_m")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())?;
    let seg_col = mapping
        .get("segment_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());

    let col_idx = |name: &str| headers.iter().position(|h| h == name);
    let hi = col_idx(hole_col)?;
    let ai = col_idx(az_col)?;
    let di = col_idx(dip_col)?;
    let li = col_idx(depth_col)?;
    let si = seg_col.and_then(col_idx);

    let mut surveys = Vec::new();
    for row in rows {
        if hi >= row.len() || ai >= row.len() || di >= row.len() || li >= row.len() {
            continue;
        }
        let hole_id = row[hi].trim().to_string();
        if hole_id.is_empty() {
            continue;
        }
        let Some(azimuth_deg) = row[ai].trim().parse::<f64>().ok() else {
            continue;
        };
        let Some(dip_deg) = row[di].trim().parse::<f64>().ok() else {
            continue;
        };
        let Some(depth_m) = row[li].trim().parse::<f64>().ok() else {
            continue;
        };
        let mut qa_flags = vec!["from_ui_preview_rows".to_string()];
        if let Some(si) = si {
            if si < row.len() {
                let s = row[si].trim();
                if !s.is_empty() {
                    qa_flags.push(format!("segment_id:{s}"));
                }
            }
        }
        surveys.push(SurveyStationRecord {
            hole_id,
            depth_m,
            azimuth_deg,
            dip_deg,
            qa_flags,
        });
    }

    if surveys.is_empty() {
        None
    } else {
        Some(json!({ "surveys": surveys }))
    }
}

fn assay_payload_from_ui(node: &NodeRecord) -> Option<Value> {
    let ui = node.config.params.get("ui")?;
    let headers: Vec<String> = serde_json::from_value(ui.get("csv_headers")?.clone()).ok()?;
    if headers.is_empty() {
        return None;
    }
    let rows: Vec<Vec<String>> = rows_from_ui(ui)?;
    let mapping = ui.get("mapping")?.as_object()?;
    let hole_col = mapping.get("hole_id")?.as_str()?;
    if hole_col.is_empty() {
        return None;
    }
    let from_col = mapping
        .get("from_m")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())?;
    let to_col = mapping
        .get("to_m")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())?;

    let col_idx = |name: &str| headers.iter().position(|h| h == name);
    let hi = col_idx(hole_col)?;
    let fi = col_idx(from_col)?;
    let ti = col_idx(to_col)?;
    let required_cols = [hole_col, from_col, to_col];

    let mut assays = Vec::new();
    for row in rows {
        if hi >= row.len() || fi >= row.len() || ti >= row.len() {
            continue;
        }
        let hole_id = row[hi].trim().to_string();
        if hole_id.is_empty() {
            continue;
        }
        let Some(from_m) = row[fi].trim().parse::<f64>().ok() else {
            continue;
        };
        let Some(to_m) = row[ti].trim().parse::<f64>().ok() else {
            continue;
        };
        let mut attrs = serde_json::Map::new();
        for (idx, h) in headers.iter().enumerate() {
            if required_cols.contains(&h.as_str()) || idx >= row.len() {
                continue;
            }
            let raw = row[idx].trim();
            if raw.is_empty() {
                continue;
            }
            if let Ok(v) = raw.parse::<f64>() {
                attrs.insert(h.clone(), json!(v));
            } else if let Ok(v) = raw.parse::<i64>() {
                attrs.insert(h.clone(), json!(v));
            } else {
                attrs.insert(h.clone(), json!(raw));
            }
        }
        assays.push(IntervalSampleRecord {
            hole_id,
            from_m,
            to_m,
            attributes: Value::Object(attrs),
            qa_flags: vec!["from_ui_preview_rows".into()],
        });
    }

    if assays.is_empty() {
        None
    } else {
        Some(json!({ "assays": assays }))
    }
}

fn lithology_payload_from_ui(node: &NodeRecord, project_crs: Option<&CrsRecord>) -> Option<Value> {
    let ui = node.config.params.get("ui")?;
    let headers: Vec<String> = serde_json::from_value(ui.get("csv_headers")?.clone()).ok()?;
    if headers.is_empty() {
        return None;
    }
    let rows: Vec<Vec<String>> = rows_from_ui(ui)?;
    let mapping = ui.get("mapping")?.as_object()?;
    let hole_col = mapping.get("hole_id")?.as_str()?;
    let from_col = mapping.get("from_m")?.as_str()?;
    let to_col = mapping.get("to_m")?.as_str()?;
    let formation_col = mapping.get("formation")?.as_str()?;
    if hole_col.is_empty() || from_col.is_empty() || to_col.is_empty() || formation_col.is_empty() {
        return None;
    }

    let use_project = ui
        .get("use_project_crs")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let source_crs = if use_project {
        project_crs
            .cloned()
            .unwrap_or_else(|| CrsRecord::epsg(4326))
    } else {
        let epsg = ui
            .get("source_crs_epsg")
            .and_then(|v| v.as_u64())
            .map(|u| u as i32)
            .unwrap_or(4326);
        CrsRecord::epsg(epsg)
    };

    let col_idx = |name: &str| headers.iter().position(|h| h == name);
    let hi = col_idx(hole_col)?;
    let fi = col_idx(from_col)?;
    let ti = col_idx(to_col)?;
    let gfi = col_idx(formation_col)?;
    let group_idx = mapping
        .get("group")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .and_then(col_idx);
    let code_idx = mapping
        .get("lithology_code")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .and_then(col_idx);

    let required_cols = [hole_col, from_col, to_col, formation_col];
    let mut intervals = Vec::new();
    for row in rows {
        if hi >= row.len() || fi >= row.len() || ti >= row.len() || gfi >= row.len() {
            continue;
        }
        let hole_id = row[hi].trim().to_string();
        let formation = row[gfi].trim().to_string();
        if hole_id.is_empty() || formation.is_empty() {
            continue;
        }
        let Some(from_m) = row[fi].trim().parse::<f64>().ok() else {
            continue;
        };
        let Some(to_m) = row[ti].trim().parse::<f64>().ok() else {
            continue;
        };

        let mut attrs = serde_json::Map::new();
        attrs.insert("formation".into(), json!(formation));
        if let Some(idx) = group_idx {
            if idx < row.len() && !row[idx].trim().is_empty() {
                attrs.insert("group".into(), json!(row[idx].trim()));
            }
        }
        if let Some(idx) = code_idx {
            if idx < row.len() && !row[idx].trim().is_empty() {
                attrs.insert("lithology_code".into(), json!(row[idx].trim()));
            }
        }
        for (idx, h) in headers.iter().enumerate() {
            if idx >= row.len()
                || required_cols.contains(&h.as_str())
                || Some(idx) == group_idx
                || Some(idx) == code_idx
            {
                continue;
            }
            let raw = row[idx].trim();
            if raw.is_empty() {
                continue;
            }
            if let Ok(v) = raw.parse::<f64>() {
                attrs.insert(h.clone(), json!(v));
            } else if let Ok(v) = raw.parse::<i64>() {
                attrs.insert(h.clone(), json!(v));
            } else {
                attrs.insert(h.clone(), json!(raw));
            }
        }

        intervals.push(IntervalSampleRecord {
            hole_id,
            from_m,
            to_m,
            attributes: Value::Object(attrs),
            qa_flags: vec!["from_ui_preview_rows".into()],
        });
    }

    if intervals.is_empty() {
        None
    } else {
        Some(json!({ "intervals": intervals, "source_crs": source_crs }))
    }
}

fn orientation_payload_from_ui(
    node: &NodeRecord,
    project_crs: Option<&CrsRecord>,
) -> Option<Value> {
    let ui = node.config.params.get("ui")?;
    let headers: Vec<String> = serde_json::from_value(ui.get("csv_headers")?.clone()).ok()?;
    if headers.is_empty() {
        return None;
    }
    let rows: Vec<Vec<String>> = rows_from_ui(ui)?;
    let mapping = ui.get("mapping")?.as_object()?;
    let formation_col = mapping.get("formation")?.as_str()?;
    let x_col = mapping.get("x")?.as_str()?;
    let y_col = mapping.get("y")?.as_str()?;
    let z_col = mapping.get("z")?.as_str()?;
    if formation_col.is_empty() || x_col.is_empty() || y_col.is_empty() || z_col.is_empty() {
        return None;
    }

    let use_project = ui
        .get("use_project_crs")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let source_crs = if use_project {
        project_crs
            .cloned()
            .unwrap_or_else(|| CrsRecord::epsg(4326))
    } else {
        let epsg = ui
            .get("source_crs_epsg")
            .and_then(|v| v.as_u64())
            .map(|u| u as i32)
            .unwrap_or(4326);
        CrsRecord::epsg(epsg)
    };

    let col_idx = |name: &str| headers.iter().position(|h| h == name);
    let fi = col_idx(formation_col)?;
    let xi = col_idx(x_col)?;
    let yi = col_idx(y_col)?;
    let zi = col_idx(z_col)?;
    let dip_idx = mapping
        .get("dip_deg")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .and_then(col_idx);
    let az_idx = mapping
        .get("azimuth_deg")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .and_then(col_idx);
    let pvx_idx = mapping
        .get("pole_x")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .and_then(col_idx);
    let pvy_idx = mapping
        .get("pole_y")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .and_then(col_idx);
    let pvz_idx = mapping
        .get("pole_z")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .and_then(col_idx);

    let mut out_rows = Vec::<Value>::new();
    for row in rows {
        if fi >= row.len() || xi >= row.len() || yi >= row.len() || zi >= row.len() {
            continue;
        }
        let formation = row[fi].trim().to_string();
        if formation.is_empty() {
            continue;
        }
        let (Some(x), Some(y), Some(z)) = (
            row[xi].trim().parse::<f64>().ok(),
            row[yi].trim().parse::<f64>().ok(),
            row[zi].trim().parse::<f64>().ok(),
        ) else {
            continue;
        };
        let dip_deg = dip_idx
            .filter(|i| *i < row.len())
            .and_then(|i| row[i].trim().parse::<f64>().ok());
        let azimuth_deg = az_idx
            .filter(|i| *i < row.len())
            .and_then(|i| row[i].trim().parse::<f64>().ok());
        let pole_vector = match (pvx_idx, pvy_idx, pvz_idx) {
            (Some(ix), Some(iy), Some(iz))
                if ix < row.len() && iy < row.len() && iz < row.len() =>
            {
                match (
                    row[ix].trim().parse::<f64>().ok(),
                    row[iy].trim().parse::<f64>().ok(),
                    row[iz].trim().parse::<f64>().ok(),
                ) {
                    (Some(px), Some(py), Some(pz)) => Some(json!([px, py, pz])),
                    _ => None,
                }
            }
            _ => None,
        };
        let mut obj = Map::new();
        obj.insert("formation".into(), json!(formation));
        obj.insert("x".into(), json!(x));
        obj.insert("y".into(), json!(y));
        obj.insert("z".into(), json!(z));
        if let Some(v) = dip_deg {
            obj.insert("dip_deg".into(), json!(v));
        }
        if let Some(v) = azimuth_deg {
            obj.insert("azimuth_deg".into(), json!(v));
        }
        if let Some(v) = pole_vector {
            obj.insert("pole_vector".into(), v);
        }
        obj.insert("source_kind".into(), json!("observed"));
        obj.insert("confidence".into(), json!(1.0));
        out_rows.push(Value::Object(obj));
    }

    if out_rows.is_empty() {
        None
    } else {
        Some(json!({ "rows": out_rows, "source_crs": source_crs }))
    }
}

fn surface_sample_payload_from_ui(
    node: &NodeRecord,
    project_crs: Option<&CrsRecord>,
) -> Option<Value> {
    let ui = node.config.params.get("ui")?;
    let headers: Vec<String> = serde_json::from_value(ui.get("csv_headers")?.clone()).ok()?;
    if headers.is_empty() {
        return None;
    }
    let rows: Vec<Vec<String>> = rows_from_ui(ui)?;
    let mapping = ui.get("mapping")?.as_object()?;
    let x_col = mapping
        .get("x")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())?;
    let y_col = mapping
        .get("y")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())?;
    let id_col = mapping
        .get("sample_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    let z_col = mapping
        .get("z")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());

    let use_project = ui
        .get("use_project_crs")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let crs = if use_project {
        project_crs.cloned().unwrap_or_else(|| CrsRecord {
            epsg: Some(4326),
            wkt: None,
        })
    } else {
        let epsg = ui
            .get("source_crs_epsg")
            .and_then(|v| v.as_u64())
            .map(|u| u as i32)
            .unwrap_or(4326);
        CrsRecord {
            epsg: Some(epsg),
            wkt: None,
        }
    };

    let col_idx = |name: &str| headers.iter().position(|h| h == name);
    let xi = col_idx(x_col)?;
    let yi = col_idx(y_col)?;
    let ii = id_col.and_then(col_idx);
    let zi = z_col.and_then(col_idx);

    let mut points = Vec::new();
    for (row_idx, row) in rows.iter().enumerate() {
        if xi >= row.len() || yi >= row.len() {
            continue;
        }
        let Some(x) = row[xi].trim().parse::<f64>().ok() else {
            continue;
        };
        let Some(y) = row[yi].trim().parse::<f64>().ok() else {
            continue;
        };

        let id = match ii {
            Some(i) if i < row.len() => {
                let v = row[i].trim();
                if v.is_empty() {
                    format!("S{}", row_idx + 1)
                } else {
                    v.to_string()
                }
            }
            _ => format!("S{}", row_idx + 1),
        };

        let mut attrs = serde_json::Map::new();
        for (idx, h) in headers.iter().enumerate() {
            if idx >= row.len() {
                continue;
            }
            if Some(idx) == ii || Some(idx) == zi || idx == xi || idx == yi {
                continue;
            }
            let raw = row[idx].trim();
            if raw.is_empty() {
                continue;
            }
            if let Ok(v) = raw.parse::<f64>() {
                attrs.insert(h.clone(), json!(v));
            } else if let Ok(v) = raw.parse::<i64>() {
                attrs.insert(h.clone(), json!(v));
            } else {
                attrs.insert(h.clone(), json!(raw));
            }
        }

        let z = match zi {
            Some(i) if i < row.len() => row[i].trim().parse::<f64>().ok(),
            _ => None,
        };

        points.push(json!({
            "id": id,
            "x": x,
            "y": y,
            "z": z,
            "crs": crs,
            "attributes": attrs,
            "qa_flags": ["from_ui_preview_rows"]
        }));
    }

    if points.is_empty() {
        None
    } else {
        Some(json!({ "points": points }))
    }
}

fn magnetic_payload_from_ui(node: &NodeRecord, project_crs: Option<&CrsRecord>) -> Option<Value> {
    let ui = node.config.params.get("ui")?;
    let headers: Vec<String> = serde_json::from_value(ui.get("csv_headers")?.clone()).ok()?;
    if headers.is_empty() {
        return None;
    }
    let rows: Vec<Vec<String>> = rows_from_ui(ui)?;
    let use_project = ui
        .get("use_project_crs")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let source_crs = if use_project {
        project_crs
            .cloned()
            .unwrap_or_else(|| CrsRecord::epsg(4326))
    } else {
        let epsg = ui
            .get("source_crs_epsg")
            .and_then(|v| v.as_u64())
            .map(|u| u as i32)
            .unwrap_or(4326);
        CrsRecord::epsg(epsg)
    };
    let mut out_rows = Vec::<Value>::new();
    for row in rows {
        let mut obj = Map::new();
        for (i, h) in headers.iter().enumerate() {
            if i >= row.len() {
                continue;
            }
            let raw = row[i].trim();
            if raw.is_empty() {
                continue;
            }
            if let Ok(v) = raw.replace(',', ".").parse::<f64>() {
                obj.insert(h.clone(), json!(v));
            } else {
                obj.insert(h.clone(), json!(raw));
            }
        }
        if !obj.is_empty() {
            out_rows.push(Value::Object(obj));
        }
    }
    if out_rows.is_empty() {
        None
    } else {
        Some(json!({ "rows": out_rows, "source_crs": source_crs }))
    }
}
