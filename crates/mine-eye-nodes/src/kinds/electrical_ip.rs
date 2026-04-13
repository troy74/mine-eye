use std::collections::{BTreeSet, HashMap, HashSet};

use mine_eye_types::{
    CrsRecord, IpElectrodeRecord, IpMeasurementRecord, JobEnvelope, JobResult, JobStatus,
};
use serde_json::{json, Map, Value};

use crate::executor::ExecutionContext;
use crate::NodeError;

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

fn rows_from_csv_bytes(bytes: &[u8], delimiter: u8) -> Option<Vec<Map<String, Value>>> {
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
            let Some(cell) = rec.get(i) else {
                continue;
            };
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
        if !obj.is_empty() {
            rows.push(obj);
        }
    }
    if rows.is_empty() {
        None
    } else {
        Some(rows)
    }
}

fn delimiter_from_ui(job: &JobEnvelope) -> u8 {
    if let Some(payload) = job.input_payload.as_ref() {
        if let Some(delim) = payload
            .get("csv_delimiter")
            .and_then(|v| v.as_str())
            .and_then(|s| s.as_bytes().first().copied())
        {
            return delim;
        }
    }
    job.output_spec
        .pointer("/node_ui/csv_delimiter")
        .and_then(|v| v.as_str())
        .and_then(|s| s.as_bytes().first().copied())
        .unwrap_or(b',')
}

async fn ingest_rows_from_job(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<(Vec<Map<String, Value>>, Option<CrsRecord>, String), NodeError> {
    if let Some(payload) = job.input_payload.as_ref() {
        if let Some(rows) = payload.get("rows").and_then(|v| v.as_array()) {
            let out = rows
                .iter()
                .filter_map(|row| row.as_object().cloned())
                .collect::<Vec<_>>();
            if !out.is_empty() {
                let crs = payload
                    .get("crs")
                    .cloned()
                    .and_then(|v| serde_json::from_value::<CrsRecord>(v).ok())
                    .or_else(|| job.project_crs.clone());
                return Ok((out, crs, "input_payload.rows".to_string()));
            }
        }
        if let Some(source_key) = payload
            .get("csv_artifact_key")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let raw = super::runtime::read_artifact_bytes(ctx, source_key).await?;
            let delimiter = delimiter_from_ui(job);
            let rows = rows_from_csv_bytes(&raw, delimiter).ok_or_else(|| {
                NodeError::InvalidConfig(
                    "ip_survey_ingest could not parse CSV rows from input payload artifact".into(),
                )
            })?;
            let crs = payload
                .get("crs")
                .cloned()
                .and_then(|v| serde_json::from_value::<CrsRecord>(v).ok())
                .or_else(|| job.project_crs.clone());
            return Ok((rows, crs, source_key.to_string()));
        }
    }

    let source_key = job
        .output_spec
        .pointer("/node_ui/csv_artifact_key")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            NodeError::InvalidConfig(
                "ip_survey_ingest requires input_payload.rows or node_ui.csv_artifact_key".into(),
            )
        })?;
    let raw = super::runtime::read_artifact_bytes(ctx, source_key).await?;
    let delimiter = delimiter_from_ui(job);
    let rows = rows_from_csv_bytes(&raw, delimiter).ok_or_else(|| {
        NodeError::InvalidConfig("ip_survey_ingest could not parse upstream CSV rows".into())
    })?;
    Ok((rows, job.project_crs.clone(), source_key.to_string()))
}

fn str_field(row: &Map<String, Value>, key: &str) -> Option<String> {
    row.get(key)
        .and_then(|v| match v {
            Value::String(s) => Some(s.trim().to_string()),
            Value::Number(n) => Some(n.to_string()),
            _ => None,
        })
        .filter(|s| !s.is_empty())
}

fn num_field(row: &Map<String, Value>, key: &str) -> Option<f64> {
    row.get(key).and_then(parse_num)
}

fn collect_extra_attrs(row: &Map<String, Value>, reserved: &[&str]) -> Value {
    let reserved = reserved
        .iter()
        .map(|s| s.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let mut out = Map::new();
    for (k, v) in row {
        if reserved.contains(&k.to_ascii_lowercase()) {
            continue;
        }
        out.insert(k.clone(), v.clone());
    }
    Value::Object(out)
}

fn build_contract_rows(measurements: &[IpMeasurementRecord]) -> Vec<Value> {
    measurements
        .iter()
        .map(|m| {
            let mut obj = Map::new();
            obj.insert("measurement_id".into(), json!(m.measurement_id));
            obj.insert("line_id".into(), json!(m.line_id));
            obj.insert("survey_mode".into(), json!(m.survey_mode));
            obj.insert("array_type".into(), json!(m.array_type));
            obj.insert("a_id".into(), json!(m.a_id));
            obj.insert("b_id".into(), json!(m.b_id));
            obj.insert("m_id".into(), json!(m.m_id));
            obj.insert("n_id".into(), json!(m.n_id));
            obj.insert("current_ma".into(), json!(m.current_ma));
            obj.insert("voltage_mv".into(), json!(m.voltage_mv));
            obj.insert(
                "apparent_resistivity_ohm_m".into(),
                json!(m.apparent_resistivity_ohm_m),
            );
            obj.insert("chargeability_mv_v".into(), json!(m.chargeability_mv_v));
            obj.insert("gate_start_ms".into(), json!(m.gate_start_ms));
            obj.insert("gate_end_ms".into(), json!(m.gate_end_ms));
            obj.insert("stack_count".into(), json!(m.stack_count));
            obj.insert(
                "reciprocity_error_pct".into(),
                json!(m.reciprocity_error_pct),
            );
            obj.insert("qa_flags".into(), json!(m.qa_flags));
            if let Some(attrs) = m.attributes.as_object() {
                for (k, v) in attrs {
                    obj.insert(k.clone(), v.clone());
                }
            }
            Value::Object(obj)
        })
        .collect()
}

fn table_columns(rows: &[Value]) -> Vec<String> {
    let mut cols = BTreeSet::new();
    for row in rows {
        if let Some(obj) = row.as_object() {
            for k in obj.keys() {
                cols.insert(k.clone());
            }
        }
    }
    cols.into_iter().collect()
}

fn min_max(values: &[f64]) -> (Option<f64>, Option<f64>) {
    if values.is_empty() {
        (None, None)
    } else {
        let mut lo = f64::INFINITY;
        let mut hi = f64::NEG_INFINITY;
        for v in values {
            lo = lo.min(*v);
            hi = hi.max(*v);
        }
        (Some(lo), Some(hi))
    }
}

fn points_payload(
    electrodes: &[IpElectrodeRecord],
    crs: &CrsRecord,
    numeric_fields: &[&str],
) -> Value {
    json!({
        "schema_id": "point_set.ip_electrodes.v1",
        "type": "ip_electrodes",
        "crs": crs,
        "measure_candidates": numeric_fields,
        "display_contract": {
            "renderer": "sample_points",
            "display_pointer": "scene3d.sample_points",
            "editable": ["visible", "opacity", "size", "measure", "palette"]
        },
        "points": electrodes.iter().map(|e| {
            json!({
                "x": e.x,
                "y": e.y,
                "z": e.z,
                "segment_id": e.line_id,
                "attributes": {
                    "electrode_id": e.electrode_id,
                    "line_id": e.line_id,
                    "qa_flags": e.qa_flags,
                }
            })
        }).collect::<Vec<_>>()
    })
}

fn electrode_lookup(electrodes: &[IpElectrodeRecord]) -> HashMap<String, IpElectrodeRecord> {
    electrodes
        .iter()
        .cloned()
        .map(|e| (e.electrode_id.clone(), e))
        .collect()
}

fn parse_inline_rows(
    rows: Vec<Map<String, Value>>,
    fallback_crs: Option<CrsRecord>,
) -> Result<(Vec<IpElectrodeRecord>, Vec<IpMeasurementRecord>, CrsRecord), NodeError> {
    let crs = fallback_crs.unwrap_or_else(|| CrsRecord::epsg(32630));
    let mut electrode_map = HashMap::<String, IpElectrodeRecord>::new();
    let mut measurements = Vec::<IpMeasurementRecord>::new();

    for (idx, row) in rows.iter().enumerate() {
        let measurement_id =
            str_field(row, "measurement_id").unwrap_or_else(|| format!("ipm_{:04}", idx + 1));
        let survey_mode = str_field(row, "survey_mode").unwrap_or_else(|| "tdip".to_string());
        let array_type =
            str_field(row, "array_type").unwrap_or_else(|| "dipole_dipole".to_string());
        let line_id = str_field(row, "line_id");

        let mut get_electrode = |prefix: &str| -> Result<String, NodeError> {
            let id_key = format!("{}_id", prefix);
            let x_key = format!("{}_x", prefix);
            let y_key = format!("{}_y", prefix);
            let z_key = format!("{}_z", prefix);
            let electrode_id = str_field(row, &id_key).ok_or_else(|| {
                NodeError::InvalidConfig(format!(
                    "ip_survey_ingest row '{}' missing {}",
                    measurement_id, id_key
                ))
            })?;
            let x = num_field(row, &x_key).ok_or_else(|| {
                NodeError::InvalidConfig(format!(
                    "ip_survey_ingest row '{}' missing {}",
                    measurement_id, x_key
                ))
            })?;
            let y = num_field(row, &y_key).ok_or_else(|| {
                NodeError::InvalidConfig(format!(
                    "ip_survey_ingest row '{}' missing {}",
                    measurement_id, y_key
                ))
            })?;
            let z = num_field(row, &z_key).unwrap_or(0.0);
            electrode_map
                .entry(electrode_id.clone())
                .or_insert_with(|| IpElectrodeRecord {
                    electrode_id: electrode_id.clone(),
                    line_id: line_id.clone(),
                    x,
                    y,
                    z,
                    crs: crs.clone(),
                    qa_flags: Vec::new(),
                });
            Ok(electrode_id)
        };

        let a_id = get_electrode("a")?;
        let b_id = get_electrode("b")?;
        let m_id = get_electrode("m")?;
        let n_id = get_electrode("n")?;

        let current_ma = num_field(row, "current_ma").ok_or_else(|| {
            NodeError::InvalidConfig(format!(
                "ip_survey_ingest row '{}' missing current_ma",
                measurement_id
            ))
        })?;
        let voltage_mv = num_field(row, "voltage_mv").ok_or_else(|| {
            NodeError::InvalidConfig(format!(
                "ip_survey_ingest row '{}' missing voltage_mv",
                measurement_id
            ))
        })?;
        let apparent_resistivity_ohm_m =
            num_field(row, "apparent_resistivity_ohm_m").ok_or_else(|| {
                NodeError::InvalidConfig(format!(
                    "ip_survey_ingest row '{}' missing apparent_resistivity_ohm_m",
                    measurement_id
                ))
            })?;
        let chargeability_mv_v = num_field(row, "chargeability_mv_v").ok_or_else(|| {
            NodeError::InvalidConfig(format!(
                "ip_survey_ingest row '{}' missing chargeability_mv_v",
                measurement_id
            ))
        })?;

        let reserved = [
            "measurement_id",
            "line_id",
            "survey_mode",
            "array_type",
            "a_id",
            "a_x",
            "a_y",
            "a_z",
            "b_id",
            "b_x",
            "b_y",
            "b_z",
            "m_id",
            "m_x",
            "m_y",
            "m_z",
            "n_id",
            "n_x",
            "n_y",
            "n_z",
            "current_ma",
            "voltage_mv",
            "apparent_resistivity_ohm_m",
            "chargeability_mv_v",
            "gate_start_ms",
            "gate_end_ms",
            "stack_count",
            "reciprocity_error_pct",
        ];

        measurements.push(IpMeasurementRecord {
            measurement_id,
            line_id,
            survey_mode,
            array_type,
            a_id,
            b_id,
            m_id,
            n_id,
            current_ma,
            voltage_mv,
            apparent_resistivity_ohm_m,
            chargeability_mv_v,
            gate_start_ms: num_field(row, "gate_start_ms"),
            gate_end_ms: num_field(row, "gate_end_ms"),
            stack_count: num_field(row, "stack_count").map(|v| v.round().max(0.0) as u32),
            reciprocity_error_pct: num_field(row, "reciprocity_error_pct"),
            qa_flags: Vec::new(),
            attributes: collect_extra_attrs(row, &reserved),
        });
    }

    let mut electrodes = electrode_map.into_values().collect::<Vec<_>>();
    electrodes.sort_by(|a, b| a.electrode_id.cmp(&b.electrode_id));
    Ok((electrodes, measurements, crs))
}

pub async fn run_ip_survey_ingest(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let (rows, fallback_crs, source_label) = ingest_rows_from_job(ctx, job).await?;
    ctx.report_progress(
        "parse_rows",
        Some(0.18),
        Some("Parsing TDIP survey rows".to_string()),
        Some(json!({ "rows": rows.len() })),
    );

    let (electrodes, measurements, crs) = parse_inline_rows(rows, fallback_crs)?;
    let contract_rows = build_contract_rows(&measurements);
    let contract_columns = table_columns(&contract_rows);
    let survey_payload = json!({
        "schema_id": "geophysics.ip_survey.v1",
        "schema_version": 1,
        "type": "ip_survey",
        "survey_mode": "tdip",
        "crs": crs,
        "display_contract": {
            "renderer": "table",
            "display_pointer": "table.rows",
            "editable": ["visible"]
        },
        "measure_candidates": [
            "apparent_resistivity_ohm_m",
            "chargeability_mv_v",
            "current_ma",
            "voltage_mv",
            "reciprocity_error_pct"
        ],
        "electrodes": electrodes,
        "measurements": measurements,
        "rows": contract_rows,
        "meta": {
            "row_count": measurements.len(),
            "electrode_count": electrodes.len(),
            "columns": contract_columns,
            "source": source_label
        }
    });

    let electrode_payload = points_payload(&electrodes, &crs, &["z"]);
    let report_payload = json!({
        "schema_id": "report.ip_survey_ingest.v1",
        "type": "ip_survey_ingest_report",
        "summary": {
            "survey_mode": "tdip",
            "measurement_count": measurements.len(),
            "electrode_count": electrodes.len(),
            "line_count": electrodes.iter().filter_map(|e| e.line_id.clone()).collect::<BTreeSet<_>>().len(),
            "source": source_label
        },
        "notes": [
            "Canonical IP survey contract keeps electrode geometry separate from measurement rows.",
            "Use the QC node before pseudosection or inversion so reciprocity and malformed rows are surfaced explicitly."
        ]
    });

    let survey_key = format!(
        "graphs/{}/nodes/{}/ip_survey.json",
        job.graph_id, job.node_id
    );
    let points_key = format!(
        "graphs/{}/nodes/{}/ip_electrodes.json",
        job.graph_id, job.node_id
    );
    let report_key = format!(
        "graphs/{}/nodes/{}/ip_survey_ingest_report.json",
        job.graph_id, job.node_id
    );

    let survey_ref = super::runtime::write_artifact(
        ctx,
        &survey_key,
        &serde_json::to_vec(&survey_payload)?,
        Some("application/json"),
    )
    .await?;
    let points_ref = super::runtime::write_artifact(
        ctx,
        &points_key,
        &serde_json::to_vec(&electrode_payload)?,
        Some("application/json"),
    )
    .await?;
    let report_ref = super::runtime::write_artifact(
        ctx,
        &report_key,
        &serde_json::to_vec(&report_payload)?,
        Some("application/json"),
    )
    .await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![survey_ref.clone(), points_ref.clone(), report_ref.clone()],
        content_hashes: vec![
            survey_ref.content_hash,
            points_ref.content_hash,
            report_ref.content_hash,
        ],
        error_message: None,
    })
}

fn qc_params(job: &JobEnvelope) -> (f64, f64, f64) {
    let num = |p: &str, default: f64| {
        job.output_spec
            .pointer(p)
            .and_then(parse_num)
            .unwrap_or(default)
    };
    (
        num("/node_ui/max_reciprocity_error_pct", 15.0).max(0.0),
        num("/node_ui/min_chargeability_mv_v", -1.0),
        num("/node_ui/max_chargeability_mv_v", 80.0).max(1.0),
    )
}

pub async fn run_ip_qc_normalize(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let mut survey = None;
    for ar in &job.input_artifact_refs {
        let v = super::runtime::read_json_artifact(ctx, &ar.key).await?;
        if v.get("schema_id").and_then(|x| x.as_str()) == Some("geophysics.ip_survey.v1") {
            survey = Some(v);
            break;
        }
    }
    let survey = survey.ok_or_else(|| {
        NodeError::InvalidConfig("ip_qc_normalize requires upstream geophysics.ip_survey.v1".into())
    })?;

    let crs = survey
        .get("crs")
        .cloned()
        .and_then(|v| serde_json::from_value::<CrsRecord>(v).ok())
        .unwrap_or_else(|| CrsRecord::epsg(32630));
    let electrodes: Vec<IpElectrodeRecord> = serde_json::from_value(
        survey
            .get("electrodes")
            .cloned()
            .unwrap_or_else(|| json!([])),
    )
    .unwrap_or_default();
    let mut measurements: Vec<IpMeasurementRecord> = serde_json::from_value(
        survey
            .get("measurements")
            .cloned()
            .unwrap_or_else(|| json!([])),
    )
    .unwrap_or_default();

    let known_electrodes = electrodes
        .iter()
        .map(|e| e.electrode_id.clone())
        .collect::<HashSet<_>>();
    let (max_recip, min_chargeability, max_chargeability) = qc_params(job);
    let mut cleaned = Vec::<IpMeasurementRecord>::new();
    let mut rejected_rows = Vec::<Value>::new();
    let mut seen = HashSet::<(String, String, String, String)>::new();
    let mut rejected_reasons = HashMap::<String, usize>::new();

    for mut m in measurements.drain(..) {
        let mut reject_reason: Option<String> = None;
        let key = (
            m.a_id.clone(),
            m.b_id.clone(),
            m.m_id.clone(),
            m.n_id.clone(),
        );
        if !known_electrodes.contains(&m.a_id)
            || !known_electrodes.contains(&m.b_id)
            || !known_electrodes.contains(&m.m_id)
            || !known_electrodes.contains(&m.n_id)
        {
            reject_reason = Some("unknown_electrode".into());
        } else if !m.apparent_resistivity_ohm_m.is_finite() || m.apparent_resistivity_ohm_m <= 0.0 {
            reject_reason = Some("invalid_apparent_resistivity".into());
        } else if !m.chargeability_mv_v.is_finite()
            || m.chargeability_mv_v < min_chargeability
            || m.chargeability_mv_v > max_chargeability
        {
            reject_reason = Some("chargeability_out_of_bounds".into());
        } else if m
            .reciprocity_error_pct
            .map(|v| !v.is_finite() || v.abs() > max_recip)
            .unwrap_or(false)
        {
            reject_reason = Some("reciprocity_above_threshold".into());
        } else if !seen.insert(key) {
            reject_reason = Some("duplicate_quadrupole".into());
        }

        if let Some(reason) = reject_reason {
            *rejected_reasons.entry(reason.clone()).or_insert(0) += 1;
            rejected_rows.push(json!({
                "measurement_id": m.measurement_id,
                "line_id": m.line_id,
                "array_type": m.array_type,
                "a_id": m.a_id,
                "b_id": m.b_id,
                "m_id": m.m_id,
                "n_id": m.n_id,
                "apparent_resistivity_ohm_m": m.apparent_resistivity_ohm_m,
                "chargeability_mv_v": m.chargeability_mv_v,
                "reciprocity_error_pct": m.reciprocity_error_pct,
                "reject_reason": reason,
            }));
            continue;
        }

        if m.chargeability_mv_v < 0.0 {
            m.qa_flags.push("negative_but_retained".into());
        }
        cleaned.push(m);
    }

    let cleaned_rows = build_contract_rows(&cleaned);
    let cleaned_columns = table_columns(&cleaned_rows);
    let rejected_columns = table_columns(&rejected_rows);
    let cleaned_payload = json!({
        "schema_id": "geophysics.ip_survey.v1",
        "schema_version": 1,
        "type": "ip_survey",
        "quality": "cleaned",
        "crs": crs,
        "display_contract": {
            "renderer": "table",
            "display_pointer": "table.rows",
            "editable": ["visible"]
        },
        "measure_candidates": [
            "apparent_resistivity_ohm_m",
            "chargeability_mv_v",
            "current_ma",
            "voltage_mv",
            "reciprocity_error_pct"
        ],
        "electrodes": electrodes,
        "measurements": cleaned,
        "rows": cleaned_rows,
        "meta": {
            "row_count": cleaned.len(),
            "columns": cleaned_columns,
            "reject_count": rejected_rows.len(),
            "qc_thresholds": {
                "max_reciprocity_error_pct": max_recip,
                "min_chargeability_mv_v": min_chargeability,
                "max_chargeability_mv_v": max_chargeability
            }
        }
    });
    let rejected_payload = json!({
        "schema_id": "data_model.table.v1",
        "schema_version": 1,
        "kind": "ip_qc_rejected_rows",
        "display_contract": {
            "renderer":"table",
            "display_pointer":"table.rows",
            "editable":["visible"]
        },
        "meta": {
            "row_count": rejected_rows.len(),
            "columns": rejected_columns
        },
        "rows": rejected_rows
    });
    let report_payload = json!({
        "schema_id": "report.ip_qc.v1",
        "type": "ip_qc_report",
        "summary": {
            "input_measurements": survey.pointer("/measurements").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0),
            "retained_measurements": cleaned_payload.pointer("/measurements").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0),
            "rejected_measurements": rejected_payload.pointer("/rows").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0),
            "electrode_count": survey.pointer("/electrodes").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0),
        },
        "thresholds": {
            "max_reciprocity_error_pct": max_recip,
            "min_chargeability_mv_v": min_chargeability,
            "max_chargeability_mv_v": max_chargeability
        },
        "rejected_reason_counts": rejected_reasons.into_iter().map(|(reason, count)| json!({"reason": reason, "count": count})).collect::<Vec<_>>(),
        "notes": [
            "This QC pass is intentionally conservative and surfaces bad quadrupoles early.",
            "Next stage should derive pseudosection-ready points and DOI/sensitivity diagnostics from the cleaned survey contract."
        ]
    });

    let cleaned_key = format!(
        "graphs/{}/nodes/{}/ip_survey_clean.json",
        job.graph_id, job.node_id
    );
    let points_key = format!(
        "graphs/{}/nodes/{}/ip_electrodes.json",
        job.graph_id, job.node_id
    );
    let rejected_key = format!(
        "graphs/{}/nodes/{}/ip_rejected_rows.json",
        job.graph_id, job.node_id
    );
    let report_key = format!(
        "graphs/{}/nodes/{}/ip_qc_report.json",
        job.graph_id, job.node_id
    );

    let cleaned_ref = super::runtime::write_artifact(
        ctx,
        &cleaned_key,
        &serde_json::to_vec(&cleaned_payload)?,
        Some("application/json"),
    )
    .await?;
    let points_ref = super::runtime::write_artifact(
        ctx,
        &points_key,
        &serde_json::to_vec(&points_payload(&electrodes, &crs, &["z"]))?,
        Some("application/json"),
    )
    .await?;
    let rejected_ref = super::runtime::write_artifact(
        ctx,
        &rejected_key,
        &serde_json::to_vec(&rejected_payload)?,
        Some("application/json"),
    )
    .await?;
    let report_ref = super::runtime::write_artifact(
        ctx,
        &report_key,
        &serde_json::to_vec(&report_payload)?,
        Some("application/json"),
    )
    .await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![
            cleaned_ref.clone(),
            points_ref.clone(),
            rejected_ref.clone(),
            report_ref.clone(),
        ],
        content_hashes: vec![
            cleaned_ref.content_hash,
            points_ref.content_hash,
            rejected_ref.content_hash,
            report_ref.content_hash,
        ],
        error_message: None,
    })
}

pub async fn run_ip_pseudosection(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let mut survey = None;
    for ar in &job.input_artifact_refs {
        let v = super::runtime::read_json_artifact(ctx, &ar.key).await?;
        if v.get("schema_id").and_then(|x| x.as_str()) == Some("geophysics.ip_survey.v1") {
            survey = Some(v);
            break;
        }
    }
    let survey = survey.ok_or_else(|| {
        NodeError::InvalidConfig(
            "ip_pseudosection requires upstream geophysics.ip_survey.v1".into(),
        )
    })?;

    let crs = survey
        .get("crs")
        .cloned()
        .and_then(|v| serde_json::from_value::<CrsRecord>(v).ok())
        .unwrap_or_else(|| CrsRecord::epsg(32630));
    let electrodes: Vec<IpElectrodeRecord> = serde_json::from_value(
        survey
            .get("electrodes")
            .cloned()
            .unwrap_or_else(|| json!([])),
    )
    .unwrap_or_default();
    let measurements: Vec<IpMeasurementRecord> = serde_json::from_value(
        survey
            .get("measurements")
            .cloned()
            .unwrap_or_else(|| json!([])),
    )
    .unwrap_or_default();
    let by_id = electrode_lookup(&electrodes);

    let mut rows = Vec::<Value>::new();
    let mut points = Vec::<Value>::new();
    let mut line_stats = HashMap::<String, usize>::new();
    let mut charge_vals = Vec::<f64>::new();
    let mut rho_vals = Vec::<f64>::new();

    for m in &measurements {
        let Some(a) = by_id.get(&m.a_id) else {
            continue;
        };
        let Some(b) = by_id.get(&m.b_id) else {
            continue;
        };
        let Some(mm) = by_id.get(&m.m_id) else {
            continue;
        };
        let Some(n) = by_id.get(&m.n_id) else {
            continue;
        };

        let tx_mid_x = 0.5 * (a.x + b.x);
        let tx_mid_y = 0.5 * (a.y + b.y);
        let rx_mid_x = 0.5 * (mm.x + n.x);
        let rx_mid_y = 0.5 * (mm.y + n.y);
        let center_x = 0.5 * (tx_mid_x + rx_mid_x);
        let center_y = 0.5 * (tx_mid_y + rx_mid_y);
        let avg_z = 0.25 * (a.z + b.z + mm.z + n.z);
        let tx_dipole = ((a.x - b.x).powi(2) + (a.y - b.y).powi(2)).sqrt().max(1e-6);
        let rx_dipole = ((mm.x - n.x).powi(2) + (mm.y - n.y).powi(2))
            .sqrt()
            .max(1e-6);
        let b_to_m = ((b.x - mm.x).powi(2) + (b.y - mm.y).powi(2)).sqrt();
        let n_level = (b_to_m / tx_dipole).round().max(1.0);
        let pseudo_depth = (0.75 * tx_dipole * n_level).max(0.5 * rx_dipole);
        let pseudo_z = avg_z - pseudo_depth;
        let line_id = m
            .line_id
            .clone()
            .or_else(|| a.line_id.clone())
            .unwrap_or_else(|| "unknown".to_string());

        *line_stats.entry(line_id.clone()).or_insert(0) += 1;
        charge_vals.push(m.chargeability_mv_v);
        rho_vals.push(m.apparent_resistivity_ohm_m);

        let row = json!({
            "measurement_id": m.measurement_id,
            "line_id": line_id,
            "array_type": m.array_type,
            "survey_mode": m.survey_mode,
            "pseudo_x": center_x,
            "pseudo_y": center_y,
            "pseudo_z": pseudo_z,
            "pseudo_depth_m": pseudo_depth,
            "tx_mid_x": tx_mid_x,
            "tx_mid_y": tx_mid_y,
            "rx_mid_x": rx_mid_x,
            "rx_mid_y": rx_mid_y,
            "dipole_length_m": tx_dipole,
            "receiver_dipole_length_m": rx_dipole,
            "n_level": n_level,
            "apparent_resistivity_ohm_m": m.apparent_resistivity_ohm_m,
            "chargeability_mv_v": m.chargeability_mv_v,
            "current_ma": m.current_ma,
            "voltage_mv": m.voltage_mv,
            "reciprocity_error_pct": m.reciprocity_error_pct,
            "qa_flags": m.qa_flags,
        });
        rows.push(row.clone());
        points.push(json!({
            "x": center_x,
            "y": center_y,
            "z": pseudo_z,
            "segment_id": line_id,
            "attributes": row,
        }));
    }

    let point_payload = json!({
        "schema_id": "point_set.ip_pseudosection.v1",
        "type": "ip_pseudosection_points",
        "crs": crs,
        "measure_candidates": [
            "chargeability_mv_v",
            "apparent_resistivity_ohm_m",
            "pseudo_depth_m",
            "n_level",
            "current_ma",
            "voltage_mv",
            "reciprocity_error_pct"
        ],
        "display_contract": {
            "renderer": "sample_points",
            "display_pointer": "scene3d.sample_points",
            "editable": ["visible", "opacity", "size", "measure", "palette"]
        },
        "points": points
    });
    let table_payload = json!({
        "schema_id": "geophysics.ip_pseudosection.v1",
        "schema_version": 1,
        "type": "ip_pseudosection",
        "crs": crs,
        "display_contract": {
            "renderer":"table",
            "display_pointer":"table.rows",
            "editable":["visible"]
        },
        "meta": {
            "row_count": rows.len(),
            "columns": table_columns(&rows)
        },
        "rows": rows
    });
    let min_max = |vals: &[f64]| -> (Option<f64>, Option<f64>) {
        if vals.is_empty() {
            (None, None)
        } else {
            let mut lo = f64::INFINITY;
            let mut hi = f64::NEG_INFINITY;
            for v in vals {
                lo = lo.min(*v);
                hi = hi.max(*v);
            }
            (Some(lo), Some(hi))
        }
    };
    let (charge_min, charge_max) = min_max(&charge_vals);
    let (rho_min, rho_max) = min_max(&rho_vals);
    let report_payload = json!({
        "schema_id": "report.ip_pseudosection.v1",
        "type": "ip_pseudosection_report",
        "summary": {
            "point_count": table_payload.pointer("/rows").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0),
            "line_count": line_stats.len(),
            "chargeability_min_mv_v": charge_min,
            "chargeability_max_mv_v": charge_max,
            "apparent_resistivity_min_ohm_m": rho_min,
            "apparent_resistivity_max_ohm_m": rho_max
        },
        "line_counts": line_stats.into_iter().map(|(line_id, count)| json!({"line_id": line_id, "count": count})).collect::<Vec<_>>(),
        "notes": [
            "Pseudo-depth positions are for visualization/QC and not inversion cells.",
            "This output is intended for fast 3D curtain-style visibility before full IP inversion is added."
        ]
    });

    let points_key = format!(
        "graphs/{}/nodes/{}/ip_pseudosection_points.json",
        job.graph_id, job.node_id
    );
    let table_key = format!(
        "graphs/{}/nodes/{}/ip_pseudosection_rows.json",
        job.graph_id, job.node_id
    );
    let report_key = format!(
        "graphs/{}/nodes/{}/ip_pseudosection_report.json",
        job.graph_id, job.node_id
    );

    let points_ref = super::runtime::write_artifact(
        ctx,
        &points_key,
        &serde_json::to_vec(&point_payload)?,
        Some("application/json"),
    )
    .await?;
    let table_ref = super::runtime::write_artifact(
        ctx,
        &table_key,
        &serde_json::to_vec(&table_payload)?,
        Some("application/json"),
    )
    .await?;
    let report_ref = super::runtime::write_artifact(
        ctx,
        &report_key,
        &serde_json::to_vec(&report_payload)?,
        Some("application/json"),
    )
    .await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![points_ref.clone(), table_ref.clone(), report_ref.clone()],
        content_hashes: vec![
            points_ref.content_hash,
            table_ref.content_hash,
            report_ref.content_hash,
        ],
        error_message: None,
    })
}

pub async fn run_ip_corridor_model(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let mut pseudo = None;
    for ar in &job.input_artifact_refs {
        let v = super::runtime::read_json_artifact(ctx, &ar.key).await?;
        if v.get("schema_id").and_then(|x| x.as_str()) == Some("geophysics.ip_pseudosection.v1") {
            pseudo = Some(v);
            break;
        }
    }
    let pseudo = pseudo.ok_or_else(|| {
        NodeError::InvalidConfig(
            "ip_corridor_model requires upstream geophysics.ip_pseudosection.v1".into(),
        )
    })?;

    let crs = pseudo
        .get("crs")
        .cloned()
        .and_then(|v| serde_json::from_value::<CrsRecord>(v).ok())
        .unwrap_or_else(|| CrsRecord::epsg(32630));
    let rows = pseudo
        .get("rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let corridor_half_width_m = job
        .output_spec
        .pointer("/node_ui/corridor_half_width_m")
        .and_then(parse_num)
        .unwrap_or(12.5)
        .clamp(2.0, 100.0);
    let depth_cell_scale = job
        .output_spec
        .pointer("/node_ui/depth_cell_scale")
        .and_then(parse_num)
        .unwrap_or(0.9)
        .clamp(0.2, 3.0);
    let min_cell_thickness_m = job
        .output_spec
        .pointer("/node_ui/min_cell_thickness_m")
        .and_then(parse_num)
        .unwrap_or(10.0)
        .clamp(1.0, 80.0);

    let mut blocks = Vec::<Value>::new();
    let mut centers = Vec::<Value>::new();
    let mut charge_vals = Vec::<f64>::new();
    let mut rho_vals = Vec::<f64>::new();

    for row in &rows {
        let Some(obj) = row.as_object() else { continue };
        let x = obj.get("pseudo_x").and_then(parse_num).unwrap_or(0.0);
        let y = obj.get("pseudo_y").and_then(parse_num).unwrap_or(0.0);
        let z = obj.get("pseudo_z").and_then(parse_num).unwrap_or(0.0);
        let pseudo_depth_m = obj
            .get("pseudo_depth_m")
            .and_then(parse_num)
            .unwrap_or(20.0);
        let dipole_length_m = obj
            .get("dipole_length_m")
            .and_then(parse_num)
            .unwrap_or(20.0);
        let chargeability_mv_v = obj
            .get("chargeability_mv_v")
            .and_then(parse_num)
            .unwrap_or(0.0);
        let apparent_resistivity_ohm_m = obj
            .get("apparent_resistivity_ohm_m")
            .and_then(parse_num)
            .unwrap_or(0.0);
        let n_level = obj.get("n_level").and_then(parse_num).unwrap_or(1.0);
        let reciprocity_error_pct = obj
            .get("reciprocity_error_pct")
            .and_then(parse_num)
            .unwrap_or(0.0);
        let confidence = (1.0 / (1.0 + reciprocity_error_pct.max(0.0) / 12.0)).clamp(0.05, 1.0);
        let dx = dipole_length_m.max(5.0);
        let dy = corridor_half_width_m * 2.0;
        let dz = (pseudo_depth_m * depth_cell_scale).max(min_cell_thickness_m);

        charge_vals.push(chargeability_mv_v);
        rho_vals.push(apparent_resistivity_ohm_m);

        let attrs = json!({
            "chargeability_mv_v": chargeability_mv_v,
            "apparent_resistivity_ohm_m": apparent_resistivity_ohm_m,
            "pseudo_depth_m": pseudo_depth_m,
            "n_level": n_level,
            "confidence": confidence,
            "reciprocity_error_pct": reciprocity_error_pct,
            "line_id": obj.get("line_id").cloned().unwrap_or(Value::Null),
            "measurement_id": obj.get("measurement_id").cloned().unwrap_or(Value::Null),
        });

        blocks.push(json!({
            "x": x,
            "y": y,
            "z": z,
            "dx": dx,
            "dy": dy,
            "dz": dz,
            "above_cutoff": true,
            "attributes": attrs,
        }));
        centers.push(json!({
            "x": x,
            "y": y,
            "z": z,
            "attributes": attrs,
        }));
    }

    let (charge_min, charge_max) = min_max(&charge_vals);
    let (rho_min, rho_max) = min_max(&rho_vals);

    let voxels_payload = json!({
        "schema_id": "scene3d.block_voxels.v1",
        "type": "ip_corridor_voxels",
        "crs": crs,
        "display_contract": {
            "renderer": "block_voxels",
            "display_pointer": "scene3d.block_voxels",
            "editable": ["visible", "opacity", "measure", "palette", "cutoff", "below_cutoff_opacity"]
        },
        "measure_candidates": [
            "chargeability_mv_v",
            "apparent_resistivity_ohm_m",
            "pseudo_depth_m",
            "confidence",
            "n_level",
            "reciprocity_error_pct"
        ],
        "style_defaults": {
            "palette": "inferno",
            "cutoff_grade": 0.0,
            "below_cutoff_opacity": 0.15
        },
        "blocks": blocks,
        "stats": {
            "voxel_count": centers.len(),
            "chargeability_min_mv_v": charge_min,
            "chargeability_max_mv_v": charge_max,
            "apparent_resistivity_min_ohm_m": rho_min,
            "apparent_resistivity_max_ohm_m": rho_max,
            "corridor_half_width_m": corridor_half_width_m,
            "depth_cell_scale": depth_cell_scale,
            "min_cell_thickness_m": min_cell_thickness_m,
            "model_kind": "pseudosection_corridor"
        }
    });
    let centers_payload = json!({
        "schema_id": "point_set.ip_corridor_centers.v1",
        "type": "ip_corridor_centers",
        "crs": crs,
        "measure_candidates": [
            "chargeability_mv_v",
            "apparent_resistivity_ohm_m",
            "pseudo_depth_m",
            "confidence",
            "n_level"
        ],
        "display_contract": {
            "renderer": "sample_points",
            "display_pointer": "scene3d.sample_points",
            "editable": ["visible", "opacity", "size", "measure", "palette"]
        },
        "points": centers
    });
    let report_payload = json!({
        "schema_id": "report.ip_corridor_model.v1",
        "type": "ip_corridor_model_report",
        "summary": {
            "voxel_count": centers_payload.pointer("/points").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0),
            "chargeability_min_mv_v": charge_min,
            "chargeability_max_mv_v": charge_max,
            "apparent_resistivity_min_ohm_m": rho_min,
            "apparent_resistivity_max_ohm_m": rho_max
        },
        "parameters": {
            "corridor_half_width_m": corridor_half_width_m,
            "depth_cell_scale": depth_cell_scale,
            "min_cell_thickness_m": min_cell_thickness_m
        },
        "notes": [
            "This is a pseudo-volume stitched from pseudosection positions, not a physics inversion result.",
            "Use it for rapid 3D context, targeting, and interface validation before full TDIP inversion is introduced."
        ]
    });

    let voxels_key = format!(
        "graphs/{}/nodes/{}/ip_corridor_voxels.json",
        job.graph_id, job.node_id
    );
    let centers_key = format!(
        "graphs/{}/nodes/{}/ip_corridor_centers.json",
        job.graph_id, job.node_id
    );
    let report_key = format!(
        "graphs/{}/nodes/{}/ip_corridor_model_report.json",
        job.graph_id, job.node_id
    );

    let voxels_ref = super::runtime::write_artifact(
        ctx,
        &voxels_key,
        &serde_json::to_vec(&voxels_payload)?,
        Some("application/json"),
    )
    .await?;
    let centers_ref = super::runtime::write_artifact(
        ctx,
        &centers_key,
        &serde_json::to_vec(&centers_payload)?,
        Some("application/json"),
    )
    .await?;
    let report_ref = super::runtime::write_artifact(
        ctx,
        &report_key,
        &serde_json::to_vec(&report_payload)?,
        Some("application/json"),
    )
    .await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![voxels_ref.clone(), centers_ref.clone(), report_ref.clone()],
        content_hashes: vec![
            voxels_ref.content_hash,
            centers_ref.content_hash,
            report_ref.content_hash,
        ],
        error_message: None,
    })
}

fn read_ip_pseudosection_payload<'a>(
    inputs: &'a [mine_eye_types::ArtifactRef],
    payloads: &'a [Value],
) -> Option<&'a Value> {
    for (idx, _ar) in inputs.iter().enumerate() {
        let Some(v) = payloads.get(idx) else { continue };
        if v.get("schema_id").and_then(|x| x.as_str()) == Some("geophysics.ip_pseudosection.v1") {
            return Some(v);
        }
    }
    None
}

fn read_ip_inversion_mesh_payload<'a>(
    inputs: &'a [mine_eye_types::ArtifactRef],
    payloads: &'a [Value],
) -> Option<&'a Value> {
    for (idx, _ar) in inputs.iter().enumerate() {
        let Some(v) = payloads.get(idx) else { continue };
        if v.get("schema_id").and_then(|x| x.as_str()) == Some("geophysics.ip_inversion_mesh.v1") {
            return Some(v);
        }
    }
    None
}

async fn load_input_payloads(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<Vec<Value>, NodeError> {
    let mut payloads = Vec::with_capacity(job.input_artifact_refs.len());
    for ar in &job.input_artifact_refs {
        payloads.push(super::runtime::read_json_artifact(ctx, &ar.key).await?);
    }
    Ok(payloads)
}

pub async fn run_ip_inversion_mesh(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payloads = load_input_payloads(ctx, job).await?;
    let pseudo =
        read_ip_pseudosection_payload(&job.input_artifact_refs, &payloads).ok_or_else(|| {
            NodeError::InvalidConfig(
                "ip_inversion_mesh requires upstream geophysics.ip_pseudosection.v1".into(),
            )
        })?;

    let crs = pseudo
        .get("crs")
        .cloned()
        .and_then(|v| serde_json::from_value::<CrsRecord>(v).ok())
        .unwrap_or_else(|| CrsRecord::epsg(32630));
    let rows = pseudo
        .get("rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if rows.is_empty() {
        return Err(NodeError::InvalidConfig(
            "ip_inversion_mesh received an empty pseudosection dataset".into(),
        ));
    }

    let mut xs = Vec::new();
    let mut ys = Vec::new();
    let mut zs = Vec::new();
    let mut dipoles = Vec::new();
    let mut depths = Vec::new();
    for row in &rows {
        let Some(obj) = row.as_object() else { continue };
        if let Some(v) = obj.get("pseudo_x").and_then(parse_num) {
            xs.push(v);
        }
        if let Some(v) = obj.get("pseudo_y").and_then(parse_num) {
            ys.push(v);
        }
        if let Some(v) = obj.get("pseudo_z").and_then(parse_num) {
            zs.push(v);
        }
        if let Some(v) = obj.get("dipole_length_m").and_then(parse_num) {
            dipoles.push(v.max(1.0));
        }
        if let Some(v) = obj.get("pseudo_depth_m").and_then(parse_num) {
            depths.push(v.max(1.0));
        }
    }
    if xs.is_empty() || ys.is_empty() || zs.is_empty() {
        return Err(NodeError::InvalidConfig(
            "ip_inversion_mesh could not derive extents from pseudosection rows".into(),
        ));
    }

    let x_min = xs.iter().fold(f64::INFINITY, |a, b| a.min(*b));
    let x_max = xs.iter().fold(f64::NEG_INFINITY, |a, b| a.max(*b));
    let y_min = ys.iter().fold(f64::INFINITY, |a, b| a.min(*b));
    let y_max = ys.iter().fold(f64::NEG_INFINITY, |a, b| a.max(*b));
    let z_min = zs.iter().fold(f64::INFINITY, |a, b| a.min(*b));
    let z_max = zs.iter().fold(f64::NEG_INFINITY, |a, b| a.max(*b));
    let median_dipole = if dipoles.is_empty() {
        25.0
    } else {
        let mut vals = dipoles;
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        vals[vals.len() / 2]
    };
    let max_depth = depths
        .iter()
        .fold(0.0_f64, |a, b| a.max(*b))
        .max((z_max - z_min).abs());

    let cell_x_m = job
        .output_spec
        .pointer("/node_ui/cell_x_m")
        .and_then(parse_num)
        .unwrap_or(median_dipole)
        .clamp(5.0, 200.0);
    let cell_y_m = job
        .output_spec
        .pointer("/node_ui/cell_y_m")
        .and_then(parse_num)
        .unwrap_or((median_dipole * 0.75).max(10.0))
        .clamp(5.0, 200.0);
    let cell_z_m = job
        .output_spec
        .pointer("/node_ui/cell_z_m")
        .and_then(parse_num)
        .unwrap_or((median_dipole * 0.6).max(10.0))
        .clamp(5.0, 200.0);
    let lateral_padding_m = job
        .output_spec
        .pointer("/node_ui/lateral_padding_m")
        .and_then(parse_num)
        .unwrap_or(median_dipole * 1.5)
        .clamp(0.0, 500.0);
    let depth_padding_m = job
        .output_spec
        .pointer("/node_ui/depth_padding_m")
        .and_then(parse_num)
        .unwrap_or((0.75 * max_depth).max(cell_z_m))
        .clamp(cell_z_m, 1000.0);
    let max_cells = job
        .output_spec
        .pointer("/node_ui/max_cells")
        .and_then(|v| v.as_u64())
        .unwrap_or(18_000) as usize;

    let origin_x = x_min - lateral_padding_m;
    let origin_y = y_min - lateral_padding_m;
    let top_z = z_max + (0.5 * cell_z_m);
    let bottom_z = z_min - depth_padding_m;
    let nx = (((x_max - x_min) + 2.0 * lateral_padding_m) / cell_x_m)
        .ceil()
        .max(1.0) as usize;
    let ny = (((y_max - y_min) + 2.0 * lateral_padding_m) / cell_y_m)
        .ceil()
        .max(1.0) as usize;
    let nz = (((top_z - bottom_z).abs()) / cell_z_m).ceil().max(1.0) as usize;
    let estimated_cells = nx.saturating_mul(ny).saturating_mul(nz);
    if estimated_cells > max_cells {
        return Err(NodeError::InvalidConfig(format!(
            "ip_inversion_mesh estimated {} cells which exceeds max_cells={}; increase cell size or raise the cap",
            estimated_cells, max_cells
        )));
    }

    let mut cells = Vec::with_capacity(estimated_cells);
    let mut points = Vec::with_capacity(estimated_cells);
    for ix in 0..nx {
        for iy in 0..ny {
            for iz in 0..nz {
                let x = origin_x + ((ix as f64) + 0.5) * cell_x_m;
                let y = origin_y + ((iy as f64) + 0.5) * cell_y_m;
                let z = top_z - ((iz as f64) + 0.5) * cell_z_m;
                let cell_id = format!("ipmesh_{}_{}_{}", ix, iy, iz);
                let row = json!({
                    "cell_id": cell_id,
                    "ix": ix,
                    "iy": iy,
                    "iz": iz,
                    "x": x,
                    "y": y,
                    "z": z,
                    "dx": cell_x_m,
                    "dy": cell_y_m,
                    "dz": cell_z_m
                });
                points.push(json!({
                    "x": x,
                    "y": y,
                    "z": z,
                    "attributes": row.clone()
                }));
                cells.push(row);
            }
        }
    }

    let mesh_payload = json!({
        "schema_id": "geophysics.ip_inversion_mesh.v1",
        "schema_version": 1,
        "type": "ip_inversion_mesh",
        "crs": crs,
        "grid": {
            "origin_x": origin_x,
            "origin_y": origin_y,
            "origin_z_top": top_z,
            "cell_x_m": cell_x_m,
            "cell_y_m": cell_y_m,
            "cell_z_m": cell_z_m,
            "nx": nx,
            "ny": ny,
            "nz": nz,
            "lateral_padding_m": lateral_padding_m,
            "depth_padding_m": depth_padding_m
        },
        "display_contract": {
            "renderer":"table",
            "display_pointer":"table.rows",
            "editable":["visible"]
        },
        "meta": {
            "row_count": cells.len(),
            "columns": table_columns(&cells)
        },
        "rows": cells
    });
    let points_payload = json!({
        "schema_id": "point_set.ip_inversion_mesh_centers.v1",
        "type": "ip_inversion_mesh_centers",
        "crs": crs,
        "measure_candidates": ["ix", "iy", "iz"],
        "display_contract": {
            "renderer": "sample_points",
            "display_pointer": "scene3d.sample_points",
            "editable": ["visible", "opacity", "size", "measure", "palette"]
        },
        "points": points
    });
    let report_payload = json!({
        "schema_id": "report.ip_inversion_mesh.v1",
        "type": "ip_inversion_mesh_report",
        "summary": {
            "cell_count": estimated_cells,
            "nx": nx,
            "ny": ny,
            "nz": nz,
            "median_dipole_m": median_dipole,
            "depth_of_investigation_m": max_depth
        },
        "parameters": {
            "cell_x_m": cell_x_m,
            "cell_y_m": cell_y_m,
            "cell_z_m": cell_z_m,
            "lateral_padding_m": lateral_padding_m,
            "depth_padding_m": depth_padding_m,
            "max_cells": max_cells
        },
        "notes": [
            "This is a regular preview mesh for TDIP display/testing, not a finite-element inversion mesh.",
            "A future physics inversion can consume the same survey semantics and replace this grid generator."
        ]
    });

    let mesh_key = format!(
        "graphs/{}/nodes/{}/ip_inversion_mesh.json",
        job.graph_id, job.node_id
    );
    let points_key = format!(
        "graphs/{}/nodes/{}/ip_inversion_mesh_centers.json",
        job.graph_id, job.node_id
    );
    let report_key = format!(
        "graphs/{}/nodes/{}/ip_inversion_mesh_report.json",
        job.graph_id, job.node_id
    );
    let mesh_ref = super::runtime::write_artifact(
        ctx,
        &mesh_key,
        &serde_json::to_vec(&mesh_payload)?,
        Some("application/json"),
    )
    .await?;
    let points_ref = super::runtime::write_artifact(
        ctx,
        &points_key,
        &serde_json::to_vec(&points_payload)?,
        Some("application/json"),
    )
    .await?;
    let report_ref = super::runtime::write_artifact(
        ctx,
        &report_key,
        &serde_json::to_vec(&report_payload)?,
        Some("application/json"),
    )
    .await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![mesh_ref.clone(), points_ref.clone(), report_ref.clone()],
        content_hashes: vec![
            mesh_ref.content_hash,
            points_ref.content_hash,
            report_ref.content_hash,
        ],
        error_message: None,
    })
}

pub async fn run_ip_inversion_preview(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payloads = load_input_payloads(ctx, job).await?;
    let pseudo =
        read_ip_pseudosection_payload(&job.input_artifact_refs, &payloads).ok_or_else(|| {
            NodeError::InvalidConfig(
                "ip_inversion_preview requires upstream geophysics.ip_pseudosection.v1".into(),
            )
        })?;
    let mesh =
        read_ip_inversion_mesh_payload(&job.input_artifact_refs, &payloads).ok_or_else(|| {
            NodeError::InvalidConfig(
                "ip_inversion_preview requires upstream geophysics.ip_inversion_mesh.v1".into(),
            )
        })?;

    let crs = mesh
        .get("crs")
        .cloned()
        .and_then(|v| serde_json::from_value::<CrsRecord>(v).ok())
        .or_else(|| {
            pseudo
                .get("crs")
                .cloned()
                .and_then(|v| serde_json::from_value::<CrsRecord>(v).ok())
        })
        .unwrap_or_else(|| CrsRecord::epsg(32630));

    let pseudo_rows = pseudo
        .get("rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let mesh_rows = mesh
        .get("rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if pseudo_rows.is_empty() || mesh_rows.is_empty() {
        return Err(NodeError::InvalidConfig(
            "ip_inversion_preview requires non-empty pseudosection and mesh inputs".into(),
        ));
    }

    let influence_radius_m = job
        .output_spec
        .pointer("/node_ui/influence_radius_m")
        .and_then(parse_num)
        .unwrap_or(90.0)
        .clamp(10.0, 1000.0);
    let power = job
        .output_spec
        .pointer("/node_ui/idw_power")
        .and_then(parse_num)
        .unwrap_or(2.0)
        .clamp(0.5, 8.0);
    let min_support = job
        .output_spec
        .pointer("/node_ui/min_support")
        .and_then(|v| v.as_u64())
        .unwrap_or(2) as usize;
    let conductivity_bias = job
        .output_spec
        .pointer("/node_ui/conductivity_bias")
        .and_then(parse_num)
        .unwrap_or(0.35)
        .clamp(0.0, 1.0);

    let mut pseudo_points = Vec::<(f64, f64, f64, f64, f64, f64)>::new();
    for row in &pseudo_rows {
        let Some(obj) = row.as_object() else { continue };
        let x = obj.get("pseudo_x").and_then(parse_num).unwrap_or(0.0);
        let y = obj.get("pseudo_y").and_then(parse_num).unwrap_or(0.0);
        let z = obj.get("pseudo_z").and_then(parse_num).unwrap_or(0.0);
        let charge = obj
            .get("chargeability_mv_v")
            .and_then(parse_num)
            .unwrap_or(0.0);
        let rho = obj
            .get("apparent_resistivity_ohm_m")
            .and_then(parse_num)
            .unwrap_or(0.0)
            .max(1e-6);
        let recip = obj
            .get("reciprocity_error_pct")
            .and_then(parse_num)
            .unwrap_or(0.0)
            .max(0.0);
        pseudo_points.push((x, y, z, charge, rho, recip));
    }

    let mut blocks = Vec::<Value>::new();
    let mut centers = Vec::<Value>::new();
    let mut result_rows = Vec::<Value>::new();
    let mut charge_vals = Vec::<f64>::new();
    let mut rho_vals = Vec::<f64>::new();
    let mut conf_vals = Vec::<f64>::new();

    for row in &mesh_rows {
        let Some(obj) = row.as_object() else { continue };
        let x = obj.get("x").and_then(parse_num).unwrap_or(0.0);
        let y = obj.get("y").and_then(parse_num).unwrap_or(0.0);
        let z = obj.get("z").and_then(parse_num).unwrap_or(0.0);
        let dx = obj.get("dx").and_then(parse_num).unwrap_or(25.0);
        let dy = obj.get("dy").and_then(parse_num).unwrap_or(25.0);
        let dz = obj.get("dz").and_then(parse_num).unwrap_or(15.0);

        let mut sum_w = 0.0;
        let mut sum_charge = 0.0;
        let mut sum_log_rho = 0.0;
        let mut support = 0usize;
        let mut min_dist = f64::INFINITY;
        let mut sum_quality = 0.0;

        for (px, py, pz, charge, rho, recip) in &pseudo_points {
            let dx0 = x - *px;
            let dy0 = y - *py;
            let dz0 = (z - *pz) * 1.35;
            let dist = (dx0 * dx0 + dy0 * dy0 + dz0 * dz0).sqrt();
            if dist > influence_radius_m {
                continue;
            }
            let base_w = 1.0 / dist.max(1.0).powf(power);
            let quality = (1.0 / (1.0 + recip / 10.0)).clamp(0.05, 1.0);
            let w = base_w * quality;
            support += 1;
            min_dist = min_dist.min(dist);
            sum_w += w;
            sum_quality += quality;
            sum_charge += w * *charge;
            sum_log_rho += w * rho.ln();
        }
        if support < min_support || sum_w <= 0.0 {
            continue;
        }

        let chargeability_mv_v = sum_charge / sum_w;
        let apparent_resistivity_ohm_m = (sum_log_rho / sum_w).exp();
        let support_confidence = ((support as f64) / ((min_support as f64) + 3.0)).clamp(0.0, 1.0);
        let distance_confidence = (1.0 - (min_dist / influence_radius_m)).clamp(0.0, 1.0);
        let data_confidence = (sum_quality / (support as f64)).clamp(0.0, 1.0);
        let confidence =
            (0.45 * support_confidence + 0.35 * distance_confidence + 0.20 * data_confidence)
                .clamp(0.0, 1.0);
        let conductivity_proxy = 1.0 / apparent_resistivity_ohm_m.max(1e-6);
        let normalized_conductivity = conductivity_proxy / (1.0 + conductivity_proxy);
        let target_index = (chargeability_mv_v * (1.0 - conductivity_bias))
            + (1000.0 * normalized_conductivity * conductivity_bias);

        let attrs = json!({
            "cell_id": obj.get("cell_id").cloned().unwrap_or(Value::Null),
            "chargeability_mv_v": chargeability_mv_v,
            "apparent_resistivity_ohm_m": apparent_resistivity_ohm_m,
            "confidence": confidence,
            "support_count": support,
            "nearest_sample_distance_m": min_dist,
            "conductivity_proxy_s_m": conductivity_proxy,
            "target_index": target_index
        });
        let out_row = json!({
            "cell_id": obj.get("cell_id").cloned().unwrap_or(Value::Null),
            "ix": obj.get("ix").cloned().unwrap_or(Value::Null),
            "iy": obj.get("iy").cloned().unwrap_or(Value::Null),
            "iz": obj.get("iz").cloned().unwrap_or(Value::Null),
            "x": x,
            "y": y,
            "z": z,
            "dx": dx,
            "dy": dy,
            "dz": dz,
            "chargeability_mv_v": chargeability_mv_v,
            "apparent_resistivity_ohm_m": apparent_resistivity_ohm_m,
            "conductivity_proxy_s_m": conductivity_proxy,
            "target_index": target_index,
            "confidence": confidence,
            "support_count": support,
            "nearest_sample_distance_m": min_dist
        });

        charge_vals.push(chargeability_mv_v);
        rho_vals.push(apparent_resistivity_ohm_m);
        conf_vals.push(confidence);
        result_rows.push(out_row.clone());
        blocks.push(json!({
            "x": x,
            "y": y,
            "z": z,
            "dx": dx,
            "dy": dy,
            "dz": dz,
            "above_cutoff": true,
            "attributes": attrs.clone(),
        }));
        centers.push(json!({
            "x": x,
            "y": y,
            "z": z,
            "attributes": attrs,
        }));
    }

    let (charge_min, charge_max) = min_max(&charge_vals);
    let (rho_min, rho_max) = min_max(&rho_vals);
    let (conf_min, conf_max) = min_max(&conf_vals);
    let voxels_payload = json!({
        "schema_id": "scene3d.block_voxels.v1",
        "type": "ip_inversion_preview_voxels",
        "crs": crs,
        "display_contract": {
            "renderer": "block_voxels",
            "display_pointer": "scene3d.block_voxels",
            "editable": ["visible", "opacity", "measure", "palette", "cutoff", "below_cutoff_opacity"]
        },
        "measure_candidates": [
            "chargeability_mv_v",
            "apparent_resistivity_ohm_m",
            "conductivity_proxy_s_m",
            "target_index",
            "confidence",
            "support_count",
            "nearest_sample_distance_m"
        ],
        "style_defaults": {
            "palette": "inferno",
            "cutoff_grade": 0.0,
            "below_cutoff_opacity": 0.08
        },
        "blocks": blocks,
        "stats": {
            "voxel_count": result_rows.len(),
            "chargeability_min_mv_v": charge_min,
            "chargeability_max_mv_v": charge_max,
            "apparent_resistivity_min_ohm_m": rho_min,
            "apparent_resistivity_max_ohm_m": rho_max,
            "confidence_min": conf_min,
            "confidence_max": conf_max,
            "model_kind": "tdip_inversion_preview"
        }
    });
    let centers_payload = json!({
        "schema_id": "point_set.ip_inversion_preview_centers.v1",
        "type": "ip_inversion_preview_centers",
        "crs": crs,
        "measure_candidates": [
            "chargeability_mv_v",
            "apparent_resistivity_ohm_m",
            "conductivity_proxy_s_m",
            "target_index",
            "confidence",
            "support_count"
        ],
        "display_contract": {
            "renderer": "sample_points",
            "display_pointer": "scene3d.sample_points",
            "editable": ["visible", "opacity", "size", "measure", "palette"]
        },
        "points": centers
    });
    let model_payload = json!({
        "schema_id": "geophysics.ip_inversion_preview.v1",
        "schema_version": 1,
        "type": "ip_inversion_preview",
        "crs": crs,
        "display_contract": {
            "renderer":"table",
            "display_pointer":"table.rows",
            "editable":["visible"]
        },
        "meta": {
            "row_count": result_rows.len(),
            "columns": table_columns(&result_rows)
        },
        "rows": result_rows
    });
    let report_payload = json!({
        "schema_id": "report.ip_inversion_preview.v1",
        "type": "ip_inversion_preview_report",
        "summary": {
            "voxel_count": model_payload.pointer("/rows").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0),
            "chargeability_min_mv_v": charge_min,
            "chargeability_max_mv_v": charge_max,
            "apparent_resistivity_min_ohm_m": rho_min,
            "apparent_resistivity_max_ohm_m": rho_max,
            "confidence_min": conf_min,
            "confidence_max": conf_max
        },
        "parameters": {
            "influence_radius_m": influence_radius_m,
            "idw_power": power,
            "min_support": min_support,
            "conductivity_bias": conductivity_bias
        },
        "notes": [
            "This is a heuristic interpolation preview for 3D testing, not a physical inversion result.",
            "Use confidence, support count, and nearest-sample distance to gauge where the preview is likely to be misleading."
        ]
    });

    let voxels_key = format!(
        "graphs/{}/nodes/{}/ip_inversion_preview_voxels.json",
        job.graph_id, job.node_id
    );
    let centers_key = format!(
        "graphs/{}/nodes/{}/ip_inversion_preview_centers.json",
        job.graph_id, job.node_id
    );
    let model_key = format!(
        "graphs/{}/nodes/{}/ip_inversion_preview_rows.json",
        job.graph_id, job.node_id
    );
    let report_key = format!(
        "graphs/{}/nodes/{}/ip_inversion_preview_report.json",
        job.graph_id, job.node_id
    );
    let voxels_ref = super::runtime::write_artifact(
        ctx,
        &voxels_key,
        &serde_json::to_vec(&voxels_payload)?,
        Some("application/json"),
    )
    .await?;
    let centers_ref = super::runtime::write_artifact(
        ctx,
        &centers_key,
        &serde_json::to_vec(&centers_payload)?,
        Some("application/json"),
    )
    .await?;
    let model_ref = super::runtime::write_artifact(
        ctx,
        &model_key,
        &serde_json::to_vec(&model_payload)?,
        Some("application/json"),
    )
    .await?;
    let report_ref = super::runtime::write_artifact(
        ctx,
        &report_key,
        &serde_json::to_vec(&report_payload)?,
        Some("application/json"),
    )
    .await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![
            voxels_ref.clone(),
            centers_ref.clone(),
            model_ref.clone(),
            report_ref.clone(),
        ],
        content_hashes: vec![
            voxels_ref.content_hash,
            centers_ref.content_hash,
            model_ref.content_hash,
            report_ref.content_hash,
        ],
        error_message: None,
    })
}
