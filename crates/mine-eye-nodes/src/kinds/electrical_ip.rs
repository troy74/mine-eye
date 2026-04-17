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

fn canonical_ip_field_names() -> &'static [&'static str] {
    &[
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
    ]
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

fn ip_field_mapping(job: &JobEnvelope) -> HashMap<String, String> {
    let raw = job
        .input_payload
        .as_ref()
        .and_then(|p| p.get("mapping"))
        .or_else(|| job.output_spec.pointer("/node_ui/mapping"));
    let Some(obj) = raw.and_then(|v| v.as_object()) else {
        return HashMap::new();
    };
    canonical_ip_field_names()
        .iter()
        .filter_map(|canonical| {
            obj.get(*canonical)
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|source| ((*canonical).to_string(), source.to_string()))
        })
        .collect()
}

fn remap_ip_rows(
    rows: Vec<Map<String, Value>>,
    mapping: &HashMap<String, String>,
) -> Vec<Map<String, Value>> {
    if mapping.is_empty() {
        return rows;
    }
    rows.into_iter()
        .map(|mut row| {
            for (canonical, source) in mapping {
                let Some(value) = row.get(source).cloned() else {
                    continue;
                };
                row.insert(canonical.clone(), value);
                if source != canonical {
                    row.remove(source);
                }
            }
            row
        })
        .collect()
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
    let rows = remap_ip_rows(rows, &ip_field_mapping(job));
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
        "schema_id": "geophysics.ip_observations.v1",
        "schema_version": 1,
        "type": "ip_observations",
        "survey_mode": "tdip",
        "processing_stage": "raw_ingest",
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
            "source": source_label,
            "model_family": "electrical_ip"
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
            "Canonical IP observations contract keeps electrode geometry separate from measurement rows.",
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
        if v.get("schema_id").and_then(|x| x.as_str()) == Some("geophysics.ip_observations.v1") {
            survey = Some(v);
            break;
        }
    }
    let survey = survey.ok_or_else(|| {
        NodeError::InvalidConfig(
            "ip_qc_normalize requires upstream geophysics.ip_observations.v1".into(),
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
        "schema_id": "geophysics.ip_observations.v1",
        "schema_version": 1,
        "type": "ip_observations",
        "processing_stage": "qc_cleaned",
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
            "model_family": "electrical_ip",
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
        if v.get("schema_id").and_then(|x| x.as_str()) == Some("geophysics.ip_observations.v1") {
            survey = Some(v);
            break;
        }
    }
    let survey = survey.ok_or_else(|| {
        NodeError::InvalidConfig(
            "ip_pseudosection requires upstream geophysics.ip_observations.v1".into(),
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
            "editable": ["visible", "opacity", "measure", "palette"]
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
            "palette": "inferno"
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

fn read_ip_observations_payload<'a>(
    inputs: &'a [mine_eye_types::ArtifactRef],
    payloads: &'a [Value],
) -> Option<&'a Value> {
    for (idx, _ar) in inputs.iter().enumerate() {
        let Some(v) = payloads.get(idx) else { continue };
        let schema_id = v.get("schema_id").and_then(|x| x.as_str());
        if schema_id == Some("geophysics.ip_observations.v1")
            || schema_id == Some("geophysics.ip_survey.v1")
        {
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
        if v.get("schema_id").and_then(|x| x.as_str()) == Some("geophysics.ip_mesh.v1") {
            return Some(v);
        }
    }
    None
}

fn read_ip_inversion_result_payload<'a>(
    inputs: &'a [mine_eye_types::ArtifactRef],
    payloads: &'a [Value],
) -> Option<&'a Value> {
    for (idx, _ar) in inputs.iter().enumerate() {
        let Some(v) = payloads.get(idx) else { continue };
        if v.get("schema_id").and_then(|x| x.as_str()) == Some("geophysics.ip_inversion_result.v1")
        {
            return Some(v);
        }
    }
    None
}

fn read_ip_inversion_input_payload<'a>(
    inputs: &'a [mine_eye_types::ArtifactRef],
    payloads: &'a [Value],
) -> Option<&'a Value> {
    for (idx, _ar) in inputs.iter().enumerate() {
        let Some(v) = payloads.get(idx) else { continue };
        if v.get("schema_id").and_then(|x| x.as_str()) == Some("geophysics.ip_inversion_input.v1") {
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
        "schema_id": "geophysics.ip_mesh.v1",
        "schema_version": 1,
        "type": "ip_mesh",
        "crs": crs,
        "mesh_kind": "regular_preview_grid",
        "model_family": "electrical_ip",
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
        "domain": {
            "x_min": origin_x,
            "x_max": origin_x + (nx as f64) * cell_x_m,
            "y_min": origin_y,
            "y_max": origin_y + (ny as f64) * cell_y_m,
            "z_top": top_z,
            "z_bottom": bottom_z
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

pub async fn run_ip_inversion_input(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payloads = load_input_payloads(ctx, job).await?;
    let observations = read_ip_observations_payload(&job.input_artifact_refs, &payloads)
        .ok_or_else(|| {
            NodeError::InvalidConfig(
                "ip_inversion_input requires upstream geophysics.ip_observations.v1".into(),
            )
        })?;
    let pseudo =
        read_ip_pseudosection_payload(&job.input_artifact_refs, &payloads).ok_or_else(|| {
            NodeError::InvalidConfig(
                "ip_inversion_input requires upstream geophysics.ip_pseudosection.v1".into(),
            )
        })?;
    let mesh =
        read_ip_inversion_mesh_payload(&job.input_artifact_refs, &payloads).ok_or_else(|| {
            NodeError::InvalidConfig(
                "ip_inversion_input requires upstream geophysics.ip_mesh.v1".into(),
            )
        })?;

    let crs = observations
        .get("crs")
        .cloned()
        .and_then(|v| serde_json::from_value::<CrsRecord>(v).ok())
        .or_else(|| {
            mesh.get("crs")
                .cloned()
                .and_then(|v| serde_json::from_value::<CrsRecord>(v).ok())
        })
        .or_else(|| {
            pseudo
                .get("crs")
                .cloned()
                .and_then(|v| serde_json::from_value::<CrsRecord>(v).ok())
        })
        .unwrap_or_else(|| CrsRecord::epsg(32630));

    let observation_rows = observations
        .get("rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
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
    if observation_rows.is_empty() || pseudo_rows.is_empty() || mesh_rows.is_empty() {
        return Err(NodeError::InvalidConfig(
            "ip_inversion_input requires non-empty observations, pseudosection, and mesh inputs"
                .into(),
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

    let inversion_input_payload = json!({
        "schema_id": "geophysics.ip_inversion_input.v1",
        "schema_version": 1,
        "type": "ip_inversion_input",
        "crs": crs,
        "model_family": "electrical_ip",
        "solver_profile": "preview_interpolation_v1",
        "target_properties": [
            "chargeability_mv_v",
            "apparent_resistivity_ohm_m"
        ],
        "input_summary": {
            "observation_row_count": observation_rows.len(),
            "pseudosection_row_count": pseudo_rows.len(),
            "mesh_row_count": mesh_rows.len(),
            "electrode_count": observations.get("electrodes").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0)
        },
        "parameters": {
            "influence_radius_m": influence_radius_m,
            "idw_power": power,
            "min_support": min_support,
            "conductivity_bias": conductivity_bias
        },
        "observations": {
            "schema_id": "geophysics.ip_observations.v1",
            "processing_stage": observations.get("processing_stage").cloned().unwrap_or(Value::Null),
            "quality": observations.get("quality").cloned().unwrap_or(Value::Null),
            "rows": observation_rows
        },
        "derived_inputs": {
            "pseudosection_schema_id": "geophysics.ip_pseudosection.v1",
            "pseudosection_rows": pseudo_rows
        },
        "mesh": {
            "schema_id": "geophysics.ip_mesh.v1",
            "mesh_kind": mesh.get("mesh_kind").cloned().unwrap_or(Value::Null),
            "grid": mesh.get("grid").cloned().unwrap_or(Value::Null),
            "domain": mesh.get("domain").cloned().unwrap_or(Value::Null),
            "rows": mesh_rows
        }
    });
    let report_payload = json!({
        "schema_id": "report.ip_inversion_input.v1",
        "type": "ip_inversion_input_report",
        "summary": {
            "observation_row_count": inversion_input_payload.pointer("/input_summary/observation_row_count").and_then(|v| v.as_u64()).unwrap_or(0),
            "pseudosection_row_count": inversion_input_payload.pointer("/input_summary/pseudosection_row_count").and_then(|v| v.as_u64()).unwrap_or(0),
            "mesh_row_count": inversion_input_payload.pointer("/input_summary/mesh_row_count").and_then(|v| v.as_u64()).unwrap_or(0),
            "solver_profile": "preview_interpolation_v1"
        },
        "parameters": inversion_input_payload.get("parameters").cloned().unwrap_or(Value::Null),
        "notes": [
            "This contract is the hardened handoff into IP modelling so solver nodes do not scrape UI or arbitrary upstream payloads.",
            "Current preview modelling still uses pseudosection-derived rows, but future Rust solvers can consume the same prepared contract."
        ]
    });

    let input_key = format!(
        "graphs/{}/nodes/{}/ip_inversion_input.json",
        job.graph_id, job.node_id
    );
    let report_key = format!(
        "graphs/{}/nodes/{}/ip_inversion_input_report.json",
        job.graph_id, job.node_id
    );
    let input_ref = super::runtime::write_artifact(
        ctx,
        &input_key,
        &serde_json::to_vec(&inversion_input_payload)?,
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
        output_artifact_refs: vec![input_ref.clone(), report_ref.clone()],
        content_hashes: vec![input_ref.content_hash, report_ref.content_hash],
        error_message: None,
    })
}

pub async fn run_ip_invert(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payloads = load_input_payloads(ctx, job).await?;
    let inversion_input = read_ip_inversion_input_payload(&job.input_artifact_refs, &payloads)
        .ok_or_else(|| {
            NodeError::InvalidConfig(
                "ip_invert requires upstream geophysics.ip_inversion_input.v1".into(),
            )
        })?;

    let crs = inversion_input
        .get("crs")
        .cloned()
        .and_then(|v| serde_json::from_value::<CrsRecord>(v).ok())
        .unwrap_or_else(|| CrsRecord::epsg(32630));
    let pseudo_rows = inversion_input
        .pointer("/derived_inputs/pseudosection_rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let mesh_rows = inversion_input
        .pointer("/mesh/rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if pseudo_rows.is_empty() || mesh_rows.is_empty() {
        return Err(NodeError::InvalidConfig(
            "ip_invert requires non-empty prepared pseudosection and mesh rows".into(),
        ));
    }

    let influence_radius_m = inversion_input
        .pointer("/parameters/influence_radius_m")
        .and_then(parse_num)
        .unwrap_or(90.0)
        .clamp(10.0, 1000.0);
    let power = inversion_input
        .pointer("/parameters/idw_power")
        .and_then(parse_num)
        .unwrap_or(2.0)
        .clamp(0.5, 8.0);
    let min_support = inversion_input
        .pointer("/parameters/min_support")
        .and_then(|v| v.as_u64())
        .unwrap_or(2) as usize;
    let conductivity_bias = inversion_input
        .pointer("/parameters/conductivity_bias")
        .and_then(parse_num)
        .unwrap_or(0.35)
        .clamp(0.0, 1.0);
    let smoothing_lambda = job
        .output_spec
        .pointer("/node_ui/smoothing_lambda")
        .and_then(parse_num)
        .unwrap_or(0.85)
        .clamp(0.0, 10.0);
    let depth_weight = job
        .output_spec
        .pointer("/node_ui/depth_weight")
        .and_then(parse_num)
        .unwrap_or(0.2)
        .clamp(0.0, 4.0);
    let iterations = job
        .output_spec
        .pointer("/node_ui/max_iterations")
        .and_then(|v| v.as_u64())
        .unwrap_or(6) as usize;

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

    let mut mesh_meta = Vec::<(Map<String, Value>, f64, f64, f64, f64, f64, f64)>::new();
    let mut initial_charge = Vec::<f64>::new();
    let mut initial_log_rho = Vec::<f64>::new();
    let mut confidences = Vec::<f64>::new();
    let mut supports = Vec::<usize>::new();
    let mut nearest_dists = Vec::<f64>::new();
    let mut index_by_cell = HashMap::<(i64, i64, i64), usize>::new();

    for row in &mesh_rows {
        let Some(obj) = row.as_object() else { continue };
        let x = obj.get("x").and_then(parse_num).unwrap_or(0.0);
        let y = obj.get("y").and_then(parse_num).unwrap_or(0.0);
        let z = obj.get("z").and_then(parse_num).unwrap_or(0.0);
        let dx = obj.get("dx").and_then(parse_num).unwrap_or(25.0);
        let dy = obj.get("dy").and_then(parse_num).unwrap_or(25.0);
        let dz = obj.get("dz").and_then(parse_num).unwrap_or(15.0);
        let ix = obj.get("ix").and_then(|v| v.as_i64()).unwrap_or(0);
        let iy = obj.get("iy").and_then(|v| v.as_i64()).unwrap_or(0);
        let iz = obj.get("iz").and_then(|v| v.as_i64()).unwrap_or(0);

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

        let idx = mesh_meta.len();
        index_by_cell.insert((ix, iy, iz), idx);
        mesh_meta.push((obj.clone(), x, y, z, dx, dy, dz));
        initial_charge.push(chargeability_mv_v);
        initial_log_rho.push(apparent_resistivity_ohm_m.ln());
        confidences.push(confidence);
        supports.push(support);
        nearest_dists.push(min_dist);
    }

    if mesh_meta.is_empty() {
        return Err(NodeError::InvalidConfig(
            "ip_invert could not derive any supported model cells from the inversion input".into(),
        ));
    }

    let mut charge_model = initial_charge.clone();
    let mut log_rho_model = initial_log_rho.clone();
    let mut max_delta_charge = 0.0_f64;
    let mut max_delta_log_rho = 0.0_f64;

    for _ in 0..iterations {
        let prev_charge = charge_model.clone();
        let prev_log_rho = log_rho_model.clone();
        max_delta_charge = 0.0;
        max_delta_log_rho = 0.0;
        for (idx, (obj, _, _, _, _, _, _)) in mesh_meta.iter().enumerate() {
            let ix = obj.get("ix").and_then(|v| v.as_i64()).unwrap_or(0);
            let iy = obj.get("iy").and_then(|v| v.as_i64()).unwrap_or(0);
            let iz = obj.get("iz").and_then(|v| v.as_i64()).unwrap_or(0);
            let mut neigh_charge = 0.0;
            let mut neigh_log_rho = 0.0;
            let mut neigh_w = 0.0;
            let neighbors = [
                (ix - 1, iy, iz),
                (ix + 1, iy, iz),
                (ix, iy - 1, iz),
                (ix, iy + 1, iz),
                (ix, iy, iz - 1),
                (ix, iy, iz + 1),
            ];
            for nkey in neighbors {
                let Some(nidx) = index_by_cell.get(&nkey).copied() else {
                    continue;
                };
                let vertical_factor = if nkey.2 != iz {
                    1.0 + depth_weight
                } else {
                    1.0
                };
                neigh_charge += prev_charge[nidx] * vertical_factor;
                neigh_log_rho += prev_log_rho[nidx] * vertical_factor;
                neigh_w += vertical_factor;
            }
            if neigh_w <= 0.0 {
                continue;
            }
            let data_weight = (0.4 + 1.6 * confidences[idx]).clamp(0.2, 2.0);
            let smooth_weight = smoothing_lambda * (1.0 + (1.0 - confidences[idx]) * 0.75);
            let next_charge = (data_weight * initial_charge[idx]
                + smooth_weight * (neigh_charge / neigh_w))
                / (data_weight + smooth_weight);
            let next_log_rho = (data_weight * initial_log_rho[idx]
                + smooth_weight * (neigh_log_rho / neigh_w))
                / (data_weight + smooth_weight);
            max_delta_charge = max_delta_charge.max((next_charge - prev_charge[idx]).abs());
            max_delta_log_rho = max_delta_log_rho.max((next_log_rho - prev_log_rho[idx]).abs());
            charge_model[idx] = next_charge;
            log_rho_model[idx] = next_log_rho;
        }
    }

    let mut blocks = Vec::<Value>::new();
    let mut centers = Vec::<Value>::new();
    let mut result_rows = Vec::<Value>::new();
    let mut charge_vals = Vec::<f64>::new();
    let mut rho_vals = Vec::<f64>::new();
    let mut conf_vals = Vec::<f64>::new();

    for (idx, (obj, x, y, z, dx, dy, dz)) in mesh_meta.iter().enumerate() {
        let chargeability_mv_v = charge_model[idx];
        let apparent_resistivity_ohm_m = log_rho_model[idx].exp();
        let conductivity_proxy = 1.0 / apparent_resistivity_ohm_m.max(1e-6);
        let normalized_conductivity = conductivity_proxy / (1.0 + conductivity_proxy);
        let target_index = (chargeability_mv_v * (1.0 - conductivity_bias))
            + (1000.0 * normalized_conductivity * conductivity_bias);
        let confidence = confidences[idx];
        let support = supports[idx];
        let min_dist = nearest_dists[idx];

        let attrs = json!({
            "cell_id": obj.get("cell_id").cloned().unwrap_or(Value::Null),
            "chargeability_mv_v": chargeability_mv_v,
            "apparent_resistivity_ohm_m": apparent_resistivity_ohm_m,
            "confidence": confidence,
            "support_count": support,
            "nearest_sample_distance_m": min_dist,
            "conductivity_proxy_s_m": conductivity_proxy,
            "target_index": target_index,
            "solver_profile": "regularized_surrogate_inversion_v1"
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
            "attributes": attrs.clone()
        }));
        centers.push(json!({
            "x": x,
            "y": y,
            "z": z,
            "attributes": attrs
        }));
    }

    let (charge_min, charge_max) = min_max(&charge_vals);
    let (rho_min, rho_max) = min_max(&rho_vals);
    let (conf_min, conf_max) = min_max(&conf_vals);
    let voxels_payload = json!({
        "schema_id": "scene3d.block_voxels.v1",
        "type": "ip_inversion_voxels",
        "crs": crs,
        "display_contract": {
            "renderer": "block_voxels",
            "display_pointer": "scene3d.block_voxels",
            "editable": ["visible", "opacity", "measure", "palette"]
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
            "palette": "inferno"
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
            "model_kind": "tdip_inversion_surrogate"
        }
    });
    let centers_payload = json!({
        "schema_id": "point_set.ip_inversion_centers.v1",
        "type": "ip_inversion_centers",
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
        "schema_id": "geophysics.ip_inversion_result.v1",
        "schema_version": 1,
        "type": "ip_inversion_result",
        "crs": crs,
        "model_kind": "regularized_surrogate_inversion",
        "model_family": "electrical_ip",
        "mesh_schema_id": "geophysics.ip_mesh.v1",
        "physical_properties": [
            "chargeability_mv_v",
            "apparent_resistivity_ohm_m",
            "conductivity_proxy_s_m",
            "target_index"
        ],
        "confidence_measures": [
            "confidence",
            "support_count",
            "nearest_sample_distance_m"
        ],
        "display_contract": {
            "renderer":"table",
            "display_pointer":"table.rows",
            "editable":["visible"]
        },
        "meta": {
            "row_count": result_rows.len(),
            "columns": table_columns(&result_rows),
            "source_pseudosection_rows": pseudo_rows.len(),
            "source_mesh_rows": mesh_rows.len(),
            "solver_profile": "regularized_surrogate_inversion_v1"
        },
        "rows": result_rows
    });
    let diagnostics_payload = json!({
        "schema_id": "geophysics.ip_diagnostics.v1",
        "schema_version": 1,
        "type": "ip_diagnostics",
        "model_kind": "regularized_surrogate_inversion",
        "summary": {
            "input_sample_count": pseudo_points.len(),
            "model_cell_count": result_rows.len(),
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
            "conductivity_bias": conductivity_bias,
            "smoothing_lambda": smoothing_lambda,
            "depth_weight": depth_weight,
            "max_iterations": iterations
        },
        "coverage": {
            "retained_fraction": if mesh_rows.is_empty() { 0.0 } else { (result_rows.len() as f64) / (mesh_rows.len() as f64) },
            "dropped_cell_count": mesh_rows.len().saturating_sub(result_rows.len())
        },
        "confidence": {
            "min": conf_min,
            "max": conf_max
        },
        "convergence": {
            "max_delta_chargeability": max_delta_charge,
            "max_delta_log_resistivity": max_delta_log_rho,
            "iterations": iterations
        },
        "notes": [
            "This is a first-pass regularized surrogate inversion in Rust, using a local data term plus mesh-neighborhood smoothing.",
            "It is more solver-like than the preview interpolation, but it is still not a physics-complete TDIP inversion."
        ]
    });
    let report_payload = json!({
        "schema_id": "report.ip_inversion.v1",
        "type": "ip_inversion_report",
        "summary": {
            "voxel_count": result_rows.len(),
            "chargeability_min_mv_v": charge_min,
            "chargeability_max_mv_v": charge_max,
            "apparent_resistivity_min_ohm_m": rho_min,
            "apparent_resistivity_max_ohm_m": rho_max,
            "confidence_min": conf_min,
            "confidence_max": conf_max
        },
        "parameters": diagnostics_payload.get("parameters").cloned().unwrap_or(Value::Null),
        "convergence": diagnostics_payload.get("convergence").cloned().unwrap_or(Value::Null),
        "notes": [
            "Downstream slice and 3D viewer nodes can consume this inversion result without any schema change.",
            "Compare this output with the preview node to judge how much regularization helps visibility and stability."
        ]
    });

    let voxels_key = format!(
        "graphs/{}/nodes/{}/ip_inversion_voxels.json",
        job.graph_id, job.node_id
    );
    let centers_key = format!(
        "graphs/{}/nodes/{}/ip_inversion_centers.json",
        job.graph_id, job.node_id
    );
    let model_key = format!(
        "graphs/{}/nodes/{}/ip_inversion_result.json",
        job.graph_id, job.node_id
    );
    let diagnostics_key = format!(
        "graphs/{}/nodes/{}/ip_inversion_diagnostics.json",
        job.graph_id, job.node_id
    );
    let report_key = format!(
        "graphs/{}/nodes/{}/ip_inversion_report.json",
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
    let diagnostics_ref = super::runtime::write_artifact(
        ctx,
        &diagnostics_key,
        &serde_json::to_vec(&diagnostics_payload)?,
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
            diagnostics_ref.clone(),
            report_ref.clone(),
        ],
        content_hashes: vec![
            voxels_ref.content_hash,
            centers_ref.content_hash,
            model_ref.content_hash,
            diagnostics_ref.content_hash,
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
    let inversion_input = read_ip_inversion_input_payload(&job.input_artifact_refs, &payloads)
        .ok_or_else(|| {
            NodeError::InvalidConfig(
                "ip_inversion_preview requires upstream geophysics.ip_inversion_input.v1".into(),
            )
        })?;

    let crs = inversion_input
        .get("crs")
        .cloned()
        .and_then(|v| serde_json::from_value::<CrsRecord>(v).ok())
        .unwrap_or_else(|| CrsRecord::epsg(32630));

    let pseudo_rows = inversion_input
        .pointer("/derived_inputs/pseudosection_rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let mesh_rows = inversion_input
        .pointer("/mesh/rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if pseudo_rows.is_empty() || mesh_rows.is_empty() {
        return Err(NodeError::InvalidConfig(
            "ip_inversion_preview requires non-empty prepared pseudosection and mesh rows".into(),
        ));
    }

    let influence_radius_m = inversion_input
        .pointer("/parameters/influence_radius_m")
        .and_then(parse_num)
        .unwrap_or(90.0)
        .clamp(10.0, 1000.0);
    let power = inversion_input
        .pointer("/parameters/idw_power")
        .and_then(parse_num)
        .unwrap_or(2.0)
        .clamp(0.5, 8.0);
    let min_support = inversion_input
        .pointer("/parameters/min_support")
        .and_then(|v| v.as_u64())
        .unwrap_or(2) as usize;
    let conductivity_bias = inversion_input
        .pointer("/parameters/conductivity_bias")
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
            "editable": ["visible", "opacity", "measure", "palette"]
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
            "palette": "inferno"
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
        "schema_id": "geophysics.ip_inversion_result.v1",
        "schema_version": 1,
        "type": "ip_inversion_result",
        "crs": crs,
        "model_kind": "preview_interpolation",
        "model_family": "electrical_ip",
        "mesh_schema_id": "geophysics.ip_mesh.v1",
        "physical_properties": [
            "chargeability_mv_v",
            "apparent_resistivity_ohm_m",
            "conductivity_proxy_s_m",
            "target_index"
        ],
        "confidence_measures": [
            "confidence",
            "support_count",
            "nearest_sample_distance_m"
        ],
        "display_contract": {
            "renderer":"table",
            "display_pointer":"table.rows",
            "editable":["visible"]
        },
        "meta": {
            "row_count": result_rows.len(),
            "columns": table_columns(&result_rows),
            "source_pseudosection_rows": pseudo_rows.len(),
            "source_mesh_rows": mesh_rows.len()
        },
        "rows": result_rows
    });
    let diagnostics_payload = json!({
        "schema_id": "geophysics.ip_diagnostics.v1",
        "schema_version": 1,
        "type": "ip_diagnostics",
        "model_kind": "preview_interpolation",
        "summary": {
            "input_sample_count": pseudo_points.len(),
            "model_cell_count": result_rows.len(),
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
        "coverage": {
            "retained_fraction": if mesh_rows.is_empty() { 0.0 } else { (result_rows.len() as f64) / (mesh_rows.len() as f64) },
            "dropped_cell_count": mesh_rows.len().saturating_sub(result_rows.len())
        },
        "confidence": {
            "min": conf_min,
            "max": conf_max
        },
        "notes": [
            "This diagnostics artifact describes preview-model support and confidence separately from display styling.",
            "Treat low-support or long-distance cells as interpretive context rather than resolved inversion truth."
        ]
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
    let diagnostics_key = format!(
        "graphs/{}/nodes/{}/ip_inversion_diagnostics.json",
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
    let diagnostics_ref = super::runtime::write_artifact(
        ctx,
        &diagnostics_key,
        &serde_json::to_vec(&diagnostics_payload)?,
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
            diagnostics_ref.clone(),
            report_ref.clone(),
        ],
        content_hashes: vec![
            voxels_ref.content_hash,
            centers_ref.content_hash,
            model_ref.content_hash,
            diagnostics_ref.content_hash,
            report_ref.content_hash,
        ],
        error_message: None,
    })
}

pub async fn run_ip_section_slice(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payloads = load_input_payloads(ctx, job).await?;
    let inversion = read_ip_inversion_result_payload(&job.input_artifact_refs, &payloads);
    let pseudo = read_ip_pseudosection_payload(&job.input_artifact_refs, &payloads);
    if inversion.is_none() && pseudo.is_none() {
        return Err(NodeError::InvalidConfig(
            "ip_section_slice requires upstream geophysics.ip_inversion_result.v1 or geophysics.ip_pseudosection.v1".into(),
        ));
    }

    let crs = inversion
        .and_then(|v| {
            v.get("crs")
                .cloned()
                .and_then(|x| serde_json::from_value::<CrsRecord>(x).ok())
        })
        .or_else(|| {
            pseudo.and_then(|v| {
                v.get("crs")
                    .cloned()
                    .and_then(|x| serde_json::from_value::<CrsRecord>(x).ok())
            })
        })
        .unwrap_or_else(|| CrsRecord::epsg(32630));

    let measure_names = if inversion.is_some() {
        vec![
            "chargeability_mv_v",
            "apparent_resistivity_ohm_m",
            "conductivity_proxy_s_m",
            "target_index",
            "confidence",
        ]
    } else {
        vec![
            "chargeability_mv_v",
            "apparent_resistivity_ohm_m",
            "pseudo_depth_m",
        ]
    };

    let mut rows = Vec::<Map<String, Value>>::new();
    if let Some(inv) = inversion {
        for row in inv
            .get("rows")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
        {
            if let Some(obj) = row.as_object() {
                rows.push(obj.clone());
            }
        }
    } else if let Some(ps) = pseudo {
        for row in ps
            .get("rows")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
        {
            if let Some(obj) = row.as_object() {
                rows.push(obj.clone());
            }
        }
    }
    if rows.is_empty() {
        return Err(NodeError::InvalidConfig(
            "ip_section_slice received an empty upstream dataset".into(),
        ));
    }

    let mut xs = Vec::<f64>::new();
    let mut ys = Vec::<f64>::new();
    let mut zs = Vec::<f64>::new();
    for row in &rows {
        let x = row
            .get("x")
            .or_else(|| row.get("pseudo_x"))
            .and_then(parse_num)
            .unwrap_or(0.0);
        let y = row
            .get("y")
            .or_else(|| row.get("pseudo_y"))
            .and_then(parse_num)
            .unwrap_or(0.0);
        let z = row
            .get("z")
            .or_else(|| row.get("pseudo_z"))
            .and_then(parse_num)
            .unwrap_or(0.0);
        xs.push(x);
        ys.push(y);
        zs.push(z);
    }
    let cx = xs.iter().sum::<f64>() / xs.len() as f64;
    let cy = ys.iter().sum::<f64>() / ys.len() as f64;
    let mut sxx = 0.0;
    let mut syy = 0.0;
    let mut sxy = 0.0;
    for (x, y) in xs.iter().zip(ys.iter()) {
        let dx = *x - cx;
        let dy = *y - cy;
        sxx += dx * dx;
        syy += dy * dy;
        sxy += dx * dy;
    }
    let theta = 0.5 * (2.0 * sxy).atan2(sxx - syy);
    let ux = theta.cos();
    let uy = theta.sin();

    let mut projected = Vec::<(f64, f64, &Map<String, Value>)>::new();
    for row in &rows {
        let x = row
            .get("x")
            .or_else(|| row.get("pseudo_x"))
            .and_then(parse_num)
            .unwrap_or(0.0);
        let y = row
            .get("y")
            .or_else(|| row.get("pseudo_y"))
            .and_then(parse_num)
            .unwrap_or(0.0);
        let z = row
            .get("z")
            .or_else(|| row.get("pseudo_z"))
            .and_then(parse_num)
            .unwrap_or(0.0);
        let s = (x - cx) * ux + (y - cy) * uy;
        projected.push((s, z, row));
    }
    let s_min = projected
        .iter()
        .map(|(s, _, _)| *s)
        .fold(f64::INFINITY, f64::min);
    let s_max = projected
        .iter()
        .map(|(s, _, _)| *s)
        .fold(f64::NEG_INFINITY, f64::max);
    let z_min = zs.iter().copied().fold(f64::INFINITY, f64::min);
    let z_max = zs.iter().copied().fold(f64::NEG_INFINITY, f64::max);

    let nx = job
        .output_spec
        .pointer("/node_ui/nx")
        .and_then(|v| v.as_u64())
        .unwrap_or(96) as usize;
    let nz = job
        .output_spec
        .pointer("/node_ui/nz")
        .and_then(|v| v.as_u64())
        .unwrap_or(56) as usize;
    let lateral_margin_m = job
        .output_spec
        .pointer("/node_ui/lateral_margin_m")
        .and_then(parse_num)
        .unwrap_or(10.0)
        .max(0.0);
    let vertical_margin_m = job
        .output_spec
        .pointer("/node_ui/vertical_margin_m")
        .and_then(parse_num)
        .unwrap_or(10.0)
        .max(0.0);

    let s0 = s_min - lateral_margin_m;
    let s1 = s_max + lateral_margin_m;
    let z0 = z_max + vertical_margin_m;
    let z1 = z_min - vertical_margin_m;
    let ds = ((s1 - s0) / nx.max(1) as f64).max(1.0);
    let dz = ((z0 - z1).abs() / nz.max(1) as f64).max(1.0);

    let mut grids = Map::<String, Value>::new();
    for measure in &measure_names {
        let mut values = Vec::<Value>::with_capacity(nx * nz);
        for iz in 0..nz {
            let zc = z0 - ((iz as f64) + 0.5) * dz;
            for ix in 0..nx {
                let sc = s0 + ((ix as f64) + 0.5) * ds;
                let mut sum_w = 0.0;
                let mut sum_v = 0.0;
                for (s, z, row) in &projected {
                    let Some(v) = row.get(*measure).and_then(parse_num) else {
                        continue;
                    };
                    let dist = (((sc - *s) / ds.max(1.0)).powi(2)
                        + ((zc - *z) / dz.max(1.0)).powi(2))
                    .sqrt();
                    let w = 1.0 / dist.max(0.6).powf(2.0);
                    sum_w += w;
                    sum_v += w * v;
                }
                if sum_w > 0.0 {
                    values.push(json!(sum_v / sum_w));
                } else {
                    values.push(Value::Null);
                }
            }
        }
        grids.insert((*measure).to_string(), Value::Array(values));
    }

    let plane_payload = json!({
        "schema_id": "scene3d.section_plane.v1",
        "type": "ip_section_plane",
        "crs": crs,
        "measure_candidates": measure_names,
        "display_contract": {
            "renderer": "section_plane",
            "display_pointer": "scene3d.section_plane",
            "editable": ["visible", "opacity", "measure", "palette"]
        },
        "section": {
            "center_x": cx,
            "center_y": cy,
            "azimuth_deg": theta.to_degrees(),
            "s_min": s0,
            "s_max": s1,
            "z_top": z0,
            "z_bottom": z1,
            "nx": nx,
            "nz": nz,
            "cell_s_m": ds,
            "cell_z_m": dz,
            "measure_grids": grids
        }
    });
    let report_payload = json!({
        "schema_id": "report.ip_section_slice.v1",
        "type": "ip_section_slice_report",
        "summary": {
            "input_row_count": rows.len(),
            "nx": nx,
            "nz": nz,
            "azimuth_deg": theta.to_degrees()
        },
        "notes": [
            "Section plane follows the dominant survey trend in plan and renders a vertical coloured slice.",
            "Use this for rapid section interpretation alongside the voxel preview."
        ]
    });

    let plane_key = format!(
        "graphs/{}/nodes/{}/ip_section_plane.json",
        job.graph_id, job.node_id
    );
    let report_key = format!(
        "graphs/{}/nodes/{}/ip_section_slice_report.json",
        job.graph_id, job.node_id
    );
    let plane_ref = super::runtime::write_artifact(
        ctx,
        &plane_key,
        &serde_json::to_vec(&plane_payload)?,
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
        output_artifact_refs: vec![plane_ref.clone(), report_ref.clone()],
        content_hashes: vec![plane_ref.content_hash, report_ref.content_hash],
        error_message: None,
    })
}
