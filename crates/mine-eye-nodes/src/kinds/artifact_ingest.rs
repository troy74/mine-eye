use std::collections::BTreeSet;

use mine_eye_types::{CrsRecord, JobEnvelope, JobResult, JobStatus};
use serde_json::{json, Value};

use crate::executor::ExecutionContext;
use crate::NodeError;

fn parse_num(v: Option<&Value>) -> Option<f64> {
    match v {
        Some(Value::Number(n)) => n.as_f64().filter(|x| x.is_finite()),
        Some(Value::String(s)) => s.trim().replace(',', ".").parse::<f64>().ok().filter(|x| x.is_finite()),
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
    if format == "tsv" { b'\t' } else { b',' }
}

pub async fn run_artifact_ingest(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let ui = job
        .output_spec
        .pointer("/node_ui")
        .and_then(|v| v.as_object())
        .ok_or_else(|| NodeError::InvalidConfig("artifact_ingest missing node_ui".into()))?;

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
        .ok_or_else(|| NodeError::InvalidConfig("artifact_ingest missing csv_artifact_key".into()))?;

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

    let mut headers: Vec<String> = Vec::new();
    let mut row_count_estimate = 0usize;
    let mut preview_rows: Vec<Vec<String>> = Vec::new();
    let mut tail_rows: Vec<Vec<String>> = Vec::new();

    if format == "json" || format == "geojson" {
        if let Ok(v) = serde_json::from_slice::<Value>(&raw) {
            match v {
                Value::Array(arr) => {
                    row_count_estimate = arr.len();
                    for r in arr.iter().take(8) {
                        if let Some(o) = r.as_object() {
                            for k in o.keys() {
                                headers.push(k.clone());
                            }
                            preview_rows.push(
                                o.iter()
                                    .map(|(k, v)| format!("{}={}", k, v))
                                    .collect::<Vec<_>>(),
                            );
                        }
                    }
                }
                Value::Object(obj) => {
                    if let Some(features) = obj.get("features").and_then(|v| v.as_array()) {
                        row_count_estimate = features.len();
                    } else {
                        row_count_estimate = 1;
                    }
                    headers = obj.keys().cloned().collect();
                    preview_rows = vec![headers.iter().map(|k| format!("{}=...", k)).collect()];
                }
                _ => {
                    row_count_estimate = 1;
                }
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
            row_count_estimate += 1;
            let row = rec.iter().map(|s| s.to_string()).collect::<Vec<_>>();
            if preview_rows.len() < 8 {
                preview_rows.push(row.clone());
            }
            tail_rows.push(row);
            if tail_rows.len() > 6 {
                tail_rows.remove(0);
            }
        }
    }

    if headers.is_empty() {
        let mut uniq = BTreeSet::new();
        for line in sample_text.lines().take(30) {
            for token in line.split_whitespace().take(16) {
                if token.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
                    uniq.insert(token.to_string());
                }
            }
        }
        headers = uniq.into_iter().take(24).collect();
    }

    let preview_text = ui
        .get("csv_preview_text")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            let mut out = String::new();
            if !headers.is_empty() {
                out.push_str(&headers.join(" | "));
                out.push('\n');
            }
            for r in preview_rows.iter().take(6) {
                out.push_str(&r.join(" | "));
                out.push('\n');
            }
            if !tail_rows.is_empty() {
                out.push_str("...\n");
                for r in tail_rows.iter().rev().take(4).rev() {
                    out.push_str(&r.join(" | "));
                    out.push('\n');
                }
            }
            if out.trim().is_empty() {
                sample_text.lines().take(20).collect::<Vec<_>>().join("\n")
            } else {
                out
            }
        });

    let pointer_doc = json!({
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
        "headers": headers,
        "preview_rows": preview_rows,
        "tail_rows": tail_rows,
        "preview_text": preview_text,
        "line_count_estimate": row_count_estimate,
    });

    let report_doc = json!({
        "schema_id": "report.artifact_ingest.v1",
        "type": "artifact_ingest_report",
        "summary": {
            "source_key": source_key,
            "source_content_hash": source_hash,
            "format": pointer_doc.pointer("/source/format").cloned().unwrap_or(json!("unknown")),
            "delimiter": pointer_doc.pointer("/source/delimiter").cloned().unwrap_or(json!(",")),
            "headers_count": pointer_doc.get("headers").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0),
            "line_count_estimate": row_count_estimate,
            "size_bytes": raw.len(),
        },
        "notes": [
            "Pointer-first ingestion: downstream nodes should read source artifact via key+hash.",
            "Preview rows are sampled for mapping/QA and do not replace full-file processing."
        ]
    });

    let pointer_key = format!(
        "graphs/{}/nodes/{}/artifact_ingest_table_pointer.json",
        job.graph_id, job.node_id
    );
    let report_key = format!(
        "graphs/{}/nodes/{}/artifact_ingest_report.json",
        job.graph_id, job.node_id
    );

    let pointer_bytes = serde_json::to_vec(&pointer_doc)?;
    let report_bytes = serde_json::to_vec(&report_doc)?;

    let pointer_art = super::runtime::write_artifact(
        ctx,
        &pointer_key,
        &pointer_bytes,
        Some("application/json"),
    )
    .await?;
    let report_art =
        super::runtime::write_artifact(ctx, &report_key, &report_bytes, Some("application/json"))
            .await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![pointer_art.clone(), report_art.clone()],
        content_hashes: vec![pointer_art.content_hash, report_art.content_hash],
        error_message: None,
    })
}
