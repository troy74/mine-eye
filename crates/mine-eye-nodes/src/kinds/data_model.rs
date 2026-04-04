use mine_eye_types::{JobEnvelope, JobResult, JobStatus};

use crate::executor::ExecutionContext;
use crate::NodeError;

pub async fn run_data_model_transform(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let source_key = job
        .output_spec
        .pointer("/node_ui/source_key")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let select_columns: Vec<String> = job
        .output_spec
        .pointer("/node_ui/select_columns")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let rename_map = job
        .output_spec
        .pointer("/node_ui/rename_map")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    let derive_constants = job
        .output_spec
        .pointer("/node_ui/derive_constants")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();

    let mut rows: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
    let mut input_keys: Vec<String> = Vec::new();
    for ar in &job.input_artifact_refs {
        input_keys.push(ar.key.clone());
        let v = super::runtime::read_json_artifact(ctx, &ar.key).await?;
        let mut push_arr = |arr: &Vec<serde_json::Value>| {
            for r in arr {
                if let Some(obj) = r.as_object() {
                    rows.push(obj.clone());
                }
            }
        };
        if let Some(arr) = v.as_array() {
            push_arr(arr);
            continue;
        }
        if let Some(obj) = v.as_object() {
            if let Some(sk) = &source_key {
                if let Some(arr) = obj.get(sk).and_then(|x| x.as_array()) {
                    push_arr(arr);
                    continue;
                }
            }
            let preferred = [
                "rows",
                "assays",
                "assay_points",
                "collars",
                "surveys",
                "segments",
                "points",
                "samples",
                "surface_samples",
                "trajectory",
            ];
            for k in preferred {
                if let Some(arr) = obj.get(k).and_then(|x| x.as_array()) {
                    push_arr(arr);
                    break;
                }
            }
        }
    }

    let mut out_rows: Vec<serde_json::Value> = Vec::with_capacity(rows.len());
    for mut row in rows {
        for (old_key, new_val) in &rename_map {
            let Some(new_key) = new_val.as_str() else {
                continue;
            };
            if old_key == new_key {
                continue;
            }
            if let Some(v) = row.remove(old_key) {
                row.insert(new_key.to_string(), v);
            }
        }
        for (k, v) in &derive_constants {
            row.insert(k.clone(), v.clone());
        }
        if !select_columns.is_empty() {
            let mut selected = serde_json::Map::new();
            for c in &select_columns {
                if let Some(v) = row.get(c) {
                    selected.insert(c.clone(), v.clone());
                }
            }
            row = selected;
        }
        out_rows.push(serde_json::Value::Object(row));
    }
    let columns = if out_rows.is_empty() {
        Vec::<String>::new()
    } else {
        let mut set = std::collections::BTreeSet::<String>::new();
        for r in &out_rows {
            if let Some(o) = r.as_object() {
                for k in o.keys() {
                    set.insert(k.clone());
                }
            }
        }
        set.into_iter().collect::<Vec<_>>()
    };

    let out = serde_json::json!({
        "schema_id": "data_model.table.v1",
        "schema_version": 1,
        "kind": "data_model_transform",
        "display_contract": {
            "renderer":"table",
            "display_pointer":"table.rows",
            "editable":["visible"]
        },
        "meta": {
            "row_count": out_rows.len(),
            "columns": columns,
            "source_key": source_key,
            "input_artifact_keys": input_keys
        },
        "rows": out_rows
    });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!(
        "graphs/{}/nodes/{}/data_model_table.json",
        job.graph_id, job.node_id
    );
    let artifact =
        super::runtime::write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}
