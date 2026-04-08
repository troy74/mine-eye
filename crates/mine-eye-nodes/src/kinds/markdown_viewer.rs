use std::env;

use mine_eye_types::{JobEnvelope, JobResult, JobStatus};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde_json::{json, Value};

use crate::executor::ExecutionContext;
use crate::NodeError;

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn n(v: &Value) -> Option<f64> {
    match v {
        Value::Number(x) => x.as_f64().filter(|k| k.is_finite()),
        Value::String(s) => s.trim().parse::<f64>().ok().filter(|k| k.is_finite()),
        _ => None,
    }
}

fn render_simple_html_from_markdown(title: &str, markdown: &str) -> String {
    let mut body = String::new();
    body.push_str(&format!("<h1>{}</h1>", escape_html(title)));
    for line in markdown.lines() {
        let t = line.trim_end();
        if let Some(rest) = t.strip_prefix("# ") {
            body.push_str(&format!("<h1>{}</h1>", escape_html(rest)));
        } else if let Some(rest) = t.strip_prefix("## ") {
            body.push_str(&format!("<h2>{}</h2>", escape_html(rest)));
        } else if let Some(rest) = t.strip_prefix("### ") {
            body.push_str(&format!("<h3>{}</h3>", escape_html(rest)));
        } else if let Some(rest) = t.strip_prefix("- ") {
            body.push_str(&format!("<li>{}</li>", escape_html(rest)));
        } else if t.is_empty() {
            body.push_str("<br/>");
        } else {
            body.push_str(&format!("<p>{}</p>", escape_html(t)));
        }
    }
    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"/><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"/><title>{}</title><style>body{{font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:20px;line-height:1.5;color:#0f172a}}h1,h2,h3{{margin-top:1em;margin-bottom:.4em}}p{{margin:.35em 0}}li{{margin:.15em 0}}code{{background:#f3f4f6;padding:1px 4px;border-radius:4px}}</style></head><body>{}</body></html>",
        escape_html(title),
        body
    )
}

fn fmt_num(x: Option<f64>, digits: usize) -> String {
    match x {
        Some(v) => format!("{:.1$}", v, digits),
        None => "n/a".to_string(),
    }
}

fn compact_summary_payload(source: &Value) -> Value {
    let semantic = source.pointer("/semantic_summary").cloned();
    let summary = source.pointer("/summary").cloned();
    let hist = source
        .pointer("/grade_histogram")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().take(6).cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    let notes = source
        .pointer("/notes")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().take(4).cloned().collect::<Vec<_>>())
        .unwrap_or_default();

    if semantic.is_some() || summary.is_some() {
        json!({
            "semantic_summary": semantic,
            "summary": summary,
            "grade_histogram_top_bins": hist,
            "notes": notes,
        })
    } else {
        source.clone()
    }
}

fn score_semantic_report(v: &Value) -> i32 {
    let mut s = 0i32;
    let schema = v.pointer("/schema_id").and_then(|x| x.as_str()).unwrap_or("");
    let typ = v.pointer("/type").and_then(|x| x.as_str()).unwrap_or("");
    if schema == "report.block_resource.v2" {
        s += 1000;
    }
    if typ == "block_resource_report" {
        s += 600;
    }
    if v.pointer("/semantic_summary").is_some() {
        s += 500;
    }
    if v.pointer("/summary/total_tonnage_t").and_then(n).is_some() {
        s += 250;
    }
    if v.pointer("/summary/above_cutoff_tonnage_t").and_then(n).is_some() {
        s += 250;
    }
    if v.pointer("/points").is_some() {
        s -= 200;
    }
    s
}

fn fallback_markdown(title: &str, src_key: &str, compact: &Value) -> String {
    let sem = compact.pointer("/semantic_summary").unwrap_or(compact);
    let summary = compact.pointer("/summary").unwrap_or(compact);

    let element_field = sem
        .get("element_field")
        .or_else(|| summary.get("element_field"))
        .and_then(|v| v.as_str())
        .unwrap_or("grade");
    let grade_unit = sem
        .get("grade_unit")
        .or_else(|| summary.get("grade_unit"))
        .and_then(|v| v.as_str())
        .unwrap_or("unit");

    let cutoff = sem
        .get("cutoff_grade")
        .or_else(|| summary.get("cutoff_grade"));
    let mean_grade = sem
        .get("mean_grade")
        .or_else(|| summary.get("mean_grade"));
    let max_grade = sem
        .get("max_grade")
        .or_else(|| summary.get("max_grade"));
    let tonnage = sem
        .get("above_cutoff_tonnage_t")
        .or_else(|| summary.get("above_cutoff_tonnage_t"));
    let ounces = sem
        .get("above_cutoff_contained_metal_oz")
        .or_else(|| summary.get("above_cutoff_contained_metal_oz"));
    let share = sem
        .get("above_cutoff_share_blocks_pct")
        .or_else(|| summary.get("above_cutoff_share_blocks_pct"));

    let mut md = String::new();
    md.push_str(&format!("# {}\n\n", title));
    md.push_str(&format!("Source artifact: `{}`\n\n", src_key));
    md.push_str("## Executive Summary\n\n");
    md.push_str(&format!(
        "Block model for **{}** with cutoff **{} {}** yields approximately **{} t** above cutoff and **{} oz** contained metal.\n\n",
        element_field,
        fmt_num(n(cutoff.unwrap_or(&Value::Null)), 3),
        grade_unit,
        fmt_num(n(tonnage.unwrap_or(&Value::Null)), 0),
        fmt_num(n(ounces.unwrap_or(&Value::Null)), 0)
    ));

    md.push_str("## Key Metrics\n\n");
    md.push_str("| Metric | Value |\n|---|---:|\n");
    md.push_str(&format!("| Mean grade ({}) | {} |\n", grade_unit, fmt_num(n(mean_grade.unwrap_or(&Value::Null)), 3)));
    md.push_str(&format!("| Max grade ({}) | {} |\n", grade_unit, fmt_num(n(max_grade.unwrap_or(&Value::Null)), 3)));
    md.push_str(&format!("| Above-cutoff share (%) | {} |\n", fmt_num(n(share.unwrap_or(&Value::Null)), 1)));
    md.push_str(&format!("| Above-cutoff tonnage (t) | {} |\n", fmt_num(n(tonnage.unwrap_or(&Value::Null)), 0)));
    md.push_str(&format!("| Above-cutoff contained metal (oz) | {} |\n\n", fmt_num(n(ounces.unwrap_or(&Value::Null)), 0)));

    md.push_str("## Geological Commentary (Preliminary)\n\n");
    md.push_str("- Elevated grades relative to cutoff suggest a potentially coherent high-grade core where drill support density is strongest.\n");
    md.push_str("- Interpret tonnage/ounces as early-stage, interpolation-driven indicators pending domain constraints and geostatistical validation.\n");
    md.push_str("- Next confidence gain comes from domain clipping and tighter search constraints to reduce extrapolation at model edges.\n");

    md
}

async fn summarize_with_openrouter(title: &str, src_key: &str, compact: &Value) -> Result<String, String> {
    let api_key = env::var("OPENROUTER_API_KEY")
        .or_else(|_| env::var("OPENROUTER_KEY"))
        .map_err(|_| "OPENROUTER_API_KEY is not set".to_string())?;

    let system_prompt = "You are a senior resource geologist writing an internal technical memo. Produce sharp, practical markdown in plain English. Be specific and numeric. Avoid generic filler.";
    let user_prompt = format!(
        "Create a concise decision memo titled '{}'. Source artifact key: {}.\n\nRequired structure:\n1) Executive summary (2-4 bullets, with numbers)\n2) KPI table (only key metrics)\n3) Modelling setup (method, cutoff, block size, search/sample settings)\n4) Geological interpretation (what pattern is suggested)\n5) Risk and caveats (interpolation limits, clipping/domain assumptions)\n6) Recommended next actions (3 bullets)\n\nStyle rules:\n- Prefer concrete values and percentages.\n- Do not repeat raw JSON keys unless needed.\n- If a value is missing, state that explicitly once and move on.\n- Keep total length around 220-420 words.\n\nInput semantic JSON:\n\n{}",
        title,
        src_key,
        serde_json::to_string_pretty(compact).unwrap_or_else(|_| "{}".to_string())
    );

    let body = json!({
        "model": "openai/gpt-5-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0.25
    });

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(40))
        .build()
        .map_err(|e| format!("http client error: {e}"))?;

    let resp = client
        .post("https://openrouter.ai/api/v1/chat/completions")
        .header(AUTHORIZATION, format!("Bearer {}", api_key))
        .header(CONTENT_TYPE, "application/json")
        .header("HTTP-Referer", "https://mine-eye.local")
        .header("X-Title", "mine-eye-md-viewer")
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("openrouter request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let txt = resp.text().await.unwrap_or_else(|_| "<no body>".to_string());
        return Err(format!("openrouter http {}: {}", status, txt));
    }

    let v: Value = resp
        .json()
        .await
        .map_err(|e| format!("openrouter response parse failed: {e}"))?;

    let content = v
        .pointer("/choices/0/message/content")
        .and_then(|x| x.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "openrouter empty content".to_string())?;

    Ok(content)
}

pub async fn run_md_viewer(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let Some(first_ref) = job.input_artifact_refs.first() else {
        return Err(NodeError::InvalidConfig(
            "md_viewer requires an upstream semantic JSON artifact".into(),
        ));
    };

    let mut chosen_key = first_ref.key.clone();
    let mut chosen_source: Option<Value> = None;
    let mut best_score = i32::MIN;

    for ar in &job.input_artifact_refs {
        let Ok(v) = super::runtime::read_json_artifact(ctx, &ar.key).await else {
            continue;
        };
        let score = score_semantic_report(&v);
        if score > best_score {
            best_score = score;
            chosen_key = ar.key.clone();
            chosen_source = Some(v);
        }
    }

    let Some(source) = chosen_source else {
        return Err(NodeError::InvalidConfig(
            "md_viewer could not parse any upstream artifact as JSON".into(),
        ));
    };
    let title = job
        .output_spec
        .pointer("/node_ui/title")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "Semantic JSON Report".to_string());
    let llm_enabled = job
        .output_spec
        .pointer("/node_ui/llm_enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    let compact = compact_summary_payload(&source);
    let (markdown, llm_meta) = if llm_enabled {
        match summarize_with_openrouter(&title, &chosen_key, &compact).await {
            Ok(md) => (
                md,
                json!({
                    "used": true,
                    "provider": "openrouter",
                    "model": "openai/gpt-5-mini",
                    "fallback_used": false
                }),
            ),
            Err(reason) => (
                fallback_markdown(&title, &chosen_key, &compact),
                json!({
                    "used": false,
                    "provider": "openrouter",
                    "model": "openai/gpt-5-mini",
                    "fallback_used": true,
                    "fallback_reason": reason
                }),
            ),
        }
    } else {
        (
            fallback_markdown(&title, &chosen_key, &compact),
            json!({"used": false, "fallback_used": true, "fallback_reason": "llm_disabled"}),
        )
    };

    let html = render_simple_html_from_markdown(&title, &markdown);

    let doc = json!({
        "schema_id": "report.markdown_doc.v2",
        "type": "md_view_doc",
        "title": title,
        "source_artifact_key": chosen_key,
        "input_semantic_summary": compact,
        "llm": llm_meta,
        "markdown": markdown,
        "html": html,
        "display_contract": {
            "renderer": "markdown_doc",
            "display_pointer": "report.markdown_doc",
            "editable": ["visible"]
        }
    });

    let json_key = format!(
        "graphs/{}/nodes/{}/md_view_doc.json",
        job.graph_id, job.node_id
    );
    let md_key = format!(
        "graphs/{}/nodes/{}/md_view_doc.md",
        job.graph_id, job.node_id
    );
    let html_key = format!(
        "graphs/{}/nodes/{}/md_view_doc.html",
        job.graph_id, job.node_id
    );
    let doc_bytes = serde_json::to_vec(&doc)?;
    let md_bytes = doc
        .get("markdown")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .as_bytes()
        .to_vec();
    let html_bytes = doc
        .get("html")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .as_bytes()
        .to_vec();

    let doc_ref =
        super::runtime::write_artifact(ctx, &json_key, &doc_bytes, Some("application/json")).await?;
    let md_ref =
        super::runtime::write_artifact(ctx, &md_key, &md_bytes, Some("text/markdown")).await?;
    let html_ref =
        super::runtime::write_artifact(ctx, &html_key, &html_bytes, Some("text/html")).await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![doc_ref.clone(), md_ref.clone(), html_ref.clone()],
        content_hashes: vec![doc_ref.content_hash, md_ref.content_hash, html_ref.content_hash],
        error_message: None,
    })
}
