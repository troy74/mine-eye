use std::env;

use mine_eye_types::{JobEnvelope, JobResult, JobStatus};
use pulldown_cmark::{html, Options, Parser};
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

fn decode_basic_entities(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
}

fn promote_mermaid_blocks(input: &str) -> String {
    let start_tag = "<pre><code class=\"language-mermaid\">";
    let end_tag = "</code></pre>";
    let mut out = String::with_capacity(input.len() + 128);
    let mut rest = input;
    while let Some(start) = rest.find(start_tag) {
        out.push_str(&rest[..start]);
        let after = &rest[start + start_tag.len()..];
        let Some(end) = after.find(end_tag) else {
            out.push_str(&rest[start..]);
            return out;
        };
        let code = decode_basic_entities(&after[..end]);
        out.push_str("<pre class=\"mermaid\">");
        out.push_str(&code);
        out.push_str("</pre>");
        rest = &after[end + end_tag.len()..];
    }
    out.push_str(rest);
    out
}

fn render_simple_html_from_markdown(title: &str, markdown: &str) -> String {
    let mut options = Options::empty();
    options.insert(Options::ENABLE_TABLES);
    options.insert(Options::ENABLE_STRIKETHROUGH);
    options.insert(Options::ENABLE_TASKLISTS);
    options.insert(Options::ENABLE_FOOTNOTES);
    options.insert(Options::ENABLE_HEADING_ATTRIBUTES);
    let parser = Parser::new_ext(markdown, options);
    let mut body = String::new();
    html::push_html(&mut body, parser);
    body = promote_mermaid_blocks(&body);
    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"/><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"/><title>{}</title><style>body{{font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:20px;line-height:1.55;color:#0f172a}}main{{max-width:1100px}}h1,h2,h3{{margin-top:1em;margin-bottom:.4em}}p{{margin:.35em 0}}li{{margin:.15em 0}}img{{max-width:100%;height:auto;border-radius:8px;border:1px solid #e5e7eb}}code{{background:#f3f4f6;padding:1px 4px;border-radius:4px}}pre{{background:#f8fafc;border:1px solid #e5e7eb;border-radius:8px;padding:10px;overflow:auto}}table{{border-collapse:collapse;width:100%;margin:12px 0;display:table;table-layout:auto}}th,td{{border:1px solid #e5e7eb;padding:8px 10px;text-align:left;vertical-align:top}}thead{{background:#f3f4f6}}tbody tr:nth-child(even){{background:#fafafa}}</style><script src=\"https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js\"></script><script>if(window.mermaid){{window.mermaid.initialize({{startOnLoad:true,securityLevel:'loose'}});}}</script></head><body><main><h1>{}</h1>{}</main></body></html>",
        escape_html(title),
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
    let schema = source.pointer("/schema_id").cloned().unwrap_or(Value::Null);
    let typ = source.pointer("/type").cloned().unwrap_or(Value::Null);
    let hist = source
        .pointer("/grade_histogram")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().take(6).cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    let cutoff_sensitivity = source
        .pointer("/cutoff_sensitivity")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().take(12).cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    let variogram = source.pointer("/variogram").cloned().or_else(|| {
        let bins = source
            .pointer("/bins")?
            .as_array()?
            .iter()
            .take(12)
            .cloned()
            .collect::<Vec<_>>();
        Some(json!({
            "bins": bins,
            "lags": source.pointer("/lags").and_then(|v| n(v)),
            "max_pairs": source.pointer("/max_pairs").and_then(|v| n(v)),
            "max_range_m": source.pointer("/max_range_m").and_then(|v| n(v)),
        }))
    });
    let notes = source
        .pointer("/notes")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().take(4).cloned().collect::<Vec<_>>())
        .unwrap_or_default();

    if semantic.is_some() || summary.is_some() {
        json!({
            "schema_id": schema,
            "type": typ,
            "semantic_summary": semantic,
            "summary": summary,
            "grade_histogram_top_bins": hist,
            "cutoff_sensitivity": cutoff_sensitivity,
            "variogram": variogram,
            "notes": notes,
        })
    } else {
        source.clone()
    }
}

fn score_semantic_report(v: &Value) -> i32 {
    let mut s = 0i32;
    let schema = v
        .pointer("/schema_id")
        .and_then(|x| x.as_str())
        .unwrap_or("");
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
    if v.pointer("/summary/above_cutoff_tonnage_t")
        .and_then(n)
        .is_some()
    {
        s += 250;
    }
    if v.pointer("/schema_id").and_then(|x| x.as_str()) == Some("report.variogram.v1") {
        s += 450;
    }
    if v.pointer("/type").and_then(|x| x.as_str()) == Some("variogram_report") {
        s += 350;
    }
    if v.pointer("/points").is_some() {
        s -= 200;
    }
    s
}

fn infer_title(source: &Value) -> String {
    let typ = source
        .pointer("/type")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let schema = source
        .pointer("/schema_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let element = source
        .pointer("/semantic_summary/element_field")
        .or_else(|| source.pointer("/summary/element_field"))
        .or_else(|| source.pointer("/element_field"))
        .and_then(|v| v.as_str())
        .unwrap_or("grade");
    let grade_unit = source
        .pointer("/semantic_summary/grade_unit")
        .or_else(|| source.pointer("/summary/grade_unit"))
        .or_else(|| source.pointer("/grade_unit"))
        .and_then(|v| v.as_str())
        .unwrap_or("unit");
    if schema == "report.variogram.v1" || typ == "variogram_report" {
        return format!("Variogram Analysis ({}, {})", element, grade_unit);
    }
    if schema == "report.block_resource.v2" || typ == "block_resource_report" {
        return format!("Block Resource Summary ({}, {})", element, grade_unit);
    }
    if !typ.is_empty() {
        return typ.replace('_', " ");
    }
    "Technical Report".to_string()
}

fn merge_companion_reports(primary: &mut Value, all_sources: &[(String, Value)]) {
    let is_resource = primary.pointer("/schema_id").and_then(|v| v.as_str())
        == Some("report.block_resource.v2")
        || primary.pointer("/type").and_then(|v| v.as_str()) == Some("block_resource_report");
    if !is_resource {
        return;
    }
    let has_variogram = primary
        .pointer("/variogram/bins")
        .and_then(|v| v.as_array())
        .map(|a| !a.is_empty())
        .unwrap_or(false);
    if has_variogram {
        return;
    }
    let Some((src_key, vg)) = all_sources.iter().find(|(_, v)| {
        v.pointer("/schema_id").and_then(|x| x.as_str()) == Some("report.variogram.v1")
            || v.pointer("/type").and_then(|x| x.as_str()) == Some("variogram_report")
    }) else {
        return;
    };
    let merged = json!({
        "bins": vg.pointer("/bins").cloned().unwrap_or_else(|| json!([])),
        "lags": vg.pointer("/lags").and_then(|v| n(v)),
        "max_pairs": vg.pointer("/max_pairs").and_then(|v| n(v)),
        "max_range_m": vg.pointer("/max_range_m").and_then(|v| n(v)),
        "source_artifact_key": src_key,
    });
    if let Some(obj) = primary.as_object_mut() {
        obj.insert("variogram".to_string(), merged);
    }
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
    let mean_grade = sem.get("mean_grade").or_else(|| summary.get("mean_grade"));
    let max_grade = sem.get("max_grade").or_else(|| summary.get("max_grade"));
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
    md.push_str(&format!(
        "| Mean grade ({}) | {} |\n",
        grade_unit,
        fmt_num(n(mean_grade.unwrap_or(&Value::Null)), 3)
    ));
    md.push_str(&format!(
        "| Max grade ({}) | {} |\n",
        grade_unit,
        fmt_num(n(max_grade.unwrap_or(&Value::Null)), 3)
    ));
    md.push_str(&format!(
        "| Above-cutoff share (%) | {} |\n",
        fmt_num(n(share.unwrap_or(&Value::Null)), 1)
    ));
    md.push_str(&format!(
        "| Above-cutoff tonnage (t) | {} |\n",
        fmt_num(n(tonnage.unwrap_or(&Value::Null)), 0)
    ));
    md.push_str(&format!(
        "| Above-cutoff contained metal (oz) | {} |\n\n",
        fmt_num(n(ounces.unwrap_or(&Value::Null)), 0)
    ));

    md.push_str("## Geological Commentary (Preliminary)\n\n");
    md.push_str("- Elevated grades relative to cutoff suggest a potentially coherent high-grade core where drill support density is strongest.\n");
    md.push_str("- Interpret tonnage/ounces as early-stage, interpolation-driven indicators pending domain constraints and geostatistical validation.\n");
    md.push_str("- Next confidence gain comes from domain clipping and tighter search constraints to reduce extrapolation at model edges.\n");

    md
}

async fn summarize_with_openrouter(
    title: &str,
    src_key: &str,
    compact: &Value,
) -> Result<String, String> {
    let api_key = env::var("OPENROUTER_API_KEY")
        .or_else(|_| env::var("OPENROUTER_KEY"))
        .map_err(|_| "OPENROUTER_API_KEY is not set".to_string())?;

    let system_prompt = "You are a senior resource geologist writing an internal technical memo. Produce crisp, practical markdown in plain English with concrete numbers and caveats. No fluff.";
    let is_variogram = compact.pointer("/schema_id").and_then(|v| v.as_str())
        == Some("report.variogram.v1")
        || compact.pointer("/type").and_then(|v| v.as_str()) == Some("variogram_report");
    let structure = if is_variogram {
        "Required structure:\n1) Executive summary (2-4 bullets)\n2) Variogram diagnostics table (lag range, pairs, gamma)\n3) Interpretation (nugget/sill/range behavior and continuity implications)\n4) Risks and caveats\n5) Recommended next actions (3 bullets)"
    } else {
        "Required structure:\n1) Executive summary (2-4 bullets, with numbers)\n2) KPI table (only key metrics)\n3) Modelling setup (method, cutoff, block size, search/sample settings)\n4) Variogram diagnostics (if present)\n5) Geological interpretation\n6) Risk and caveats\n7) Recommended next actions (3 bullets)"
    };
    let user_prompt = format!(
        "Create a concise decision memo titled '{}'. Source artifact key: {}.\n\n{}\n\nStyle rules:\n- Start with '# {}' as the first line.\n- Prefer concrete values and percentages.\n- Do not call the report 'Semantic JSON Report'.\n- Do not repeat raw JSON keys unless needed.\n- If a value is missing, state that explicitly once and move on.\n- Keep total length around 260-520 words.\n\nInput semantic JSON:\n\n{}",
        title,
        src_key,
        structure,
        title,
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
        let txt = resp
            .text()
            .await
            .unwrap_or_else(|_| "<no body>".to_string());
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
    let mut parsed_sources: Vec<(String, Value)> = Vec::new();
    let mut best_score = i32::MIN;

    for ar in &job.input_artifact_refs {
        let Ok(v) = super::runtime::read_json_artifact(ctx, &ar.key).await else {
            continue;
        };
        parsed_sources.push((ar.key.clone(), v.clone()));
        let score = score_semantic_report(&v);
        if score > best_score {
            best_score = score;
            chosen_key = ar.key.clone();
            chosen_source = Some(v);
        }
    }

    let Some(mut source) = chosen_source else {
        return Err(NodeError::InvalidConfig(
            "md_viewer could not parse any upstream artifact as JSON".into(),
        ));
    };
    merge_companion_reports(&mut source, &parsed_sources);
    let configured_title = job
        .output_spec
        .pointer("/node_ui/title")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let title = match configured_title {
        Some(t) if !t.eq_ignore_ascii_case("Semantic JSON Report") => t,
        _ => infer_title(&source),
    };
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
        super::runtime::write_artifact(ctx, &json_key, &doc_bytes, Some("application/json"))
            .await?;
    let md_ref =
        super::runtime::write_artifact(ctx, &md_key, &md_bytes, Some("text/markdown")).await?;
    let html_ref =
        super::runtime::write_artifact(ctx, &html_key, &html_bytes, Some("text/html")).await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![doc_ref.clone(), md_ref.clone(), html_ref.clone()],
        content_hashes: vec![
            doc_ref.content_hash,
            md_ref.content_hash,
            html_ref.content_hash,
        ],
        error_message: None,
    })
}
