use mine_eye_types::{JobEnvelope, JobResult, JobStatus};
use serde_json::Value;

use crate::executor::ExecutionContext;
use crate::NodeError;

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn value_inline(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        _ => serde_json::to_string(v).unwrap_or_else(|_| "<invalid-json>".into()),
    }
}

fn looks_like_image_url(s: &str) -> bool {
    let t = s.trim();
    let lower = t.to_ascii_lowercase();
    (lower.starts_with("http://") || lower.starts_with("https://") || lower.starts_with("/files/"))
        && (lower.ends_with(".png")
            || lower.ends_with(".jpg")
            || lower.ends_with(".jpeg")
            || lower.ends_with(".webp")
            || lower.ends_with(".gif")
            || lower.ends_with(".svg"))
}

fn render_markdown_block(
    key: &str,
    v: &Value,
    level: usize,
    out: &mut String,
    image_urls: &mut Vec<String>,
) {
    let h = "#".repeat(level.clamp(1, 6));
    match v {
        Value::Object(map) => {
            out.push_str(&format!("\n{} {}\n\n", h, key));
            for (k, vv) in map {
                match vv {
                    Value::Object(_) | Value::Array(_) => render_markdown_block(k, vv, level + 1, out, image_urls),
                    Value::String(s) if looks_like_image_url(s) => {
                        image_urls.push(s.clone());
                        out.push_str(&format!("- **{}**\n\n  ![{}]({})\n\n", k, k, s));
                    }
                    _ => {
                        out.push_str(&format!("- **{}**: {}\n", k, value_inline(vv)));
                    }
                }
            }
            out.push('\n');
        }
        Value::Array(arr) => {
            out.push_str(&format!("\n{} {}\n\n", h, key));
            if arr.iter().all(|x| !x.is_object() && !x.is_array()) {
                for vv in arr {
                    out.push_str(&format!("- {}\n", value_inline(vv)));
                }
                out.push('\n');
                return;
            }
            for (i, vv) in arr.iter().enumerate() {
                render_markdown_block(&format!("{} #{}", key, i + 1), vv, (level + 1).clamp(1, 6), out, image_urls);
            }
        }
        Value::String(s) if looks_like_image_url(s) => {
            image_urls.push(s.clone());
            out.push_str(&format!("\n{} {}\n\n![{}]({})\n\n", h, key, key, s));
        }
        _ => {
            out.push_str(&format!("\n{} {}\n\n{}\n\n", h, key, value_inline(v)));
        }
    }
}

fn render_html_block(key: &str, v: &Value, level: usize, out: &mut String) {
    let tag = format!("h{}", level.clamp(1, 6));
    out.push_str(&format!("<{tag}>{}</{tag}>", escape_html(key)));
    match v {
        Value::Object(map) => {
            out.push_str("<ul>");
            for (k, vv) in map {
                match vv {
                    Value::Object(_) | Value::Array(_) => {
                        out.push_str("<li>");
                        render_html_block(k, vv, (level + 1).clamp(1, 6), out);
                        out.push_str("</li>");
                    }
                    Value::String(s) if looks_like_image_url(s) => {
                        out.push_str(&format!(
                            "<li><strong>{}</strong><div><img src=\"{}\" alt=\"{}\" style=\"max-width:100%;height:auto;border-radius:8px;border:1px solid #e5e7eb\"/></div></li>",
                            escape_html(k),
                            escape_html(s),
                            escape_html(k)
                        ));
                    }
                    _ => {
                        out.push_str(&format!(
                            "<li><strong>{}</strong>: {}</li>",
                            escape_html(k),
                            escape_html(&value_inline(vv))
                        ));
                    }
                }
            }
            out.push_str("</ul>");
        }
        Value::Array(arr) => {
            out.push_str("<ol>");
            for vv in arr {
                out.push_str("<li>");
                match vv {
                    Value::Object(_) | Value::Array(_) => render_html_block("item", vv, (level + 1).clamp(1, 6), out),
                    Value::String(s) if looks_like_image_url(s) => out.push_str(&format!(
                        "<img src=\"{}\" alt=\"{}\" style=\"max-width:100%;height:auto;border-radius:8px;border:1px solid #e5e7eb\"/>",
                        escape_html(s),
                        escape_html(key)
                    )),
                    _ => out.push_str(&escape_html(&value_inline(vv))),
                }
                out.push_str("</li>");
            }
            out.push_str("</ol>");
        }
        Value::String(s) if looks_like_image_url(s) => {
            out.push_str(&format!(
                "<img src=\"{}\" alt=\"{}\" style=\"max-width:100%;height:auto;border-radius:8px;border:1px solid #e5e7eb\"/>",
                escape_html(s),
                escape_html(key)
            ));
        }
        _ => out.push_str(&format!("<p>{}</p>", escape_html(&value_inline(v)))),
    }
}

pub async fn run_md_viewer(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let Some(first) = job.input_artifact_refs.first() else {
        return Err(NodeError::InvalidConfig(
            "md_viewer requires an upstream semantic JSON artifact".into(),
        ));
    };
    let source = super::runtime::read_json_artifact(ctx, &first.key).await?;
    let title = job
        .output_spec
        .pointer("/node_ui/title")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "Semantic JSON Report".to_string());

    let mut image_urls = Vec::<String>::new();
    let mut markdown = String::new();
    markdown.push_str(&format!("# {}\n\n", title));
    markdown.push_str(&format!("Source artifact: `{}`\n\n", first.key));
    render_markdown_block("Report", &source, 2, &mut markdown, &mut image_urls);

    let mut html_body = String::new();
    html_body.push_str(&format!("<h1>{}</h1>", escape_html(&title)));
    html_body.push_str(&format!(
        "<p style=\"color:#6b7280\">Source artifact: <code>{}</code></p>",
        escape_html(&first.key)
    ));
    render_html_block("Report", &source, 2, &mut html_body);
    let html = format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"/><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"/><title>{}</title><style>body{{font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:20px;line-height:1.5;color:#0f172a}}code{{background:#f3f4f6;padding:1px 4px;border-radius:4px}}h1,h2,h3,h4,h5,h6{{margin-top:1.1em}}ul,ol{{padding-left:1.3em}}</style></head><body>{}</body></html>",
        escape_html(&title),
        html_body
    );

    let doc = serde_json::json!({
        "schema_id": "report.markdown_doc.v1",
        "type": "md_view_doc",
        "title": title,
        "source_artifact_key": first.key,
        "image_urls": image_urls,
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
    let md_bytes = markdown.into_bytes();
    let html_bytes = html.into_bytes();

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

