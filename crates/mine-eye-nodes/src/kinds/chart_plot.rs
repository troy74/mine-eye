use std::collections::{BTreeMap, HashMap};
use std::env;

use mine_eye_types::{JobEnvelope, JobResult, JobStatus};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde_json::{json, Map, Value};

use crate::executor::ExecutionContext;
use crate::NodeError;

#[derive(Clone)]
struct ChartParams {
    template_key: String,
    template_snapshot: Option<Value>,
    data_fragment: Option<String>,
    data_json_pointer: Option<String>,
    title: Option<String>,
    llm_enabled: bool,
    user_objective: Option<String>,
    max_context_rows: usize,
    max_render_rows: usize,
}

fn parse_params(job: &JobEnvelope) -> ChartParams {
    let ui = |p: &str| job.output_spec.pointer(p);
    let parse_usize = |p: &str, d: usize| {
        ui(p)
            .and_then(|v| v.as_u64().map(|x| x as usize))
            .unwrap_or(d)
    };
    ChartParams {
        template_key: ui("/node_ui/template_key")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "variogram".to_string()),
        template_snapshot: ui("/node_ui/template_snapshot").cloned(),
        data_fragment: ui("/node_ui/data_fragment")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        data_json_pointer: ui("/node_ui/data_json_pointer")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        title: ui("/node_ui/title")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        llm_enabled: ui("/node_ui/llm_enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        user_objective: ui("/node_ui/user_objective")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        max_context_rows: parse_usize("/node_ui/max_context_rows", 8).clamp(3, 40),
        max_render_rows: parse_usize("/node_ui/max_render_rows", 3000).clamp(100, 50000),
    }
}

fn n(v: &Value) -> Option<f64> {
    match v {
        Value::Number(x) => x.as_f64().filter(|k| k.is_finite()),
        Value::String(s) => s
            .trim()
            .replace(',', ".")
            .parse::<f64>()
            .ok()
            .filter(|k| k.is_finite()),
        _ => None,
    }
}

fn infer_template_defaults(template_key: &str) -> Value {
    match template_key {
        "scatter" => json!({
            "template": "scatter",
            "default_data_pointer_candidates": ["/points", "/rows"],
            "defaults": { "layers": ["point"] }
        }),
        "histogram" => json!({
            "template": "histogram",
            "default_data_pointer_candidates": ["/grade_histogram", "/histogram", "/bins"],
            "defaults": { "layers": ["bar"] }
        }),
        "profile" => json!({
            "template": "profile",
            "default_data_pointer_candidates": ["/points", "/rows"],
            "defaults": { "layers": ["line", "point"] }
        }),
        _ => json!({
            "template": "variogram",
            "default_data_pointer_candidates": ["/variogram/bins", "/bins"],
            "defaults": {
                "x": "lag_mid_m",
                "y": "gamma",
                "weight": "pairs",
                "point_size_expr": "sqrt(pairs)",
                "layers": ["line", "point"],
                "axis": { "x_label": "Lag distance (m)", "y_label": "Semivariance" },
                "title": "Experimental Variogram"
            }
        }),
    }
}

fn extract_array_rows(
    source: &Value,
    pointer: Option<&str>,
    candidates: &[String],
) -> (Vec<Map<String, Value>>, String) {
    let mut targets = Vec::<String>::new();
    if let Some(p) = pointer {
        targets.push(p.to_string());
    }
    for c in candidates {
        if !targets.contains(c) {
            targets.push(c.clone());
        }
    }
    if targets.is_empty() {
        targets.push("/rows".to_string());
        targets.push("/points".to_string());
        targets.push("/bins".to_string());
    }
    for p in targets {
        if let Some(v) = source.pointer(&p) {
            if let Some(arr) = v.as_array() {
                let mut rows = Vec::<Map<String, Value>>::new();
                for item in arr {
                    if let Some(obj) = item.as_object() {
                        rows.push(obj.clone());
                    }
                }
                if !rows.is_empty() {
                    return (rows, p);
                }
            }
        }
    }
    (Vec::new(), String::new())
}

fn inject_root_metadata(rows: &mut [Map<String, Value>], source: &Value) {
    let mut root_fields = BTreeMap::<String, Value>::new();
    for key in ["schema_id", "type", "element_field", "grade_unit"] {
        if let Some(v) = source.get(key) {
            root_fields.insert(key.to_string(), v.clone());
        }
    }
    if let Some(v) = source.pointer("/semantic_summary/element_field") {
        root_fields.insert("element_field".to_string(), v.clone());
    }
    if let Some(v) = source.pointer("/semantic_summary/grade_unit") {
        root_fields.insert("grade_unit".to_string(), v.clone());
    }
    for row in rows {
        for (k, v) in &root_fields {
            row.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }
}

fn numeric_columns(rows: &[Map<String, Value>]) -> Vec<String> {
    let mut score = HashMap::<String, usize>::new();
    for row in rows {
        for (k, v) in row {
            if n(v).is_some() {
                *score.entry(k.clone()).or_insert(0) += 1;
            }
        }
    }
    let mut cols = score.into_iter().collect::<Vec<_>>();
    cols.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    cols.into_iter().map(|x| x.0).collect()
}

fn default_plot_plan(
    template_key: &str,
    rows: &[Map<String, Value>],
    title_hint: Option<&str>,
) -> Value {
    let numeric = numeric_columns(rows);
    let x = if template_key == "variogram" {
        if numeric.iter().any(|c| c == "lag_mid_m") {
            "lag_mid_m".to_string()
        } else {
            numeric.first().cloned().unwrap_or_else(|| "x".to_string())
        }
    } else {
        numeric.first().cloned().unwrap_or_else(|| "x".to_string())
    };
    let y = if template_key == "variogram" {
        if numeric.iter().any(|c| c == "gamma") {
            "gamma".to_string()
        } else {
            numeric.get(1).cloned().unwrap_or_else(|| "y".to_string())
        }
    } else {
        numeric.get(1).cloned().unwrap_or_else(|| "y".to_string())
    };
    let title = title_hint
        .map(|s| s.to_string())
        .unwrap_or_else(|| match template_key {
            "histogram" => "Histogram".to_string(),
            "scatter" => "Scatter plot".to_string(),
            "profile" => "Profile plot".to_string(),
            _ => "Experimental Variogram".to_string(),
        });
    let mut mapping = json!({ "x": x });
    if template_key != "histogram" {
        mapping["y"] = json!(y);
    }
    if rows.iter().any(|r| r.get("pairs").is_some()) {
        mapping["weight"] = json!("pairs");
        mapping["point_size"] = json!("point_size");
    }
    json!({
        "template": template_key,
        "mapping": mapping,
        "layers": if template_key == "histogram" { json!(["bar"]) } else if template_key == "profile" { json!(["line", "point"]) } else if template_key == "variogram" { json!(["line", "point"]) } else { json!(["point"]) },
        "axis": {
            "x_label": mapping.pointer("/x").and_then(|v| v.as_str()).unwrap_or("x"),
            "y_label": mapping.pointer("/y").and_then(|v| v.as_str()).unwrap_or("y")
        },
        "title": title
    })
}

fn apply_simple_data_plan(rows: &mut [Map<String, Value>], plot_plan: &mut Value) {
    let weight_col = plot_plan
        .pointer("/mapping/weight")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let mut point_size_col = plot_plan
        .pointer("/mapping/point_size")
        .and_then(|v| v.as_str())
        .unwrap_or("point_size")
        .to_string();
    if let Some(wc) = weight_col.as_deref() {
        if plot_plan.pointer("/mapping/point_size").is_none() {
            if let Some(obj) = plot_plan.get_mut("mapping").and_then(|m| m.as_object_mut()) {
                obj.insert("point_size".to_string(), json!("point_size"));
            }
            point_size_col = "point_size".to_string();
        }
        let mut raw = Vec::<f64>::new();
        for row in rows.iter() {
            if let Some(w) = row.get(wc).and_then(n) {
                raw.push(w.max(0.0).sqrt());
            }
        }
        let rmin = raw.iter().copied().fold(f64::INFINITY, |a, b| a.min(b));
        let rmax = raw.iter().copied().fold(f64::NEG_INFINITY, |a, b| a.max(b));
        let span = (rmax - rmin).max(1e-9);
        for row in rows {
            if row.get(&point_size_col).is_none() {
                if let Some(w) = row.get(wc).and_then(n) {
                    let rs = w.max(0.0).sqrt();
                    // Normalize into a visible but bounded marker-size range.
                    let s = 6.0 + 14.0 * ((rs - rmin) / span).clamp(0.0, 1.0);
                    row.insert(point_size_col.clone(), json!(s));
                }
            }
        }
        return;
    }
}

fn sort_rows(rows: &mut [Map<String, Value>], x_col: &str) {
    rows.sort_by(|a, b| {
        let ax = a.get(x_col).and_then(n).unwrap_or(f64::INFINITY);
        let bx = b.get(x_col).and_then(n).unwrap_or(f64::INFINITY);
        ax.partial_cmp(&bx).unwrap_or(std::cmp::Ordering::Equal)
    });
}

fn render_plotly_html(plot_plan: &Value, rows: &[Map<String, Value>]) -> String {
    let template = plot_plan
        .pointer("/template")
        .and_then(|v| v.as_str())
        .unwrap_or("scatter");
    let title = plot_plan
        .pointer("/title")
        .and_then(|v| v.as_str())
        .unwrap_or("Chart");
    let x_col = plot_plan
        .pointer("/mapping/x")
        .and_then(|v| v.as_str())
        .unwrap_or("x");
    let y_col = plot_plan
        .pointer("/mapping/y")
        .and_then(|v| v.as_str())
        .unwrap_or("y");
    let x_label = plot_plan
        .pointer("/axis/x_label")
        .and_then(|v| v.as_str())
        .unwrap_or(x_col);
    let y_label = plot_plan
        .pointer("/axis/y_label")
        .and_then(|v| v.as_str())
        .unwrap_or(y_col);
    let size_col = plot_plan
        .pointer("/mapping/point_size")
        .and_then(|v| v.as_str())
        .or_else(|| {
            plot_plan
                .pointer("/mapping/weight")
                .and_then(|v| v.as_str())
        })
        .unwrap_or("point_size");
    let mut x = Vec::<Value>::new();
    let mut y = Vec::<Value>::new();
    let mut s = Vec::<Value>::new();
    for row in rows {
        if let Some(vx) = row.get(x_col) {
            x.push(vx.clone());
        } else {
            x.push(Value::Null);
        }
        if template != "histogram" {
            if let Some(vy) = row.get(y_col) {
                y.push(vy.clone());
            } else {
                y.push(Value::Null);
            }
        }
        if let Some(vs) = row.get(size_col).and_then(n) {
            s.push(json!(vs));
        } else {
            s.push(json!(6.0));
        }
    }
    let mut traces = Vec::<Value>::new();
    if template == "histogram" {
        traces.push(json!({
            "type": "bar",
            "x": x,
            "marker": { "color": "#3b82f6" }
        }));
    } else if template == "variogram" || template == "profile" {
        traces.push(json!({
            "type": "scatter",
            "mode": "lines+markers",
            "x": x,
            "y": y,
            "marker": { "size": s, "sizemode":"diameter", "sizemin": 4, "opacity": 0.86, "color": "#f97316" },
            "line": { "width": 2, "color": "#0ea5e9" },
            "name": "Experimental"
        }));
    } else {
        traces.push(json!({
            "type": "scatter",
            "mode": "markers",
            "x": x,
            "y": y,
            "marker": { "size": s, "sizemode":"diameter", "sizemin": 4, "opacity": 0.82, "color": "#8b5cf6" }
        }));
    }

    let mut shapes = Vec::<Value>::new();
    let mut annotations = Vec::<Value>::new();
    if template == "variogram" {
        let mut pts = rows
            .iter()
            .filter_map(|r| Some((r.get(x_col).and_then(n)?, r.get(y_col).and_then(n)?)))
            .collect::<Vec<_>>();
        pts.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        if pts.len() >= 2 {
            let nugget = pts[0];
            let mut ys = pts.iter().map(|p| p.1).collect::<Vec<_>>();
            ys.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let top_n = (ys.len() / 3).max(1);
            let sill = ys[ys.len() - top_n..].iter().sum::<f64>() / top_n as f64;
            let range = pts
                .iter()
                .find(|(_, gy)| *gy >= 0.95 * sill)
                .map(|(lx, _)| *lx)
                .unwrap_or_else(|| pts.last().map(|p| p.0).unwrap_or(nugget.0));

            shapes.push(json!({
                "type": "line",
                "xref": "x", "yref": "y",
                "x0": pts.first().map(|p| p.0).unwrap_or(0.0), "x1": pts.last().map(|p| p.0).unwrap_or(range),
                "y0": sill, "y1": sill,
                "line": {"color":"#ef4444","dash":"dash","width":1.5}
            }));
            shapes.push(json!({
                "type": "line",
                "xref": "x", "yref": "y",
                "x0": range, "x1": range,
                "y0": pts.iter().map(|p| p.1).fold(f64::INFINITY, |a,b| a.min(b)),
                "y1": sill,
                "line": {"color":"#22c55e","dash":"dot","width":1.5}
            }));
            traces.push(json!({
                "type": "scatter",
                "mode": "markers+text",
                "x": [nugget.0],
                "y": [nugget.1],
                "text": ["Nugget"],
                "textposition": "top right",
                "marker": {"size": 10, "color":"#111827", "symbol":"diamond"},
                "name": "Nugget"
            }));
            annotations.push(json!({
                "x": pts.last().map(|p| p.0).unwrap_or(range),
                "y": sill,
                "xref": "x", "yref": "y",
                "text": format!("Sill ≈ {:.3}", sill),
                "showarrow": false,
                "xanchor": "right",
                "yanchor": "bottom",
                "font": {"size": 11, "color":"#ef4444"}
            }));
            annotations.push(json!({
                "x": range,
                "y": pts.iter().map(|p| p.1).fold(f64::INFINITY, |a,b| a.min(b)),
                "xref": "x", "yref": "y",
                "text": format!("Range ≈ {:.1} m", range),
                "showarrow": false,
                "xanchor": "left",
                "yanchor": "top",
                "font": {"size": 11, "color":"#16a34a"}
            }));
        }
    }

    let data_json = serde_json::to_string(&traces).unwrap_or_else(|_| "[]".to_string());
    let layout_json = serde_json::to_string(&json!({
        "title": title,
        "margin": { "l": 56, "r": 20, "t": 56, "b": 56 },
        "xaxis": { "title": x_label },
        "yaxis": { "title": y_label },
        "shapes": shapes,
        "annotations": annotations,
        "legend": {"orientation":"h", "x":0.0, "y":1.12}
    }))
    .unwrap_or_else(|_| "{}".to_string());
    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"/><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"/><title>{}</title><script src=\"https://cdn.plot.ly/plotly-2.35.2.min.js\"></script><style>body{{margin:0;background:#fff;color:#0f172a;font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif}}#plot{{width:100vw;height:100vh}}</style></head><body><div id=\"plot\"></div><script>const data={};const layout={};Plotly.newPlot('plot',data,layout,{{responsive:true,displaylogo:false}});</script></body></html>",
        title, data_json, layout_json
    )
}

fn summarise_numeric(rows: &[Map<String, Value>], cols: &[String]) -> Value {
    let mut out = Map::<String, Value>::new();
    for c in cols {
        let vals = rows
            .iter()
            .filter_map(|r| r.get(c).and_then(n))
            .collect::<Vec<_>>();
        if vals.is_empty() {
            continue;
        }
        let min = vals.iter().copied().fold(f64::INFINITY, |a, b| a.min(b));
        let max = vals
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, |a, b| a.max(b));
        let mean = vals.iter().sum::<f64>() / vals.len() as f64;
        out.insert(
            c.clone(),
            json!({ "min": min, "max": max, "mean": mean, "count": vals.len() }),
        );
    }
    Value::Object(out)
}

fn parse_llm_json(content: &str) -> Option<Value> {
    if let Ok(v) = serde_json::from_str::<Value>(content) {
        return Some(v);
    }
    let start = content.find('{')?;
    let end = content.rfind('}')?;
    if end <= start {
        return None;
    }
    serde_json::from_str::<Value>(&content[start..=end]).ok()
}

async fn llm_plan(compact: &Value, template_key: &str, objective: Option<&str>) -> Option<Value> {
    let api_key = env::var("OPENROUTER_API_KEY")
        .or_else(|_| env::var("OPENROUTER_KEY"))
        .ok()?;
    let sys = "You are a plotting planner. Return STRICT JSON only with keys: data_plan, plot_plan, title, commentary. Do not output markdown.";
    let usr = format!(
        "Template key: {}\nObjective: {}\nInput context JSON:\n{}\n\nReturn compact JSON only.",
        template_key,
        objective.unwrap_or("Produce a useful chart with valid mappings."),
        serde_json::to_string_pretty(compact).ok()?
    );
    let body = json!({
        "model": "openai/gpt-5-mini",
        "messages": [
            {"role": "system", "content": sys},
            {"role": "user", "content": usr}
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
    parse_llm_json(content)
}

fn score_source(v: &Value, template_key: &str) -> i32 {
    let mut s = 0;
    let t = v.pointer("/type").and_then(|x| x.as_str()).unwrap_or("");
    if t == "block_resource_report" {
        s += 600;
    }
    if t == "variogram_report" {
        s += 550;
    }
    if template_key == "variogram"
        && v.pointer("/variogram/bins")
            .and_then(|x| x.as_array())
            .is_some()
    {
        s += 800;
    }
    if template_key == "variogram" && v.pointer("/bins").and_then(|x| x.as_array()).is_some() {
        s += 500;
    }
    if v.pointer("/points").and_then(|x| x.as_array()).is_some() {
        s += 180;
    }
    s
}

pub async fn run_plot_chart(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let params = parse_params(job);
    let template = params
        .template_snapshot
        .clone()
        .unwrap_or_else(|| infer_template_defaults(&params.template_key));
    let candidate_ptrs = template
        .pointer("/default_data_pointer_candidates")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let mut best_source: Option<Value> = None;
    let mut best_key = String::new();
    let mut best_score = i32::MIN;
    for ar in &job.input_artifact_refs {
        let Ok(v) = super::runtime::read_json_artifact(ctx, &ar.key).await else {
            continue;
        };
        let sc = score_source(&v, &params.template_key);
        if sc > best_score {
            best_score = sc;
            best_key = ar.key.clone();
            best_source = Some(v);
        }
    }
    let Some(source) = best_source else {
        return Err(NodeError::InvalidConfig(
            "plot_chart requires upstream semantic JSON".into(),
        ));
    };

    let (mut rows, pointer_used) = extract_array_rows(
        &source,
        params.data_json_pointer.as_deref().or_else(|| {
            params
                .data_fragment
                .as_deref()
                .filter(|f| !f.eq_ignore_ascii_case("auto") && !f.eq_ignore_ascii_case("custom"))
        }),
        &candidate_ptrs,
    );
    if rows.is_empty() {
        return Err(NodeError::InvalidConfig(
            "plot_chart could not resolve a tabular JSON fragment; set data_json_pointer".into(),
        ));
    }
    inject_root_metadata(&mut rows, &source);

    let inferred_title = params.title.clone().unwrap_or_else(|| {
        if params.template_key == "variogram" {
            let element = source
                .pointer("/summary/element_field")
                .or_else(|| source.pointer("/element_field"))
                .and_then(|v| v.as_str())
                .unwrap_or("grade");
            format!("Variogram Analysis ({})", element)
        } else {
            format!("{} chart", params.template_key)
        }
    });
    let mut plot_plan = default_plot_plan(&params.template_key, &rows, Some(&inferred_title));
    let numeric_cols = numeric_columns(&rows);
    let top = rows
        .iter()
        .take(params.max_context_rows)
        .cloned()
        .collect::<Vec<_>>();
    let tail = rows
        .iter()
        .rev()
        .take(params.max_context_rows)
        .cloned()
        .collect::<Vec<_>>();
    let context = json!({
        "template_key": params.template_key,
        "source_type": source.pointer("/type"),
        "source_schema_id": source.pointer("/schema_id"),
        "source_artifact_key": best_key,
        "pointer_used": pointer_used,
        "row_count": rows.len(),
        "field_inventory": rows.first().map(|r| r.keys().cloned().collect::<Vec<_>>()).unwrap_or_default(),
        "numeric_columns": numeric_cols,
        "numeric_summaries": summarise_numeric(&rows, &numeric_columns(&rows)),
        "top_rows": top,
        "tail_rows": tail,
    });
    let mut llm_meta =
        json!({"used": false, "provider": "openrouter", "model": "openai/gpt-5-mini"});
    if params.llm_enabled {
        if let Some(plan) = llm_plan(
            &context,
            &params.template_key,
            params.user_objective.as_deref(),
        )
        .await
        {
            if let Some(pp) = plan.get("plot_plan") {
                plot_plan = pp.clone();
            }
            if let Some(t) = plan.get("title").and_then(|x| x.as_str()) {
                if let Some(obj) = plot_plan.as_object_mut() {
                    obj.insert("title".to_string(), json!(t));
                }
            }
            llm_meta = json!({
                "used": true,
                "provider": "openrouter",
                "model": "openai/gpt-5-mini",
                "raw_plan": plan
            });
        } else {
            llm_meta["fallback"] = json!("deterministic_default");
        }
    }

    apply_simple_data_plan(&mut rows, &mut plot_plan);
    let x_col = plot_plan
        .pointer("/mapping/x")
        .and_then(|v| v.as_str())
        .unwrap_or("x");
    sort_rows(&mut rows, x_col);
    if rows.len() > params.max_render_rows {
        rows.truncate(params.max_render_rows);
    }
    let html = render_plotly_html(&plot_plan, &rows);

    let chart_spec = json!({
        "schema_id": "report.chart_spec.v1",
        "type": "plot_chart_spec",
        "template_key": params.template_key,
        "template_snapshot": template,
        "source_artifact_key": best_key,
        "data_json_pointer_used": pointer_used,
        "plot_plan": plot_plan,
        "context_summary": {
            "row_count": rows.len(),
            "numeric_summaries": summarise_numeric(&rows, &numeric_columns(&rows)),
        },
        "llm": llm_meta
    });
    let chart_view = json!({
        "schema_id": "report.chart_doc.v1",
        "type": "chart_view_doc",
        "title": chart_spec.pointer("/plot_plan/title").and_then(|v| v.as_str()).unwrap_or("Chart"),
        "html": html,
        "display_contract": {
            "renderer": "chart_doc",
            "display_pointer": "report.chart_doc",
            "editable": ["visible"]
        }
    });
    let snapshot = json!({
        "schema_id": "report.chart_data_snapshot.v1",
        "type": "chart_data_snapshot",
        "rows": rows
    });

    let spec_bytes = serde_json::to_vec(&chart_spec)?;
    let view_bytes = serde_json::to_vec(&chart_view)?;
    let snap_bytes = serde_json::to_vec(&snapshot)?;
    let html_bytes = chart_view
        .get("html")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .as_bytes()
        .to_vec();
    let spec_key = format!(
        "graphs/{}/nodes/{}/plot_chart_spec.json",
        job.graph_id, job.node_id
    );
    let view_key = format!(
        "graphs/{}/nodes/{}/plot_chart_view.json",
        job.graph_id, job.node_id
    );
    let snap_key = format!(
        "graphs/{}/nodes/{}/plot_chart_data_snapshot.json",
        job.graph_id, job.node_id
    );
    let html_key = format!(
        "graphs/{}/nodes/{}/plot_chart_view.html",
        job.graph_id, job.node_id
    );
    let spec_ref =
        super::runtime::write_artifact(ctx, &spec_key, &spec_bytes, Some("application/json"))
            .await?;
    let view_ref =
        super::runtime::write_artifact(ctx, &view_key, &view_bytes, Some("application/json"))
            .await?;
    let snap_ref =
        super::runtime::write_artifact(ctx, &snap_key, &snap_bytes, Some("application/json"))
            .await?;
    let html_ref =
        super::runtime::write_artifact(ctx, &html_key, &html_bytes, Some("text/html")).await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![
            spec_ref.clone(),
            view_ref.clone(),
            snap_ref.clone(),
            html_ref.clone(),
        ],
        content_hashes: vec![
            spec_ref.content_hash,
            view_ref.content_hash,
            snap_ref.content_hash,
            html_ref.content_hash,
        ],
        error_message: None,
    })
}
