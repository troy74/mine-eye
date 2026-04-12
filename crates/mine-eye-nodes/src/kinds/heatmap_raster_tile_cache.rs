use std::collections::BTreeSet;
use std::io::Cursor;

use image::{DynamicImage, ImageBuffer, ImageFormat, Rgba, RgbaImage};
use mine_eye_types::{CrsRecord, JobEnvelope, JobResult, JobStatus};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use crate::executor::ExecutionContext;
use crate::NodeError;

#[derive(Clone, Copy)]
struct Sample {
    x: f64,
    y: f64,
    v: f64,
}

fn hash_string(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    hex::encode(h.finalize())
}

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

fn lookup_ci<'a>(obj: &'a Map<String, Value>, key: &str) -> Option<&'a Value> {
    if let Some(v) = obj.get(key) {
        return Some(v);
    }
    let lk = key.to_ascii_lowercase();
    obj.iter()
        .find(|(k, _)| k.to_ascii_lowercase() == lk)
        .map(|(_, v)| v)
}

fn pick_color(palette: &str, t: f64) -> [u8; 3] {
    let t = t.clamp(0.0, 1.0);
    let stops: &[(f64, [u8; 3])] = match palette.to_ascii_lowercase().as_str() {
        "inferno" => &[
            (0.0, [0, 0, 4]),
            (0.2, [43, 10, 90]),
            (0.45, [120, 28, 109]),
            (0.7, [209, 58, 47]),
            (1.0, [255, 59, 47]),
        ],
        "viridis" => &[
            (0.0, [68, 1, 84]),
            (0.25, [59, 82, 139]),
            (0.5, [33, 144, 140]),
            (0.75, [93, 200, 99]),
            (1.0, [253, 231, 37]),
        ],
        "terrain" => &[
            (0.0, [43, 131, 186]),
            (0.35, [171, 221, 164]),
            (0.6, [102, 189, 99]),
            (0.8, [253, 174, 97]),
            (1.0, [215, 25, 28]),
        ],
        _ => &[
            (0.0, [44, 123, 182]),
            (0.25, [0, 166, 202]),
            (0.5, [0, 204, 106]),
            (0.75, [249, 208, 87]),
            (1.0, [215, 25, 28]),
        ],
    };
    for i in 1..stops.len() {
        let (ta, ca) = stops[i - 1];
        let (tb, cb) = stops[i];
        if t <= tb {
            let r = ((t - ta) / (tb - ta).max(1e-9)).clamp(0.0, 1.0);
            let lerp = |a: u8, b: u8| -> u8 {
                let av = a as f64;
                let bv = b as f64;
                (av + (bv - av) * r).round().clamp(0.0, 255.0) as u8
            };
            return [lerp(ca[0], cb[0]), lerp(ca[1], cb[1]), lerp(ca[2], cb[2])];
        }
    }
    stops.last().map(|(_, c)| *c).unwrap_or([0, 0, 0])
}

fn interpolate(samples: &[Sample], x: f64, y: f64, method: &str, pwr: f64, max_points: usize) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let mut near = Vec::<(f64, f64)>::with_capacity(samples.len());
    for s in samples {
        let dx = x - s.x;
        let dy = y - s.y;
        let d2 = dx * dx + dy * dy;
        if d2 <= 1e-12 {
            return Some(s.v);
        }
        near.push((d2, s.v));
    }
    near.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    near.truncate(max_points.max(1).min(near.len()));
    if near.is_empty() {
        return None;
    }
    if method.eq_ignore_ascii_case("nearest") {
        return Some(near[0].1);
    }
    let mut num = 0.0;
    let mut den = 0.0;
    let p = pwr.clamp(1.0, 6.0);
    for (d2, v) in near {
        let w = 1.0 / d2.powf(0.5 * p);
        num += w * v;
        den += w;
    }
    (den > 0.0).then_some(num / den)
}

fn render_png(grid: &[Option<f64>], nx: usize, ny: usize, palette: &str, lo: f64, hi: f64, opacity: f64) -> RgbaImage {
    let mut img: RgbaImage = ImageBuffer::new(nx as u32, ny as u32);
    let alpha = (opacity.clamp(0.0, 1.0) * 255.0).round() as u8;
    for iy in 0..ny {
        for ix in 0..nx {
            let src_y = ny - 1 - iy;
            let idx = src_y * nx + ix;
            let px = match grid.get(idx).and_then(|v| *v) {
                Some(v) => {
                    let t = if hi > lo {
                        (v - lo) / (hi - lo)
                    } else {
                        0.5
                    };
                    let [r, g, b] = pick_color(palette, t);
                    Rgba([r, g, b, alpha])
                }
                None => Rgba([0, 0, 0, 0]),
            };
            img.put_pixel(ix as u32, iy as u32, px);
        }
    }
    img
}

fn tile_from_base(base: &RgbaImage, z: u32, x: u32, y: u32, tile_size: u32) -> RgbaImage {
    let n = 1u32 << z;
    let bw = base.width() as f64;
    let bh = base.height() as f64;
    let mut out: RgbaImage = ImageBuffer::new(tile_size, tile_size);
    for ty in 0..tile_size {
        for tx in 0..tile_size {
            let u = (x as f64 + (tx as f64 + 0.5) / tile_size as f64) / n as f64;
            let v = (y as f64 + (ty as f64 + 0.5) / tile_size as f64) / n as f64;
            let sx = (u * bw).clamp(0.0, (base.width().saturating_sub(1)) as f64) as u32;
            let sy = (v * bh).clamp(0.0, (base.height().saturating_sub(1)) as f64) as u32;
            out.put_pixel(tx, ty, *base.get_pixel(sx, sy));
        }
    }
    out
}

fn encode_png(img: &RgbaImage) -> Result<Vec<u8>, NodeError> {
    let mut buf = Vec::<u8>::new();
    let mut cursor = Cursor::new(&mut buf);
    let dyn_img = DynamicImage::ImageRgba8(img.clone());
    dyn_img
        .write_to(&mut cursor, ImageFormat::Png)
        .map_err(|e| NodeError::InvalidConfig(format!("png encode failed: {}", e)))?;
    Ok(buf)
}

fn tile_count_for_zoom_range(min_zoom: u32, max_zoom: u32) -> u64 {
    let mut out = 0u64;
    for z in min_zoom..=max_zoom {
        let n = 1u64 << z;
        out = out.saturating_add(n.saturating_mul(n));
    }
    out
}

pub async fn run_heatmap_raster_tile_cache(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let ui = |p: &str| job.output_spec.pointer(p);
    let parse_f64 = |p: &str, d: f64| ui(p).and_then(parse_num).unwrap_or(d);
    let parse_u64 = |p: &str, d: u64| ui(p).and_then(|v| v.as_u64()).unwrap_or(d);

    let measure = ui("/node_ui/measure")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let method = ui("/node_ui/method")
        .and_then(|v| v.as_str())
        .unwrap_or("idw")
        .trim()
        .to_string();
    let palette = ui("/node_ui/palette")
        .and_then(|v| v.as_str())
        .unwrap_or("rainbow")
        .trim()
        .to_string();
    let opacity = parse_f64("/node_ui/opacity", 0.72).clamp(0.05, 1.0);
    let idw_power = parse_f64("/node_ui/idw_power", 2.0).clamp(1.0, 6.0);
    let max_points = parse_u64("/node_ui/max_points", 32).clamp(4, 256) as usize;
    let clamp_low_pct = parse_f64("/node_ui/clamp_low_pct", 2.0).clamp(0.0, 100.0);
    let clamp_high_pct = parse_f64("/node_ui/clamp_high_pct", 98.0).clamp(0.0, 100.0);
    let nx = parse_u64("/node_ui/grid_nx", 384).clamp(64, 2048) as usize;
    let ny = parse_u64("/node_ui/grid_ny", 384).clamp(64, 2048) as usize;
    let tile_size = parse_u64("/node_ui/tile_size", 256).clamp(128, 512) as u32;
    let ws_default_min_zoom = parse_u64("/workspace_cache_settings/default_min_zoom", 0).clamp(0, 10) as u32;
    let ws_default_max_zoom = parse_u64("/workspace_cache_settings/default_max_zoom", 4).clamp(ws_default_min_zoom as u64, 12) as u32;
    let min_zoom = parse_u64("/node_ui/min_zoom", ws_default_min_zoom as u64).clamp(0, 10) as u32;
    let mut max_zoom = parse_u64("/node_ui/max_zoom", ws_default_max_zoom as u64).clamp(min_zoom as u64, 12) as u32;
    let ws_max_tiles = parse_u64("/workspace_cache_settings/max_tiles", 200_000).max(1024);
    let ws_max_bytes = parse_u64("/workspace_cache_settings/max_bytes", 2_147_483_648).max(4_194_304);
    let estimated_bytes_per_tile = (tile_size as u64).saturating_mul(tile_size as u64).saturating_mul(4);
    while max_zoom > min_zoom {
        let tc = tile_count_for_zoom_range(min_zoom, max_zoom);
        let est_bytes = tc.saturating_mul(estimated_bytes_per_tile);
        if tc <= ws_max_tiles && est_bytes <= ws_max_bytes {
            break;
        }
        max_zoom = max_zoom.saturating_sub(1);
    }

    let mut points: Vec<Value> = Vec::new();
    let mut source_crs = job.project_crs.clone().unwrap_or_else(|| CrsRecord::epsg(4326));
    for ar in &job.input_artifact_refs {
        let v = super::runtime::read_json_artifact(ctx, &ar.key).await?;
        if let Some(crs_v) = v.get("crs") {
            if let Ok(c) = serde_json::from_value::<CrsRecord>(crs_v.clone()) {
                source_crs = c;
            }
        }
        if let Some(arr) = v.get("points").and_then(|x| x.as_array()) {
            points.extend(arr.iter().cloned());
            continue;
        }
        if let Some(arr) = v.get("assay_points").and_then(|x| x.as_array()) {
            points.extend(arr.iter().cloned());
            continue;
        }
    }
    if points.is_empty() {
        return Err(NodeError::InvalidConfig(
            "heatmap_raster_tile_cache requires upstream point_set with points[]".into(),
        ));
    }

    let mut measure_candidates = BTreeSet::<String>::new();
    let mut raw = Vec::<(f64, f64, Map<String, Value>)>::new();
    for p in points {
        let Some(obj) = p.as_object() else { continue };
        let Some(x) = lookup_ci(obj, "x").and_then(parse_num) else { continue };
        let Some(y) = lookup_ci(obj, "y").and_then(parse_num) else { continue };
        let attrs = obj
            .get("attributes")
            .and_then(|a| a.as_object())
            .cloned()
            .unwrap_or_default();
        for (k, v) in &attrs {
            if parse_num(v).is_some() {
                measure_candidates.insert(k.clone());
            }
        }
        raw.push((x, y, attrs));
    }
    if raw.is_empty() {
        return Err(NodeError::InvalidConfig(
            "heatmap_raster_tile_cache found no valid point XY rows".into(),
        ));
    }
    let selected_measure = if !measure.is_empty() {
        measure
    } else {
        measure_candidates
            .iter()
            .next()
            .cloned()
            .ok_or_else(|| NodeError::InvalidConfig("no numeric measure candidates found".into()))?
    };

    let mut samples = Vec::<Sample>::new();
    let mut vals = Vec::<f64>::new();
    let mut xmin = f64::INFINITY;
    let mut xmax = f64::NEG_INFINITY;
    let mut ymin = f64::INFINITY;
    let mut ymax = f64::NEG_INFINITY;
    for (x, y, attrs) in &raw {
        let Some(v) = attrs.get(&selected_measure).and_then(parse_num) else { continue };
        if !v.is_finite() {
            continue;
        }
        samples.push(Sample { x: *x, y: *y, v });
        vals.push(v);
        xmin = xmin.min(*x);
        xmax = xmax.max(*x);
        ymin = ymin.min(*y);
        ymax = ymax.max(*y);
    }
    if samples.len() < 3 {
        return Err(NodeError::InvalidConfig(format!(
            "measure '{}' has too few numeric samples",
            selected_measure
        )));
    }
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let pct = |p: f64| -> f64 {
        let t = p.clamp(0.0, 100.0) / 100.0;
        let i = ((vals.len().saturating_sub(1)) as f64 * t).round() as usize;
        vals[i.min(vals.len().saturating_sub(1))]
    };
    let lo = pct(clamp_low_pct);
    let hi = pct(clamp_high_pct).max(lo + 1e-9);

    let mut grid = vec![None; nx * ny];
    let sx = (xmax - xmin).max(1e-9);
    let sy = (ymax - ymin).max(1e-9);
    for iy in 0..ny {
        let y = ymin + (iy as f64 + 0.5) / ny as f64 * sy;
        for ix in 0..nx {
            let x = xmin + (ix as f64 + 0.5) / nx as f64 * sx;
            let mut v = interpolate(&samples, x, y, &method, idw_power, max_points);
            if let Some(iv) = v {
                v = Some(iv.clamp(lo, hi));
            }
            grid[iy * nx + ix] = v;
        }
    }

    let base = render_png(&grid, nx, ny, &palette, lo, hi, opacity);
    let base_png = encode_png(&base)?;
    let raster_key = format!(
        "graphs/{}/nodes/{}/heatmap_raster.png",
        job.graph_id, job.node_id
    );
    let raster_ref =
        super::runtime::write_artifact(ctx, &raster_key, &base_png, Some("image/png")).await?;

    let tile_count = tile_count_for_zoom_range(min_zoom, max_zoom);
    let est_bytes = tile_count.saturating_mul(estimated_bytes_per_tile);
    let style_hash = hash_string(&format!(
        "{}:{}:{:.3}:{:.3}:{}:{}:{}:{}",
        selected_measure, palette, clamp_low_pct, clamp_high_pct, nx, ny, min_zoom, max_zoom
    ));
    let tile_base = format!("graphs/{}/nodes/{}/tiles/{}", job.graph_id, job.node_id, &style_hash[..12]);
    let mut tile_refs = Vec::new();
    for z in min_zoom..=max_zoom {
        let n = 1u32 << z;
        for x in 0..n {
            for y in 0..n {
                let tile = tile_from_base(&base, z, x, y, tile_size);
                let bytes = encode_png(&tile)?;
                let key = format!("{}/{}/{}/{}.png", tile_base, z, x, y);
                let r = super::runtime::write_artifact(ctx, &key, &bytes, Some("image/png")).await?;
                tile_refs.push(r);
            }
        }
    }

    let tile_manifest = json!({
        "schema_id": "raster.tile_cache.v1",
        "type": "raster_tile_cache",
        "measure": selected_measure,
        "measure_candidates": measure_candidates.into_iter().collect::<Vec<_>>(),
        "source_crs": source_crs,
        "bounds": { "xmin": xmin, "xmax": xmax, "ymin": ymin, "ymax": ymax },
        "grid": { "nx": nx, "ny": ny, "values": grid },
        "render": {
            "palette": palette,
            "opacity": opacity,
            "method": method,
            "idw_power": idw_power,
            "clamp_low_pct": clamp_low_pct,
            "clamp_high_pct": clamp_high_pct,
            "min_visible_render": lo,
            "max_visible_render": hi
        },
        "tiles": {
            "scheme": "xyz_local",
            "tile_size": tile_size,
            "min_zoom": min_zoom,
            "max_zoom": max_zoom,
            "tile_count_estimate": tile_count,
            "estimated_bytes_raw_rgba": est_bytes,
            "style_hash": style_hash,
            "tile_url_template": format!("/files/{}/{{z}}/{{x}}/{{y}}.png", tile_base)
        },
        "image_url": format!("/files/{}", raster_key),
        "display_contract": {
            "renderer": "heat_surface",
            "editable": ["visible", "opacity", "palette"],
            "defaults": { "measure": selected_measure, "palette": palette, "opacity": opacity }
        },
        "heatmap_config": {
            "measure": selected_measure,
            "render_measure": selected_measure,
            "palette": palette,
            "method": method,
            "idw_power": idw_power,
            "clamp_low_pct": clamp_low_pct,
            "clamp_high_pct": clamp_high_pct,
            "min_visible_render": lo,
            "max_visible_render": hi
        }
    });
    let manifest_key = format!(
        "graphs/{}/nodes/{}/raster_tile_manifest.json",
        job.graph_id, job.node_id
    );
    let manifest_bytes = serde_json::to_vec(&tile_manifest)?;
    let manifest_ref = super::runtime::write_artifact(
        ctx,
        &manifest_key,
        &manifest_bytes,
        Some("application/json"),
    )
    .await?;

    let drape_contract = json!({
        "schema_id": "scene3d.tilebroker_response.v1",
        "schema_version": 1,
        "provider_id": "heatmap_raster_tile_cache",
        "provider_label": "Heatmap raster cache",
        "attribution": "Mine Eye heatmap raster cache",
        "source_crs": source_crs,
        "target_crs": source_crs,
        "texture_mode": "tile_template",
        "image_url": format!("/files/{}", raster_key),
        "image_url_candidates": [format!("/files/{}", raster_key)],
        "tile_url_template": format!("/files/{}/{{z}}/{{x}}/{{y}}.png", tile_base),
        "tile_scheme": "xyz_local",
        "tile_min_zoom": min_zoom,
        "tile_max_zoom": max_zoom,
        "tile_size": tile_size,
        "bounds": { "xmin": xmin, "xmax": xmax, "ymin": ymin, "ymax": ymax },
        "z_mode": "flat",
        "display_contract": {
            "display_pointer": "scene3d.imagery_drape",
            "renderer": "drape",
            "editable": ["visible", "opacity", "provider"]
        },
        "cache": {
            "scope": "workspace",
            "status": "miss",
            "style_hash": style_hash,
            "workspace_limits": {
                "max_tiles": ws_max_tiles,
                "max_bytes": ws_max_bytes
            }
        }
    });
    let drape_key = format!(
        "graphs/{}/nodes/{}/heatmap_imagery_drape.json",
        job.graph_id, job.node_id
    );
    let drape_bytes = serde_json::to_vec(&drape_contract)?;
    let drape_ref =
        super::runtime::write_artifact(ctx, &drape_key, &drape_bytes, Some("application/json"))
            .await?;

    let report = json!({
        "schema_id": "report.raster_tile_cache.v1",
        "type": "raster_tile_cache_report",
        "summary": {
            "measure": selected_measure,
            "sample_count": samples.len(),
            "grid_nx": nx,
            "grid_ny": ny,
            "tile_count": tile_refs.len(),
            "min_zoom": min_zoom,
            "max_zoom": max_zoom,
            "tile_count_estimate": tile_count
        },
        "workspace_limits_applied": {
            "max_tiles": ws_max_tiles,
            "max_bytes": ws_max_bytes
        },
        "style": {
            "palette": palette,
            "opacity": opacity,
            "method": method,
            "idw_power": idw_power,
            "clamp_low_pct": clamp_low_pct,
            "clamp_high_pct": clamp_high_pct
        }
    });
    let report_key = format!(
        "graphs/{}/nodes/{}/raster_tile_report.json",
        job.graph_id, job.node_id
    );
    let report_bytes = serde_json::to_vec(&report)?;
    let report_ref =
        super::runtime::write_artifact(ctx, &report_key, &report_bytes, Some("application/json"))
            .await?;

    let mut outputs = vec![manifest_ref.clone(), drape_ref.clone(), raster_ref.clone(), report_ref.clone()];
    outputs.extend(tile_refs.iter().cloned());
    let hashes = outputs.iter().map(|a| a.content_hash.clone()).collect::<Vec<_>>();
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: outputs,
        content_hashes: hashes,
        error_message: None,
    })
}
