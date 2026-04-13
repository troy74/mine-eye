use std::collections::BTreeSet;
use std::io::Cursor;
use std::path::PathBuf;

use image::{DynamicImage, ImageBuffer, ImageFormat, Rgba, RgbaImage};
use kiddo::{KdTree, SquaredEuclidean};
use mine_eye_types::{ArtifactRef, CrsRecord, JobEnvelope, JobResult, JobStatus};
use rayon::prelude::*;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use tokio::fs;

use crate::executor::ExecutionContext;
use crate::NodeError;
use super::colour::interpolate_palette;
use super::parse_util::parse_numeric_value;

// ── Helpers ──────────────────────────────────────────────────────────────────

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

fn hash_bytes_local(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

/// Thin wrappers so call sites in this file don't need to change.
#[inline]
fn parse_num(v: &Value) -> Option<f64> {
    parse_numeric_value(v)
}

#[inline]
fn lookup_ci<'a>(obj: &'a Map<String, Value>, key: &str) -> Option<&'a Value> {
    if let Some(v) = obj.get(key) {
        return Some(v);
    }
    let lk = key.to_ascii_lowercase();
    obj.iter()
        .find(|(k, _)| k.to_ascii_lowercase() == lk)
        .map(|(_, v)| v)
}

#[inline]
fn pick_color(palette: &str, t: f64) -> [u8; 3] {
    interpolate_palette(palette, t)
}

// ── Fix #1: k-d tree IDW ──────────────────────────────────────────────────────
//
// Previously `interpolate` did a linear scan over all samples (O(N)) and then
// sorted to find the k nearest, costing O(N log N) per grid cell.  With a
// 384×384 grid and 50 k samples that is ~7 billion distance ops.
//
// We now build a 2-D k-d tree once from the normalised sample coordinates and
// query it for k-nearest neighbours in O(k log N) per cell — roughly two
// orders of magnitude faster for large surveys.
//
// Coordinates are normalised to [0, 1]² before insertion so the tree's
// distance metric is unaffected by survey aspect ratio.

struct IdwIndex {
    tree: KdTree<f64, 2>,
    values: Vec<f64>,       // parallel to tree leaf order (insertion order)
}

impl IdwIndex {
    fn build(samples: &[Sample], xmin: f64, xrange: f64, ymin: f64, yrange: f64) -> Self {
        let mut tree: KdTree<f64, 2> = KdTree::new();
        let mut values = Vec::with_capacity(samples.len());
        for (i, s) in samples.iter().enumerate() {
            let nx = (s.x - xmin) / xrange.max(1e-12);
            let ny = (s.y - ymin) / yrange.max(1e-12);
            tree.add(&[nx, ny], i as u64);
            values.push(s.v);
        }
        Self { tree, values }
    }

    fn query(&self, nx: f64, ny: f64, method: &str, pwr: f64, k: usize) -> Option<f64> {
        if self.values.is_empty() {
            return None;
        }
        let k = k.min(self.values.len());
        let neighbours = self.tree.nearest_n::<SquaredEuclidean>(&[nx, ny], k);
        if neighbours.is_empty() {
            return None;
        }
        // Exact hit: squared distance ≈ 0
        if neighbours[0].distance < 1e-24 {
            return Some(self.values[neighbours[0].item as usize]);
        }
        if method.eq_ignore_ascii_case("nearest") {
            return Some(self.values[neighbours[0].item as usize]);
        }
        let p = pwr.clamp(1.0, 6.0);
        let mut num = 0.0_f64;
        let mut den = 0.0_f64;
        for n in &neighbours {
            // n.distance is squared_euclidean, so actual distance = sqrt(d2)
            // weight = 1 / d^p = 1 / (d2^(p/2))
            let w = 1.0 / n.distance.powf(0.5 * p).max(1e-30);
            num += w * self.values[n.item as usize];
            den += w;
        }
        (den > 0.0).then_some(num / den)
    }
}

// ── Raster rendering ──────────────────────────────────────────────────────────

fn render_png(grid: &[Option<f64>], nx: usize, ny: usize, palette: &str, lo: f64, hi: f64, opacity: f64) -> RgbaImage {
    let mut img: RgbaImage = ImageBuffer::new(nx as u32, ny as u32);
    let alpha = (opacity.clamp(0.0, 1.0) * 255.0).round() as u8;
    for iy in 0..ny {
        for ix in 0..nx {
            let src_y = ny - 1 - iy;
            let idx = src_y * nx + ix;
            let px = match grid.get(idx).and_then(|v| *v) {
                Some(v) => {
                    let t = if hi > lo { (v - lo) / (hi - lo) } else { 0.5 };
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
    let w_max = base.width().saturating_sub(1);
    let h_max = base.height().saturating_sub(1);
    let mut out: RgbaImage = ImageBuffer::new(tile_size, tile_size);
    for ty in 0..tile_size {
        for tx in 0..tile_size {
            let u = (x as f64 + (tx as f64 + 0.5) / tile_size as f64) / n as f64;
            let v = (y as f64 + (ty as f64 + 0.5) / tile_size as f64) / n as f64;

            // Bilinear sample aligned to pixel centres.
            let px = (u * bw - 0.5).max(0.0);
            let py = (v * bh - 0.5).max(0.0);
            let x0 = (px.floor() as u32).min(w_max);
            let y0 = (py.floor() as u32).min(h_max);
            let x1 = (x0 + 1).min(w_max);
            let y1 = (y0 + 1).min(h_max);
            let fx = (px - px.floor()) as f32;
            let fy = (py - py.floor()) as f32;

            let p00 = base.get_pixel(x0, y0);
            let p10 = base.get_pixel(x1, y0);
            let p01 = base.get_pixel(x0, y1);
            let p11 = base.get_pixel(x1, y1);

            let blend = |c00: u8, c10: u8, c01: u8, c11: u8| -> u8 {
                let top = c00 as f32 * (1.0 - fx) + c10 as f32 * fx;
                let bot = c01 as f32 * (1.0 - fx) + c11 as f32 * fx;
                (top * (1.0 - fy) + bot * fy + 0.5) as u8
            };
            out.put_pixel(tx, ty, image::Rgba([
                blend(p00[0], p10[0], p01[0], p11[0]),
                blend(p00[1], p10[1], p01[1], p11[1]),
                blend(p00[2], p10[2], p01[2], p11[2]),
                blend(p00[3], p10[3], p01[3], p11[3]),
            ]));
        }
    }
    out
}

// Fix #8: removed unnecessary .clone() — DynamicImage::ImageRgba8 takes ownership.
fn encode_png(img: RgbaImage) -> Result<Vec<u8>, NodeError> {
    let mut buf = Vec::<u8>::new();
    let mut cursor = Cursor::new(&mut buf);
    DynamicImage::ImageRgba8(img)
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

// ── Async tile writer (Fix #3) ────────────────────────────────────────────────
//
// Writes a single pre-encoded tile to disk without borrowing ExecutionContext,
// so it can be spawned as an independent tokio task.

async fn write_tile(
    artifact_root: PathBuf,
    relative_key: String,
    bytes: Vec<u8>,
) -> Result<ArtifactRef, NodeError> {
    let path = artifact_root.join(&relative_key);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    fs::write(&path, &bytes).await?;
    let content_hash = hash_bytes_local(&bytes);
    Ok(ArtifactRef {
        key: relative_key,
        content_hash,
        media_type: Some("image/png".into()),
    })
}

// ── Main node entrypoint ──────────────────────────────────────────────────────

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
    let opacity      = parse_f64("/node_ui/opacity",         0.72).clamp(0.05, 1.0);
    let idw_power    = parse_f64("/node_ui/idw_power",       2.0).clamp(1.0, 6.0);
    let max_points   = parse_u64("/node_ui/max_points",      32).clamp(4, 256) as usize;
    let clamp_low_pct  = parse_f64("/node_ui/clamp_low_pct",  2.0).clamp(0.0, 100.0);
    let clamp_high_pct = parse_f64("/node_ui/clamp_high_pct", 98.0).clamp(0.0, 100.0);
    let tile_size    = parse_u64("/node_ui/tile_size",       256).clamp(128, 512) as u32;

    let ws_default_min_zoom = parse_u64("/workspace_cache_settings/default_min_zoom", 0).clamp(0, 10) as u32;
    let ws_default_max_zoom = parse_u64("/workspace_cache_settings/default_max_zoom", 6).clamp(ws_default_min_zoom as u64, 12) as u32;
    let min_zoom = parse_u64("/node_ui/min_zoom", ws_default_min_zoom as u64).clamp(0, 10) as u32;
    let mut max_zoom = parse_u64("/node_ui/max_zoom", ws_default_max_zoom as u64).clamp(min_zoom as u64, 12) as u32;

    let ws_max_tiles = parse_u64("/workspace_cache_settings/max_tiles", 200_000).max(1024);
    let ws_max_bytes = parse_u64("/workspace_cache_settings/max_bytes", 2_147_483_648).max(4_194_304);
    let estimated_bytes_per_tile = (tile_size as u64).saturating_mul(tile_size as u64).saturating_mul(4);
    while max_zoom > min_zoom {
        let tc = tile_count_for_zoom_range(min_zoom, max_zoom);
        if tc <= ws_max_tiles && tc.saturating_mul(estimated_bytes_per_tile) <= ws_max_bytes {
            break;
        }
        max_zoom = max_zoom.saturating_sub(1);
    }

    // Fix #5: Base image resolution scales with the tile pyramid.
    // At max_zoom z the pyramid has 2^z tiles per axis; each tile is tile_size pixels.
    // Setting nx/ny = tile_size × 2^max_zoom means every zoom-level tile maps to native
    // resolution without upscaling artefacts.  We cap at 4096 to avoid OOM on high zoom.
    // User-supplied grid_nx/grid_ny still act as an explicit override.
    let derived_nx = ((tile_size as u64) << max_zoom).min(4096) as usize;
    let derived_ny = derived_nx;
    let nx = parse_u64("/node_ui/grid_nx", derived_nx as u64).clamp(64, 4096) as usize;
    let ny = parse_u64("/node_ui/grid_ny", derived_ny as u64).clamp(64, 4096) as usize;

    // ── Load and validate points ───────────────────────────────────────────

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
    let mut vals    = Vec::<f64>::new();
    let mut xmin = f64::INFINITY;
    let mut xmax = f64::NEG_INFINITY;
    let mut ymin = f64::INFINITY;
    let mut ymax = f64::NEG_INFINITY;
    for (x, y, attrs) in &raw {
        let Some(v) = attrs.get(&selected_measure).and_then(parse_num) else { continue };
        if !v.is_finite() { continue; }
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

    // ── Fix #1: build k-d tree index, then interpolate in parallel ─────────
    //
    // Coordinates are normalised to [0,1]² so the euclidean distance in the
    // k-d tree is consistent regardless of survey aspect ratio.

    let xrange = xmax - xmin;
    let yrange = ymax - ymin;
    let index = IdwIndex::build(&samples, xmin, xrange, ymin, yrange);

    let sx = xrange.max(1e-9);
    let sy = yrange.max(1e-9);
    let method_ref = method.as_str();

    // Rayon parallel map over every (ix, iy) cell.
    // Using a flat linear index avoids nested closures that would need to move
    // `index` — instead the `Fn + Sync` closure borrows it from the enclosing scope.
    // KdTree is read-only during queries and implements Sync, so this is safe.
    let grid: Vec<Option<f64>> = (0..(nx * ny))
        .into_par_iter()
        .map(|idx| {
            let ix = idx % nx;
            let iy = idx / nx;
            let x_world = xmin + (ix as f64 + 0.5) / nx as f64 * sx;
            let y_world = ymin + (iy as f64 + 0.5) / ny as f64 * sy;
            let nx_coord = (x_world - xmin) / xrange.max(1e-12);
            let ny_coord = (y_world - ymin) / yrange.max(1e-12);
            index
                .query(nx_coord, ny_coord, method_ref, idw_power, max_points)
                .map(|v| v.clamp(lo, hi))
        })
        .collect();

    // ── Render base image and write it ─────────────────────────────────────

    let base = render_png(&grid, nx, ny, &palette, lo, hi, opacity);
    let base_png = encode_png(base)?;
    let raster_key = format!(
        "graphs/{}/nodes/{}/heatmap_raster.png",
        job.graph_id, job.node_id
    );
    let raster_ref =
        super::runtime::write_artifact(ctx, &raster_key, &base_png, Some("image/png")).await?;

    // ── Fix #6: include input content-hashes in style hash ─────────────────
    //
    // Previously the hash only covered UI parameters.  If upstream data
    // changed without UI changes the hash stayed the same and stale tiles
    // would be served.  Including the input artifact content hashes ensures
    // the tile prefix changes whenever data changes.

    let input_hash_fragment: String = {
        let mut sorted: Vec<&str> = job.input_artifact_refs
            .iter()
            .map(|a| a.content_hash.as_str())
            .collect();
        sorted.sort_unstable();
        sorted.join(",")
    };

    let style_hash = hash_string(&format!(
        "{}:{}:{:.3}:{:.3}:{}:{}:{}:{}:{}",
        selected_measure, palette, clamp_low_pct, clamp_high_pct,
        nx, ny, min_zoom, max_zoom, input_hash_fragment
    ));
    let tile_base = format!(
        "graphs/{}/nodes/{}/tiles/{}",
        job.graph_id, job.node_id, &style_hash[..12]
    );

    // ── Fix #3: encode tiles in parallel (rayon), write concurrently ───────
    //
    // 1. Collect (z, x, y) tuples.
    // 2. Encode every tile on the rayon thread pool (CPU-bound) in parallel.
    // 3. Spawn an independent tokio task per tile for the async write (I/O-bound).
    //    Tasks own their data — no borrow of ExecutionContext needed.
    // 4. Await all tasks together, preserving error propagation.

    let tile_coords: Vec<(u32, u32, u32)> = (min_zoom..=max_zoom)
        .flat_map(|z| {
            let n = 1u32 << z;
            (0..n).flat_map(move |x| (0..n).map(move |y| (z, x, y)))
        })
        .collect();

    // We need the base RgbaImage for tile slicing.  Re-decode from the PNG bytes once.
    let base_img: RgbaImage = image::load_from_memory(&base_png)
        .map_err(|e| NodeError::InvalidConfig(format!("base re-decode: {}", e)))?
        .into_rgba8();

    let encoded_tiles: Vec<(String, Vec<u8>)> = tile_coords
        .par_iter()
        .map(|(z, x, y)| {
            let tile = tile_from_base(&base_img, *z, *x, *y, tile_size);
            let bytes = encode_png(tile)?;
            let key = format!("{}/{}/{}/{}.png", tile_base, z, x, y);
            Ok::<_, NodeError>((key, bytes))
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Spawn independent tokio write tasks — owned data, no lifetime constraint.
    let artifact_root: PathBuf = ctx.artifact_root.to_path_buf();
    let tile_count = encoded_tiles.len();
    let write_tasks: Vec<_> = encoded_tiles
        .into_iter()
        .map(|(key, bytes)| {
            let root = artifact_root.clone();
            tokio::spawn(write_tile(root, key, bytes))
        })
        .collect();

    let mut tile_refs: Vec<ArtifactRef> = Vec::with_capacity(tile_count);
    for handle in write_tasks {
        tile_refs.push(handle.await.map_err(|e| NodeError::InvalidConfig(e.to_string()))??);
    }

    // ── Manifest and ancillary artifacts ──────────────────────────────────

    let tile_count_est = tile_count_for_zoom_range(min_zoom, max_zoom);
    let est_bytes = tile_count_est.saturating_mul(estimated_bytes_per_tile);

    let tile_manifest = json!({
        "schema_id": "raster.tile_cache.v1",
        "type": "raster_tile_cache",
        "measure": selected_measure,
        "measure_candidates": measure_candidates.into_iter().collect::<Vec<_>>(),
        "source_crs": source_crs,
        "bounds": { "xmin": xmin, "xmax": xmax, "ymin": ymin, "ymax": ymax },
        // Grid dimensions only — values are not stored; tiles are the canonical output.
        "grid": { "nx": nx, "ny": ny },
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
            "tile_count_estimate": tile_count_est,
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
        ctx, &manifest_key, &manifest_bytes, Some("application/json"),
    ).await?;

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
    let drape_ref = super::runtime::write_artifact(
        ctx, &drape_key, &drape_bytes, Some("application/json"),
    ).await?;

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
            "tile_count_estimate": tile_count_est
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
    let report_ref = super::runtime::write_artifact(
        ctx, &report_key, &report_bytes, Some("application/json"),
    ).await?;

    let mut outputs = vec![
        manifest_ref.clone(),
        drape_ref.clone(),
        raster_ref.clone(),
        report_ref.clone(),
    ];
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
