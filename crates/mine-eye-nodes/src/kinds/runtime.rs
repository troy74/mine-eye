use mine_eye_types::{
    ArtifactRef, CollarRecord, CrsRecord, IntervalSampleRecord, JobEnvelope, JobResult, JobStatus,
    SurveyStationRecord,
};
use sha2::{Digest, Sha256};
use tokio::fs;

use crate::crs_transform::transform_xy;
use crate::executor::ExecutionContext;
use crate::kinds::tile_cache::{TileCache, DEM_TTL_S, OPEN_METEO_TTL_S};
use crate::NodeError;

pub(crate) fn collar_output_crs_mode(job: &JobEnvelope) -> &str {
    job.output_spec
        .pointer("/node_ui/output_crs_mode")
        .and_then(|v| v.as_str())
        .unwrap_or("project")
}

/// Target CRS for written collar coordinates, or `None` when output should stay in source CRS.
pub(crate) fn collar_output_target_crs(job: &JobEnvelope) -> Result<Option<CrsRecord>, NodeError> {
    match collar_output_crs_mode(job) {
        "source" => Ok(None),
        "wgs84" => Ok(Some(CrsRecord::epsg(4326))),
        "custom" => {
            let e = job
                .output_spec
                .pointer("/node_ui/output_crs_epsg")
                .and_then(|v| v.as_u64())
                .unwrap_or(4326) as i32;
            Ok(Some(CrsRecord::epsg(e)))
        }
        "project" | _ => Ok(Some(
            job.project_crs
                .clone()
                .unwrap_or_else(|| CrsRecord::epsg(4326)),
        )),
    }
}

fn hash_bytes(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

pub(crate) async fn write_artifact(
    ctx: &ExecutionContext<'_>,
    relative_key: &str,
    bytes: &[u8],
    media_type: Option<&str>,
) -> Result<ArtifactRef, NodeError> {
    let path = ctx.artifact_root.join(relative_key);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    fs::write(&path, bytes).await?;
    let content_hash = hash_bytes(bytes);
    Ok(ArtifactRef {
        key: relative_key.to_string(),
        content_hash,
        media_type: media_type.map(String::from),
    })
}

pub(crate) async fn read_json_artifact(
    ctx: &ExecutionContext<'_>,
    key: &str,
) -> Result<serde_json::Value, NodeError> {
    let path = ctx.artifact_root.join(key);
    let raw = fs::read(&path).await?;
    let v: serde_json::Value = serde_json::from_slice(&raw)?;
    Ok(v)
}

pub(crate) async fn read_artifact_bytes(
    ctx: &ExecutionContext<'_>,
    key: &str,
) -> Result<Vec<u8>, NodeError> {
    let path = ctx.artifact_root.join(key);
    let raw = fs::read(&path).await?;
    Ok(raw)
}

pub(crate) async fn collect_drillhole_inputs(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<
    (
        Vec<CollarRecord>,
        Vec<SurveyStationRecord>,
        Vec<IntervalSampleRecord>,
    ),
    NodeError,
> {
    let mut collars: Vec<CollarRecord> = Vec::new();
    let mut surveys: Vec<SurveyStationRecord> = Vec::new();
    let mut assays: Vec<IntervalSampleRecord> = Vec::new();

    for ar in &job.input_artifact_refs {
        let v = read_json_artifact(ctx, &ar.key).await?;
        if let Some(c) = v.get("collars") {
            if let Ok(mut part) = serde_json::from_value::<Vec<CollarRecord>>(c.clone()) {
                collars.append(&mut part);
            }
        }
        if let Some(s) = v.get("surveys") {
            if let Ok(mut part) = serde_json::from_value::<Vec<SurveyStationRecord>>(s.clone()) {
                surveys.append(&mut part);
            }
        }
        if let Some(a) = v.get("assays") {
            if let Ok(mut part) = serde_json::from_value::<Vec<IntervalSampleRecord>>(a.clone()) {
                assays.append(&mut part);
            }
        }
    }

    Ok((collars, surveys, assays))
}

/// Parses collars/surveys/assays from job config params JSON and writes canonical JSON artifact.
#[derive(Clone, Copy)]
struct HeatPt {
    x: f64,
    y: f64,
    v: f64,
}

fn interpolate_value(
    samples: &[HeatPt],
    x: f64,
    y: f64,
    method: &str,
    power: f64,
    search_radius_m: f64,
    min_points: usize,
    max_points: usize,
) -> Option<f64> {
    let mut near: Vec<(f64, f64)> = samples
        .iter()
        .filter_map(|s| {
            let dx = x - s.x;
            let dy = y - s.y;
            let d2 = dx * dx + dy * dy;
            if d2 < 1e-12 {
                return Some((0.0, s.v));
            }
            if search_radius_m > 0.0 && d2.sqrt() > search_radius_m {
                return None;
            }
            Some((d2, s.v))
        })
        .collect();
    if near.is_empty() {
        return None;
    }
    near.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    if near.len() > max_points {
        near.truncate(max_points);
    }
    if near.len() < min_points {
        return None;
    }
    if near[0].0 <= 0.0 {
        return Some(near[0].1);
    }

    let mut num = 0.0;
    let mut den = 0.0;
    match method {
        "nearest" => Some(near[0].1),
        "rbf" => {
            let scale = near.last().map(|x| x.0.sqrt()).unwrap_or(1.0).max(1e-9);
            for (d2, v) in near {
                let d = d2.sqrt();
                let w = (-(d / scale).powi(2)).exp();
                num += w * v;
                den += w;
            }
            (den > 0.0).then_some(num / den)
        }
        "kriging" => {
            let range = near.last().map(|x| x.0.sqrt()).unwrap_or(1.0).max(1e-9);
            let nugget = 0.05;
            for (d2, v) in near {
                let d = d2.sqrt();
                let gamma = 1.0 - (-(d / range)).exp();
                let w = 1.0 / (nugget + gamma.max(1e-6));
                num += w * v;
                den += w;
            }
            (den > 0.0).then_some(num / den)
        }
        _ => {
            let p = power.clamp(1.0, 6.0);
            for (d2, v) in near {
                let w = 1.0 / d2.powf(0.5 * p);
                num += w * v;
                den += w;
            }
            (den > 0.0).then_some(num / den)
        }
    }
}

fn marching_segments(
    grid: &[Option<f64>],
    nx: usize,
    ny: usize,
    xmin: f64,
    xmax: f64,
    ymin: f64,
    ymax: f64,
    level: f64,
) -> Vec<[[f64; 2]; 2]> {
    let mut out = Vec::new();
    if nx < 2 || ny < 2 {
        return out;
    }
    let dx = (xmax - xmin) / (nx as f64 - 1.0);
    let dy = (ymax - ymin) / (ny as f64 - 1.0);
    let idx = |ix: usize, iy: usize| -> usize { iy * nx + ix };
    let interp = |a: (f64, f64, f64), b: (f64, f64, f64)| -> [f64; 2] {
        let (ax, ay, av) = a;
        let (bx, by, bv) = b;
        let t = if (bv - av).abs() < 1e-12 {
            0.5
        } else {
            ((level - av) / (bv - av)).clamp(0.0, 1.0)
        };
        [ax + (bx - ax) * t, ay + (by - ay) * t]
    };

    for iy in 0..(ny - 1) {
        for ix in 0..(nx - 1) {
            let p00 = (xmin + ix as f64 * dx, ymin + iy as f64 * dy);
            let p10 = (xmin + (ix + 1) as f64 * dx, ymin + iy as f64 * dy);
            let p11 = (xmin + (ix + 1) as f64 * dx, ymin + (iy + 1) as f64 * dy);
            let p01 = (xmin + ix as f64 * dx, ymin + (iy + 1) as f64 * dy);

            let Some(v00) = grid[idx(ix, iy)] else {
                continue;
            };
            let Some(v10) = grid[idx(ix + 1, iy)] else {
                continue;
            };
            let Some(v11) = grid[idx(ix + 1, iy + 1)] else {
                continue;
            };
            let Some(v01) = grid[idx(ix, iy + 1)] else {
                continue;
            };

            let mut crosses: Vec<[f64; 2]> = Vec::new();
            let s00 = v00 >= level;
            let s10 = v10 >= level;
            let s11 = v11 >= level;
            let s01 = v01 >= level;
            if s00 != s10 {
                crosses.push(interp((p00.0, p00.1, v00), (p10.0, p10.1, v10)));
            }
            if s10 != s11 {
                crosses.push(interp((p10.0, p10.1, v10), (p11.0, p11.1, v11)));
            }
            if s11 != s01 {
                crosses.push(interp((p11.0, p11.1, v11), (p01.0, p01.1, v01)));
            }
            if s01 != s00 {
                crosses.push(interp((p01.0, p01.1, v01), (p00.0, p00.1, v00)));
            }

            if crosses.len() == 2 {
                out.push([crosses[0], crosses[1]]);
            } else if crosses.len() == 4 {
                out.push([crosses[0], crosses[1]]);
                out.push([crosses[2], crosses[3]]);
            }
        }
    }
    out
}

#[derive(Clone, Copy)]
pub(crate) struct XYZ {
    x: f64,
    y: f64,
    z: f64,
}

pub(crate) fn collect_xyz_points(v: &serde_json::Value) -> Vec<XYZ> {
    let mut out = Vec::new();
    let push_row = |out: &mut Vec<XYZ>, row: &serde_json::Value| {
        let Some(obj) = row.as_object() else {
            return;
        };
        let x = obj.get("x").and_then(|v| v.as_f64());
        let y = obj.get("y").and_then(|v| v.as_f64());
        let z = obj.get("z").and_then(|v| v.as_f64());
        if let (Some(x), Some(y), Some(z)) = (x, y, z) {
            if x.is_finite() && y.is_finite() && z.is_finite() {
                out.push(XYZ { x, y, z });
            }
        }
    };

    if let Some(arr) = v.get("points").and_then(|a| a.as_array()) {
        for row in arr {
            push_row(&mut out, row);
        }
    }
    if let Some(arr) = v.get("collars").and_then(|a| a.as_array()) {
        for row in arr {
            push_row(&mut out, row);
        }
    }
    if let Some(arr) = v.get("assay_points").and_then(|a| a.as_array()) {
        for row in arr {
            push_row(&mut out, row);
        }
    }
    if let Some(arr) = v.get("surveys").and_then(|a| a.as_array()) {
        for row in arr {
            push_row(&mut out, row);
        }
    }
    if let Some(arr) = v.get("segments").and_then(|a| a.as_array()) {
        for row in arr {
            let Some(obj) = row.as_object() else {
                continue;
            };
            if let Some(from_xyz) = obj.get("from_xyz").and_then(|x| x.as_array()) {
                if from_xyz.len() >= 3 {
                    if let (Some(x), Some(y), Some(z)) = (
                        from_xyz[0].as_f64(),
                        from_xyz[1].as_f64(),
                        from_xyz[2].as_f64(),
                    ) {
                        out.push(XYZ { x, y, z });
                    }
                }
            }
            if let Some(to_xyz) = obj.get("to_xyz").and_then(|x| x.as_array()) {
                if to_xyz.len() >= 3 {
                    if let (Some(x), Some(y), Some(z)) =
                        (to_xyz[0].as_f64(), to_xyz[1].as_f64(), to_xyz[2].as_f64())
                    {
                        out.push(XYZ { x, y, z });
                    }
                }
            }
            let xf = obj.get("x_from").and_then(|v| v.as_f64());
            let yf = obj.get("y_from").and_then(|v| v.as_f64());
            let zf = obj.get("z_from").and_then(|v| v.as_f64());
            if let (Some(x), Some(y), Some(z)) = (xf, yf, zf) {
                out.push(XYZ { x, y, z });
            }
            let xt = obj.get("x_to").and_then(|v| v.as_f64());
            let yt = obj.get("y_to").and_then(|v| v.as_f64());
            let zt = obj.get("z_to").and_then(|v| v.as_f64());
            if let (Some(x), Some(y), Some(z)) = (xt, yt, zt) {
                out.push(XYZ { x, y, z });
            }
        }
    }
    if v.is_array() {
        if let Some(rows) = v.as_array() {
            for row in rows {
                push_row(&mut out, row);
            }
        }
    }
    out
}

pub(crate) fn collect_xy_points(v: &serde_json::Value) -> Vec<(f64, f64)> {
    let mut out = Vec::new();
    let collect_coords = |coords: &serde_json::Value, out: &mut Vec<(f64, f64)>| {
        fn walk(node: &serde_json::Value, out: &mut Vec<(f64, f64)>) {
            if let Some(arr) = node.as_array() {
                if arr.len() >= 2 {
                    if let (Some(x), Some(y)) = (arr[0].as_f64(), arr[1].as_f64()) {
                        if x.is_finite() && y.is_finite() {
                            out.push((x, y));
                            return;
                        }
                    }
                }
                for child in arr {
                    walk(child, out);
                }
            }
        }
        walk(coords, out);
    };
    let push_row = |out: &mut Vec<(f64, f64)>, row: &serde_json::Value| {
        let Some(obj) = row.as_object() else {
            return;
        };
        let x = obj.get("x").and_then(|v| v.as_f64());
        let y = obj.get("y").and_then(|v| v.as_f64());
        if let (Some(x), Some(y)) = (x, y) {
            if x.is_finite() && y.is_finite() {
                out.push((x, y));
            }
        }
    };

    if let Some(arr) = v.get("points").and_then(|a| a.as_array()) {
        for row in arr {
            push_row(&mut out, row);
        }
    }
    if let Some(arr) = v.get("collars").and_then(|a| a.as_array()) {
        for row in arr {
            push_row(&mut out, row);
        }
    }
    if let Some(arr) = v.get("assay_points").and_then(|a| a.as_array()) {
        for row in arr {
            push_row(&mut out, row);
        }
    }
    if let Some(arr) = v.get("surveys").and_then(|a| a.as_array()) {
        for row in arr {
            push_row(&mut out, row);
        }
    }
    if let Some(arr) = v.get("segments").and_then(|a| a.as_array()) {
        for row in arr {
            let Some(obj) = row.as_object() else {
                continue;
            };
            let xf = obj.get("x_from").and_then(|v| v.as_f64());
            let yf = obj.get("y_from").and_then(|v| v.as_f64());
            if let (Some(x), Some(y)) = (xf, yf) {
                if x.is_finite() && y.is_finite() {
                    out.push((x, y));
                }
            }
            let xt = obj.get("x_to").and_then(|v| v.as_f64());
            let yt = obj.get("y_to").and_then(|v| v.as_f64());
            if let (Some(x), Some(y)) = (xt, yt) {
                if x.is_finite() && y.is_finite() {
                    out.push((x, y));
                }
            }
        }
    }
    if let Some(b) = v.get("bounds").and_then(|x| x.as_object()) {
        let xmin = b.get("xmin").and_then(|x| x.as_f64());
        let xmax = b.get("xmax").and_then(|x| x.as_f64());
        let ymin = b.get("ymin").and_then(|x| x.as_f64());
        let ymax = b.get("ymax").and_then(|x| x.as_f64());
        if let (Some(xmin), Some(xmax), Some(ymin), Some(ymax)) = (xmin, xmax, ymin, ymax) {
            if xmin.is_finite() && ymin.is_finite() {
                out.push((xmin, ymin));
            }
            if xmin.is_finite() && ymax.is_finite() {
                out.push((xmin, ymax));
            }
            if xmax.is_finite() && ymin.is_finite() {
                out.push((xmax, ymin));
            }
            if xmax.is_finite() && ymax.is_finite() {
                out.push((xmax, ymax));
            }
        }
    }
    if let Some(g) = v.get("surface_grid").and_then(|x| x.as_object()) {
        let xmin = g.get("xmin").and_then(|x| x.as_f64());
        let xmax = g.get("xmax").and_then(|x| x.as_f64());
        let ymin = g.get("ymin").and_then(|x| x.as_f64());
        let ymax = g.get("ymax").and_then(|x| x.as_f64());
        if let (Some(xmin), Some(xmax), Some(ymin), Some(ymax)) = (xmin, xmax, ymin, ymax) {
            if xmin.is_finite() && ymin.is_finite() {
                out.push((xmin, ymin));
            }
            if xmin.is_finite() && ymax.is_finite() {
                out.push((xmin, ymax));
            }
            if xmax.is_finite() && ymin.is_finite() {
                out.push((xmax, ymin));
            }
            if xmax.is_finite() && ymax.is_finite() {
                out.push((xmax, ymax));
            }
        }
    }
    if let Some(geom) = v.get("geometry").and_then(|x| x.as_object()) {
        if let Some(coords) = geom.get("coordinates") {
            collect_coords(coords, &mut out);
        }
    }
    if let Some(features) = v.get("features").and_then(|x| x.as_array()) {
        for f in features {
            if let Some(geom) = f.get("geometry").and_then(|x| x.as_object()) {
                if let Some(coords) = geom.get("coordinates") {
                    collect_coords(coords, &mut out);
                }
            }
        }
    }
    out
}

pub(crate) fn infer_extent(points: &[XYZ], pad_pct: f64) -> Option<(f64, f64, f64, f64)> {
    if points.is_empty() {
        return None;
    }
    let mut xmin = f64::INFINITY;
    let mut xmax = f64::NEG_INFINITY;
    let mut ymin = f64::INFINITY;
    let mut ymax = f64::NEG_INFINITY;
    for p in points {
        xmin = xmin.min(p.x);
        xmax = xmax.max(p.x);
        ymin = ymin.min(p.y);
        ymax = ymax.max(p.y);
    }
    let dx = (xmax - xmin).abs().max(1e-6);
    let dy = (ymax - ymin).abs().max(1e-6);
    let px = dx * pad_pct.max(0.0);
    let py = dy * pad_pct.max(0.0);
    Some((xmin - px, xmax + px, ymin - py, ymax + py))
}

pub(crate) fn infer_extent_xy(points: &[(f64, f64)], pad_pct: f64) -> Option<(f64, f64, f64, f64)> {
    if points.is_empty() {
        return None;
    }
    let mut xmin = f64::INFINITY;
    let mut xmax = f64::NEG_INFINITY;
    let mut ymin = f64::INFINITY;
    let mut ymax = f64::NEG_INFINITY;
    for (x, y) in points.iter().copied() {
        xmin = xmin.min(x);
        xmax = xmax.max(x);
        ymin = ymin.min(y);
        ymax = ymax.max(y);
    }
    if !xmin.is_finite() || !xmax.is_finite() || !ymin.is_finite() || !ymax.is_finite() {
        return None;
    }
    let dx = (xmax - xmin).abs().max(1e-6);
    let dy = (ymax - ymin).abs().max(1e-6);
    let pad = pad_pct.clamp(0.0, 2.0);
    Some((
        xmin - dx * pad,
        xmax + dx * pad,
        ymin - dy * pad,
        ymax + dy * pad,
    ))
}

pub(crate) fn merge_extents(
    a: Option<(f64, f64, f64, f64)>,
    b: Option<(f64, f64, f64, f64)>,
) -> Option<(f64, f64, f64, f64)> {
    match (a, b) {
        (Some((ax0, ax1, ay0, ay1)), Some((bx0, bx1, by0, by1))) => {
            Some((ax0.min(bx0), ax1.max(bx1), ay0.min(by0), ay1.max(by1)))
        }
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn grid_dims_from_extent(
    xmin: f64,
    xmax: f64,
    ymin: f64,
    ymax: f64,
    resolution_hint: f64,
    max_cells: usize,
) -> (usize, usize) {
    let dx = (xmax - xmin).abs().max(1e-6);
    let dy = (ymax - ymin).abs().max(1e-6);
    let res = resolution_hint.max(1e-6);
    let mut nx = ((dx / res).ceil() as usize + 1).clamp(8, 512);
    let mut ny = ((dy / res).ceil() as usize + 1).clamp(8, 512);
    while nx.saturating_mul(ny) > max_cells.max(256) {
        nx = ((nx as f64) * 0.9).floor().max(8.0) as usize;
        ny = ((ny as f64) * 0.9).floor().max(8.0) as usize;
        if nx <= 8 && ny <= 8 {
            break;
        }
    }
    (nx.max(2), ny.max(2))
}

fn idw_surface_from_xyz(
    points: &[XYZ],
    nx: usize,
    ny: usize,
    xmin: f64,
    xmax: f64,
    ymin: f64,
    ymax: f64,
) -> Vec<Option<f64>> {
    let mut out = Vec::with_capacity(nx * ny);
    if points.is_empty() {
        out.resize(nx * ny, None);
        return out;
    }
    for iy in 0..ny {
        for ix in 0..nx {
            let x = xmin + (ix as f64 / (nx.saturating_sub(1).max(1) as f64)) * (xmax - xmin);
            let y = ymin + (iy as f64 / (ny.saturating_sub(1).max(1) as f64)) * (ymax - ymin);
            let mut num = 0.0;
            let mut den = 0.0;
            let mut snapped: Option<f64> = None;
            for p in points {
                let dx = x - p.x;
                let dy = y - p.y;
                let d2 = dx * dx + dy * dy;
                if d2 <= 1e-12 {
                    snapped = Some(p.z);
                    break;
                }
                let w = 1.0 / d2;
                num += w * p.z;
                den += w;
            }
            out.push(snapped.or_else(|| (den > 0.0).then_some(num / den)));
        }
    }
    out
}

pub(crate) fn bilinear_from_grid(
    nx: usize,
    ny: usize,
    xmin: f64,
    xmax: f64,
    ymin: f64,
    ymax: f64,
    values: &[Option<f64>],
    x: f64,
    y: f64,
) -> Option<f64> {
    if nx < 2 || ny < 2 {
        return None;
    }
    if x < xmin || x > xmax || y < ymin || y > ymax {
        return None;
    }
    let tx = ((x - xmin) / (xmax - xmin).max(1e-9)).clamp(0.0, 1.0) * ((nx - 1) as f64);
    let ty = ((y - ymin) / (ymax - ymin).max(1e-9)).clamp(0.0, 1.0) * ((ny - 1) as f64);
    let ix0 = tx.floor() as usize;
    let iy0 = ty.floor() as usize;
    let ix1 = (ix0 + 1).min(nx - 1);
    let iy1 = (iy0 + 1).min(ny - 1);
    let fx = tx - ix0 as f64;
    let fy = ty - iy0 as f64;
    let idx = |ix: usize, iy: usize| -> usize { iy * nx + ix };
    let v00 = values.get(idx(ix0, iy0)).copied().flatten()?;
    let v10 = values.get(idx(ix1, iy0)).copied().flatten()?;
    let v01 = values.get(idx(ix0, iy1)).copied().flatten()?;
    let v11 = values.get(idx(ix1, iy1)).copied().flatten()?;
    let a = v00 * (1.0 - fx) + v10 * fx;
    let b = v01 * (1.0 - fx) + v11 * fx;
    Some(a * (1.0 - fy) + b * fy)
}

/// Phase-2 heatmap node: configurable interpolation + contour and diagnostics artifacts.
pub(crate) async fn run_assay_heatmap_impl(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let mut points: Vec<serde_json::Value> = Vec::new();
    for ar in &job.input_artifact_refs {
        let v = read_json_artifact(ctx, &ar.key).await?;
        if let Some(arr) = v.get("assay_points").and_then(|x| x.as_array()) {
            points.extend(arr.iter().cloned());
        } else if let Some(arr) = v.get("points").and_then(|x| x.as_array()) {
            points.extend(arr.iter().cloned());
        }
    }

    let measure = job
        .output_spec
        .pointer("/node_ui/measure")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let method = job
        .output_spec
        .pointer("/node_ui/method")
        .and_then(|v| v.as_str())
        .unwrap_or("idw")
        .to_string();
    let scale = job
        .output_spec
        .pointer("/node_ui/scale")
        .and_then(|v| v.as_str())
        .unwrap_or("linear")
        .to_string();
    let palette = job
        .output_spec
        .pointer("/node_ui/palette")
        .and_then(|v| v.as_str())
        .unwrap_or("rainbow")
        .to_string();
    let clamp_low = job
        .output_spec
        .pointer("/node_ui/clamp_low_pct")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let clamp_high = job
        .output_spec
        .pointer("/node_ui/clamp_high_pct")
        .and_then(|v| v.as_f64())
        .unwrap_or(100.0);
    let idw_power = job
        .output_spec
        .pointer("/node_ui/idw_power")
        .and_then(|v| v.as_f64())
        .unwrap_or(2.0)
        .clamp(1.0, 4.0);
    let smoothness = job
        .output_spec
        .pointer("/node_ui/smoothness")
        .and_then(|v| v.as_u64())
        .unwrap_or(256)
        .clamp(128, 512);
    let search_radius_m = job
        .output_spec
        .pointer("/node_ui/search_radius_m")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0)
        .max(0.0);
    let min_points = job
        .output_spec
        .pointer("/node_ui/min_points")
        .and_then(|v| v.as_u64())
        .unwrap_or(3)
        .max(1) as usize;
    let max_points = job
        .output_spec
        .pointer("/node_ui/max_points")
        .and_then(|v| v.as_u64())
        .unwrap_or(32)
        .max(min_points as u64) as usize;
    let contours_enabled = job
        .output_spec
        .pointer("/node_ui/contours_enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let contour_mode = job
        .output_spec
        .pointer("/node_ui/contour_mode")
        .and_then(|v| v.as_str())
        .unwrap_or("fixed_interval")
        .to_string();
    let contour_interval = job
        .output_spec
        .pointer("/node_ui/contour_interval")
        .and_then(|v| v.as_f64())
        .unwrap_or(1.0)
        .max(0.0001);
    let contour_levels = job
        .output_spec
        .pointer("/node_ui/contour_levels")
        .and_then(|v| v.as_u64())
        .unwrap_or(10)
        .max(2) as usize;
    let contour_levels_list: Vec<f64> = job
        .output_spec
        .pointer("/node_ui/contour_levels_list")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_f64())
                .filter(|x| x.is_finite())
                .collect::<Vec<f64>>()
        })
        .unwrap_or_default();
    let gradient_enabled = job
        .output_spec
        .pointer("/node_ui/gradient_enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let gradient_mode = job
        .output_spec
        .pointer("/node_ui/gradient_mode")
        .and_then(|v| v.as_str())
        .unwrap_or("magnitude")
        .to_string();
    let min_visible_raw = job
        .output_spec
        .pointer("/node_ui/min_visible_value")
        .and_then(|v| v.as_f64());
    let max_visible_raw = job
        .output_spec
        .pointer("/node_ui/max_visible_value")
        .and_then(|v| v.as_f64());

    let mut measure_candidates: Vec<String> = Vec::new();
    for p in &points {
        let Some(obj) = p.as_object() else {
            continue;
        };
        let attrs = obj
            .get("attributes")
            .and_then(|a| a.as_object())
            .cloned()
            .unwrap_or_default();
        for (k, v) in attrs {
            if v.is_number() && !measure_candidates.contains(&k) {
                measure_candidates.push(k);
            }
        }
    }
    measure_candidates.sort();
    let selected_measure = if !measure.is_empty() {
        measure.clone()
    } else {
        measure_candidates.first().cloned().unwrap_or_default()
    };

    let mut raw_values: Vec<f64> = Vec::new();
    for p in &points {
        let Some(obj) = p.as_object() else {
            continue;
        };
        let Some(attrs) = obj.get("attributes").and_then(|a| a.as_object()) else {
            continue;
        };
        let Some(v) = attrs.get(&selected_measure).and_then(|x| x.as_f64()) else {
            continue;
        };
        if v.is_finite() {
            raw_values.push(v);
        }
    }
    raw_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let pctl = |pct: f64| -> f64 {
        if raw_values.is_empty() {
            return 0.0;
        }
        let t = pct.clamp(0.0, 100.0) / 100.0;
        let idx = ((raw_values.len().saturating_sub(1)) as f64 * t).round() as usize;
        raw_values[idx.min(raw_values.len().saturating_sub(1))]
    };
    let lo = pctl(clamp_low);
    let hi = pctl(clamp_high).max(lo);
    let transform = |v0: f64| -> Option<f64> {
        let mut v = v0.clamp(lo, hi);
        match scale.as_str() {
            "log10" => {
                if v <= 0.0 {
                    return None;
                }
                v = v.log10();
            }
            "ln" => {
                if v <= 0.0 {
                    return None;
                }
                v = v.ln();
            }
            "sqrt" => {
                if v < 0.0 {
                    return None;
                }
                v = v.sqrt();
            }
            _ => {}
        }
        Some(v)
    };

    let mut transformed_values: Vec<f64> = Vec::new();
    let mut enriched_points: Vec<serde_json::Value> = Vec::new();
    for mut p in points {
        let Some(obj) = p.as_object_mut() else {
            continue;
        };
        let attrs = obj
            .entry("attributes")
            .or_insert_with(|| serde_json::json!({}));
        let Some(attrs_obj) = attrs.as_object_mut() else {
            continue;
        };
        if let Some(raw) = attrs_obj.get(&selected_measure).and_then(|x| x.as_f64()) {
            if let Some(tv) = transform(raw) {
                transformed_values.push(tv);
                attrs_obj.insert("__heatmap_value".into(), serde_json::json!(tv));
            }
        }
        enriched_points.push(serde_json::Value::Object(obj.clone()));
    }
    transformed_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let (vmin, vmax) = match (
        transformed_values.first().copied(),
        transformed_values.last().copied(),
    ) {
        (Some(a), Some(b)) => (a, b),
        _ => (0.0, 0.0),
    };

    let mut contour_breaks: Vec<f64> = Vec::new();
    if contours_enabled && vmax > vmin {
        if !contour_levels_list.is_empty() {
            contour_breaks = contour_levels_list
                .iter()
                .filter_map(|v| transform(*v))
                .collect();
            contour_breaks.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            contour_breaks.dedup_by(|a, b| (*a - *b).abs() < 1e-9);
        } else if contour_mode == "quantile" {
            for i in 1..contour_levels {
                let t = (i as f64) / (contour_levels as f64);
                let idx =
                    ((transformed_values.len().saturating_sub(1)) as f64 * t).round() as usize;
                contour_breaks
                    .push(transformed_values[idx.min(transformed_values.len().saturating_sub(1))]);
            }
        } else {
            let mut x = vmin + contour_interval;
            while x < vmax {
                contour_breaks.push(x);
                x += contour_interval;
            }
        }
    }

    let min_visible_render = min_visible_raw.and_then(transform);
    let max_visible_render = max_visible_raw.and_then(transform);

    let heat_samples: Vec<HeatPt> = enriched_points
        .iter()
        .filter_map(|p| {
            let obj = p.as_object()?;
            let x = obj.get("x")?.as_f64()?;
            let y = obj.get("y")?.as_f64()?;
            let attrs = obj.get("attributes")?.as_object()?;
            let v = attrs.get("__heatmap_value")?.as_f64()?;
            Some(HeatPt { x, y, v })
        })
        .collect();

    let (xmin, xmax, ymin, ymax) = if heat_samples.is_empty() {
        (0.0, 1.0, 0.0, 1.0)
    } else {
        let xs: Vec<f64> = heat_samples.iter().map(|s| s.x).collect();
        let ys: Vec<f64> = heat_samples.iter().map(|s| s.y).collect();
        (
            xs.iter().copied().fold(f64::INFINITY, f64::min),
            xs.iter().copied().fold(f64::NEG_INFINITY, f64::max),
            ys.iter().copied().fold(f64::INFINITY, f64::min),
            ys.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        )
    };

    let grid_n = (smoothness as usize / 4).clamp(48, 128);
    let nx = grid_n;
    let ny = grid_n;
    let gx = (xmax - xmin).max(1e-9);
    let gy = (ymax - ymin).max(1e-9);
    let mut grid_values: Vec<Option<f64>> = vec![None; nx * ny];
    for iy in 0..ny {
        for ix in 0..nx {
            let x = xmin + (ix as f64 + 0.5) / (nx as f64) * gx;
            let y = ymin + (iy as f64 + 0.5) / (ny as f64) * gy;
            grid_values[iy * nx + ix] = interpolate_value(
                &heat_samples,
                x,
                y,
                &method,
                idw_power,
                search_radius_m,
                min_points,
                max_points,
            );
        }
    }

    let mut residual_features: Vec<serde_json::Value> = Vec::new();
    let mut residuals: Vec<f64> = Vec::new();
    for (i, s) in heat_samples.iter().enumerate() {
        let mut others = Vec::with_capacity(heat_samples.len().saturating_sub(1));
        for (j, o) in heat_samples.iter().enumerate() {
            if i != j {
                others.push(*o);
            }
        }
        let pred = interpolate_value(
            &others,
            s.x,
            s.y,
            &method,
            idw_power,
            search_radius_m,
            min_points,
            max_points,
        );
        if let Some(pv) = pred {
            let r = s.v - pv;
            residuals.push(r);
            residual_features.push(serde_json::json!({
                "type":"Feature",
                "geometry":{"type":"Point","coordinates":[s.x,s.y]},
                "properties":{"observed":s.v,"predicted":pv,"residual":r}
            }));
        }
    }
    let mae = if residuals.is_empty() {
        0.0
    } else {
        residuals.iter().map(|r| r.abs()).sum::<f64>() / residuals.len() as f64
    };
    let rmse = if residuals.is_empty() {
        0.0
    } else {
        (residuals.iter().map(|r| r * r).sum::<f64>() / residuals.len() as f64).sqrt()
    };
    let bias = if residuals.is_empty() {
        0.0
    } else {
        residuals.iter().sum::<f64>() / residuals.len() as f64
    };

    let mut contour_features: Vec<serde_json::Value> = Vec::new();
    if contours_enabled {
        for br in &contour_breaks {
            let segs = marching_segments(&grid_values, nx, ny, xmin, xmax, ymin, ymax, *br);
            for seg in segs {
                contour_features.push(serde_json::json!({
                    "type":"Feature",
                    "geometry":{"type":"LineString","coordinates":[[seg[0][0],seg[0][1]],[seg[1][0],seg[1][1]]]},
                    "properties":{"level":br}
                }));
            }
        }
    }

    let mut gradient_values: Vec<Option<f64>> = Vec::new();
    if gradient_enabled {
        gradient_values = vec![None; nx * ny];
        for iy in 1..(ny.saturating_sub(1)) {
            for ix in 1..(nx.saturating_sub(1)) {
                let c = grid_values[iy * nx + ix];
                let l = grid_values[iy * nx + (ix - 1)];
                let r = grid_values[iy * nx + (ix + 1)];
                let b = grid_values[(iy - 1) * nx + ix];
                let t = grid_values[(iy + 1) * nx + ix];
                let (Some(_cv), Some(lv), Some(rv), Some(bv), Some(tv)) = (c, l, r, b, t) else {
                    continue;
                };
                let gx = (rv - lv) * 0.5;
                let gy = (tv - bv) * 0.5;
                let g = if gradient_mode == "directional" {
                    gy.atan2(gx)
                } else {
                    (gx * gx + gy * gy).sqrt()
                };
                gradient_values[iy * nx + ix] = Some(g);
            }
        }
    }

    let out = serde_json::json!({
        "type": "assay_heatmap_surface",
        "measure": selected_measure,
        "measure_candidates": measure_candidates,
        "points": enriched_points,
        "stats": {
            "raw_count": raw_values.len(),
            "value_min": vmin,
            "value_max": vmax
        },
        "heatmap_config": {
            "measure": selected_measure,
            "render_measure": "__heatmap_value",
            "method": method,
            "scale": scale,
            "palette": palette,
            "clamp_low_pct": clamp_low,
            "clamp_high_pct": clamp_high,
            "idw_power": idw_power,
            "smoothness": smoothness,
            "search_radius_m": search_radius_m,
            "min_points": min_points,
            "max_points": max_points,
            "contours_enabled": contours_enabled,
            "contour_mode": contour_mode,
            "contour_interval": contour_interval,
            "contour_levels": contour_levels,
            "contour_levels_list": contour_levels_list,
            "contour_breaks": contour_breaks,
            "gradient_enabled": gradient_enabled,
            "gradient_mode": gradient_mode,
            "opacity": 0.52,
            "min_visible_value": min_visible_raw,
            "max_visible_value": max_visible_raw,
            "min_visible_render": min_visible_render,
            "max_visible_render": max_visible_render
        },
        "display_contract": {
            "renderer": "heat_surface",
            "editable": ["visible", "opacity", "palette"],
            "defaults": {
                "measure": "__heatmap_value",
                "opacity": 0.52,
                "palette": palette
            }
        },
        "surface_grid": {
            "nx": nx,
            "ny": ny,
            "xmin": xmin,
            "xmax": xmax,
            "ymin": ymin,
            "ymax": ymax,
            "values": grid_values
        }
    });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!("graphs/{}/nodes/{}/heatmap.json", job.graph_id, job.node_id);
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;

    let mut outputs = vec![artifact.clone()];
    let mut hashes = vec![artifact.content_hash.clone()];

    let residual_json = serde_json::json!({
        "type":"FeatureCollection",
        "features": residual_features,
        "metrics": {"n": residuals.len(), "mae": mae, "rmse": rmse, "bias": bias}
    });
    let residual_bytes = serde_json::to_vec(&residual_json)?;
    let residual_key = format!(
        "graphs/{}/nodes/{}/residuals.json",
        job.graph_id, job.node_id
    );
    let residual_ref = write_artifact(
        ctx,
        &residual_key,
        &residual_bytes,
        Some("application/json"),
    )
    .await?;
    outputs.push(residual_ref.clone());
    hashes.push(residual_ref.content_hash.clone());

    if contours_enabled {
        let contours_json = serde_json::json!({
            "type":"FeatureCollection",
            "features": contour_features
        });
        let contour_bytes = serde_json::to_vec(&contours_json)?;
        let contour_key = format!(
            "graphs/{}/nodes/{}/contours.geojson",
            job.graph_id, job.node_id
        );
        let contour_ref = write_artifact(
            ctx,
            &contour_key,
            &contour_bytes,
            Some("application/geo+json"),
        )
        .await?;
        outputs.push(contour_ref.clone());
        hashes.push(contour_ref.content_hash.clone());
    }

    if gradient_enabled {
        let gradient_json = serde_json::json!({
            "type":"gradient_grid",
            "mode": gradient_mode,
            "nx": nx,
            "ny": ny,
            "xmin": xmin,
            "xmax": xmax,
            "ymin": ymin,
            "ymax": ymax,
            "values": gradient_values
        });
        let gradient_bytes = serde_json::to_vec(&gradient_json)?;
        let gradient_key = format!(
            "graphs/{}/nodes/{}/gradient.json",
            job.graph_id, job.node_id
        );
        let gradient_ref = write_artifact(
            ctx,
            &gradient_key,
            &gradient_bytes,
            Some("application/json"),
        )
        .await?;
        outputs.push(gradient_ref.clone());
        hashes.push(gradient_ref.content_hash.clone());
    }

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: outputs,
        content_hashes: hashes,
        error_message: None,
    })
}

/// Derive iso-contours from any upstream artifact containing `surface_grid`.
pub(crate) async fn run_surface_iso_extract_impl(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let mut grid_src: Option<serde_json::Value> = None;
    for ar in &job.input_artifact_refs {
        let v = read_json_artifact(ctx, &ar.key).await?;
        if v.get("surface_grid").is_some() {
            grid_src = Some(v);
            break;
        }
    }
    let Some(root) = grid_src else {
        return Err(NodeError::InvalidConfig(
            "surface_iso_extract requires upstream artifact with surface_grid".into(),
        ));
    };
    let Some(grid) = root.get("surface_grid").and_then(|g| g.as_object()) else {
        return Err(NodeError::InvalidConfig("surface_grid missing".into()));
    };
    let nx = grid.get("nx").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
    let ny = grid.get("ny").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
    let xmin = grid.get("xmin").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let xmax = grid.get("xmax").and_then(|v| v.as_f64()).unwrap_or(1.0);
    let ymin = grid.get("ymin").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let ymax = grid.get("ymax").and_then(|v| v.as_f64()).unwrap_or(1.0);
    let values_raw = grid
        .get("values")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if nx < 2 || ny < 2 || values_raw.len() != nx * ny {
        return Err(NodeError::InvalidConfig(
            "surface_grid dimensions invalid for iso extraction".into(),
        ));
    }
    let values: Vec<Option<f64>> = values_raw.iter().map(|v| v.as_f64()).collect();
    let finite_vals: Vec<f64> = values.iter().copied().flatten().collect();
    if finite_vals.is_empty() {
        return Err(NodeError::InvalidConfig(
            "surface_grid has no finite values".into(),
        ));
    }
    let vmin = finite_vals.iter().copied().fold(f64::INFINITY, f64::min);
    let vmax = finite_vals
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);

    let mode = job
        .output_spec
        .pointer("/node_ui/mode")
        .and_then(|v| v.as_str())
        .unwrap_or("fixed_interval");
    let interval = job
        .output_spec
        .pointer("/node_ui/interval")
        .and_then(|v| v.as_f64())
        .unwrap_or(((vmax - vmin) / 10.0).max(1e-6))
        .max(1e-6);
    let levels = job
        .output_spec
        .pointer("/node_ui/levels")
        .and_then(|v| v.as_u64())
        .unwrap_or(10)
        .max(2) as usize;
    let z_base = job
        .output_spec
        .pointer("/node_ui/z_base")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let z_scale = job
        .output_spec
        .pointer("/node_ui/z_scale")
        .and_then(|v| v.as_f64())
        .unwrap_or(1.0);

    let mut breaks: Vec<f64> = Vec::new();
    if mode == "quantile" {
        let mut sorted = finite_vals.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        for i in 1..levels {
            let t = (i as f64) / (levels as f64);
            let idx = ((sorted.len().saturating_sub(1) as f64) * t).round() as usize;
            breaks.push(sorted[idx.min(sorted.len().saturating_sub(1))]);
        }
    } else {
        let mut x = vmin + interval;
        while x < vmax {
            breaks.push(x);
            x += interval;
        }
    }

    let mut features = Vec::new();
    for br in &breaks {
        let segs = marching_segments(&values, nx, ny, xmin, xmax, ymin, ymax, *br);
        let z = z_base + z_scale * *br;
        for seg in segs {
            features.push(serde_json::json!({
                "type":"Feature",
                "geometry":{"type":"LineString","coordinates":[[seg[0][0],seg[0][1],z],[seg[1][0],seg[1][1],z]]},
                "properties":{"level":br,"z":z}
            }));
        }
    }

    let contour_geo = serde_json::json!({
        "type":"FeatureCollection",
        "features": features
    });
    let contour_bytes = serde_json::to_vec(&contour_geo)?;
    let contour_key = format!(
        "graphs/{}/nodes/{}/iso_contours.geojson",
        job.graph_id, job.node_id
    );
    let contour_ref = write_artifact(
        ctx,
        &contour_key,
        &contour_bytes,
        Some("application/geo+json"),
    )
    .await?;

    let meta = serde_json::json!({
        "type":"iso_extract_meta",
        "mode":mode,
        "interval":interval,
        "levels":levels,
        "breaks":breaks,
        "z_base":z_base,
        "z_scale":z_scale,
        "source_surface_stats":{"min":vmin,"max":vmax}
    });
    let meta_bytes = serde_json::to_vec(&meta)?;
    let meta_key = format!(
        "graphs/{}/nodes/{}/iso_meta.json",
        job.graph_id, job.node_id
    );
    let meta_ref = write_artifact(ctx, &meta_key, &meta_bytes, Some("application/json")).await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![contour_ref.clone(), meta_ref.clone()],
        content_hashes: vec![contour_ref.content_hash, meta_ref.content_hash],
        error_message: None,
    })
}

/// Fit / nudge DEM-like `surface_grid` against control points with known XYZ.
pub(crate) async fn run_terrain_adjust_impl(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let fit_mode = job
        .output_spec
        .pointer("/node_ui/fit_mode")
        .and_then(|v| v.as_str())
        .unwrap_or("vertical_bias");
    let manual_dx = job
        .output_spec
        .pointer("/node_ui/manual_shift_x")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let manual_dy = job
        .output_spec
        .pointer("/node_ui/manual_shift_y")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let mut grid_root: Option<serde_json::Value> = None;
    let mut control_points: Vec<XYZ> = Vec::new();
    for ar in &job.input_artifact_refs {
        let v = read_json_artifact(ctx, &ar.key).await?;
        let has_grid = v.get("surface_grid").is_some();
        if grid_root.is_none() && has_grid {
            grid_root = Some(v.clone());
        }
        if !has_grid {
            control_points.extend(collect_xyz_points(&v));
        }
    }
    let Some(root) = grid_root else {
        return Err(NodeError::InvalidConfig(
            "terrain_adjust requires upstream artifact with surface_grid".into(),
        ));
    };
    let Some(grid) = root.get("surface_grid").and_then(|g| g.as_object()) else {
        return Err(NodeError::InvalidConfig("surface_grid missing".into()));
    };
    let nx = grid.get("nx").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
    let ny = grid.get("ny").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
    let xmin = grid.get("xmin").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let xmax = grid.get("xmax").and_then(|v| v.as_f64()).unwrap_or(1.0);
    let ymin = grid.get("ymin").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let ymax = grid.get("ymax").and_then(|v| v.as_f64()).unwrap_or(1.0);
    let values_raw = grid
        .get("values")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if nx < 2 || ny < 2 || values_raw.len() != nx * ny {
        return Err(NodeError::InvalidConfig(
            "surface_grid dimensions invalid for terrain_adjust".into(),
        ));
    }
    let values: Vec<Option<f64>> = values_raw.iter().map(|v| v.as_f64()).collect();

    let mut matched: Vec<(XYZ, f64)> = Vec::new();
    for cp in &control_points {
        if let Some(pred) = bilinear_from_grid(nx, ny, xmin, xmax, ymin, ymax, &values, cp.x, cp.y)
        {
            matched.push((*cp, pred));
        }
    }
    if matched.len() < 3 {
        return Err(NodeError::InvalidConfig(
            "terrain_adjust needs at least 3 control points overlapping the DEM extent".into(),
        ));
    }

    let mut sum_r = 0.0;
    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_xx = 0.0;
    let mut sum_yy = 0.0;
    let mut sum_xy = 0.0;
    let mut sum_xr = 0.0;
    let mut sum_yr = 0.0;
    let n = matched.len() as f64;
    let cx = matched.iter().map(|m| m.0.x).sum::<f64>() / n;
    let cy = matched.iter().map(|m| m.0.y).sum::<f64>() / n;
    let mut rmse_before_acc = 0.0;
    for (cp, pred) in &matched {
        let rx = cp.x - cx;
        let ry = cp.y - cy;
        let r = cp.z - *pred;
        rmse_before_acc += r * r;
        sum_r += r;
        sum_x += rx;
        sum_y += ry;
        sum_xx += rx * rx;
        sum_yy += ry * ry;
        sum_xy += rx * ry;
        sum_xr += rx * r;
        sum_yr += ry * r;
    }
    let rmse_before = (rmse_before_acc / n).sqrt();

    let mut dz = sum_r / n;
    let mut ax = 0.0;
    let mut ay = 0.0;
    if fit_mode == "affine_xy_z" {
        let det = sum_xx * sum_yy - sum_xy * sum_xy;
        if det.abs() > 1e-9 {
            ax = (sum_xr * sum_yy - sum_yr * sum_xy) / det;
            ay = (sum_yr * sum_xx - sum_xr * sum_xy) / det;
        }
        dz = (sum_r - ax * sum_x - ay * sum_y) / n;
    }

    let dx = manual_dx;
    let dy = manual_dy;
    let mut adjusted_values: Vec<Option<f64>> = Vec::with_capacity(values.len());
    for iy in 0..ny {
        for ix in 0..nx {
            let idx = iy * nx + ix;
            let Some(v0) = values[idx] else {
                adjusted_values.push(None);
                continue;
            };
            let x = xmin + (ix as f64 + 0.5) / (nx as f64) * (xmax - xmin).max(1e-9);
            let y = ymin + (iy as f64 + 0.5) / (ny as f64) * (ymax - ymin).max(1e-9);
            let corr = dz + ax * (x - cx) + ay * (y - cy);
            adjusted_values.push(Some(v0 + corr));
        }
    }

    let mut rmse_after_acc = 0.0;
    for (cp, pred) in &matched {
        let corr = dz + ax * (cp.x - cx) + ay * (cp.y - cy);
        let after = cp.z - (*pred + corr);
        rmse_after_acc += after * after;
    }
    let rmse_after = (rmse_after_acc / n).sqrt();

    let out = serde_json::json!({
        "type":"terrain_adjusted",
        "fit_mode":fit_mode,
        "display_contract": {
            "renderer":"terrain",
            "editable":["visible","opacity"]
        },
        "adjustment":{
            "dx":dx,
            "dy":dy,
            "dz":dz,
            "tilt_x":ax,
            "tilt_y":ay,
            "origin":[cx,cy]
        },
        "qc":{
            "control_points_used": matched.len(),
            "rmse_before": rmse_before,
            "rmse_after": rmse_after
        },
        "surface_grid":{
            "nx":nx,
            "ny":ny,
            "xmin":xmin + dx,
            "xmax":xmax + dx,
            "ymin":ymin + dy,
            "ymax":ymax + dy,
            "values":adjusted_values
        },
        "control_points": matched.iter().map(|(cp,pred)|{
            let corr = dz + ax * (cp.x - cx) + ay * (cp.y - cy);
            serde_json::json!({
                "x":cp.x + dx,
                "y":cp.y + dy,
                "z_obs":cp.z,
                "z_dem_before":pred,
                "z_dem_after": pred + corr,
                "residual_before": cp.z - pred,
                "residual_after": cp.z - (pred + corr)
            })
        }).collect::<Vec<_>>()
    });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!(
        "graphs/{}/nodes/{}/terrain_adjusted.json",
        job.graph_id, job.node_id
    );
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

/// Build a surface_grid from any upstream XYZ-style datasets (points/collars/segments/assay_points).
pub(crate) async fn run_xyz_to_surface_impl(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let resolution_hint = job
        .output_spec
        .pointer("/node_ui/resolution")
        .and_then(|v| v.as_f64())
        .unwrap_or(25.0)
        .max(0.01);
    let max_cells = job
        .output_spec
        .pointer("/node_ui/max_cells")
        .and_then(|v| v.as_u64())
        .unwrap_or(65536) as usize;
    let pad_pct = job
        .output_spec
        .pointer("/node_ui/pad_pct")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.25);
    let nx_cfg = job
        .output_spec
        .pointer("/node_ui/nx")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);
    let ny_cfg = job
        .output_spec
        .pointer("/node_ui/ny")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);

    let mut xyz_points: Vec<XYZ> = Vec::new();
    let mut xy_points: Vec<(f64, f64)> = Vec::new();
    for ar in &job.input_artifact_refs {
        let v = read_json_artifact(ctx, &ar.key).await?;
        xyz_points.extend(collect_xyz_points(&v));
        xy_points.extend(collect_xy_points(&v));
    }
    if xyz_points.len() < 3 {
        return Err(NodeError::InvalidConfig(
            "xyz_to_surface requires at least 3 XYZ points from upstream artifacts".into(),
        ));
    }

    let (xmin, xmax, ymin, ymax) = infer_extent(&xyz_points, pad_pct)
        .ok_or_else(|| NodeError::InvalidConfig("unable to infer extent from XYZ points".into()))?;
    let (mut nx, mut ny) =
        grid_dims_from_extent(xmin, xmax, ymin, ymax, resolution_hint, max_cells);
    if let Some(v) = nx_cfg {
        nx = v.clamp(2, 1024);
    }
    if let Some(v) = ny_cfg {
        ny = v.clamp(2, 1024);
    }
    let values = idw_surface_from_xyz(&xyz_points, nx, ny, xmin, xmax, ymin, ymax);
    let finite_vals: Vec<f64> = values.iter().copied().flatten().collect();
    let (zmin, zmax) = if finite_vals.is_empty() {
        (0.0, 0.0)
    } else {
        (
            finite_vals.iter().copied().fold(f64::INFINITY, f64::min),
            finite_vals
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max),
        )
    };

    let out = serde_json::json!({
        "type":"xyz_surface",
        "source":"idw_interpolation",
        "display_contract": {
            "renderer":"terrain",
            "display_pointer":"scene3d.terrain",
            "editable":["visible","opacity"]
        },
        "stats":{
            "input_points": xyz_points.len(),
            "z_min": zmin,
            "z_max": zmax
        },
        "surface_grid":{
            "nx":nx,
            "ny":ny,
            "xmin":xmin,
            "xmax":xmax,
            "ymin":ymin,
            "ymax":ymax,
            "values":values
        },
        "crs": job.project_crs.clone().unwrap_or_else(|| CrsRecord::epsg(4326))
    });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!(
        "graphs/{}/nodes/{}/xyz_surface.json",
        job.graph_id, job.node_id
    );
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

fn bilinear_sample_grid(
    ncols: usize,
    nrows: usize,
    xll: f64,
    yll: f64,
    cell: f64,
    vals: &[Option<f64>],
    lon: f64,
    lat: f64,
) -> Option<f64> {
    if ncols < 2 || nrows < 2 || cell <= 0.0 {
        return None;
    }
    let gx = (lon - xll) / cell;
    let gy = (lat - yll) / cell;
    if !gx.is_finite() || !gy.is_finite() {
        return None;
    }
    if gx < 0.0 || gy < 0.0 || gx > (ncols - 1) as f64 || gy > (nrows - 1) as f64 {
        return None;
    }
    let ix0 = gx.floor().clamp(0.0, (ncols - 1) as f64) as usize;
    let iy0s = gy.floor().clamp(0.0, (nrows - 1) as f64) as usize;
    let ix1 = (ix0 + 1).min(ncols - 1);
    let iy1s = (iy0s + 1).min(nrows - 1);
    // AAIGrid rows are north->south; gy is from south->north.
    let iy0 = (nrows - 1).saturating_sub(iy0s);
    let iy1 = (nrows - 1).saturating_sub(iy1s);
    let fx = (gx - ix0 as f64).clamp(0.0, 1.0);
    let fy = (gy - iy0s as f64).clamp(0.0, 1.0);
    let idx = |x: usize, y: usize| -> usize { y * ncols + x };
    let v00 = vals.get(idx(ix0, iy0)).copied().flatten()?;
    let v10 = vals.get(idx(ix1, iy0)).copied().flatten()?;
    let v01 = vals.get(idx(ix0, iy1)).copied().flatten()?;
    let v11 = vals.get(idx(ix1, iy1)).copied().flatten()?;
    let a = v00 * (1.0 - fx) + v10 * fx;
    let b = v01 * (1.0 - fx) + v11 * fx;
    Some(a * (1.0 - fy) + b * fy)
}

fn parse_aai_grid(text: &str) -> Option<(usize, usize, f64, f64, f64, f64, Vec<Option<f64>>)> {
    let mut ncols: Option<usize> = None;
    let mut nrows: Option<usize> = None;
    let mut xll: Option<f64> = None;
    let mut yll: Option<f64> = None;
    let mut cell: Option<f64> = None;
    let mut nodata: f64 = -32768.0;
    let mut values_start = 0usize;
    let lines: Vec<&str> = text.lines().collect();
    for (i, ln) in lines.iter().enumerate() {
        let parts: Vec<&str> = ln.split_whitespace().collect();
        if parts.len() < 2 {
            continue;
        }
        let k = parts[0].to_ascii_lowercase();
        match k.as_str() {
            "ncols" => ncols = parts[1].parse::<usize>().ok(),
            "nrows" => nrows = parts[1].parse::<usize>().ok(),
            "xllcorner" | "xllcenter" => xll = parts[1].parse::<f64>().ok(),
            "yllcorner" | "yllcenter" => yll = parts[1].parse::<f64>().ok(),
            "cellsize" => cell = parts[1].parse::<f64>().ok(),
            "nodata_value" => nodata = parts[1].parse::<f64>().unwrap_or(nodata),
            _ => {
                values_start = i;
                break;
            }
        }
        values_start = i + 1;
    }
    let (Some(ncols), Some(nrows), Some(xll), Some(yll), Some(cell)) =
        (ncols, nrows, xll, yll, cell)
    else {
        return None;
    };
    let mut vals: Vec<Option<f64>> = Vec::with_capacity(ncols * nrows);
    for ln in lines.iter().skip(values_start) {
        for t in ln.split_whitespace() {
            let v = t.parse::<f64>().ok();
            let vv = v.filter(|x| (x - nodata).abs() > 1e-9);
            vals.push(vv);
        }
    }
    if vals.len() < ncols * nrows {
        vals.resize(ncols * nrows, None);
    } else if vals.len() > ncols * nrows {
        vals.truncate(ncols * nrows);
    }
    Some((ncols, nrows, xll, yll, cell, nodata, vals))
}

// ── Cached elevation fetch wrappers ──────────────────────────────────────────
//
// These wrappers sit in front of the raw API functions and transparently read
// from / write to the disk tile cache.  A cache miss falls through to the
// real HTTP request; the response is stored so subsequent calls with the same
// logical inputs skip the network entirely.

/// Cache-aware wrapper around `fetch_opentopography_elevations`.
///
/// Cache key: `"opentopo:{api_key_prefix8}:{s:.4}:{w:.4}:{n:.4}:{e:.4}"` where
/// s/w/n/e are the **padded** bbox values (what would be sent to the API),
/// rounded to 4 decimal places (≈ 11 m at the equator) to absorb tiny
/// floating-point variations without producing spurious misses.
///
/// Cache value: raw AAI Grid text, re-sampled on every cache hit so the caller
/// receives correctly-sized `Vec<Option<f64>>` for any point set within the bbox.
async fn fetch_opentopography_cached(
    lat_lon: &[(f64, f64)],
    api_key: &str,
    timeout_ms: u64,
    cache: &TileCache,
) -> Vec<Option<f64>> {
    if lat_lon.is_empty() || api_key.trim().is_empty() {
        return Vec::new();
    }

    // Compute the padded bbox (mirrors the logic inside the raw fetch fn).
    let (mut south, mut north, mut west, mut east) = {
        let mut s = f64::INFINITY;
        let mut n = f64::NEG_INFINITY;
        let mut w = f64::INFINITY;
        let mut e = f64::NEG_INFINITY;
        for &(lat, lon) in lat_lon {
            s = s.min(lat);
            n = n.max(lat);
            w = w.min(lon);
            e = e.max(lon);
        }
        (s, n, w, e)
    };
    if !south.is_finite() || !north.is_finite() || !west.is_finite() || !east.is_finite() {
        return vec![None; lat_lon.len()];
    }
    let pad_lat = ((north - south).abs() * 0.01).max(1e-5);
    let pad_lon = ((east - west).abs() * 0.01).max(1e-5);
    south = (south - pad_lat).max(-90.0);
    north = (north + pad_lat).min(90.0);
    west = (west - pad_lon).max(-180.0);
    east = (east + pad_lon).min(180.0);

    // Use only the first 8 hex chars of the API key hash (never log the key).
    let key_tag = {
        use sha2::{Digest, Sha256};
        let h = hex::encode(Sha256::digest(api_key.as_bytes()));
        h[..8].to_string()
    };
    let cache_key = format!(
        "opentopo:{key_tag}:{:.4}:{:.4}:{:.4}:{:.4}",
        south, west, north, east
    );

    // ── Cache hit: re-sample the stored AAI grid for the requested points ──
    if let Some(raw) = cache.get("dem", &cache_key).await {
        if let Ok(text) = std::str::from_utf8(&raw) {
            if let Some((ncols, nrows, xll, yll, cell, _nodata, vals)) = parse_aai_grid(text) {
                let sampled: Vec<Option<f64>> = lat_lon
                    .iter()
                    .map(|(lat, lon)| {
                        bilinear_sample_grid(ncols, nrows, xll, yll, cell, &vals, *lon, *lat)
                    })
                    .collect();
                // Only trust the hit if we got a reasonable number of values
                if sampled.iter().filter(|v| v.is_some()).count() > 0 {
                    return sampled;
                }
            }
        }
    }

    // ── Cache miss: perform the real HTTP fetch ────────────────────────────
    //
    // We re-implement the fetch here (duplicating a bit of code from the
    // private `fetch_opentopography_elevations`) so we can intercept the raw
    // AAI text before it is parsed — that text is what we cache.
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(timeout_ms.max(1000)))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());

    let text = match client
        .get("https://portal.opentopography.org/API/globaldem")
        .query(&[
            ("demtype", "COP30"),
            ("south", &format!("{south:.8}")),
            ("north", &format!("{north:.8}")),
            ("west", &format!("{west:.8}")),
            ("east", &format!("{east:.8}")),
            ("outputFormat", "AAIGrid"),
            ("API_Key", api_key),
        ])
        .send()
        .await
    {
        Ok(resp) => match resp.text().await {
            Ok(t) => t,
            Err(_) => return vec![None; lat_lon.len()],
        },
        Err(_) => return vec![None; lat_lon.len()],
    };

    // Cache the raw AAI text (only if it actually parsed successfully).
    let result = if let Some((ncols, nrows, xll, yll, cell, _nodata, vals)) = parse_aai_grid(&text)
    {
        let sampled: Vec<Option<f64>> = lat_lon
            .iter()
            .map(|(lat, lon)| bilinear_sample_grid(ncols, nrows, xll, yll, cell, &vals, *lon, *lat))
            .collect();
        // Store the raw text so future runs with the same bbox skip the fetch.
        cache
            .put("dem", &cache_key, text.as_bytes(), DEM_TTL_S)
            .await;
        sampled
    } else {
        vec![None; lat_lon.len()]
    };

    result
}

/// Cache-aware wrapper around `fetch_open_meteo_elevations`.
///
/// Each 100-point batch is cached independently.  Cache key:
/// `"open-meteo:{hash_of_rounded_lat_lons}"` where lat/lon values are rounded
/// to 4 decimal places before hashing.
async fn fetch_open_meteo_cached(
    lat_lon: &[(f64, f64)],
    timeout_ms: u64,
    cache: &TileCache,
) -> Vec<Option<f64>> {
    if lat_lon.is_empty() {
        return Vec::new();
    }

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(timeout_ms.max(1000)))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());

    let mut out: Vec<Option<f64>> = Vec::with_capacity(lat_lon.len());
    let chunk = 100usize;

    for part in lat_lon.chunks(chunk) {
        // Build a stable, rounded string for the cache key.
        let rounded_key: String = part
            .iter()
            .map(|(lat, lon)| format!("{:.4},{:.4}", lat, lon))
            .collect::<Vec<_>>()
            .join("|");
        let cache_key = format!("open-meteo:{rounded_key}");

        // ── Cache hit ──────────────────────────────────────────────────────
        if let Some(raw) = cache.get("dem", &cache_key).await {
            if let Ok(v) = serde_json::from_slice::<Vec<Option<f64>>>(&raw) {
                if v.len() == part.len() {
                    out.extend(v);
                    continue;
                }
            }
        }

        // ── Cache miss: real HTTP request ──────────────────────────────────
        let lat_csv = part
            .iter()
            .map(|p| format!("{:.7}", p.0))
            .collect::<Vec<_>>()
            .join(",");
        let lon_csv = part
            .iter()
            .map(|p| format!("{:.7}", p.1))
            .collect::<Vec<_>>()
            .join(",");

        let resp = match client
            .get("https://api.open-meteo.com/v1/elevation")
            .query(&[("latitude", lat_csv), ("longitude", lon_csv)])
            .send()
            .await
        {
            Ok(r) => r,
            Err(_) => {
                out.extend((0..part.len()).map(|_| None));
                continue;
            }
        };
        let json = match resp.json::<serde_json::Value>().await {
            Ok(v) => v,
            Err(_) => {
                out.extend((0..part.len()).map(|_| None));
                continue;
            }
        };
        let batch: Vec<Option<f64>> =
            if let Some(arr) = json.get("elevation").and_then(|v| v.as_array()) {
                (0..part.len())
                    .map(|i| arr.get(i).and_then(|v| v.as_f64()))
                    .collect()
            } else {
                // API returned an error response (e.g. rate-limit JSON).  Do NOT
                // cache the empty result — a stale all-None entry would poison
                // every subsequent run for up to OPEN_METEO_TTL_S seconds.
                out.extend((0..part.len()).map(|_| None));
                continue;
            };

        // Only cache batches that contain at least one real elevation value.
        let has_data = batch.iter().any(|v| v.is_some());
        if has_data {
            if let Ok(bytes) = serde_json::to_vec(&batch) {
                cache.put("dem", &cache_key, &bytes, OPEN_METEO_TTL_S).await;
            }
        }
        out.extend(batch);
    }

    out
}

/// Fetch DEM elevations for inferred AOI using a public API; fallback to XYZ-IDW when network unavailable.
pub(crate) async fn run_dem_fetch_impl(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let fit_mode = job
        .output_spec
        .pointer("/node_ui/fit_mode")
        .and_then(|v| v.as_str())
        .unwrap_or("vertical_bias");
    let fit_min_points = job
        .output_spec
        .pointer("/node_ui/fit_min_points")
        .and_then(|v| v.as_u64())
        .unwrap_or(3) as usize;
    let low_density_cells = job
        .output_spec
        .pointer("/node_ui/low_density_cells")
        .and_then(|v| v.as_f64())
        .unwrap_or(8.0)
        .max(1.0);
    let anchor_cells = job
        .output_spec
        .pointer("/node_ui/anchor_cells")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.75)
        .max(0.05);
    let timeout_ms = job
        .output_spec
        .pointer("/node_ui/timeout_ms")
        .and_then(|v| v.as_u64())
        .unwrap_or(60_000); // large DEMs (e.g. 55 km × 55 km from OpenTopography) can take 30+ s
    let resolution_hint = job
        .output_spec
        .pointer("/node_ui/resolution")
        .and_then(|v| v.as_f64())
        .unwrap_or(50.0)
        .max(0.01);
    let max_cells = job
        .output_spec
        .pointer("/node_ui/max_cells")
        .and_then(|v| v.as_u64())
        .unwrap_or(32768) as usize;
    let pad_pct = job
        .output_spec
        .pointer("/node_ui/pad_pct")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.25);
    let nx_cfg = job
        .output_spec
        .pointer("/node_ui/nx")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);
    let ny_cfg = job
        .output_spec
        .pointer("/node_ui/ny")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);
    let source_epsg_hint = job
        .output_spec
        .pointer("/node_ui/source_epsg")
        .and_then(|v| v.as_i64())
        .map(|v| v as i32);
    let bbox_cfg = job
        .output_spec
        .pointer("/node_ui/bbox")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let mut xyz_points: Vec<XYZ> = Vec::new();
    let mut xy_points: Vec<(f64, f64)> = Vec::new();
    let mut input_bbox: Option<(f64, f64, f64, f64)> = None;
    let mut aoi_bbox: Option<(f64, f64, f64, f64)> = None;
    for ar in &job.input_artifact_refs {
        let v = read_json_artifact(ctx, &ar.key).await?;
        xyz_points.extend(collect_xyz_points(&v));
        xy_points.extend(collect_xy_points(&v));
        let schema_id = v.get("schema_id").and_then(|x| x.as_str()).unwrap_or("");
        if let Some(b) = v.get("bounds").and_then(|b| b.as_object()) {
            let xmin = b.get("xmin").and_then(|x| x.as_f64());
            let xmax = b.get("xmax").and_then(|x| x.as_f64());
            let ymin = b.get("ymin").and_then(|y| y.as_f64());
            let ymax = b.get("ymax").and_then(|y| y.as_f64());
            if let (Some(xmin), Some(xmax), Some(ymin), Some(ymax)) = (xmin, xmax, ymin, ymax) {
                if schema_id == "spatial.aoi.v1" {
                    aoi_bbox = Some((xmin, xmax, ymin, ymax));
                } else if input_bbox.is_none() {
                    input_bbox = Some((xmin, xmax, ymin, ymax));
                }
            }
        }
        if aoi_bbox.is_none() {
            if let Some(geom) = v.get("geometry") {
                let gb = bbox_from_geojson_like(geom);
                if schema_id == "spatial.aoi.v1" {
                    if gb.is_some() {
                        aoi_bbox = gb;
                    }
                } else if input_bbox.is_none() {
                    input_bbox = gb;
                }
            }
        }
    }
    let input_bbox = aoi_bbox.or(input_bbox);
    let input_artifact_keys: Vec<String> = job
        .input_artifact_refs
        .iter()
        .map(|a| a.key.clone())
        .collect();

    let (extent, extent_source): (Option<(f64, f64, f64, f64)>, &str) = if input_bbox.is_some() {
        (input_bbox, "input_bounds_or_geometry")
    } else if bbox_cfg.len() >= 4 {
        let x0 = bbox_cfg[0].as_f64().unwrap_or(0.0);
        let y0 = bbox_cfg[1].as_f64().unwrap_or(0.0);
        let x1 = bbox_cfg[2].as_f64().unwrap_or(1.0);
        let y1 = bbox_cfg[3].as_f64().unwrap_or(1.0);
        (
            Some((x0.min(x1), x0.max(x1), y0.min(y1), y0.max(y1))),
            "node_ui_bbox",
        )
    } else {
        let from_xyz = infer_extent(&xyz_points, pad_pct);
        let from_xy = infer_extent_xy(&xy_points, pad_pct);
        let merged = merge_extents(from_xyz, from_xy);
        let src = match (from_xyz.is_some(), from_xy.is_some()) {
            (true, true) => "xyz_xy_input",
            (true, false) => "xyz_input",
            (false, true) => "xy_input",
            (false, false) => "none",
        };
        (merged, src)
    };
    let (xmin, xmax, ymin, ymax) = extent.ok_or_else(|| {
        NodeError::InvalidConfig(
            "dem_fetch needs either upstream XYZ inputs or node_ui.bbox=[xmin,ymin,xmax,ymax]"
                .into(),
        )
    })?;

    let (mut nx, mut ny) =
        grid_dims_from_extent(xmin, xmax, ymin, ymax, resolution_hint, max_cells);
    if let Some(v) = nx_cfg {
        nx = v.clamp(2, 1024);
    }
    if let Some(v) = ny_cfg {
        ny = v.clamp(2, 1024);
    }

    let source_crs = source_epsg_hint
        .or_else(|| job.project_crs.as_ref().and_then(|c| c.epsg))
        .unwrap_or(4326);

    let src = CrsRecord::epsg(source_crs);
    let wgs84 = CrsRecord::epsg(4326);
    let mut lat_lon: Vec<(f64, f64)> = Vec::with_capacity(nx * ny);
    for iy in 0..ny {
        for ix in 0..nx {
            let x = xmin + (ix as f64 / (nx.saturating_sub(1).max(1) as f64)) * (xmax - xmin);
            let y = ymin + (iy as f64 / (ny.saturating_sub(1).max(1) as f64)) * (ymax - ymin);
            let ll = if source_crs == 4326 && x >= -180.0 && x <= 180.0 && y >= -90.0 && y <= 90.0 {
                Some((x, y))
            } else {
                transform_xy(&src, &wgs84, x, y).ok()
            };
            if let Some((lon, lat)) = ll {
                if lon.is_finite()
                    && lat.is_finite()
                    && lon >= -180.0
                    && lon <= 180.0
                    && lat >= -90.0
                    && lat <= 90.0
                {
                    lat_lon.push((lat, lon));
                    continue;
                }
            }
            if source_crs == 4326 {
                if let Ok((lon, lat)) = transform_xy(&CrsRecord::epsg(3857), &wgs84, x, y) {
                    if lon.is_finite()
                        && lat.is_finite()
                        && lon >= -180.0
                        && lon <= 180.0
                        && lat >= -90.0
                        && lat <= 90.0
                    {
                        lat_lon.push((lat, lon));
                        continue;
                    }
                }
            }
            lat_lon.push((f64::NAN, f64::NAN));
        }
    }

    let valid_mask: Vec<bool> = lat_lon
        .iter()
        .map(|(lat, lon)| lat.is_finite() && lon.is_finite())
        .collect();
    let request_pts: Vec<(f64, f64)> = lat_lon
        .iter()
        .copied()
        .filter(|(lat, lon)| lat.is_finite() && lon.is_finite())
        .collect();

    let ot_key = std::env::var("OPENTTOPOGRAPHY_API_KEY")
        .ok()
        .or_else(|| std::env::var("OPENTOPOGRAPHY_API_KEY").ok())
        .unwrap_or_default();

    // Construct the tile cache from the artifact root.  All elevation
    // responses are stored under {artifact_root}/../tile-cache/dem/ and
    // survive across pipeline re-runs, avoiding redundant API calls.
    let tile_cache = TileCache::from_artifact_root(ctx.artifact_root);

    let mut provider_name = "open_meteo_elevation_api";
    let fetched = if !ot_key.trim().is_empty() {
        let ot = fetch_opentopography_cached(&request_pts, &ot_key, timeout_ms, &tile_cache).await;
        let ok = ot.iter().filter(|v| v.is_some()).count();
        if ok > 0 {
            provider_name = "opentopography_globaldem_cop30";
            ot
        } else {
            fetch_open_meteo_cached(&request_pts, timeout_ms, &tile_cache).await
        }
    } else {
        fetch_open_meteo_cached(&request_pts, timeout_ms, &tile_cache).await
    };
    let mut fetch_iter = fetched.into_iter();
    let mut grid_values: Vec<Option<f64>> = Vec::with_capacity(nx * ny);
    let mut provider_mask: Vec<bool> = Vec::with_capacity(nx * ny);
    for ok in &valid_mask {
        if *ok {
            let v = fetch_iter.next().flatten();
            provider_mask.push(v.is_some());
            grid_values.push(v);
        } else {
            provider_mask.push(false);
            grid_values.push(None);
        }
    }
    let mut filled_mask = vec![false; nx * ny];

    let mut source_used = provider_name;
    let fetch_success = grid_values.iter().filter(|v| v.is_some()).count();
    if fetch_success < (nx * ny) / 4 {
        if xyz_points.len() >= 3 {
            grid_values = idw_surface_from_xyz(&xyz_points, nx, ny, xmin, xmax, ymin, ymax);
            source_used = "fallback_idw_from_xyz";
        } else {
            return Err(NodeError::InvalidConfig(
                "dem_fetch could not retrieve external elevation samples and has no XYZ fallback (need >=3 XYZ points with z)".into(),
            ));
        }
    } else if fetch_success < (nx * ny) {
        // Partial DEM responses can create hard cliffs if null cells are rendered as zero downstream.
        // Fill only missing cells using IDW from successfully fetched DEM cells.
        let mut seeds: Vec<XYZ> = Vec::with_capacity(fetch_success);
        for iy in 0..ny {
            for ix in 0..nx {
                let idx = iy * nx + ix;
                let Some(z) = grid_values[idx] else {
                    continue;
                };
                let x = xmin + (ix as f64 / (nx.saturating_sub(1).max(1) as f64)) * (xmax - xmin);
                let y = ymin + (iy as f64 / (ny.saturating_sub(1).max(1) as f64)) * (ymax - ymin);
                seeds.push(XYZ { x, y, z });
            }
        }
        if seeds.len() >= 3 {
            let filled = idw_surface_from_xyz(&seeds, nx, ny, xmin, xmax, ymin, ymax);
            for i in 0..grid_values.len() {
                if grid_values[i].is_none() {
                    grid_values[i] = filled[i];
                    filled_mask[i] = grid_values[i].is_some();
                }
            }
            source_used = "open_meteo_elevation_api_filled";
        }
    }

    // Optional DEM fitting stage: nudge provider DEM to upstream XYZ control points.
    let mut fit_qc = serde_json::Value::Null;
    let mut fit_applied = false;
    if fit_mode != "none"
        && xyz_points.len() >= fit_min_points
        && source_used != "fallback_idw_from_xyz"
    {
        let mut matched: Vec<(XYZ, f64)> = Vec::new();
        for cp in &xyz_points {
            if let Some(pred) =
                bilinear_from_grid(nx, ny, xmin, xmax, ymin, ymax, &grid_values, cp.x, cp.y)
            {
                matched.push((*cp, pred));
            }
        }
        if matched.len() >= fit_min_points {
            let n = matched.len() as f64;
            let cx = matched.iter().map(|m| m.0.x).sum::<f64>() / n;
            let cy = matched.iter().map(|m| m.0.y).sum::<f64>() / n;
            let mut sum_r = 0.0;
            let mut sum_x = 0.0;
            let mut sum_y = 0.0;
            let mut sum_xx = 0.0;
            let mut sum_yy = 0.0;
            let mut sum_xy = 0.0;
            let mut sum_xr = 0.0;
            let mut sum_yr = 0.0;
            let mut rmse_before_acc = 0.0;
            for (cp, pred) in &matched {
                let rx = cp.x - cx;
                let ry = cp.y - cy;
                let r = cp.z - *pred;
                rmse_before_acc += r * r;
                sum_r += r;
                sum_x += rx;
                sum_y += ry;
                sum_xx += rx * rx;
                sum_yy += ry * ry;
                sum_xy += rx * ry;
                sum_xr += rx * r;
                sum_yr += ry * r;
            }
            let rmse_before = (rmse_before_acc / n).sqrt();
            let mut dz = sum_r / n;
            let mut ax = 0.0;
            let mut ay = 0.0;
            if fit_mode == "affine_xy_z" {
                let det = sum_xx * sum_yy - sum_xy * sum_xy;
                if det.abs() > 1e-9 {
                    ax = (sum_xr * sum_yy - sum_yr * sum_xy) / det;
                    ay = (sum_yr * sum_xx - sum_xr * sum_xy) / det;
                }
                dz = (sum_r - ax * sum_x - ay * sum_y) / n;
            }

            for iy in 0..ny {
                for ix in 0..nx {
                    let idx = iy * nx + ix;
                    let Some(v0) = grid_values[idx] else {
                        continue;
                    };
                    let x =
                        xmin + (ix as f64 / (nx.saturating_sub(1).max(1) as f64)) * (xmax - xmin);
                    let y =
                        ymin + (iy as f64 / (ny.saturating_sub(1).max(1) as f64)) * (ymax - ymin);
                    let corr = dz + ax * (x - cx) + ay * (y - cy);
                    grid_values[idx] = Some(v0 + corr);
                }
            }

            let mut rmse_after_acc = 0.0;
            for (cp, pred) in &matched {
                let corr = dz + ax * (cp.x - cx) + ay * (cp.y - cy);
                let after = cp.z - (*pred + corr);
                rmse_after_acc += after * after;
            }
            fit_applied = true;
            fit_qc = serde_json::json!({
                "mode": fit_mode,
                "control_points_used": matched.len(),
                "rmse_before": rmse_before,
                "rmse_after": (rmse_after_acc / n).sqrt(),
                "dz": dz,
                "tilt_x": ax,
                "tilt_y": ay,
                "origin": [cx, cy]
            });
            source_used = if source_used.contains("opentopography") {
                "opentopography_globaldem_cop30_fitted"
            } else {
                "open_meteo_elevation_api_fitted"
            };
        }
    }

    // Confidence classification for downstream overlays.
    // 0: low_density_or_missing, 1: interpolated, 2: provider_raw, 3: provider_adjusted, 4: control_anchor, 5: xyz_fallback
    let mut confidence_class: Vec<u8> = vec![0; nx * ny];
    if source_used == "fallback_idw_from_xyz" {
        confidence_class.fill(5);
    } else {
        for i in 0..confidence_class.len() {
            confidence_class[i] = if provider_mask[i] {
                if fit_applied {
                    3
                } else {
                    2
                }
            } else if filled_mask[i] && grid_values[i].is_some() {
                1
            } else {
                0
            };
        }
    }
    if !xyz_points.is_empty() {
        let cell_size = ((xmax - xmin).abs() / nx.max(1) as f64)
            .max((ymax - ymin).abs() / ny.max(1) as f64)
            .max(1e-9);
        let low_density_radius = low_density_cells * cell_size;
        let anchor_radius = anchor_cells * cell_size;
        for iy in 0..ny {
            for ix in 0..nx {
                let idx = iy * nx + ix;
                if grid_values[idx].is_none() {
                    confidence_class[idx] = 0;
                    continue;
                }
                let x = xmin + (ix as f64 / (nx.saturating_sub(1).max(1) as f64)) * (xmax - xmin);
                let y = ymin + (iy as f64 / (ny.saturating_sub(1).max(1) as f64)) * (ymax - ymin);
                let mut dmin = f64::INFINITY;
                for cp in &xyz_points {
                    let dx = cp.x - x;
                    let dy = cp.y - y;
                    dmin = dmin.min((dx * dx + dy * dy).sqrt());
                }
                if dmin <= anchor_radius {
                    confidence_class[idx] = 4;
                } else if dmin > low_density_radius && confidence_class[idx] != 5 {
                    confidence_class[idx] = 0;
                }
            }
        }
    }
    let confidence_score: Vec<f64> = confidence_class
        .iter()
        .map(|c| match c {
            5 => 0.8,
            4 => 1.0,
            3 => 0.82,
            2 => 0.62,
            1 => 0.38,
            _ => 0.12,
        })
        .collect();

    let finite_vals: Vec<f64> = grid_values.iter().copied().flatten().collect();
    let (zmin, zmax) = if finite_vals.is_empty() {
        (0.0, 0.0)
    } else {
        (
            finite_vals.iter().copied().fold(f64::INFINITY, f64::min),
            finite_vals
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max),
        )
    };
    let out = serde_json::json!({
        "type":"dem_fetch_surface",
        "source": source_used,
        "display_contract":{
            "renderer":"terrain",
            "display_pointer":"scene3d.terrain",
            "editable":["visible","opacity"]
        },
        "bounds":{
            "xmin":xmin,
            "xmax":xmax,
            "ymin":ymin,
            "ymax":ymax
        },
        "stats":{
            "fetch_success_cells": fetch_success,
            "total_cells": nx * ny,
            "z_min": zmin,
            "z_max": zmax,
            "fit_applied": fit_applied
        },
        "meta":{
            "extent_source": extent_source,
            "inferred_from_points": xyz_points.len(),
            "inferred_from_xy_points": xy_points.len(),
            "fit_mode": fit_mode,
            "fit_qc": fit_qc,
            "input_artifact_keys": input_artifact_keys,
            "selected_input_bbox": input_bbox.map(|(x0,x1,y0,y1)| serde_json::json!({"xmin":x0,"xmax":x1,"ymin":y0,"ymax":y1})).unwrap_or(serde_json::Value::Null),
            "selected_aoi_bbox": aoi_bbox.map(|(x0,x1,y0,y1)| serde_json::json!({"xmin":x0,"xmax":x1,"ymin":y0,"ymax":y1})).unwrap_or(serde_json::Value::Null)
        },
        "confidence_grid":{
            "nx":nx,
            "ny":ny,
            "xmin":xmin,
            "xmax":xmax,
            "ymin":ymin,
            "ymax":ymax,
            "class_ids": confidence_class,
            "scores": confidence_score,
            "legend":{
                "0":"low_density_or_missing",
                "1":"interpolated_or_extrapolated",
                "2":"provider_raw",
                "3":"provider_adjusted_to_control",
                "4":"ground_truth_anchor",
                "5":"xyz_idw_fallback"
            }
        },
        "surface_grid":{
            "nx":nx,
            "ny":ny,
            "xmin":xmin,
            "xmax":xmax,
            "ymin":ymin,
            "ymax":ymax,
            "values":grid_values
        },
        "crs": CrsRecord::epsg(source_crs)
    });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!(
        "graphs/{}/nodes/{}/dem_surface.json",
        job.graph_id, job.node_id
    );
    let artifact = write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

fn bbox_from_geojson_like(geom: &serde_json::Value) -> Option<(f64, f64, f64, f64)> {
    let coords = geom.get("coordinates")?.as_array()?;
    let first_ring = coords.first()?.as_array()?;
    let mut xmin = f64::INFINITY;
    let mut xmax = f64::NEG_INFINITY;
    let mut ymin = f64::INFINITY;
    let mut ymax = f64::NEG_INFINITY;
    for p in first_ring {
        let a = p.as_array()?;
        if a.len() < 2 {
            continue;
        }
        let x = a[0].as_f64()?;
        let y = a[1].as_f64()?;
        xmin = xmin.min(x);
        xmax = xmax.max(x);
        ymin = ymin.min(y);
        ymax = ymax.max(y);
    }
    if xmin.is_finite() && xmax.is_finite() && ymin.is_finite() && ymax.is_finite() {
        Some((xmin, xmax, ymin, ymax))
    } else {
        None
    }
}

fn lonlat_to_web_mercator(lon_deg: f64, lat_deg: f64) -> (f64, f64) {
    let max_lat = 85.051_128_78_f64;
    let lat = lat_deg.max(-max_lat).min(max_lat);
    let x = lon_deg * 20_037_508.34_f64 / 180.0;
    let rad = lat.to_radians();
    let y = (std::f64::consts::PI / 4.0 + rad / 2.0).tan().ln() * 6_378_137.0;
    (x, y)
}

fn imagery_url_host_variants(url: &str) -> Vec<String> {
    let mut out = vec![url.to_string()];
    if url.contains("services.arcgisonline.com") {
        out.push(url.replace("services.arcgisonline.com", "server.arcgisonline.com"));
    } else if url.contains("server.arcgisonline.com") {
        out.push(url.replace("server.arcgisonline.com", "services.arcgisonline.com"));
    }
    out.sort();
    out.dedup();
    out
}

fn imagery_provider_meta(
    provider_id: &str,
) -> (&'static str, &'static str, &'static str, &'static str) {
    match provider_id {
        "esri_world_topo" => (
            "Esri World Topo",
            "Esri, HERE, Garmin, FAO, NOAA, USGS",
            "https://services.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/export",
            "jpg",
        ),
        "esri_natgeo" => (
            "Esri NatGeo World",
            "Esri, National Geographic, Garmin, HERE, UNEP-WCMC, USGS, NASA",
            "https://services.arcgisonline.com/ArcGIS/rest/services/NatGeo_World_Map/MapServer/export",
            "jpg",
        ),
        "usgs_imagery" => (
            "USGS Imagery",
            "USGS National Map",
            "https://basemap.nationalmap.gov/arcgis/rest/services/USGSImageryOnly/MapServer/export",
            "jpg",
        ),
        _ => (
            "Esri World Imagery",
            "Esri, Maxar, Earthstar Geographics, and the GIS User Community",
            "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/export",
            "jpg",
        ),
    }
}

fn build_imagery_urls(
    export_url: &str,
    image_format: &str,
    xmin: f64,
    xmax: f64,
    ymin: f64,
    ymax: f64,
    crs_preference: &[i64],
    resolution_ladder: &[i64],
) -> Result<Vec<String>, NodeError> {
    let mut urls: Vec<String> = Vec::new();
    let is_wgs84_bounds = xmin >= -180.0
        && xmax <= 180.0
        && ymin >= -90.0
        && ymax <= 90.0
        && xmin < xmax
        && ymin < ymax;
    if !is_wgs84_bounds {
        return Ok(urls);
    }
    let size_bases = if resolution_ladder.is_empty() {
        vec![1024_i64, 768_i64, 512_i64]
    } else {
        resolution_ladder
            .iter()
            .copied()
            .map(|x| x.max(256).min(4096))
            .collect::<Vec<_>>()
    };
    let crs_order = if crs_preference.is_empty() {
        vec![3857_i64, 4326_i64]
    } else {
        crs_preference.to_vec()
    };
    let (x0, y0) = lonlat_to_web_mercator(xmin, ymin);
    let (x1, y1) = lonlat_to_web_mercator(xmax, ymax);
    for w in size_bases {
        for sr in &crs_order {
            let mut u = reqwest::Url::parse(export_url).map_err(|e| {
                NodeError::InvalidConfig(format!("invalid provider export url: {e}"))
            })?;
            match *sr {
                3857 => {
                    u.query_pairs_mut()
                        .append_pair(
                            "bbox",
                            &format!(
                                "{},{},{},{}",
                                x0.min(x1),
                                y0.min(y1),
                                x0.max(x1),
                                y0.max(y1)
                            ),
                        )
                        .append_pair("bboxSR", "3857")
                        .append_pair("imageSR", "3857");
                }
                _ => {
                    u.query_pairs_mut()
                        .append_pair("bbox", &format!("{xmin},{ymin},{xmax},{ymax}"))
                        .append_pair("bboxSR", "4326")
                        .append_pair("imageSR", "4326");
                }
            }
            u.query_pairs_mut()
                .append_pair("size", &format!("{w},{w}"))
                .append_pair("format", image_format)
                .append_pair("transparent", "false")
                .append_pair("f", "image");
            for v in imagery_url_host_variants(&u.to_string()) {
                urls.push(v);
            }
        }
    }
    urls.sort();
    urls.dedup();
    Ok(urls)
}

pub(crate) async fn build_imagery_like_contract(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
    schema_id: &str,
    node_kind: &str,
) -> Result<serde_json::Value, NodeError> {
    let aoi_margin_raw = job
        .output_spec
        .pointer("/node_ui/aoi_margin_pct")
        .and_then(|v| v.as_f64())
        .or_else(|| {
            job.output_spec
                .pointer("/node_ui/margin_pct")
                .and_then(|v| v.as_f64())
        })
        .unwrap_or(25.0);
    let aoi_margin_pct = if aoi_margin_raw > 1.0 {
        aoi_margin_raw / 100.0
    } else {
        aoi_margin_raw
    }
    .max(0.0);
    let provider_precedence = job
        .output_spec
        .pointer("/node_ui/provider_precedence")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.trim().to_string()))
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let provider_id_cfg = job
        .output_spec
        .pointer("/node_ui/provider_id")
        .and_then(|v| v.as_str())
        .unwrap_or("esri_world_imagery")
        .to_string();
    let provider_order = if provider_precedence.is_empty() {
        vec![provider_id_cfg.clone()]
    } else {
        provider_precedence
    };
    let provider_id = provider_order
        .first()
        .map(String::as_str)
        .unwrap_or("esri_world_imagery");
    let custom_tileset = job
        .output_spec
        .pointer("/node_ui/custom_tileset")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string);
    let crs_preference = job
        .output_spec
        .pointer("/node_ui/crs_preference")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|x| x.as_i64()).collect::<Vec<_>>())
        .unwrap_or_else(|| vec![3857, 4326]);
    let resolution_ladder = job
        .output_spec
        .pointer("/node_ui/resolution_ladder_px")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|x| x.as_i64()).collect::<Vec<_>>())
        .unwrap_or_else(|| vec![1024, 768, 512]);
    let retry_limit = job
        .output_spec
        .pointer("/node_ui/retry_limit")
        .and_then(|v| v.as_i64())
        .unwrap_or(2)
        .max(0);
    let timeout_ms = job
        .output_spec
        .pointer("/node_ui/timeout_ms")
        .and_then(|v| v.as_i64())
        .unwrap_or(8000)
        .max(500);
    let max_candidates = job
        .output_spec
        .pointer("/node_ui/max_candidates")
        .and_then(|v| v.as_i64())
        .unwrap_or(16)
        .max(1);
    let cache_scope = job
        .output_spec
        .pointer("/node_ui/cache_scope")
        .and_then(|v| v.as_str())
        .unwrap_or("project");
    let cache_ttl_s = job
        .output_spec
        .pointer("/node_ui/cache_ttl_s")
        .and_then(|v| v.as_i64())
        .unwrap_or(604800)
        .max(60);
    let allow_stale_on_error = job
        .output_spec
        .pointer("/node_ui/allow_stale_on_error")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let debounce_profile = job
        .output_spec
        .pointer("/node_ui/debounce_profile")
        .and_then(|v| v.as_str())
        .unwrap_or("free_default");
    let (provider_label, attribution, export_url, image_format) =
        imagery_provider_meta(provider_id);

    let mut bbox: Option<(f64, f64, f64, f64)> = None;
    let mut aoi_source_used = String::from("fallback_default");
    let mut target_crs = job
        .project_crs
        .clone()
        .unwrap_or_else(|| CrsRecord::epsg(4326));
    let mut has_surface_grid = false;
    let mut passthrough_surface_grid: Option<serde_json::Value> = None;
    let mut passthrough_surface_grid_cells: usize = 0;
    let mut xyz_points: Vec<XYZ> = Vec::new();
    let mut xy_points: Vec<(f64, f64)> = Vec::new();
    for ar in &job.input_artifact_refs {
        let v = read_json_artifact(ctx, &ar.key).await?;
        xyz_points.extend(collect_xyz_points(&v));
        xy_points.extend(collect_xy_points(&v));
        if bbox.is_none() {
            if let Some(b) = v.get("bounds").and_then(|b| b.as_object()) {
                let xmin = b.get("xmin").and_then(|x| x.as_f64());
                let xmax = b.get("xmax").and_then(|x| x.as_f64());
                let ymin = b.get("ymin").and_then(|y| y.as_f64());
                let ymax = b.get("ymax").and_then(|y| y.as_f64());
                if let (Some(xmin), Some(xmax), Some(ymin), Some(ymax)) = (xmin, xmax, ymin, ymax) {
                    bbox = Some((xmin, xmax, ymin, ymax));
                    let sid = v.get("schema_id").and_then(|x| x.as_str()).unwrap_or("");
                    aoi_source_used = if sid == "spatial.aoi.v1" {
                        "aoi_input".into()
                    } else if v.get("surface_grid").is_some() {
                        "terrain_input".into()
                    } else {
                        "bounds_input".into()
                    };
                }
            }
        }
        if bbox.is_none() {
            if let Some(g) = v.get("surface_grid").and_then(|g| g.as_object()) {
                let xmin = g.get("xmin").and_then(|x| x.as_f64());
                let xmax = g.get("xmax").and_then(|x| x.as_f64());
                let ymin = g.get("ymin").and_then(|y| y.as_f64());
                let ymax = g.get("ymax").and_then(|y| y.as_f64());
                if let (Some(xmin), Some(xmax), Some(ymin), Some(ymax)) = (xmin, xmax, ymin, ymax) {
                    bbox = Some((xmin, xmax, ymin, ymax));
                    aoi_source_used = "terrain_input".into();
                }
            }
        }
        if bbox.is_none() {
            if let Some(geom) = v.get("geometry") {
                bbox = bbox_from_geojson_like(geom);
                if bbox.is_some() {
                    aoi_source_used = "geometry_input".into();
                }
            }
        }
        if v.get("surface_grid").is_some() {
            has_surface_grid = true;
            if let Some(g) = v.get("surface_grid").and_then(|x| x.as_object()) {
                let nx = g.get("nx").and_then(|x| x.as_u64()).unwrap_or(0) as usize;
                let ny = g.get("ny").and_then(|x| x.as_u64()).unwrap_or(0) as usize;
                let cells = nx.saturating_mul(ny);
                if cells > passthrough_surface_grid_cells {
                    passthrough_surface_grid_cells = cells;
                    passthrough_surface_grid = Some(serde_json::Value::Object(g.clone()));
                }
            }
        }
        if let Some(crs) = v.get("crs") {
            if let Ok(c) = serde_json::from_value::<CrsRecord>(crs.clone()) {
                target_crs = c;
            }
        }
    }
    if bbox.is_none() {
        let inferred = merge_extents(
            infer_extent(&xyz_points, aoi_margin_pct),
            infer_extent_xy(&xy_points, aoi_margin_pct),
        );
        if let Some(inferred) = inferred {
            bbox = Some(inferred);
            aoi_source_used = match (xyz_points.is_empty(), xy_points.is_empty()) {
                (false, false) => "xyz_xy_input".into(),
                (false, true) => "xyz_input".into(),
                (true, false) => "xy_input".into(),
                (true, true) => "fallback_default".into(),
            };
        }
    }
    let used_fallback_bbox = bbox.is_none();
    let (xmin, xmax, ymin, ymax) = bbox.unwrap_or((-0.5, 0.5, -0.5, 0.5));
    let source_crs = CrsRecord::epsg(4326);
    let is_wgs84_bounds = xmin >= -180.0
        && xmax <= 180.0
        && ymin >= -90.0
        && ymax <= 90.0
        && xmin < xmax
        && ymin < ymax;
    let mut warnings = if used_fallback_bbox {
        vec!["fallback_aoi_default".to_string()]
    } else {
        Vec::new()
    };
    let mut image_url_candidates = if custom_tileset.is_some() || used_fallback_bbox {
        Vec::new()
    } else {
        build_imagery_urls(
            export_url,
            image_format,
            xmin,
            xmax,
            ymin,
            ymax,
            &crs_preference,
            &resolution_ladder,
        )?
    };
    if used_fallback_bbox {
        warnings.push("tilebroker_missing_aoi_inputs".to_string());
    }
    if (image_url_candidates.len() as i64) > max_candidates {
        image_url_candidates.truncate(max_candidates as usize);
    }
    let image_url = image_url_candidates.first().cloned();
    let quality_flags = if custom_tileset.is_some() {
        vec!["custom_tileset_override".to_string()]
    } else if is_wgs84_bounds {
        vec!["bbox_wgs84".to_string(), "size_ladder".to_string()]
    } else {
        vec!["bbox_not_wgs84_reprojection_required".to_string()]
    };
    let fingerprint = hash_bytes(
        format!(
            "{provider_id}:{xmin:.6}:{xmax:.6}:{ymin:.6}:{ymax:.6}:{}",
            target_crs.epsg.unwrap_or(0)
        )
        .as_bytes(),
    );

    Ok(serde_json::json!({
        "schema_id":schema_id,
        "schema_version":1,
        "provider_id":provider_id,
        "provider_label":provider_label,
        "attribution":attribution,
        "license_terms_url":"https://www.esri.com/en-us/legal/terms/full-master-agreement",
        "source_crs":source_crs,
        "target_crs":target_crs,
        "texture_mode": if custom_tileset.is_some() {"tile_template"} else {"single_image"},
        "image_url": if custom_tileset.is_some() { serde_json::Value::Null } else { serde_json::json!(image_url) },
        "image_url_candidates": image_url_candidates,
        "tile_url_template": custom_tileset,
        "bounds":{"xmin":xmin,"xmax":xmax,"ymin":ymin,"ymax":ymax},
        "resolution_m_est": job.output_spec.pointer("/node_ui/target_resolution_m").and_then(|v| v.as_f64()),
        "pixel_size":{"width":1024,"height":1024},
        "z_mode": if has_surface_grid {"drape_on_surface"} else {"flat"},
        "surface_grid": passthrough_surface_grid,
        "quality_flags":quality_flags,
        "fallback_chain_used": job.output_spec.pointer("/node_ui/fallback_provider_ids").cloned().unwrap_or(serde_json::json!([])),
        "fingerprint":fingerprint,
        "display_contract":{
            "display_pointer":"scene3d.imagery_drape",
            "renderer":"drape",
            "editable":["visible","opacity","provider"]
        },
        "cache":{
            "scope":cache_scope,
            "status":"miss",
            "ttl_s": cache_ttl_s,
            "allow_stale_on_error": allow_stale_on_error
        },
        "effective_config":{
            "provider_precedence": provider_order,
            "provider_selected": provider_id,
            "custom_tileset": job.output_spec.pointer("/node_ui/custom_tileset").cloned().unwrap_or(serde_json::Value::Null),
            "crs_preference": crs_preference,
            "resolution_ladder_px": resolution_ladder,
            "retry_limit": retry_limit,
            "timeout_ms": timeout_ms,
            "max_candidates": max_candidates,
            "cache_scope": cache_scope,
            "cache_ttl_s": cache_ttl_s,
            "debounce_profile": debounce_profile
        },
        "warnings": warnings,
        "aoi_source_used": aoi_source_used,
        "diagnostics":{
            "provider_attempts": image_url_candidates.len(),
            "request_strategy":"crs_preference_with_size_ladder",
            "retry_limit": retry_limit,
            "timeout_ms": timeout_ms
        },
        "provenance":{
            "node_kind":node_kind,
            "node_id": job.node_id.to_string()
        }
    }))
}
