use std::collections::{BTreeMap, BTreeSet};

use mine_eye_types::{JobEnvelope, JobResult, JobStatus};
use serde_json::json;

use crate::executor::ExecutionContext;
use crate::NodeError;

#[derive(Clone, Copy)]
struct GradeSample {
    x: f64,
    y: f64,
    z: f64,
    value: f64,
}

#[derive(Clone)]
struct SurfaceGrid {
    nx: usize,
    ny: usize,
    xmin: f64,
    xmax: f64,
    ymin: f64,
    ymax: f64,
    values: Vec<Option<f64>>,
}

#[derive(Clone)]
struct BlockCell {
    x: f64,
    y: f64,
    z: f64,
    dx: f64,
    dy: f64,
    dz: f64,
    grade: f64,
    sg: f64,
    tonnage_t: f64,
    contained_unscaled: f64,
    above_cutoff: bool,
}

#[derive(Clone)]
struct ModelParams {
    element_field: Option<String>,
    block_size_x: f64,
    block_size_y: f64,
    block_size_z: f64,
    cutoff_grade: f64,
    sg_constant: f64,
    grade_unit: String,
    estimation_method: String,
    idw_power: f64,
    search_radius_m: f64,
    min_samples: usize,
    max_samples: usize,
    grade_min: Option<f64>,
    grade_max: Option<f64>,
    clip_mode: String,
    below_cutoff_opacity: f64,
    preferred_palette: String,
    max_blocks: usize,
}

#[derive(Clone, Copy)]
struct Extent3D {
    xmin: f64,
    xmax: f64,
    ymin: f64,
    ymax: f64,
    zmin: f64,
    zmax: f64,
}

impl Extent3D {
    fn with_padding(self, pct: f64) -> Self {
        let p = pct.max(0.0);
        let dx = (self.xmax - self.xmin).abs().max(1e-6);
        let dy = (self.ymax - self.ymin).abs().max(1e-6);
        let dz = (self.zmax - self.zmin).abs().max(1e-6);
        Self {
            xmin: self.xmin - dx * p,
            xmax: self.xmax + dx * p,
            ymin: self.ymin - dy * p,
            ymax: self.ymax + dy * p,
            zmin: self.zmin - dz * p,
            zmax: self.zmax + dz * p,
        }
    }
}

fn parse_numeric_value(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64().filter(|x| x.is_finite()),
        serde_json::Value::String(s) => s.trim().parse::<f64>().ok().filter(|x| x.is_finite()),
        _ => None,
    }
}

fn parse_surface_grid(root: &serde_json::Value) -> Option<SurfaceGrid> {
    let g = root.get("surface_grid")?.as_object()?;
    let nx = g.get("nx").and_then(parse_numeric_value).map(|v| v as usize)?;
    let ny = g.get("ny").and_then(parse_numeric_value).map(|v| v as usize)?;
    if nx < 2 || ny < 2 {
        return None;
    }
    let xmin = g.get("xmin").and_then(parse_numeric_value)?;
    let xmax = g.get("xmax").and_then(parse_numeric_value)?;
    let ymin = g.get("ymin").and_then(parse_numeric_value)?;
    let ymax = g.get("ymax").and_then(parse_numeric_value)?;
    let raw_values = g.get("values")?.as_array()?;
    if raw_values.len() != nx * ny {
        return None;
    }
    let values = raw_values.iter().map(parse_numeric_value).collect::<Vec<_>>();
    Some(SurfaceGrid {
        nx,
        ny,
        xmin,
        xmax,
        ymin,
        ymax,
        values,
    })
}

fn collect_rows<'a>(root: &'a serde_json::Value) -> Vec<&'a serde_json::Value> {
    let mut out = Vec::new();
    if let Some(arr) = root.get("assay_points").and_then(|v| v.as_array()) {
        out.extend(arr.iter());
    }
    if let Some(arr) = root.get("points").and_then(|v| v.as_array()) {
        out.extend(arr.iter());
    }
    if let Some(arr) = root.as_array() {
        out.extend(arr.iter());
    }
    out
}

fn collect_numeric_fields(
    row: &serde_json::Map<String, serde_json::Value>,
) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    for (k, v) in row.iter() {
        let key = k.to_ascii_lowercase();
        if matches!(
            key.as_str(),
            "x" | "y"
                | "z"
                | "hole_id"
                | "sample_id"
                | "segment_id"
                | "from_m"
                | "to_m"
                | "depth_m"
                | "crs"
                | "epsg"
        ) {
            continue;
        }
        if let Some(n) = parse_numeric_value(v) {
            out.insert(k.clone(), n);
        }
    }
    if let Some(attrs) = row.get("attributes").and_then(|v| v.as_object()) {
        for (k, v) in attrs {
            if let Some(n) = parse_numeric_value(v) {
                out.insert(k.clone(), n);
            }
        }
    }
    out
}

fn lookup_numeric_case_insensitive(
    obj: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<f64> {
    if let Some(v) = obj.get(key).and_then(parse_numeric_value) {
        return Some(v);
    }
    let lk = key.to_ascii_lowercase();
    for (k, v) in obj {
        if k.to_ascii_lowercase() == lk {
            if let Some(n) = parse_numeric_value(v) {
                return Some(n);
            }
        }
    }
    None
}

fn parse_params(job: &JobEnvelope) -> ModelParams {
    let ui = |ptr: &str| job.output_spec.pointer(ptr);
    let parse_f64 = |ptr: &str, d: f64| ui(ptr).and_then(parse_numeric_value).unwrap_or(d);
    let parse_str = |ptr: &str, d: &str| {
        ui(ptr)
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| d.to_string())
    };
    let parse_usize = |ptr: &str, d: usize| {
        ui(ptr)
            .and_then(parse_numeric_value)
            .map(|v| v.round() as i64)
            .filter(|v| *v > 0)
            .map(|v| v as usize)
            .unwrap_or(d)
    };

    ModelParams {
        element_field: ui("/node_ui/element_field")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        block_size_x: parse_f64("/node_ui/block_size_x", 20.0).max(0.5),
        block_size_y: parse_f64("/node_ui/block_size_y", 20.0).max(0.5),
        block_size_z: parse_f64("/node_ui/block_size_z", 10.0).max(0.5),
        cutoff_grade: parse_f64("/node_ui/cutoff_grade", 0.0),
        sg_constant: parse_f64("/node_ui/sg_constant", 2.5).clamp(0.2, 8.0),
        grade_unit: parse_str("/node_ui/grade_unit", "ppm"),
        estimation_method: parse_str("/node_ui/estimation_method", "idw"),
        idw_power: parse_f64("/node_ui/idw_power", 2.0).clamp(1.0, 4.0),
        search_radius_m: parse_f64("/node_ui/search_radius_m", 0.0).max(0.0),
        min_samples: parse_usize("/node_ui/min_samples", 3).clamp(1, 32),
        max_samples: parse_usize("/node_ui/max_samples", 24).clamp(1, 128),
        grade_min: ui("/node_ui/grade_min").and_then(parse_numeric_value),
        grade_max: ui("/node_ui/grade_max").and_then(parse_numeric_value),
        clip_mode: parse_str("/node_ui/clip_mode", "topography"),
        below_cutoff_opacity: parse_f64("/node_ui/below_cutoff_opacity", 0.08).clamp(0.0, 1.0),
        preferred_palette: parse_str("/node_ui/palette", "viridis"),
        max_blocks: parse_usize("/node_ui/max_blocks", 45000).clamp(1000, 250000),
    }
}

fn choose_element_field(
    requested: &Option<String>,
    fields: &BTreeSet<String>,
) -> Option<String> {
    if let Some(wanted) = requested {
        if fields.contains(wanted) {
            return Some(wanted.clone());
        }
    }
    fields.iter().next().cloned()
}

fn infer_extent(samples: &[GradeSample], terrain: Option<&SurfaceGrid>) -> Option<Extent3D> {
    if samples.is_empty() {
        return None;
    }
    let mut xmin = f64::INFINITY;
    let mut xmax = f64::NEG_INFINITY;
    let mut ymin = f64::INFINITY;
    let mut ymax = f64::NEG_INFINITY;
    let mut zmin = f64::INFINITY;
    let mut zmax = f64::NEG_INFINITY;
    for s in samples {
        xmin = xmin.min(s.x);
        xmax = xmax.max(s.x);
        ymin = ymin.min(s.y);
        ymax = ymax.max(s.y);
        zmin = zmin.min(s.z);
        zmax = zmax.max(s.z);
    }
    if let Some(g) = terrain {
        xmin = xmin.min(g.xmin);
        xmax = xmax.max(g.xmax);
        ymin = ymin.min(g.ymin);
        ymax = ymax.max(g.ymax);
        for v in &g.values {
            if let Some(z) = *v {
                zmax = zmax.max(z);
            }
        }
    }
    if !(xmin < xmax && ymin < ymax && zmin < zmax) {
        return None;
    }
    Some(Extent3D {
        xmin,
        xmax,
        ymin,
        ymax,
        zmin,
        zmax,
    })
}

fn estimate_block_count(ext: Extent3D, dx: f64, dy: f64, dz: f64) -> (usize, usize, usize) {
    let nx = (((ext.xmax - ext.xmin) / dx).ceil() as usize).max(1);
    let ny = (((ext.ymax - ext.ymin) / dy).ceil() as usize).max(1);
    let nz = (((ext.zmax - ext.zmin) / dz).ceil() as usize).max(1);
    (nx, ny, nz)
}

fn estimate_grade(samples: &[GradeSample], x: f64, y: f64, z: f64, p: &ModelParams) -> Option<f64> {
    let mut near: Vec<(f64, f64)> = Vec::new();
    for s in samples {
        let dx = x - s.x;
        let dy = y - s.y;
        let dz = z - s.z;
        let d = (dx * dx + dy * dy + dz * dz).sqrt();
        if p.search_radius_m > 0.0 && d > p.search_radius_m {
            continue;
        }
        near.push((d, s.value));
    }
    if near.is_empty() {
        return None;
    }
    near.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    if near.len() > p.max_samples {
        near.truncate(p.max_samples);
    }
    if near.len() < p.min_samples {
        return None;
    }
    if near[0].0 <= 1e-9 || p.estimation_method.eq_ignore_ascii_case("nearest") {
        return Some(near[0].1);
    }
    let mut num = 0.0;
    let mut den = 0.0;
    for (d, v) in near {
        let w = 1.0 / d.max(1e-9).powf(p.idw_power);
        num += w * v;
        den += w;
    }
    (den > 0.0).then_some(num / den)
}

fn compute_bins(values: &[f64]) -> Vec<serde_json::Value> {
    if values.is_empty() {
        return Vec::new();
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let min = *sorted.first().unwrap_or(&0.0);
    let max = *sorted.last().unwrap_or(&0.0);
    if max <= min {
        return vec![json!({ "from": min, "to": max, "count": sorted.len() })];
    }
    let bins = 10usize;
    let step = (max - min) / bins as f64;
    let mut counts = vec![0usize; bins];
    for v in sorted {
        let mut idx = ((v - min) / step).floor() as isize;
        if idx < 0 {
            idx = 0;
        }
        if idx as usize >= bins {
            idx = bins as isize - 1;
        }
        counts[idx as usize] += 1;
    }
    counts
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let from = min + step * i as f64;
            let to = min + step * (i as f64 + 1.0);
            json!({ "from": from, "to": to, "count": c })
        })
        .collect()
}

fn grade_unit_factor_to_fraction(unit: &str) -> f64 {
    match unit.to_ascii_lowercase().as_str() {
        "percent" | "%" => 1e-2,
        "fraction" | "ratio" => 1.0,
        "gpt" | "g/t" | "ppm" => 1e-6,
        _ => 1e-6,
    }
}

pub async fn run_block_grade_model(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let mut params = parse_params(job);
    if params.max_samples < params.min_samples {
        params.max_samples = params.min_samples;
    }

    let mut all_rows: Vec<serde_json::Value> = Vec::new();
    let mut numeric_fields = BTreeSet::new();
    let mut best_terrain: Option<SurfaceGrid> = None;
    let mut project_epsg: Option<i32> = None;

    for ar in &job.input_artifact_refs {
        let root = super::runtime::read_json_artifact(ctx, &ar.key).await?;
        if project_epsg.is_none() {
            project_epsg = root
                .pointer("/crs/epsg")
                .and_then(parse_numeric_value)
                .map(|v| v as i32);
        }
        if let Some(sg) = parse_surface_grid(&root) {
            let cells = sg.nx.saturating_mul(sg.ny);
            let best_cells = best_terrain
                .as_ref()
                .map(|g| g.nx.saturating_mul(g.ny))
                .unwrap_or(0);
            if cells > best_cells {
                best_terrain = Some(sg);
            }
        }
        for row in collect_rows(&root) {
            if let Some(obj) = row.as_object() {
                for k in collect_numeric_fields(obj).keys() {
                    numeric_fields.insert(k.clone());
                }
                all_rows.push(row.clone());
            }
        }
    }

    if all_rows.is_empty() {
        return Err(NodeError::InvalidConfig(
            "block_grade_model requires upstream 3D point rows (`assay_points` or `points`)".into(),
        ));
    }

    let Some(element_field) = choose_element_field(&params.element_field, &numeric_fields) else {
        return Err(NodeError::InvalidConfig(
            "block_grade_model could not find any numeric grade field in upstream rows".into(),
        ));
    };

    let mut samples: Vec<GradeSample> = Vec::new();
    for row in all_rows {
        let Some(obj) = row.as_object() else {
            continue;
        };
        let x = obj.get("x").and_then(parse_numeric_value);
        let y = obj.get("y").and_then(parse_numeric_value);
        let z = obj.get("z").and_then(parse_numeric_value);
        let Some(x) = x else { continue };
        let Some(y) = y else { continue };
        let Some(z) = z else { continue };

        let mut grade_value = None;
        if let Some(attrs) = obj.get("attributes").and_then(|v| v.as_object()) {
            if let Some(v) = lookup_numeric_case_insensitive(attrs, &element_field) {
                grade_value = Some(v);
            }
        }
        if grade_value.is_none() {
            grade_value = lookup_numeric_case_insensitive(obj, &element_field);
        }
        if let Some(value) = grade_value {
            samples.push(GradeSample { x, y, z, value });
        }
    }

    if samples.is_empty() {
        return Err(NodeError::InvalidConfig(format!(
            "block_grade_model found no usable values for element_field `{}`",
            element_field
        )));
    }

    let extent = infer_extent(&samples, best_terrain.as_ref())
        .ok_or_else(|| NodeError::InvalidConfig("unable to infer model extent".into()))?
        .with_padding(0.05);

    let mut dx = params.block_size_x;
    let mut dy = params.block_size_y;
    let mut dz = params.block_size_z;
    let mut grid = estimate_block_count(extent, dx, dy, dz);
    let mut total = grid.0.saturating_mul(grid.1).saturating_mul(grid.2);
    if total > params.max_blocks {
        let scale = (total as f64 / params.max_blocks as f64).cbrt();
        dx *= scale;
        dy *= scale;
        dz *= scale;
        grid = estimate_block_count(extent, dx, dy, dz);
        total = grid.0.saturating_mul(grid.1).saturating_mul(grid.2);
    }

    let mut blocks: Vec<BlockCell> = Vec::new();
    blocks.reserve(total.min(params.max_blocks));

    for iz in 0..grid.2 {
        let z = extent.zmin + (iz as f64 + 0.5) * dz;
        for iy in 0..grid.1 {
            let y = extent.ymin + (iy as f64 + 0.5) * dy;
            for ix in 0..grid.0 {
                let x = extent.xmin + (ix as f64 + 0.5) * dx;
                if params.clip_mode.eq_ignore_ascii_case("topography") {
                    if let Some(g) = best_terrain.as_ref() {
                        if let Some(top_z) = super::runtime::bilinear_from_grid(
                            g.nx,
                            g.ny,
                            g.xmin,
                            g.xmax,
                            g.ymin,
                            g.ymax,
                            &g.values,
                            x,
                            y,
                        ) {
                            if z > top_z {
                                continue;
                            }
                        }
                    }
                }

                let Some(mut grade) = estimate_grade(&samples, x, y, z, &params) else {
                    continue;
                };
                if let Some(gmin) = params.grade_min {
                    grade = grade.max(gmin);
                }
                if let Some(gmax) = params.grade_max {
                    grade = grade.min(gmax);
                }
                if !grade.is_finite() {
                    continue;
                }
                let volume_m3 = dx * dy * dz;
                let tonnage_t = volume_m3 * params.sg_constant;
                let contained_unscaled = tonnage_t * grade;
                blocks.push(BlockCell {
                    x,
                    y,
                    z,
                    dx,
                    dy,
                    dz,
                    grade,
                    sg: params.sg_constant,
                    tonnage_t,
                    contained_unscaled,
                    above_cutoff: grade >= params.cutoff_grade,
                });
            }
        }
    }

    if blocks.is_empty() {
        return Err(NodeError::InvalidConfig(
            "block_grade_model produced no blocks; check search radius / samples / clip settings"
                .into(),
        ));
    }

    let mut grade_values = Vec::with_capacity(blocks.len());
    let grade_unit_factor = grade_unit_factor_to_fraction(&params.grade_unit);
    let troy_oz_per_tonne = 32150.74656862745_f64;
    let mut total_tonnage = 0.0;
    let mut total_contained_unscaled = 0.0;
    let mut total_contained_metal_t = 0.0;
    let mut total_contained_metal_oz = 0.0;
    let mut above_cutoff_blocks = 0usize;
    let mut above_cutoff_tonnage = 0.0;
    let mut above_cutoff_contained_unscaled = 0.0;
    let mut above_cutoff_contained_metal_t = 0.0;
    let mut above_cutoff_contained_metal_oz = 0.0;
    for b in &blocks {
        grade_values.push(b.grade);
        total_tonnage += b.tonnage_t;
        total_contained_unscaled += b.contained_unscaled;
        total_contained_metal_t += b.contained_unscaled * grade_unit_factor;
        total_contained_metal_oz += b.contained_unscaled * grade_unit_factor * troy_oz_per_tonne;
        if b.above_cutoff {
            above_cutoff_blocks += 1;
            above_cutoff_tonnage += b.tonnage_t;
            above_cutoff_contained_unscaled += b.contained_unscaled;
            above_cutoff_contained_metal_t += b.contained_unscaled * grade_unit_factor;
            above_cutoff_contained_metal_oz +=
                b.contained_unscaled * grade_unit_factor * troy_oz_per_tonne;
        }
    }
    grade_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mean_grade = if total_tonnage > 0.0 {
        total_contained_unscaled / total_tonnage
    } else {
        0.0
    };
    let min_grade = *grade_values.first().unwrap_or(&0.0);
    let max_grade = *grade_values.last().unwrap_or(&0.0);

    let render_blocks = blocks
        .iter()
        .filter(|b| b.above_cutoff)
        .collect::<Vec<_>>();

    let block_rows = render_blocks
        .iter()
        .map(|b| {
            json!({
                "x": b.x,
                "y": b.y,
                "z": b.z,
                "dx": b.dx,
                "dy": b.dy,
                "dz": b.dz,
                "above_cutoff": b.above_cutoff,
                "attributes": {
                    element_field.clone(): b.grade,
                    "tonnage_t": b.tonnage_t,
                    "contained_unscaled": b.contained_unscaled,
                    "contained_metal_t": b.contained_unscaled * grade_unit_factor,
                    "sg": b.sg,
                }
            })
        })
        .collect::<Vec<_>>();

    let centers_rows = render_blocks
        .iter()
        .map(|b| {
            json!({
                "x": b.x,
                "y": b.y,
                "z": b.z,
                "attributes": {
                    element_field.clone(): b.grade,
                    "above_cutoff": if b.above_cutoff { 1.0 } else { 0.0 },
                    "tonnage_t": b.tonnage_t,
                    "contained_unscaled": b.contained_unscaled,
                    "contained_metal_t": b.contained_unscaled * grade_unit_factor,
                }
            })
        })
        .collect::<Vec<_>>();

    let measure_candidates = vec![
        element_field.clone(),
        "tonnage_t".to_string(),
        "contained_unscaled".to_string(),
        "contained_metal_t".to_string(),
        "sg".to_string(),
    ];

    let voxels_payload = json!({
        "schema_id": "scene3d.block_voxels.v1",
        "type": "block_grade_model_voxels",
        "crs": {
            "epsg": project_epsg
                .or(job.project_crs.as_ref().and_then(|c| c.epsg))
                .unwrap_or(4326)
        },
        "display_contract": {
            "renderer": "block_voxels",
            "display_pointer": "scene3d.block_voxels",
            "editable": ["visible", "opacity", "measure", "palette", "cutoff", "below_cutoff_opacity"]
        },
        "measure_candidates": measure_candidates,
        "style_defaults": {
            "palette": params.preferred_palette,
            "cutoff_grade": params.cutoff_grade,
            "below_cutoff_opacity": params.below_cutoff_opacity,
        },
        "blocks": block_rows,
        "stats": {
            "element_field": element_field,
            "estimated_blocks": render_blocks.len(),
            "estimated_cells_before_filter": total,
            "above_cutoff_blocks": above_cutoff_blocks,
            "mean_grade": mean_grade,
            "min_grade": min_grade,
            "max_grade": max_grade,
            "total_tonnage_t": total_tonnage,
            "total_contained_unscaled": total_contained_unscaled,
            "total_contained_metal_t": total_contained_metal_t,
            "total_contained_metal_oz": total_contained_metal_oz,
            "above_cutoff_tonnage_t": above_cutoff_tonnage,
            "above_cutoff_contained_unscaled": above_cutoff_contained_unscaled,
            "above_cutoff_contained_metal_t": above_cutoff_contained_metal_t,
            "above_cutoff_contained_metal_oz": above_cutoff_contained_metal_oz,
        }
    });

    let centers_payload = json!({
        "type": "block_grade_centers",
        "display_contract": {
            "renderer": "sample_points",
            "display_pointer": "scene3d.sample_points",
            "editable": ["visible", "opacity", "size", "measure", "palette"]
        },
        "measure_candidates": [
            element_field,
            "tonnage_t",
            "contained_unscaled",
            "contained_metal_t",
            "above_cutoff"
        ],
        "points": centers_rows
    });

    let grade_histogram = compute_bins(&grade_values);
    let cutoff_share_blocks_pct = if total > 0 {
        (above_cutoff_blocks as f64 / total as f64) * 100.0
    } else {
        0.0
    };
    let cutoff_share_tonnage_pct = if total_tonnage > 0.0 {
        (above_cutoff_tonnage / total_tonnage) * 100.0
    } else {
        0.0
    };

    let report_payload = json!({
        "schema_id": "report.block_resource.v2",
        "type": "block_resource_report",
        "semantic_summary": {
            "title": "Block Grade Model Resource Summary",
            "element_field": element_field,
            "grade_unit": params.grade_unit,
            "cutoff_grade": params.cutoff_grade,
            "block_size_m": { "x": dx, "y": dy, "z": dz },
            "grid_shape": { "nx": grid.0, "ny": grid.1, "nz": grid.2 },
            "estimated_blocks": render_blocks.len(),
            "above_cutoff_blocks": above_cutoff_blocks,
            "above_cutoff_share_blocks_pct": cutoff_share_blocks_pct,
            "above_cutoff_tonnage_t": above_cutoff_tonnage,
            "above_cutoff_share_tonnage_pct": cutoff_share_tonnage_pct,
            "above_cutoff_contained_metal_oz": above_cutoff_contained_metal_oz,
            "total_tonnage_t": total_tonnage,
            "total_contained_metal_oz": total_contained_metal_oz,
            "mean_grade": mean_grade,
            "min_grade": min_grade,
            "max_grade": max_grade,
            "key_parameters": {
                "estimation_method": params.estimation_method,
                "idw_power": params.idw_power,
                "search_radius_m": params.search_radius_m,
                "min_samples": params.min_samples,
                "max_samples": params.max_samples,
                "clip_mode": params.clip_mode,
                "sg_constant": params.sg_constant
            }
        },
        "summary": {
            "cutoff_grade": params.cutoff_grade,
            "sg_constant": params.sg_constant,
            "grade_unit": params.grade_unit,
            "grade_unit_factor_to_fraction": grade_unit_factor,
            "troy_oz_per_tonne": troy_oz_per_tonne,
            "element_field": element_field,
            "estimation_method": params.estimation_method,
            "idw_power": params.idw_power,
            "search_radius_m": params.search_radius_m,
            "min_samples": params.min_samples,
            "max_samples": params.max_samples,
            "clip_mode": params.clip_mode,
            "block_size_m": { "x": dx, "y": dy, "z": dz },
            "grid_shape": { "nx": grid.0, "ny": grid.1, "nz": grid.2 },
            "estimated_blocks": render_blocks.len(),
            "estimated_cells_before_filter": total,
            "above_cutoff_blocks": above_cutoff_blocks,
            "mean_grade": mean_grade,
            "min_grade": min_grade,
            "max_grade": max_grade,
            "total_tonnage_t": total_tonnage,
            "total_contained_unscaled": total_contained_unscaled,
            "total_contained_metal_t": total_contained_metal_t,
            "total_contained_metal_oz": total_contained_metal_oz,
            "above_cutoff_tonnage_t": above_cutoff_tonnage,
            "above_cutoff_contained_unscaled": above_cutoff_contained_unscaled,
            "above_cutoff_contained_metal_t": above_cutoff_contained_metal_t,
            "above_cutoff_contained_metal_oz": above_cutoff_contained_metal_oz,
        },
        "grade_histogram": grade_histogram,
        "notes": [
            "contained_unscaled is grade*tonnage in source grade units. contained_metal_t applies grade_unit_factor_to_fraction.",
            "Topography clipping currently uses block-center test against the best available surface_grid.",
        ]
    });

    let voxels_bytes = serde_json::to_vec(&voxels_payload)?;
    let voxels_key = format!(
        "graphs/{}/nodes/{}/block_grade_model_voxels.json",
        job.graph_id, job.node_id
    );
    let voxels_ref =
        super::runtime::write_artifact(ctx, &voxels_key, &voxels_bytes, Some("application/json"))
            .await?;

    let centers_bytes = serde_json::to_vec(&centers_payload)?;
    let centers_key = format!(
        "graphs/{}/nodes/{}/block_grade_model_centers.json",
        job.graph_id, job.node_id
    );
    let centers_ref =
        super::runtime::write_artifact(ctx, &centers_key, &centers_bytes, Some("application/json"))
            .await?;

    let report_bytes = serde_json::to_vec(&report_payload)?;
    let report_key = format!(
        "graphs/{}/nodes/{}/block_grade_model_report.json",
        job.graph_id, job.node_id
    );
    let report_ref =
        super::runtime::write_artifact(ctx, &report_key, &report_bytes, Some("application/json"))
            .await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![voxels_ref.clone(), centers_ref.clone(), report_ref.clone()],
        content_hashes: vec![voxels_ref.content_hash, centers_ref.content_hash, report_ref.content_hash],
        error_message: None,
    })
}
