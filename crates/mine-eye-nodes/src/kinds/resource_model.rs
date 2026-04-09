use std::collections::{BTreeMap, BTreeSet};

use mine_eye_types::{JobEnvelope, JobResult, JobStatus};
use serde_json::{json, Map, Value};

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
    n_samples_used: usize,
    nearest_sample_distance_m: f64,
    mean_sample_distance_m: f64,
    confidence_class: String,
}

#[derive(Clone)]
struct ModelParams {
    element_field: Option<String>,
    block_size_x: f64,
    block_size_y: f64,
    block_size_z: f64,
    cutoff_grade: f64,
    sg_mode: String,
    sg_field: Option<String>,
    sg_constant: f64,
    grade_unit: String,
    estimation_method: String,
    idw_power: f64,
    search_radius_m: f64,
    search_azimuth_deg: f64,
    anisotropy_x: f64,
    anisotropy_y: f64,
    anisotropy_z: f64,
    min_samples: usize,
    max_samples: usize,
    grade_min: Option<f64>,
    grade_max: Option<f64>,
    clip_mode: String,
    below_cutoff_opacity: f64,
    preferred_palette: String,
    max_blocks: usize,
    domain_mode: String,
    hull_buffer_m: f64,
    extrapolation_buffer_m: f64,
    domain_constraint_mode: String,
    sensitivity_min_cutoff: Option<f64>,
    sensitivity_max_cutoff: Option<f64>,
    sensitivity_steps: usize,
    variogram_lags: usize,
    variogram_max_pairs: usize,
    variogram_max_range_m: f64,
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

#[derive(Clone)]
struct DomainPolygon {
    vertices: Vec<(f64, f64)>,
}

#[derive(Clone, Copy)]
struct NeighborStats {
    n_used: usize,
    nearest_m: f64,
    mean_m: f64,
}

#[derive(Clone, Copy)]
struct Triangle3 {
    a: [f64; 3],
    b: [f64; 3],
    c: [f64; 3],
}

#[derive(Clone)]
struct TriangleMesh {
    tris: Vec<Triangle3>,
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

    fn with_absolute_padding(self, x_m: f64, y_m: f64, z_m: f64) -> Self {
        let px = x_m.max(0.0);
        let py = y_m.max(0.0);
        let pz = z_m.max(0.0);
        Self {
            xmin: self.xmin - px,
            xmax: self.xmax + px,
            ymin: self.ymin - py,
            ymax: self.ymax + py,
            zmin: self.zmin - pz,
            zmax: self.zmax + pz,
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
        sg_mode: parse_str("/node_ui/sg_mode", "constant"),
        sg_field: ui("/node_ui/sg_field")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        sg_constant: parse_f64("/node_ui/sg_constant", 2.5).clamp(0.2, 8.0),
        grade_unit: parse_str("/node_ui/grade_unit", "ppm"),
        estimation_method: parse_str("/node_ui/estimation_method", "idw"),
        idw_power: parse_f64("/node_ui/idw_power", 2.0).clamp(1.0, 4.0),
        search_radius_m: parse_f64("/node_ui/search_radius_m", 0.0).max(0.0),
        search_azimuth_deg: parse_f64("/node_ui/search_azimuth_deg", 0.0),
        anisotropy_x: parse_f64("/node_ui/anisotropy_x", 1.0).max(1e-6),
        anisotropy_y: parse_f64("/node_ui/anisotropy_y", 1.0).max(1e-6),
        anisotropy_z: parse_f64("/node_ui/anisotropy_z", 1.0).max(1e-6),
        min_samples: parse_usize("/node_ui/min_samples", 3).clamp(1, 32),
        max_samples: parse_usize("/node_ui/max_samples", 24).clamp(1, 128),
        grade_min: ui("/node_ui/grade_min").and_then(parse_numeric_value),
        grade_max: ui("/node_ui/grade_max").and_then(parse_numeric_value),
        clip_mode: parse_str("/node_ui/clip_mode", "topography"),
        below_cutoff_opacity: parse_f64("/node_ui/below_cutoff_opacity", 0.08).clamp(0.0, 1.0),
        preferred_palette: parse_str("/node_ui/palette", "viridis"),
        max_blocks: parse_usize("/node_ui/max_blocks", 45000).clamp(1000, 250000),
        domain_mode: parse_str("/node_ui/domain_mode", "full_extent"),
        hull_buffer_m: parse_f64("/node_ui/hull_buffer_m", 0.0).max(0.0),
        extrapolation_buffer_m: parse_f64("/node_ui/extrapolation_buffer_m", 20.0).max(0.0),
        domain_constraint_mode: parse_str("/node_ui/domain_constraint_mode", "none"),
        sensitivity_min_cutoff: ui("/node_ui/sensitivity_min_cutoff").and_then(parse_numeric_value),
        sensitivity_max_cutoff: ui("/node_ui/sensitivity_max_cutoff").and_then(parse_numeric_value),
        sensitivity_steps: parse_usize("/node_ui/sensitivity_steps", 8).clamp(3, 40),
        variogram_lags: parse_usize("/node_ui/variogram_lags", 12).clamp(6, 40),
        variogram_max_pairs: parse_usize("/node_ui/variogram_max_pairs", 300000).clamp(2000, 3_000_000),
        variogram_max_range_m: parse_f64("/node_ui/variogram_max_range_m", 0.0).max(0.0),
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

fn cross(o: (f64, f64), a: (f64, f64), b: (f64, f64)) -> f64 {
    (a.0 - o.0) * (b.1 - o.1) - (a.1 - o.1) * (b.0 - o.0)
}

fn convex_hull_xy(samples: &[GradeSample]) -> Option<DomainPolygon> {
    if samples.len() < 3 {
        return None;
    }
    let mut pts = samples.iter().map(|s| (s.x, s.y)).collect::<Vec<_>>();
    pts.sort_by(|a, b| {
        a.0.partial_cmp(&b.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
    });
    pts.dedup_by(|a, b| (a.0 - b.0).abs() < 1e-9 && (a.1 - b.1).abs() < 1e-9);
    if pts.len() < 3 {
        return None;
    }
    let mut lower: Vec<(f64, f64)> = Vec::new();
    for p in &pts {
        while lower.len() >= 2 && cross(lower[lower.len() - 2], lower[lower.len() - 1], *p) <= 0.0 {
            lower.pop();
        }
        lower.push(*p);
    }
    let mut upper: Vec<(f64, f64)> = Vec::new();
    for p in pts.iter().rev() {
        while upper.len() >= 2 && cross(upper[upper.len() - 2], upper[upper.len() - 1], *p) <= 0.0 {
            upper.pop();
        }
        upper.push(*p);
    }
    lower.pop();
    upper.pop();
    lower.extend(upper);
    if lower.len() < 3 {
        return None;
    }
    Some(DomainPolygon { vertices: lower })
}

fn point_in_polygon_xy(x: f64, y: f64, poly: &DomainPolygon) -> bool {
    let mut inside = false;
    let n = poly.vertices.len();
    for i in 0..n {
        let (x1, y1) = poly.vertices[i];
        let (x2, y2) = poly.vertices[(i + 1) % n];
        let intersects = ((y1 > y) != (y2 > y))
            && (x < (x2 - x1) * (y - y1) / (y2 - y1 + 1e-12) + x1);
        if intersects {
            inside = !inside;
        }
    }
    inside
}

fn dist_point_segment_xy(px: f64, py: f64, a: (f64, f64), b: (f64, f64)) -> f64 {
    let vx = b.0 - a.0;
    let vy = b.1 - a.1;
    let wx = px - a.0;
    let wy = py - a.1;
    let c1 = vx * wx + vy * wy;
    if c1 <= 0.0 {
        return ((px - a.0).powi(2) + (py - a.1).powi(2)).sqrt();
    }
    let c2 = vx * vx + vy * vy;
    if c2 <= c1 {
        return ((px - b.0).powi(2) + (py - b.1).powi(2)).sqrt();
    }
    let t = c1 / c2;
    let projx = a.0 + t * vx;
    let projy = a.1 + t * vy;
    ((px - projx).powi(2) + (py - projy).powi(2)).sqrt()
}

fn min_distance_to_polygon_edges_xy(x: f64, y: f64, poly: &DomainPolygon) -> f64 {
    let mut dmin = f64::INFINITY;
    let n = poly.vertices.len();
    for i in 0..n {
        let a = poly.vertices[i];
        let b = poly.vertices[(i + 1) % n];
        dmin = dmin.min(dist_point_segment_xy(x, y, a, b));
    }
    dmin
}

fn parse_domain_polygon(root: &serde_json::Value) -> Option<DomainPolygon> {
    if root.pointer("/schema_id").and_then(|v| v.as_str()) == Some("spatial.aoi.v1") {
        let b = root.get("bounds")?.as_object()?;
        let xmin = b.get("xmin").and_then(parse_numeric_value)?;
        let xmax = b.get("xmax").and_then(parse_numeric_value)?;
        let ymin = b.get("ymin").and_then(parse_numeric_value)?;
        let ymax = b.get("ymax").and_then(parse_numeric_value)?;
        if xmin < xmax && ymin < ymax {
            return Some(DomainPolygon {
                vertices: vec![(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax)],
            });
        }
    }
    let candidates = [
        root.pointer("/domain_polygon/coordinates"),
        root.pointer("/polygon/coordinates"),
        root.pointer("/aoi_polygon/coordinates"),
    ];
    for c in candidates {
        let Some(arr) = c.and_then(|v| v.as_array()) else {
            continue;
        };
        let mut verts = Vec::<(f64, f64)>::new();
        for p in arr {
            let Some(pa) = p.as_array() else { continue };
            if pa.len() < 2 {
                continue;
            }
            let Some(x) = parse_numeric_value(&pa[0]) else { continue };
            let Some(y) = parse_numeric_value(&pa[1]) else { continue };
            verts.push((x, y));
        }
        if verts.len() >= 3 {
            return Some(DomainPolygon { vertices: verts });
        }
    }
    None
}

fn parse_triangle_mesh(root: &serde_json::Value) -> Option<TriangleMesh> {
    fn push_tri(
        tris: &mut Vec<Triangle3>,
        a: [f64; 3],
        b: [f64; 3],
        c: [f64; 3],
    ) {
        tris.push(Triangle3 { a, b, c });
    }

    let mut tris: Vec<Triangle3> = Vec::new();

    if let Some(tarr) = root.get("triangles").and_then(|v| v.as_array()) {
        for t in tarr {
            let Some(tv) = t.as_array() else { continue };
            if tv.len() != 3 {
                continue;
            }
            let parse_pt = |v: &serde_json::Value| -> Option<[f64; 3]> {
                let a = v.as_array()?;
                if a.len() < 3 {
                    return None;
                }
                Some([
                    parse_numeric_value(&a[0])?,
                    parse_numeric_value(&a[1])?,
                    parse_numeric_value(&a[2])?,
                ])
            };
            let Some(a) = parse_pt(&tv[0]) else { continue };
            let Some(b) = parse_pt(&tv[1]) else { continue };
            let Some(c) = parse_pt(&tv[2]) else { continue };
            push_tri(&mut tris, a, b, c);
        }
    }

    let mesh_obj = root.get("mesh").and_then(|v| v.as_object());
    let vertices = mesh_obj
        .and_then(|m| m.get("vertices"))
        .and_then(|v| v.as_array());
    let faces = mesh_obj
        .and_then(|m| m.get("faces"))
        .and_then(|v| v.as_array());
    if let (Some(verts), Some(fcs)) = (vertices, faces) {
        let mut vv = Vec::<[f64; 3]>::new();
        for p in verts {
            let Some(pa) = p.as_array() else { continue };
            if pa.len() < 3 {
                continue;
            }
            let Some(x) = parse_numeric_value(&pa[0]) else { continue };
            let Some(y) = parse_numeric_value(&pa[1]) else { continue };
            let Some(z) = parse_numeric_value(&pa[2]) else { continue };
            vv.push([x, y, z]);
        }
        for f in fcs {
            let Some(fa) = f.as_array() else { continue };
            if fa.len() < 3 {
                continue;
            }
            let i0 = parse_numeric_value(&fa[0]).map(|x| x as usize);
            let i1 = parse_numeric_value(&fa[1]).map(|x| x as usize);
            let i2 = parse_numeric_value(&fa[2]).map(|x| x as usize);
            let (Some(i0), Some(i1), Some(i2)) = (i0, i1, i2) else {
                continue;
            };
            if i0 < vv.len() && i1 < vv.len() && i2 < vv.len() {
                push_tri(&mut tris, vv[i0], vv[i1], vv[i2]);
            }
        }
    }

    if tris.is_empty() {
        return None;
    }
    let mut xmin = f64::INFINITY;
    let mut xmax = f64::NEG_INFINITY;
    let mut ymin = f64::INFINITY;
    let mut ymax = f64::NEG_INFINITY;
    let mut zmin = f64::INFINITY;
    let mut zmax = f64::NEG_INFINITY;
    for t in &tris {
        for p in [t.a, t.b, t.c] {
            xmin = xmin.min(p[0]);
            xmax = xmax.max(p[0]);
            ymin = ymin.min(p[1]);
            ymax = ymax.max(p[1]);
            zmin = zmin.min(p[2]);
            zmax = zmax.max(p[2]);
        }
    }
    Some(TriangleMesh {
        tris,
        xmin,
        xmax,
        ymin,
        ymax,
        zmin,
        zmax,
    })
}

fn point_in_triangle_2d(px: f64, py: f64, a: [f64; 2], b: [f64; 2], c: [f64; 2]) -> bool {
    let v0 = [c[0] - a[0], c[1] - a[1]];
    let v1 = [b[0] - a[0], b[1] - a[1]];
    let v2 = [px - a[0], py - a[1]];
    let dot00 = v0[0] * v0[0] + v0[1] * v0[1];
    let dot01 = v0[0] * v1[0] + v0[1] * v1[1];
    let dot02 = v0[0] * v2[0] + v0[1] * v2[1];
    let dot11 = v1[0] * v1[0] + v1[1] * v1[1];
    let dot12 = v1[0] * v2[0] + v1[1] * v2[1];
    let denom = dot00 * dot11 - dot01 * dot01;
    if denom.abs() < 1e-12 {
        return false;
    }
    let inv = 1.0 / denom;
    let u = (dot11 * dot02 - dot01 * dot12) * inv;
    let v = (dot00 * dot12 - dot01 * dot02) * inv;
    u >= -1e-9 && v >= -1e-9 && (u + v) <= 1.0 + 1e-9
}

fn z_at_xy_on_triangle(x: f64, y: f64, tri: &Triangle3) -> Option<f64> {
    let a = tri.a;
    let b = tri.b;
    let c = tri.c;
    let v0 = [b[0] - a[0], b[1] - a[1], b[2] - a[2]];
    let v1 = [c[0] - a[0], c[1] - a[1], c[2] - a[2]];
    let v2 = [x - a[0], y - a[1], 0.0];
    let d00 = v0[0] * v0[0] + v0[1] * v0[1];
    let d01 = v0[0] * v1[0] + v0[1] * v1[1];
    let d11 = v1[0] * v1[0] + v1[1] * v1[1];
    let d20 = v2[0] * v0[0] + v2[1] * v0[1];
    let d21 = v2[0] * v1[0] + v2[1] * v1[1];
    let denom = d00 * d11 - d01 * d01;
    if denom.abs() < 1e-12 {
        return None;
    }
    let v = (d11 * d20 - d01 * d21) / denom;
    let w = (d00 * d21 - d01 * d20) / denom;
    let u = 1.0 - v - w;
    if u < -1e-9 || v < -1e-9 || w < -1e-9 {
        return None;
    }
    Some(u * a[2] + v * b[2] + w * c[2])
}

fn point_inside_mesh_vertical(x: f64, y: f64, z: f64, mesh: &TriangleMesh) -> bool {
    if x < mesh.xmin || x > mesh.xmax || y < mesh.ymin || y > mesh.ymax || z < mesh.zmin || z > mesh.zmax {
        return false;
    }
    let mut z_hits = Vec::<f64>::new();
    for tri in &mesh.tris {
        let tri_xmin = tri.a[0].min(tri.b[0]).min(tri.c[0]);
        let tri_xmax = tri.a[0].max(tri.b[0]).max(tri.c[0]);
        let tri_ymin = tri.a[1].min(tri.b[1]).min(tri.c[1]);
        let tri_ymax = tri.a[1].max(tri.b[1]).max(tri.c[1]);
        if x < tri_xmin || x > tri_xmax || y < tri_ymin || y > tri_ymax {
            continue;
        }
        if !point_in_triangle_2d(
            x,
            y,
            [tri.a[0], tri.a[1]],
            [tri.b[0], tri.b[1]],
            [tri.c[0], tri.c[1]],
        ) {
            continue;
        }
        if let Some(zh) = z_at_xy_on_triangle(x, y, tri) {
            z_hits.push(zh);
        }
    }
    if z_hits.is_empty() {
        return false;
    }
    z_hits.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mut i = 0usize;
    while i + 1 < z_hits.len() {
        let z0 = z_hits[i];
        let z1 = z_hits[i + 1];
        if z >= z0.min(z1) && z <= z0.max(z1) {
            return true;
        }
        i += 2;
    }
    false
}

fn anisotropic_distance(x: f64, y: f64, z: f64, sx: f64, sy: f64, sz: f64, p: &ModelParams) -> f64 {
    let mut dx = x - sx;
    let mut dy = y - sy;
    let dz = z - sz;
    if p.search_azimuth_deg.abs() > 1e-9 {
        let az = p.search_azimuth_deg.to_radians();
        let c = az.cos();
        let s = az.sin();
        let rx = c * dx + s * dy;
        let ry = -s * dx + c * dy;
        dx = rx;
        dy = ry;
    }
    let ax = dx / p.anisotropy_x.max(1e-6);
    let ay = dy / p.anisotropy_y.max(1e-6);
    let az = dz / p.anisotropy_z.max(1e-6);
    (ax * ax + ay * ay + az * az).sqrt()
}

fn estimate_value_with_support(
    samples: &[GradeSample],
    x: f64,
    y: f64,
    z: f64,
    p: &ModelParams,
) -> Option<(f64, NeighborStats)> {
    let mut near: Vec<(f64, f64, f64)> = Vec::new();
    for s in samples {
        let d_model = anisotropic_distance(x, y, z, s.x, s.y, s.z, p);
        if p.search_radius_m > 0.0 && d_model > p.search_radius_m {
            continue;
        }
        let dx = x - s.x;
        let dy = y - s.y;
        let dz = z - s.z;
        let d_true = (dx * dx + dy * dy + dz * dz).sqrt();
        near.push((d_model, d_true, s.value));
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
        let nearest = near[0].1;
        let mean = near.iter().map(|(_, d, _)| *d).sum::<f64>() / near.len() as f64;
        return Some((
            near[0].2,
            NeighborStats {
                n_used: near.len(),
                nearest_m: nearest,
                mean_m: mean,
            },
        ));
    }
    let mut num = 0.0;
    let mut den = 0.0;
    for (d, _, v) in &near {
        let w = 1.0 / d.max(1e-9).powf(p.idw_power);
        num += w * *v;
        den += w;
    }
    if den <= 0.0 {
        return None;
    }
    let nearest = near[0].1;
    let mean = near.iter().map(|(_, d, _)| *d).sum::<f64>() / near.len() as f64;
    Some((
        num / den,
        NeighborStats {
            n_used: near.len(),
            nearest_m: nearest,
            mean_m: mean,
        },
    ))
}

fn classify_confidence(stats: NeighborStats, block_diag_m: f64) -> String {
    if stats.n_used >= 12 && stats.nearest_m <= 0.75 * block_diag_m && stats.mean_m <= 2.0 * block_diag_m {
        "high".to_string()
    } else if stats.n_used >= 6 && stats.nearest_m <= 1.5 * block_diag_m {
        "medium".to_string()
    } else {
        "low".to_string()
    }
}

fn compute_cutoff_sensitivity(
    blocks: &[BlockCell],
    min_cutoff: f64,
    max_cutoff: f64,
    steps: usize,
    grade_unit_factor: f64,
    troy_oz_per_tonne: f64,
) -> Vec<serde_json::Value> {
    if blocks.is_empty() || steps < 2 || max_cutoff <= min_cutoff {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(steps);
    let step = (max_cutoff - min_cutoff) / (steps as f64 - 1.0);
    for i in 0..steps {
        let cutoff = min_cutoff + i as f64 * step;
        let mut n_blocks = 0usize;
        let mut tonnage = 0.0;
        let mut ounces = 0.0;
        for b in blocks {
            if b.grade >= cutoff {
                n_blocks += 1;
                tonnage += b.tonnage_t;
                ounces += b.contained_unscaled * grade_unit_factor * troy_oz_per_tonne;
            }
        }
        out.push(json!({
            "cutoff_grade": cutoff,
            "blocks": n_blocks,
            "tonnage_t": tonnage,
            "contained_metal_oz": ounces,
        }));
    }
    out
}

fn compute_variogram(samples: &[GradeSample], lags: usize, max_range: f64, max_pairs: usize) -> Vec<serde_json::Value> {
    if samples.len() < 2 || lags < 2 {
        return Vec::new();
    }
    let mut pairs_used = 0usize;
    let mut max_d = if max_range > 0.0 { max_range } else { 0.0 };
    if max_d <= 0.0 {
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
        let diag = ((xmax - xmin).powi(2) + (ymax - ymin).powi(2) + (zmax - zmin).powi(2)).sqrt();
        max_d = 0.5 * diag.max(1.0);
    }
    let lag_w = (max_d / lags as f64).max(1e-9);
    let mut gamma_sum = vec![0.0_f64; lags];
    let mut count = vec![0usize; lags];
    let step_i = ((samples.len() * samples.len()).saturating_div(max_pairs.max(1))).max(1);
    for i in 0..samples.len() {
        for j in (i + 1)..samples.len() {
            if ((j - i) % step_i) != 0 {
                continue;
            }
            let dx = samples[i].x - samples[j].x;
            let dy = samples[i].y - samples[j].y;
            let dz = samples[i].z - samples[j].z;
            let d = (dx * dx + dy * dy + dz * dz).sqrt();
            if d <= 0.0 || d > max_d {
                continue;
            }
            let b = ((d / lag_w).floor() as usize).min(lags - 1);
            let diff = samples[i].value - samples[j].value;
            gamma_sum[b] += 0.5 * diff * diff;
            count[b] += 1;
            pairs_used += 1;
            if pairs_used >= max_pairs {
                break;
            }
        }
        if pairs_used >= max_pairs {
            break;
        }
    }
    (0..lags)
        .map(|i| {
            let from = i as f64 * lag_w;
            let to = (i + 1) as f64 * lag_w;
            let c = count[i];
            let gamma = if c > 0 { gamma_sum[i] / c as f64 } else { 0.0 };
            json!({
                "lag_from_m": from,
                "lag_to_m": to,
                "lag_mid_m": 0.5 * (from + to),
                "pairs": c,
                "gamma": gamma
            })
        })
        .collect()
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
    let mut input_domain_polygon: Option<DomainPolygon> = None;
    let mut input_domain_mesh: Option<TriangleMesh> = None;

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
        if input_domain_polygon.is_none() {
            input_domain_polygon = parse_domain_polygon(&root);
        }
        if input_domain_mesh.is_none() {
            input_domain_mesh = parse_triangle_mesh(&root);
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
    let mut sg_samples: Vec<GradeSample> = Vec::new();
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
            if params.sg_mode.eq_ignore_ascii_case("field") {
                if let Some(sg_field) = params.sg_field.as_ref() {
                    let mut sg_value = None;
                    if let Some(attrs) = obj.get("attributes").and_then(|v| v.as_object()) {
                        sg_value = lookup_numeric_case_insensitive(attrs, sg_field);
                    }
                    if sg_value.is_none() {
                        sg_value = lookup_numeric_case_insensitive(obj, sg_field);
                    }
                    if let Some(sgv) = sg_value.filter(|v| v.is_finite() && *v > 0.1 && *v < 12.0) {
                        sg_samples.push(GradeSample { x, y, z, value: sgv });
                    }
                }
            }
        }
    }

    if samples.is_empty() {
        return Err(NodeError::InvalidConfig(format!(
            "block_grade_model found no usable values for element_field `{}`",
            element_field
        )));
    }

    let mut extent = infer_extent(&samples, best_terrain.as_ref())
        .ok_or_else(|| NodeError::InvalidConfig("unable to infer model extent".into()))?
        .with_padding(0.05);

    // Allow controlled extrapolation beyond sample extents.
    let ext_xy = if params.extrapolation_buffer_m > 0.0 {
        params.extrapolation_buffer_m
    } else {
        params.block_size_x.max(params.block_size_y)
    };
    let ext_z = if params.extrapolation_buffer_m > 0.0 {
        params.extrapolation_buffer_m
    } else {
        params.block_size_z
    };
    extent = extent.with_absolute_padding(ext_xy, ext_xy, ext_z);

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
    let domain_poly = if params.domain_mode.eq_ignore_ascii_case("convex_hull")
        || params.domain_mode.eq_ignore_ascii_case("buffered_hull")
    {
        convex_hull_xy(&samples)
    } else if params.domain_mode.eq_ignore_ascii_case("input_domain_mask") {
        input_domain_polygon.clone()
    } else {
        None
    };

    if params.domain_mode.eq_ignore_ascii_case("input_domain_mask") && domain_poly.is_none() {
        return Err(NodeError::InvalidConfig(
            "domain_mode=input_domain_mask but no input polygon/AOI mask was found".into(),
        ));
    }
    if params
        .domain_constraint_mode
        .eq_ignore_ascii_case("mesh_containment")
        && input_domain_mesh.is_none()
    {
        return Err(NodeError::InvalidConfig(
            "domain_constraint_mode=mesh_containment but no triangle mesh was found upstream"
                .into(),
        ));
    }
    if params
        .domain_constraint_mode
        .eq_ignore_ascii_case("mesh_clipping_planes")
    {
        return Err(NodeError::InvalidConfig(
            "domain_constraint_mode=mesh_clipping_planes is reserved for next step (mesh_containment is available now)".into(),
        ));
    }

    let block_diag_m = (dx * dx + dy * dy + dz * dz).sqrt();

    for iz in 0..grid.2 {
        let z = extent.zmin + (iz as f64 + 0.5) * dz;
        for iy in 0..grid.1 {
            let y = extent.ymin + (iy as f64 + 0.5) * dy;
            for ix in 0..grid.0 {
                let x = extent.xmin + (ix as f64 + 0.5) * dx;
                if let Some(poly) = domain_poly.as_ref() {
                    let inside = point_in_polygon_xy(x, y, poly);
                    if params.domain_mode.eq_ignore_ascii_case("convex_hull") && !inside {
                        continue;
                    }
                    if params.domain_mode.eq_ignore_ascii_case("buffered_hull") && !inside {
                        let d_edge = min_distance_to_polygon_edges_xy(x, y, poly);
                        if d_edge > params.hull_buffer_m {
                            continue;
                        }
                    }
                    if params.domain_mode.eq_ignore_ascii_case("input_domain_mask") && !inside {
                        continue;
                    }
                }
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
                if params
                    .domain_constraint_mode
                    .eq_ignore_ascii_case("mesh_containment")
                {
                    if let Some(mesh) = input_domain_mesh.as_ref() {
                        if !point_inside_mesh_vertical(x, y, z, mesh) {
                            continue;
                        }
                    }
                }

                let Some((mut grade, support)) = estimate_value_with_support(&samples, x, y, z, &params) else {
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
                let sg_here = if params.sg_mode.eq_ignore_ascii_case("field") && !sg_samples.is_empty() {
                    estimate_value_with_support(&sg_samples, x, y, z, &params)
                        .map(|(v, _)| v.clamp(0.2, 8.0))
                        .unwrap_or(params.sg_constant)
                } else {
                    params.sg_constant
                };
                let volume_m3 = dx * dy * dz;
                let tonnage_t = volume_m3 * sg_here;
                let contained_unscaled = tonnage_t * grade;
                let confidence_class = classify_confidence(support, block_diag_m);
                blocks.push(BlockCell {
                    x,
                    y,
                    z,
                    dx,
                    dy,
                    dz,
                    grade,
                    sg: sg_here,
                    tonnage_t,
                    contained_unscaled,
                    above_cutoff: grade >= params.cutoff_grade,
                    n_samples_used: support.n_used,
                    nearest_sample_distance_m: support.nearest_m,
                    mean_sample_distance_m: support.mean_m,
                    confidence_class,
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
    let mut sum_n_samples = 0.0;
    let mut sum_nearest_m = 0.0;
    let mut sum_mean_dist_m = 0.0;
    let mut conf_high = 0usize;
    let mut conf_medium = 0usize;
    let mut conf_low = 0usize;
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
        sum_n_samples += b.n_samples_used as f64;
        sum_nearest_m += b.nearest_sample_distance_m;
        sum_mean_dist_m += b.mean_sample_distance_m;
        match b.confidence_class.as_str() {
            "high" => conf_high += 1,
            "medium" => conf_medium += 1,
            _ => conf_low += 1,
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
    let mean_n_samples_used = if blocks.is_empty() { 0.0 } else { sum_n_samples / blocks.len() as f64 };
    let mean_nearest_sample_distance_m = if blocks.is_empty() { 0.0 } else { sum_nearest_m / blocks.len() as f64 };
    let mean_sample_distance_m = if blocks.is_empty() { 0.0 } else { sum_mean_dist_m / blocks.len() as f64 };

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
                    "n_samples_used": b.n_samples_used,
                    "nearest_sample_distance_m": b.nearest_sample_distance_m,
                    "mean_sample_distance_m": b.mean_sample_distance_m,
                    "confidence_class": b.confidence_class,
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
                    "sg": b.sg,
                    "n_samples_used": b.n_samples_used as f64,
                    "nearest_sample_distance_m": b.nearest_sample_distance_m,
                    "mean_sample_distance_m": b.mean_sample_distance_m,
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
        "n_samples_used".to_string(),
        "nearest_sample_distance_m".to_string(),
        "mean_sample_distance_m".to_string(),
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
    let s_min = params
        .sensitivity_min_cutoff
        .unwrap_or_else(|| min_grade.min(params.cutoff_grade));
    let s_max = params
        .sensitivity_max_cutoff
        .unwrap_or_else(|| max_grade.max(params.cutoff_grade));
    let cutoff_sensitivity = compute_cutoff_sensitivity(
        &blocks,
        s_min,
        s_max,
        params.sensitivity_steps,
        grade_unit_factor,
        troy_oz_per_tonne,
    );
    let variogram_bins = compute_variogram(
        &samples,
        params.variogram_lags,
        params.variogram_max_range_m,
        params.variogram_max_pairs,
    );

    let semantic_summary = json!({
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
                "search_azimuth_deg": params.search_azimuth_deg,
                "anisotropy_x": params.anisotropy_x,
                "anisotropy_y": params.anisotropy_y,
                "anisotropy_z": params.anisotropy_z,
                "min_samples": params.min_samples,
            "max_samples": params.max_samples,
            "clip_mode": params.clip_mode,
            "sg_mode": params.sg_mode,
            "sg_field": params.sg_field,
            "sg_constant": params.sg_constant,
                "domain_mode": params.domain_mode,
                "hull_buffer_m": params.hull_buffer_m,
                "extrapolation_buffer_m": params.extrapolation_buffer_m,
                "domain_constraint_mode": params.domain_constraint_mode
            },
        "support_diagnostics": {
            "mean_n_samples_used": mean_n_samples_used,
            "mean_nearest_sample_distance_m": mean_nearest_sample_distance_m,
            "mean_sample_distance_m": mean_sample_distance_m,
            "confidence_counts": {
                "high": conf_high,
                "medium": conf_medium,
                "low": conf_low
            }
        }
    });
    let mut summary_map = Map::<String, Value>::new();
    summary_map.insert("cutoff_grade".into(), json!(params.cutoff_grade));
    summary_map.insert("sg_constant".into(), json!(params.sg_constant));
    summary_map.insert("grade_unit".into(), json!(params.grade_unit));
    summary_map.insert(
        "grade_unit_factor_to_fraction".into(),
        json!(grade_unit_factor),
    );
    summary_map.insert("troy_oz_per_tonne".into(), json!(troy_oz_per_tonne));
    summary_map.insert("element_field".into(), json!(element_field));
    summary_map.insert("estimation_method".into(), json!(params.estimation_method));
    summary_map.insert("idw_power".into(), json!(params.idw_power));
    summary_map.insert("search_radius_m".into(), json!(params.search_radius_m));
    summary_map.insert("search_azimuth_deg".into(), json!(params.search_azimuth_deg));
    summary_map.insert("anisotropy_x".into(), json!(params.anisotropy_x));
    summary_map.insert("anisotropy_y".into(), json!(params.anisotropy_y));
    summary_map.insert("anisotropy_z".into(), json!(params.anisotropy_z));
    summary_map.insert("min_samples".into(), json!(params.min_samples));
    summary_map.insert("max_samples".into(), json!(params.max_samples));
    summary_map.insert("clip_mode".into(), json!(params.clip_mode));
    summary_map.insert("domain_mode".into(), json!(params.domain_mode));
    summary_map.insert("hull_buffer_m".into(), json!(params.hull_buffer_m));
    summary_map.insert(
        "extrapolation_buffer_m".into(),
        json!(params.extrapolation_buffer_m),
    );
    summary_map.insert(
        "domain_constraint_mode".into(),
        json!(params.domain_constraint_mode),
    );
    summary_map.insert("sg_mode".into(), json!(params.sg_mode));
    summary_map.insert("sg_field".into(), json!(params.sg_field));
    summary_map.insert("block_size_m".into(), json!({ "x": dx, "y": dy, "z": dz }));
    summary_map.insert(
        "grid_shape".into(),
        json!({ "nx": grid.0, "ny": grid.1, "nz": grid.2 }),
    );
    summary_map.insert("estimated_blocks".into(), json!(render_blocks.len()));
    summary_map.insert("estimated_cells_before_filter".into(), json!(total));
    summary_map.insert("above_cutoff_blocks".into(), json!(above_cutoff_blocks));
    summary_map.insert("mean_grade".into(), json!(mean_grade));
    summary_map.insert("min_grade".into(), json!(min_grade));
    summary_map.insert("max_grade".into(), json!(max_grade));
    summary_map.insert("total_tonnage_t".into(), json!(total_tonnage));
    summary_map.insert(
        "total_contained_unscaled".into(),
        json!(total_contained_unscaled),
    );
    summary_map.insert("total_contained_metal_t".into(), json!(total_contained_metal_t));
    summary_map.insert("total_contained_metal_oz".into(), json!(total_contained_metal_oz));
    summary_map.insert("above_cutoff_tonnage_t".into(), json!(above_cutoff_tonnage));
    summary_map.insert(
        "above_cutoff_contained_unscaled".into(),
        json!(above_cutoff_contained_unscaled),
    );
    summary_map.insert(
        "above_cutoff_contained_metal_t".into(),
        json!(above_cutoff_contained_metal_t),
    );
    summary_map.insert(
        "above_cutoff_contained_metal_oz".into(),
        json!(above_cutoff_contained_metal_oz),
    );
    summary_map.insert("mean_n_samples_used".into(), json!(mean_n_samples_used));
    summary_map.insert(
        "mean_nearest_sample_distance_m".into(),
        json!(mean_nearest_sample_distance_m),
    );
    summary_map.insert("mean_sample_distance_m".into(), json!(mean_sample_distance_m));
    summary_map.insert("confidence_high_blocks".into(), json!(conf_high));
    summary_map.insert("confidence_medium_blocks".into(), json!(conf_medium));
    summary_map.insert("confidence_low_blocks".into(), json!(conf_low));
    let summary_payload = Value::Object(summary_map);
    let variogram_payload_inner = json!({
        "bins": variogram_bins,
        "lags": params.variogram_lags,
        "max_pairs": params.variogram_max_pairs,
        "max_range_m": params.variogram_max_range_m
    });
    let report_payload = json!({
        "schema_id": "report.block_resource.v2",
        "type": "block_resource_report",
        "semantic_summary": semantic_summary,
        "summary": summary_payload,
        "grade_histogram": grade_histogram,
        "cutoff_sensitivity": cutoff_sensitivity,
        "variogram": variogram_payload_inner,
        "notes": [
            "contained_unscaled is grade*tonnage in source grade units. contained_metal_t applies grade_unit_factor_to_fraction.",
            "Topography clipping currently uses block-center test against the best available surface_grid."
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
        content_hashes: vec![
            voxels_ref.content_hash,
            centers_ref.content_hash,
            report_ref.content_hash,
        ],
        error_message: None,
    })
}
