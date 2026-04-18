use mine_eye_types::{JobEnvelope, JobResult, JobStatus, TrajectorySegment};

use crate::executor::ExecutionContext;
use crate::NodeError;

fn position_at_depth(segments: &[TrajectorySegment], depth_m: f64) -> Option<(f64, f64, f64)> {
    for s in segments {
        if depth_m < s.depth_from_m || depth_m > s.depth_to_m {
            continue;
        }
        let len = (s.depth_to_m - s.depth_from_m).abs();
        let t = if len <= f64::EPSILON {
            0.0
        } else {
            (depth_m - s.depth_from_m) / (s.depth_to_m - s.depth_from_m)
        };
        let x = s.x_from + (s.x_to - s.x_from) * t;
        let y = s.y_from + (s.y_to - s.y_from) * t;
        let z = s.z_from + (s.z_to - s.z_from) * t;
        return Some((x, y, z));
    }
    None
}

pub async fn run_formation_interface_extract(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let exclude = job
        .output_spec
        .pointer("/node_ui/exclude_formations")
        .and_then(|v| v.as_array())
        .map(|vals| {
            vals.iter()
                .filter_map(|v| v.as_str().map(|s| s.trim().to_ascii_lowercase()))
                .collect::<std::collections::HashSet<_>>()
        })
        .unwrap_or_else(|| {
            ["topo".to_string()]
                .into_iter()
                .collect::<std::collections::HashSet<_>>()
        });

    let mut hole_segments: std::collections::HashMap<String, Vec<TrajectorySegment>> =
        std::collections::HashMap::new();
    let mut lithology_rows: Vec<serde_json::Value> = Vec::new();
    let mut crs = None::<serde_json::Value>;
    let mut formation_order: Vec<String> = Vec::new();

    for ar in &job.input_artifact_refs {
        let v = super::runtime::read_json_artifact(ctx, &ar.key).await?;
        if let Ok(segs) = serde_json::from_value::<Vec<TrajectorySegment>>(v.clone()) {
            for s in segs {
                hole_segments.entry(s.hole_id.clone()).or_default().push(s);
            }
        }
        if let Some(schema_id) = v.get("schema_id").and_then(|x| x.as_str()) {
            if schema_id == "geology.lithology_intervals.v1" {
                if crs.is_none() {
                    crs = v.get("crs").cloned();
                }
                if let Some(arr) = v.get("formation_order").and_then(|x| x.as_array()) {
                    formation_order = arr
                        .iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect();
                }
                if let Some(arr) = v.get("intervals").and_then(|x| x.as_array()) {
                    lithology_rows.extend(arr.iter().cloned());
                }
            }
        }
    }

    if hole_segments.is_empty() {
        return Err(NodeError::InvalidConfig(
            "formation_interface_extract requires trajectory input".into(),
        ));
    }
    if lithology_rows.is_empty() {
        return Err(NodeError::InvalidConfig(
            "formation_interface_extract requires lithology intervals input".into(),
        ));
    }

    for segs in hole_segments.values_mut() {
        segs.sort_by(|a, b| a.depth_from_m.partial_cmp(&b.depth_from_m).unwrap());
    }

    let mut grouped = std::collections::HashMap::<String, Vec<serde_json::Value>>::new();
    for row in lithology_rows {
        let Some(hole_id) = row.get("hole_id").and_then(|v| v.as_str()) else {
            continue;
        };
        grouped.entry(hole_id.to_string()).or_default().push(row);
    }

    let mut points = Vec::<serde_json::Value>::new();
    let mut skipped_no_trace = 0usize;
    for (hole_id, mut rows) in grouped {
        let Some(segs) = hole_segments.get(&hole_id) else {
            skipped_no_trace += 1;
            continue;
        };
        rows.sort_by(|a, b| {
            let af = a.get("from_m").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let bf = b.get("from_m").and_then(|v| v.as_f64()).unwrap_or(0.0);
            af.partial_cmp(&bf).unwrap()
        });
        for (idx, row) in rows.iter().enumerate() {
            let formation = row
                .get("formation")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .unwrap_or("");
            if formation.is_empty() || exclude.contains(&formation.to_ascii_lowercase()) {
                continue;
            }
            let Some(depth_m) = row.get("from_m").and_then(|v| v.as_f64()) else {
                continue;
            };
            if depth_m <= 0.0 {
                continue;
            }
            let Some((x, y, z)) = position_at_depth(segs, depth_m) else {
                continue;
            };
            let formation_above = if idx > 0 {
                rows[idx - 1]
                    .get("formation")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            } else {
                None
            };
            let attributes = serde_json::json!({
                "formation": formation,
                "formation_above": formation_above,
                "formation_below": formation,
                "depth_m": depth_m,
            });
            points.push(serde_json::json!({
                "id": format!("{}:{}:{:.3}", hole_id, formation, depth_m),
                "hole_id": hole_id,
                "x": x,
                "y": y,
                "z": z,
                "attributes": attributes,
                "qa_flags": row.get("qa_flags").cloned().unwrap_or_else(|| serde_json::json!([])),
            }));
        }
    }

    if points.is_empty() {
        return Err(NodeError::InvalidConfig(
            "formation_interface_extract produced no interface points".into(),
        ));
    }

    let interface_payload = serde_json::json!({
        "schema_id": "geology.interface_points.v1",
        "schema_version": 1,
        "crs": crs.unwrap_or_else(|| serde_json::json!({"epsg": 4326})),
        "points": points,
        "formation_order": formation_order,
        "display_contract": {
            "renderer": "sample_points",
            "display_pointer": "scene3d.sample_points"
        },
        "provenance": {
            "node_kind": "formation_interface_extract",
            "node_id": job.node_id.to_string(),
        }
    });
    let points_key = format!(
        "graphs/{}/nodes/{}/interface_points.json",
        job.graph_id, job.node_id
    );
    let points_ref = super::runtime::write_artifact(
        ctx,
        &points_key,
        &serde_json::to_vec(&interface_payload)?,
        Some("application/json"),
    )
    .await?;

    let report_payload = serde_json::json!({
        "schema_id": "report.geology_interface_extract.v1",
        "schema_version": 1,
        "point_count": interface_payload.get("points").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0),
        "holes_with_trajectory": hole_segments.len(),
        "holes_skipped_no_trajectory": skipped_no_trace,
        "formation_order": interface_payload.get("formation_order").cloned().unwrap_or(serde_json::json!([])),
    });
    let report_key = format!(
        "graphs/{}/nodes/{}/interface_report.json",
        job.graph_id, job.node_id
    );
    let report_ref = super::runtime::write_artifact(
        ctx,
        &report_key,
        &serde_json::to_vec(&report_payload)?,
        Some("application/json"),
    )
    .await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![points_ref.clone(), report_ref.clone()],
        content_hashes: vec![points_ref.content_hash, report_ref.content_hash],
        error_message: None,
    })
}

#[derive(Clone, Copy)]
struct SurfacePt {
    x: f64,
    y: f64,
    z: f64,
}

fn infer_extent(points: &[SurfacePt], pad_pct: f64) -> Option<(f64, f64, f64, f64)> {
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
    if !xmin.is_finite() || !xmax.is_finite() || !ymin.is_finite() || !ymax.is_finite() {
        return None;
    }
    let dx = (xmax - xmin).max(1.0);
    let dy = (ymax - ymin).max(1.0);
    Some((
        xmin - dx * pad_pct,
        xmax + dx * pad_pct,
        ymin - dy * pad_pct,
        ymax + dy * pad_pct,
    ))
}

fn grid_dims_from_extent(
    xmin: f64,
    xmax: f64,
    ymin: f64,
    ymax: f64,
    resolution: f64,
    max_cells: usize,
) -> (usize, usize) {
    let width = (xmax - xmin).max(resolution);
    let height = (ymax - ymin).max(resolution);
    let mut nx = ((width / resolution).ceil() as usize + 1).clamp(2, 1024);
    let mut ny = ((height / resolution).ceil() as usize + 1).clamp(2, 1024);
    while nx * ny > max_cells && nx > 2 && ny > 2 {
        if nx >= ny {
            nx = ((nx as f64) * 0.92).floor().max(2.0) as usize;
        } else {
            ny = ((ny as f64) * 0.92).floor().max(2.0) as usize;
        }
    }
    (nx, ny)
}

fn idw_surface_from_points(
    points: &[SurfacePt],
    nx: usize,
    ny: usize,
    xmin: f64,
    xmax: f64,
    ymin: f64,
    ymax: f64,
    power: f64,
) -> Vec<Option<f64>> {
    let mut values = vec![None; nx * ny];
    for iy in 0..ny {
        let y = ymin + (iy as f64 / (ny - 1) as f64) * (ymax - ymin);
        for ix in 0..nx {
            let x = xmin + (ix as f64 / (nx - 1) as f64) * (xmax - xmin);
            let mut num = 0.0;
            let mut den = 0.0;
            let mut exact = None;
            for p in points {
                let dx = x - p.x;
                let dy = y - p.y;
                let d2 = dx * dx + dy * dy;
                if d2 <= 1e-12 {
                    exact = Some(p.z);
                    break;
                }
                let w = 1.0 / d2.powf(power * 0.5);
                num += w * p.z;
                den += w;
            }
            values[iy * nx + ix] = exact.or_else(|| if den > 0.0 { Some(num / den) } else { None });
        }
    }
    values
}

pub async fn run_stratigraphic_surface_model(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let resolution = job
        .output_spec
        .pointer("/node_ui/resolution_m")
        .and_then(|v| v.as_f64())
        .unwrap_or(250.0)
        .max(1.0);
    let max_cells = job
        .output_spec
        .pointer("/node_ui/max_cells")
        .and_then(|v| v.as_u64())
        .unwrap_or(65536) as usize;
    let pad_pct = job
        .output_spec
        .pointer("/node_ui/pad_pct")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.05)
        .max(0.0);
    let min_points = job
        .output_spec
        .pointer("/node_ui/min_points_per_surface")
        .and_then(|v| v.as_u64())
        .unwrap_or(3) as usize;

    let mut crs = None::<serde_json::Value>;
    let mut by_formation = std::collections::BTreeMap::<String, Vec<SurfacePt>>::new();
    let mut formation_order = Vec::<String>::new();
    for ar in &job.input_artifact_refs {
        let root = super::runtime::read_json_artifact(ctx, &ar.key).await?;
        if root.get("schema_id").and_then(|v| v.as_str()) != Some("geology.interface_points.v1") {
            continue;
        }
        if crs.is_none() {
            crs = root.get("crs").cloned();
        }
        if let Some(arr) = root.get("formation_order").and_then(|v| v.as_array()) {
            formation_order = arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
        }
        let points = root
            .get("points")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        for row in points {
            let Some(obj) = row.as_object() else { continue };
            let formation = obj
                .get("attributes")
                .and_then(|v| v.get("formation"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            if formation.is_empty() {
                continue;
            }
            let (Some(x), Some(y), Some(z)) = (
                obj.get("x").and_then(|v| v.as_f64()),
                obj.get("y").and_then(|v| v.as_f64()),
                obj.get("z").and_then(|v| v.as_f64()),
            ) else {
                continue;
            };
            by_formation
                .entry(formation)
                .or_default()
                .push(SurfacePt { x, y, z });
        }
    }

    if by_formation.is_empty() {
        return Err(NodeError::InvalidConfig(
            "stratigraphic_surface_model requires interface points input".into(),
        ));
    }

    let mut artifact_refs = Vec::new();
    let mut content_hashes = Vec::new();
    let mut emitted = Vec::<serde_json::Value>::new();

    let ordered_formations: Vec<String> = if formation_order.is_empty() {
        by_formation.keys().cloned().collect()
    } else {
        formation_order
    };

    for formation in ordered_formations {
        let Some(points) = by_formation.get(&formation) else {
            continue;
        };
        if points.len() < min_points {
            continue;
        }
        let Some((xmin, xmax, ymin, ymax)) = infer_extent(points, pad_pct) else {
            continue;
        };
        let (nx, ny) = grid_dims_from_extent(xmin, xmax, ymin, ymax, resolution, max_cells);
        let values = idw_surface_from_points(points, nx, ny, xmin, xmax, ymin, ymax, 2.0);
        let finite: Vec<f64> = values.iter().copied().flatten().collect();
        let (zmin, zmax) = if finite.is_empty() {
            (0.0, 0.0)
        } else {
            (
                finite.iter().copied().fold(f64::INFINITY, f64::min),
                finite.iter().copied().fold(f64::NEG_INFINITY, f64::max),
            )
        };
        let payload = serde_json::json!({
            "schema_id": "geology.stratigraphic_surface.v1",
            "schema_version": 1,
            "crs": crs.clone().unwrap_or_else(|| serde_json::json!({"epsg": 4326})),
            "surface_id": format!("{}_top", formation),
            "formation": formation,
            "surface_role": "top_contact",
            "mesh": { "vertices": [], "faces": [] },
            "surface_grid": {
                "nx": nx,
                "ny": ny,
                "xmin": xmin,
                "xmax": xmax,
                "ymin": ymin,
                "ymax": ymax,
                "values": values
            },
            "stats": {
                "input_points": points.len(),
                "z_min": zmin,
                "z_max": zmax
            },
            "display_contract": {
                "renderer": "terrain",
                "display_pointer": "scene3d.surface",
                "editable": ["visible", "opacity"]
            },
            "provenance": {
                "node_kind": "stratigraphic_surface_model",
                "node_id": job.node_id.to_string(),
            }
        });
        let slug = formation
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_lowercase()
                } else {
                    '_'
                }
            })
            .collect::<String>();
        let key = format!(
            "graphs/{}/nodes/{}/surface_{}.json",
            job.graph_id, job.node_id, slug
        );
        let artifact = super::runtime::write_artifact(
            ctx,
            &key,
            &serde_json::to_vec(&payload)?,
            Some("application/json"),
        )
        .await?;
        emitted.push(serde_json::json!({
            "formation": formation,
            "artifact_key": artifact.key,
            "point_count": points.len()
        }));
        content_hashes.push(artifact.content_hash.clone());
        artifact_refs.push(artifact);
    }

    if artifact_refs.is_empty() {
        return Err(NodeError::InvalidConfig(
            "stratigraphic_surface_model could not build any surfaces; check formation coverage"
                .into(),
        ));
    }

    let report = serde_json::json!({
        "schema_id": "report.geology_stratigraphic_surface.v1",
        "schema_version": 1,
        "surfaces": emitted
    });
    let report_key = format!(
        "graphs/{}/nodes/{}/surface_report.json",
        job.graph_id, job.node_id
    );
    let report_ref = super::runtime::write_artifact(
        ctx,
        &report_key,
        &serde_json::to_vec(&report)?,
        Some("application/json"),
    )
    .await?;
    content_hashes.push(report_ref.content_hash.clone());
    artifact_refs.push(report_ref);

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: artifact_refs,
        content_hashes,
        error_message: None,
    })
}

fn normalize_key(raw: &str) -> String {
    raw.trim().to_ascii_lowercase()
}

fn slugify(raw: &str) -> String {
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
}

fn find_schema_input<'a>(
    inputs: &'a [serde_json::Value],
    schema_id: &str,
) -> Option<&'a serde_json::Value> {
    inputs
        .iter()
        .find(|v| v.get("schema_id").and_then(|x| x.as_str()) == Some(schema_id))
}

fn catalog_name_map(
    catalog: &serde_json::Value,
) -> std::collections::HashMap<String, serde_json::Value> {
    let mut out = std::collections::HashMap::new();
    let formations = catalog
        .get("formations")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    for row in formations {
        let Some(obj) = row.as_object() else { continue };
        let canonical_name = obj
            .get("canonical_name")
            .and_then(|v| v.as_str())
            .or_else(|| obj.get("name").and_then(|v| v.as_str()))
            .unwrap_or("")
            .trim();
        if canonical_name.is_empty() {
            continue;
        }
        out.insert(normalize_key(canonical_name), row.clone());
        if let Some(aliases) = obj.get("aliases").and_then(|v| v.as_array()) {
            for alias in aliases {
                if let Some(alias) = alias.as_str() {
                    let alias = alias.trim();
                    if !alias.is_empty() {
                        out.insert(normalize_key(alias), row.clone());
                    }
                }
            }
        }
    }
    out
}

fn expand_bounds(
    xmin: &mut f64,
    xmax: &mut f64,
    ymin: &mut f64,
    ymax: &mut f64,
    zmin: &mut f64,
    zmax: &mut f64,
    x: f64,
    y: f64,
    z: f64,
) {
    *xmin = xmin.min(x);
    *xmax = xmax.max(x);
    *ymin = ymin.min(y);
    *ymax = ymax.max(y);
    *zmin = zmin.min(z);
    *zmax = zmax.max(z);
}

#[derive(Clone)]
struct SurfaceGridSpec {
    formation: String,
    nx: usize,
    ny: usize,
    xmin: f64,
    xmax: f64,
    ymin: f64,
    ymax: f64,
    values: Vec<Option<f64>>,
}

fn sample_surface_grid(grid: &SurfaceGridSpec, x: f64, y: f64) -> Option<f64> {
    if x < grid.xmin || x > grid.xmax || y < grid.ymin || y > grid.ymax {
        return None;
    }
    if grid.nx < 2 || grid.ny < 2 || grid.values.len() != grid.nx * grid.ny {
        return None;
    }
    let tx = (x - grid.xmin) / (grid.xmax - grid.xmin).max(1e-12);
    let ty = (y - grid.ymin) / (grid.ymax - grid.ymin).max(1e-12);
    let gx = tx * (grid.nx - 1) as f64;
    let gy = ty * (grid.ny - 1) as f64;
    let ix0 = gx.floor().clamp(0.0, (grid.nx - 1) as f64) as usize;
    let iy0 = gy.floor().clamp(0.0, (grid.ny - 1) as f64) as usize;
    let ix1 = (ix0 + 1).min(grid.nx - 1);
    let iy1 = (iy0 + 1).min(grid.ny - 1);
    let fx = (gx - ix0 as f64).clamp(0.0, 1.0);
    let fy = (gy - iy0 as f64).clamp(0.0, 1.0);
    let z00 = grid.values[iy0 * grid.nx + ix0]?;
    let z10 = grid.values[iy0 * grid.nx + ix1]?;
    let z01 = grid.values[iy1 * grid.nx + ix0]?;
    let z11 = grid.values[iy1 * grid.nx + ix1]?;
    let z0 = z00 * (1.0 - fx) + z10 * fx;
    let z1 = z01 * (1.0 - fx) + z11 * fx;
    Some(z0 * (1.0 - fy) + z1 * fy)
}

pub async fn run_formation_catalog_build(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let mut inputs = Vec::new();
    for ar in &job.input_artifact_refs {
        inputs.push(super::runtime::read_json_artifact(ctx, &ar.key).await?);
    }
    let lith = find_schema_input(&inputs, "geology.lithology_intervals.v1").ok_or_else(|| {
        NodeError::InvalidConfig(
            "formation_catalog_build requires lithology intervals input".into(),
        )
    })?;
    let crs = lith
        .get("crs")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({"epsg": 4326}));
    let intervals = lith
        .get("intervals")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let basement_names = job
        .output_spec
        .pointer("/node_ui/basement_names")
        .and_then(|v| v.as_array())
        .map(|vals| {
            vals.iter()
                .filter_map(|v| v.as_str())
                .map(normalize_key)
                .collect::<std::collections::HashSet<_>>()
        })
        .unwrap_or_default();

    let mut seen = std::collections::HashSet::<String>::new();
    let mut formations = Vec::<serde_json::Value>::new();
    let mut warnings = Vec::<String>::new();
    for row in intervals {
        let Some(obj) = row.as_object() else { continue };
        let name = obj
            .get("formation")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .unwrap_or("");
        if name.is_empty() {
            warnings.push("interval row missing formation".into());
            continue;
        }
        let key = normalize_key(name);
        if !seen.insert(key.clone()) {
            continue;
        }
        let group = obj.get("group").cloned().unwrap_or(serde_json::Value::Null);
        let lithology_code = obj
            .get("lithology_code")
            .and_then(|v| v.as_str())
            .map(|s| vec![s.to_string()])
            .unwrap_or_default();
        formations.push(serde_json::json!({
            "formation_id": slugify(name),
            "name": name,
            "canonical_name": name,
            "group": group,
            "aliases": [name],
            "lithology_codes": lithology_code,
            "is_basement": basement_names.contains(&key),
            "attributes": {}
        }));
    }
    if formations.is_empty() {
        return Err(NodeError::InvalidConfig(
            "formation_catalog_build produced no formations".into(),
        ));
    }
    let out = serde_json::json!({
        "schema_id": "geology.formation_catalog.v1",
        "schema_version": 1,
        "crs": crs,
        "formations": formations,
        "normalization": {
            "case_sensitive": false,
            "trim_whitespace": true
        },
        "diagnostics": { "warnings": warnings },
        "provenance": {
            "node_kind": "formation_catalog_build",
            "node_id": job.node_id.to_string()
        }
    });
    let key = format!(
        "graphs/{}/nodes/{}/formation_catalog.json",
        job.graph_id, job.node_id
    );
    let artifact = super::runtime::write_artifact(
        ctx,
        &key,
        &serde_json::to_vec(&out)?,
        Some("application/json"),
    )
    .await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

pub async fn run_stratigraphic_order_define(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let mut inputs = Vec::new();
    for ar in &job.input_artifact_refs {
        inputs.push(super::runtime::read_json_artifact(ctx, &ar.key).await?);
    }
    let catalog = find_schema_input(&inputs, "geology.formation_catalog.v1").ok_or_else(|| {
        NodeError::InvalidConfig(
            "stratigraphic_order_define requires formation catalog input".into(),
        )
    })?;
    let lith = find_schema_input(&inputs, "geology.lithology_intervals.v1");
    let explicit_order = job
        .output_spec
        .pointer("/node_ui/global_order_top_to_bottom")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.trim().to_string()))
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let mut order = if !explicit_order.is_empty() {
        explicit_order
    } else if let Some(arr) = lith
        .and_then(|v| v.get("formation_order"))
        .and_then(|v| v.as_array())
    {
        arr.iter()
            .filter_map(|v| v.as_str().map(|s| s.trim().to_string()))
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
    } else {
        catalog
            .get("formations")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| {
                row.get("canonical_name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect::<Vec<_>>()
    };

    let known = catalog_name_map(catalog);
    order.retain(|name| known.contains_key(&normalize_key(name)));
    let mut dedup = std::collections::HashSet::<String>::new();
    order.retain(|name| dedup.insert(normalize_key(name)));

    if order.is_empty() {
        return Err(NodeError::InvalidConfig(
            "stratigraphic_order_define could not determine formation order".into(),
        ));
    }

    let out = serde_json::json!({
        "schema_id": "geology.stratigraphic_order.v1",
        "schema_version": 1,
        "groups": [{
            "group_id": "default",
            "group_name": "Default",
            "relation_type": "conformable",
            "formations_top_to_bottom": order
        }],
        "global_order_top_to_bottom": order,
        "diagnostics": { "warnings": [] },
        "provenance": {
            "node_kind": "stratigraphic_order_define",
            "node_id": job.node_id.to_string()
        }
    });
    let key = format!(
        "graphs/{}/nodes/{}/stratigraphic_order.json",
        job.graph_id, job.node_id
    );
    let artifact = super::runtime::write_artifact(
        ctx,
        &key,
        &serde_json::to_vec(&out)?,
        Some("application/json"),
    )
    .await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

pub async fn run_model_domain_define(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let mut inputs = Vec::new();
    for ar in &job.input_artifact_refs {
        inputs.push((
            ar.key.clone(),
            super::runtime::read_json_artifact(ctx, &ar.key).await?,
        ));
    }

    let mut crs = None::<serde_json::Value>;
    let mut xmin = f64::INFINITY;
    let mut xmax = f64::NEG_INFINITY;
    let mut ymin = f64::INFINITY;
    let mut ymax = f64::NEG_INFINITY;
    let mut zmin = f64::INFINITY;
    let mut zmax = f64::NEG_INFINITY;
    let mut topo_key = None::<String>;

    for (key, root) in &inputs {
        if crs.is_none() {
            crs = root.get("crs").cloned();
        }
        if root.get("schema_id").and_then(|v| v.as_str()) == Some("spatial.aoi.v1") {
            if let Some(bounds) = root.get("bounds").and_then(|v| v.as_object()) {
                if let (Some(axmin), Some(axmax), Some(aymin), Some(aymax)) = (
                    bounds.get("xmin").and_then(|v| v.as_f64()),
                    bounds.get("xmax").and_then(|v| v.as_f64()),
                    bounds.get("ymin").and_then(|v| v.as_f64()),
                    bounds.get("ymax").and_then(|v| v.as_f64()),
                ) {
                    xmin = xmin.min(axmin);
                    xmax = xmax.max(axmax);
                    ymin = ymin.min(aymin);
                    ymax = ymax.max(aymax);
                }
            }
        }
        if root.get("schema_id").and_then(|v| v.as_str()) == Some("terrain.surface_grid.v1")
            || root.get("surface_grid").is_some()
        {
            if let Some(sg) = root.get("surface_grid").and_then(|v| v.as_object()) {
                if let (Some(gxmin), Some(gxmax), Some(gymin), Some(gymax), Some(vals)) = (
                    sg.get("xmin").and_then(|v| v.as_f64()),
                    sg.get("xmax").and_then(|v| v.as_f64()),
                    sg.get("ymin").and_then(|v| v.as_f64()),
                    sg.get("ymax").and_then(|v| v.as_f64()),
                    sg.get("values").and_then(|v| v.as_array()),
                ) {
                    xmin = xmin.min(gxmin);
                    xmax = xmax.max(gxmax);
                    ymin = ymin.min(gymin);
                    ymax = ymax.max(gymax);
                    for z in vals.iter().filter_map(|v| v.as_f64()) {
                        zmin = zmin.min(z);
                        zmax = zmax.max(z);
                    }
                    topo_key = Some(key.clone());
                }
            }
        }
        if root.get("schema_id").and_then(|v| v.as_str()) == Some("geology.interface_points.v1") {
            if let Some(points) = root.get("points").and_then(|v| v.as_array()) {
                for row in points {
                    let Some(obj) = row.as_object() else { continue };
                    if let (Some(x), Some(y), Some(z)) = (
                        obj.get("x").and_then(|v| v.as_f64()),
                        obj.get("y").and_then(|v| v.as_f64()),
                        obj.get("z").and_then(|v| v.as_f64()),
                    ) {
                        expand_bounds(
                            &mut xmin, &mut xmax, &mut ymin, &mut ymax, &mut zmin, &mut zmax, x, y,
                            z,
                        );
                    }
                }
            }
        }
        if root.get("schema_id").and_then(|v| v.as_str())
            == Some("geology.formation_orientations.v1")
        {
            if let Some(rows) = root.get("orientations").and_then(|v| v.as_array()) {
                for row in rows {
                    let Some(obj) = row.as_object() else { continue };
                    if let (Some(x), Some(y), Some(z)) = (
                        obj.get("x").and_then(|v| v.as_f64()),
                        obj.get("y").and_then(|v| v.as_f64()),
                        obj.get("z").and_then(|v| v.as_f64()),
                    ) {
                        expand_bounds(
                            &mut xmin, &mut xmax, &mut ymin, &mut ymax, &mut zmin, &mut zmax, x, y,
                            z,
                        );
                    }
                }
            }
        }
    }

    if !xmin.is_finite() || !xmax.is_finite() || !ymin.is_finite() || !ymax.is_finite() {
        return Err(NodeError::InvalidConfig(
            "model_domain_define requires AOI, terrain, interface points, or orientations".into(),
        ));
    }
    if !zmin.is_finite() || !zmax.is_finite() {
        zmin = -1000.0;
        zmax = 100.0;
    }

    let pad_xy = job
        .output_spec
        .pointer("/node_ui/padding_xy_percent")
        .and_then(|v| v.as_f64())
        .unwrap_or(5.0)
        .max(0.0);
    let pad_z = job
        .output_spec
        .pointer("/node_ui/padding_z_percent")
        .and_then(|v| v.as_f64())
        .unwrap_or(10.0)
        .max(0.0);
    let dx = (xmax - xmin).max(1.0);
    let dy = (ymax - ymin).max(1.0);
    let dz = (zmax - zmin).max(1.0);
    xmin -= dx * pad_xy / 100.0;
    xmax += dx * pad_xy / 100.0;
    ymin -= dy * pad_xy / 100.0;
    ymax += dy * pad_xy / 100.0;
    zmin -= dz * pad_z / 100.0;
    zmax += dz * pad_z / 100.0;

    let nx = job
        .output_spec
        .pointer("/node_ui/nx")
        .and_then(|v| v.as_u64())
        .unwrap_or(80) as usize;
    let ny = job
        .output_spec
        .pointer("/node_ui/ny")
        .and_then(|v| v.as_u64())
        .unwrap_or(80) as usize;
    let nz = job
        .output_spec
        .pointer("/node_ui/nz")
        .and_then(|v| v.as_u64())
        .unwrap_or(60) as usize;

    let out = serde_json::json!({
        "schema_id": "geology.model_domain.v1",
        "schema_version": 1,
        "crs": crs.unwrap_or_else(|| serde_json::json!({"epsg": 4326})),
        "bounds": {
            "xmin": xmin, "xmax": xmax,
            "ymin": ymin, "ymax": ymax,
            "zmin": zmin, "zmax": zmax
        },
        "grid_strategy": {
            "mode": "regular",
            "nx": nx.max(2),
            "ny": ny.max(2),
            "nz": nz.max(2)
        },
        "topography": {
            "source_artifact_key": topo_key,
            "clip_mode": "mask_above_topography"
        },
        "padding": {
            "xy_percent": pad_xy,
            "z_percent": pad_z
        },
        "diagnostics": {},
        "provenance": {
            "node_kind": "model_domain_define",
            "node_id": job.node_id.to_string()
        }
    });
    let key = format!(
        "graphs/{}/nodes/{}/model_domain.json",
        job.graph_id, job.node_id
    );
    let artifact = super::runtime::write_artifact(
        ctx,
        &key,
        &serde_json::to_vec(&out)?,
        Some("application/json"),
    )
    .await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

pub async fn run_constraint_merge(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let mut inputs = Vec::new();
    for ar in &job.input_artifact_refs {
        inputs.push(super::runtime::read_json_artifact(ctx, &ar.key).await?);
    }
    let catalog = find_schema_input(&inputs, "geology.formation_catalog.v1").ok_or_else(|| {
        NodeError::InvalidConfig("constraint_merge requires formation catalog input".into())
    })?;
    let catalog_map = catalog_name_map(catalog);
    let interface_points =
        find_schema_input(&inputs, "geology.interface_points.v1").ok_or_else(|| {
            NodeError::InvalidConfig("constraint_merge requires interface points input".into())
        })?;
    let orientations = find_schema_input(&inputs, "geology.formation_orientations.v1");
    let crs = interface_points
        .get("crs")
        .cloned()
        .or_else(|| orientations.and_then(|o| o.get("crs").cloned()))
        .unwrap_or_else(|| serde_json::json!({"epsg": 4326}));

    let mut contacts = Vec::<serde_json::Value>::new();
    let mut orientation_rows = Vec::<serde_json::Value>::new();
    let mut formation_counts = std::collections::BTreeMap::<String, usize>::new();
    let mut warnings = Vec::<String>::new();

    for (idx, row) in interface_points
        .get("points")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .enumerate()
    {
        let Some(obj) = row.as_object() else { continue };
        let attrs = obj
            .get("attributes")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        let raw_formation = attrs
            .get("formation")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim();
        if raw_formation.is_empty() {
            continue;
        }
        let Some(cat_row) = catalog_map.get(&normalize_key(raw_formation)) else {
            warnings.push(format!("unknown formation in contacts: {}", raw_formation));
            continue;
        };
        let formation = cat_row
            .get("canonical_name")
            .and_then(|v| v.as_str())
            .unwrap_or(raw_formation);
        *formation_counts.entry(formation.to_string()).or_insert(0) += 1;
        contacts.push(serde_json::json!({
            "constraint_id": format!("contact_{:04}", idx + 1),
            "formation": formation,
            "contact_kind": attrs.get("contact_kind").and_then(|v| v.as_str()).unwrap_or("top"),
            "formation_above": attrs.get("formation_above").cloned().unwrap_or(serde_json::Value::Null),
            "formation_below": attrs.get("formation_below").cloned().unwrap_or_else(|| serde_json::json!(formation)),
            "x": obj.get("x").and_then(|v| v.as_f64()).unwrap_or(0.0),
            "y": obj.get("y").and_then(|v| v.as_f64()).unwrap_or(0.0),
            "z": obj.get("z").and_then(|v| v.as_f64()).unwrap_or(0.0),
            "source_kind": "drillhole_interval",
            "confidence": 1.0,
            "qa_flags": obj.get("qa_flags").cloned().unwrap_or_else(|| serde_json::json!([]))
        }));
    }

    if let Some(orientations) = orientations {
        for (idx, row) in orientations
            .get("orientations")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .enumerate()
        {
            let Some(obj) = row.as_object() else { continue };
            let raw_formation = obj
                .get("formation")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim();
            if raw_formation.is_empty() {
                continue;
            }
            let Some(cat_row) = catalog_map.get(&normalize_key(raw_formation)) else {
                warnings.push(format!(
                    "unknown formation in orientations: {}",
                    raw_formation
                ));
                continue;
            };
            let formation = cat_row
                .get("canonical_name")
                .and_then(|v| v.as_str())
                .unwrap_or(raw_formation);
            orientation_rows.push(serde_json::json!({
                "constraint_id": obj.get("orientation_id").and_then(|v| v.as_str()).map(|s| s.to_string()).unwrap_or_else(|| format!("ori_{:04}", idx + 1)),
                "formation": formation,
                "x": obj.get("x").and_then(|v| v.as_f64()).unwrap_or(0.0),
                "y": obj.get("y").and_then(|v| v.as_f64()).unwrap_or(0.0),
                "z": obj.get("z").and_then(|v| v.as_f64()).unwrap_or(0.0),
                "pole_vector": obj.get("pole_vector").cloned().unwrap_or_else(|| serde_json::json!([0.0, 0.0, 1.0])),
                "source_kind": obj.get("source_kind").and_then(|v| v.as_str()).unwrap_or("observed"),
                "confidence": obj.get("confidence").and_then(|v| v.as_f64()).unwrap_or(1.0),
                "qa_flags": obj.get("qa_flags").cloned().unwrap_or_else(|| serde_json::json!([]))
            }));
        }
    }

    for formation in formation_counts.keys() {
        let has_orientation = orientation_rows
            .iter()
            .any(|row| row.get("formation").and_then(|v| v.as_str()) == Some(formation.as_str()));
        if !has_orientation {
            warnings.push(format!(
                "formation has contacts but no orientations: {}",
                formation
            ));
        }
    }

    let out = serde_json::json!({
        "schema_id": "geology.interpolation_constraints.v1",
        "schema_version": 1,
        "crs": crs,
        "contacts": contacts,
        "orientations": orientation_rows,
        "diagnostics": {
            "formation_counts": formation_counts,
            "warnings": warnings
        },
        "provenance": {
            "node_kind": "constraint_merge",
            "node_id": job.node_id.to_string()
        }
    });
    let key = format!(
        "graphs/{}/nodes/{}/interpolation_constraints.json",
        job.graph_id, job.node_id
    );
    let artifact = super::runtime::write_artifact(
        ctx,
        &key,
        &serde_json::to_vec(&out)?,
        Some("application/json"),
    )
    .await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

pub async fn run_structural_frame_builder(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let mut inputs = Vec::new();
    for ar in &job.input_artifact_refs {
        inputs.push((
            ar.key.clone(),
            super::runtime::read_json_artifact(ctx, &ar.key).await?,
        ));
    }
    let input_values: Vec<serde_json::Value> = inputs.iter().map(|(_, v)| v.clone()).collect();
    let catalog =
        find_schema_input(&input_values, "geology.formation_catalog.v1").ok_or_else(|| {
            NodeError::InvalidConfig(
                "structural_frame_builder requires formation catalog input".into(),
            )
        })?;
    let order =
        find_schema_input(&input_values, "geology.stratigraphic_order.v1").ok_or_else(|| {
            NodeError::InvalidConfig(
                "structural_frame_builder requires stratigraphic order input".into(),
            )
        })?;
    let constraints = find_schema_input(&input_values, "geology.interpolation_constraints.v1")
        .ok_or_else(|| {
            NodeError::InvalidConfig(
                "structural_frame_builder requires interpolation constraints input".into(),
            )
        })?;
    let domain = find_schema_input(&input_values, "geology.model_domain.v1").ok_or_else(|| {
        NodeError::InvalidConfig("structural_frame_builder requires model domain input".into())
    })?;

    let order_groups = order
        .get("groups")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let catalog_map = catalog_name_map(catalog);
    let mut formations = Vec::<serde_json::Value>::new();
    let mut warnings = Vec::<String>::new();
    for group in &order_groups {
        let Some(group_obj) = group.as_object() else {
            continue;
        };
        let group_id = group_obj
            .get("group_id")
            .and_then(|v| v.as_str())
            .unwrap_or("default");
        let ordered = group_obj
            .get("formations_top_to_bottom")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        for (idx, formation_name) in ordered.iter().enumerate() {
            let Some(formation_name) = formation_name.as_str() else {
                continue;
            };
            let Some(cat_row) = catalog_map.get(&normalize_key(formation_name)) else {
                warnings.push(format!(
                    "formation in order missing from catalog: {}",
                    formation_name
                ));
                continue;
            };
            formations.push(serde_json::json!({
                "formation_id": cat_row.get("formation_id").cloned().unwrap_or_else(|| serde_json::json!(slugify(formation_name))),
                "name": cat_row.get("canonical_name").cloned().unwrap_or_else(|| serde_json::json!(formation_name)),
                "group_id": group_id,
                "order_index": idx,
                "is_active": true
            }));
        }
    }
    if formations.is_empty() {
        return Err(NodeError::InvalidConfig(
            "structural_frame_builder produced no active formations".into(),
        ));
    }

    let out = serde_json::json!({
        "schema_id": "geology.structural_frame.v1",
        "schema_version": 1,
        "formations": formations,
        "groups": order_groups,
        "constraints_ref": {
            "schema_id": "geology.interpolation_constraints.v1",
            "contact_count": constraints.get("contacts").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0),
            "orientation_count": constraints.get("orientations").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0)
        },
        "domain_ref": {
            "schema_id": "geology.model_domain.v1",
            "bounds": domain.get("bounds").cloned().unwrap_or_else(|| serde_json::json!({}))
        },
        "diagnostics": {
            "warnings": warnings,
            "errors": []
        },
        "provenance": {
            "node_kind": "structural_frame_builder",
            "node_id": job.node_id.to_string()
        }
    });
    let key = format!(
        "graphs/{}/nodes/{}/structural_frame.json",
        job.graph_id, job.node_id
    );
    let artifact = super::runtime::write_artifact(
        ctx,
        &key,
        &serde_json::to_vec(&out)?,
        Some("application/json"),
    )
    .await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

pub async fn run_stratigraphic_interpolator(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let mut inputs = Vec::new();
    for ar in &job.input_artifact_refs {
        inputs.push(super::runtime::read_json_artifact(ctx, &ar.key).await?);
    }
    let frame = find_schema_input(&inputs, "geology.structural_frame.v1").ok_or_else(|| {
        NodeError::InvalidConfig(
            "stratigraphic_interpolator requires structural frame input".into(),
        )
    })?;
    let constraints = find_schema_input(&inputs, "geology.interpolation_constraints.v1")
        .ok_or_else(|| {
            NodeError::InvalidConfig(
                "stratigraphic_interpolator requires interpolation constraints input".into(),
            )
        })?;
    let domain = find_schema_input(&inputs, "geology.model_domain.v1").ok_or_else(|| {
        NodeError::InvalidConfig("stratigraphic_interpolator requires model domain input".into())
    })?;
    let crs = domain
        .get("crs")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({"epsg": 4326}));
    let bounds = domain
        .get("bounds")
        .and_then(|v| v.as_object())
        .ok_or_else(|| NodeError::InvalidConfig("model domain bounds missing".into()))?;
    let (xmin, xmax, ymin, ymax) = (
        bounds.get("xmin").and_then(|v| v.as_f64()).unwrap_or(0.0),
        bounds.get("xmax").and_then(|v| v.as_f64()).unwrap_or(1.0),
        bounds.get("ymin").and_then(|v| v.as_f64()).unwrap_or(0.0),
        bounds.get("ymax").and_then(|v| v.as_f64()).unwrap_or(1.0),
    );
    let grid = domain
        .get("grid_strategy")
        .and_then(|v| v.as_object())
        .ok_or_else(|| NodeError::InvalidConfig("model domain grid_strategy missing".into()))?;
    let nx = grid.get("nx").and_then(|v| v.as_u64()).unwrap_or(80) as usize;
    let ny = grid.get("ny").and_then(|v| v.as_u64()).unwrap_or(80) as usize;

    let formations_top_to_bottom = frame
        .get("groups")
        .and_then(|v| v.as_array())
        .and_then(|groups| groups.first())
        .and_then(|g| g.get("formations_top_to_bottom"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect::<Vec<_>>();
    if formations_top_to_bottom.is_empty() {
        return Err(NodeError::InvalidConfig(
            "stratigraphic_interpolator requires ordered formations in structural frame".into(),
        ));
    }

    let mut by_formation = std::collections::BTreeMap::<String, Vec<SurfacePt>>::new();
    for row in constraints
        .get("contacts")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
    {
        let Some(obj) = row.as_object() else { continue };
        let formation = obj
            .get("formation")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if formation.is_empty() {
            continue;
        }
        let (Some(x), Some(y), Some(z)) = (
            obj.get("x").and_then(|v| v.as_f64()),
            obj.get("y").and_then(|v| v.as_f64()),
            obj.get("z").and_then(|v| v.as_f64()),
        ) else {
            continue;
        };
        by_formation
            .entry(formation)
            .or_default()
            .push(SurfacePt { x, y, z });
    }

    let mut grids = Vec::<SurfaceGridSpec>::new();
    let mut payload_surfaces = Vec::<serde_json::Value>::new();
    for formation in &formations_top_to_bottom {
        let Some(points) = by_formation.get(formation) else {
            continue;
        };
        if points.len() < 2 {
            continue;
        }
        let values =
            idw_surface_from_points(points, nx.max(2), ny.max(2), xmin, xmax, ymin, ymax, 2.0);
        let finite: Vec<f64> = values.iter().flatten().copied().collect();
        let zmin = finite.iter().copied().fold(f64::INFINITY, f64::min);
        let zmax = finite.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let grid_spec = SurfaceGridSpec {
            formation: formation.clone(),
            nx: nx.max(2),
            ny: ny.max(2),
            xmin,
            xmax,
            ymin,
            ymax,
            values: values.clone(),
        };
        grids.push(grid_spec);
        payload_surfaces.push(serde_json::json!({
            "formation": formation,
            "surface_role": "top_contact",
            "surface_grid": {
                "nx": nx.max(2),
                "ny": ny.max(2),
                "xmin": xmin,
                "xmax": xmax,
                "ymin": ymin,
                "ymax": ymax,
                "values": values
            },
            "stats": {
                "input_points": points.len(),
                "z_min": if zmin.is_finite() { zmin } else { 0.0 },
                "z_max": if zmax.is_finite() { zmax } else { 0.0 }
            }
        }));
    }
    if payload_surfaces.is_empty() {
        return Err(NodeError::InvalidConfig(
            "stratigraphic_interpolator could not build any formation surfaces".into(),
        ));
    }
    let scalar = serde_json::json!({
        "schema_id": "geology.scalar_field.v1",
        "schema_version": 1,
        "crs": crs,
        "domain": domain,
        "formations_top_to_bottom": formations_top_to_bottom,
        "surface_grids": payload_surfaces,
        "diagnostics": {
            "surface_count": grids.len(),
            "orientation_count": constraints.get("orientations").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0)
        },
        "provenance": {
            "node_kind": "stratigraphic_interpolator",
            "node_id": job.node_id.to_string()
        }
    });
    let scalar_key = format!(
        "graphs/{}/nodes/{}/scalar_field.json",
        job.graph_id, job.node_id
    );
    let scalar_ref = super::runtime::write_artifact(
        ctx,
        &scalar_key,
        &serde_json::to_vec(&scalar)?,
        Some("application/json"),
    )
    .await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![scalar_ref.clone()],
        content_hashes: vec![scalar_ref.content_hash],
        error_message: None,
    })
}

pub async fn run_lith_block_model_build(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let mut inputs = Vec::new();
    for ar in &job.input_artifact_refs {
        inputs.push(super::runtime::read_json_artifact(ctx, &ar.key).await?);
    }
    let scalar = find_schema_input(&inputs, "geology.scalar_field.v1").ok_or_else(|| {
        NodeError::InvalidConfig("lith_block_model_build requires scalar field input".into())
    })?;
    let domain = scalar
        .get("domain")
        .cloned()
        .ok_or_else(|| NodeError::InvalidConfig("scalar field missing domain".into()))?;
    let bounds = domain
        .get("bounds")
        .and_then(|v| v.as_object())
        .ok_or_else(|| NodeError::InvalidConfig("scalar field domain bounds missing".into()))?;
    let grid = domain
        .get("grid_strategy")
        .and_then(|v| v.as_object())
        .ok_or_else(|| NodeError::InvalidConfig("scalar field grid strategy missing".into()))?;
    let crs = scalar
        .get("crs")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({"epsg": 4326}));
    let formations_top_to_bottom = scalar
        .get("formations_top_to_bottom")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect::<Vec<_>>();
    let surface_grids = scalar
        .get("surface_grids")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let surfaces = surface_grids
        .iter()
        .filter_map(|row| {
            let formation = row.get("formation").and_then(|v| v.as_str())?.to_string();
            let sg = row.get("surface_grid")?.as_object()?;
            let nx = sg.get("nx")?.as_u64()? as usize;
            let ny = sg.get("ny")?.as_u64()? as usize;
            let xmin = sg.get("xmin")?.as_f64()?;
            let xmax = sg.get("xmax")?.as_f64()?;
            let ymin = sg.get("ymin")?.as_f64()?;
            let ymax = sg.get("ymax")?.as_f64()?;
            let values = sg
                .get("values")?
                .as_array()?
                .iter()
                .map(|v| v.as_f64())
                .collect::<Vec<_>>();
            Some(SurfaceGridSpec {
                formation,
                nx,
                ny,
                xmin,
                xmax,
                ymin,
                ymax,
                values,
            })
        })
        .collect::<Vec<_>>();

    let (xmin, xmax, ymin, ymax, zmin, zmax) = (
        bounds.get("xmin").and_then(|v| v.as_f64()).unwrap_or(0.0),
        bounds.get("xmax").and_then(|v| v.as_f64()).unwrap_or(1.0),
        bounds.get("ymin").and_then(|v| v.as_f64()).unwrap_or(0.0),
        bounds.get("ymax").and_then(|v| v.as_f64()).unwrap_or(1.0),
        bounds.get("zmin").and_then(|v| v.as_f64()).unwrap_or(-1.0),
        bounds.get("zmax").and_then(|v| v.as_f64()).unwrap_or(1.0),
    );
    let (nx, ny, nz) = (
        grid.get("nx").and_then(|v| v.as_u64()).unwrap_or(20) as usize,
        grid.get("ny").and_then(|v| v.as_u64()).unwrap_or(20) as usize,
        grid.get("nz").and_then(|v| v.as_u64()).unwrap_or(12) as usize,
    );
    let dx = (xmax - xmin) / nx.max(1) as f64;
    let dy = (ymax - ymin) / ny.max(1) as f64;
    let dz = (zmax - zmin) / nz.max(1) as f64;

    let max_display_blocks = job
        .output_spec
        .pointer("/node_ui/max_blocks")
        .and_then(|v| v.as_u64())
        .unwrap_or(20_000) as usize;
    let shell_only = job
        .output_spec
        .pointer("/node_ui/display_shell_only")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let aggregate_vertical = job
        .output_spec
        .pointer("/node_ui/display_aggregate_vertical")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    let nx = nx.max(1);
    let ny = ny.max(1);
    let nz = nz.max(1);
    let total_cells = nx * ny * nz;
    let mut formation_idx_by_cell = vec![0usize; total_cells];

    let cell_index = |ix: usize, iy: usize, iz: usize| -> usize { iz * nx * ny + iy * nx + ix };

    for iz in 0..nz {
        let cz = zmin + (iz as f64 + 0.5) * dz;
        for iy in 0..ny {
            let cy = ymin + (iy as f64 + 0.5) * dy;
            for ix in 0..nx {
                let cx = xmin + (ix as f64 + 0.5) * dx;
                let mut assigned_index = formations_top_to_bottom.len().saturating_sub(1);
                for (fi, formation) in formations_top_to_bottom.iter().enumerate() {
                    if let Some(surface) = surfaces.iter().find(|s| s.formation == *formation) {
                        if let Some(sz) = sample_surface_grid(surface, cx, cy) {
                            if cz >= sz {
                                assigned_index = fi;
                                break;
                            }
                        }
                    }
                }
                formation_idx_by_cell[cell_index(ix, iy, iz)] = assigned_index;
            }
        }
    }

    let is_shell_cell = |ix: usize, iy: usize, iz: usize| -> bool {
        if ix == 0 || iy == 0 || iz == 0 || ix + 1 == nx || iy + 1 == ny || iz + 1 == nz {
            return true;
        }
        let idx = cell_index(ix, iy, iz);
        let fi = formation_idx_by_cell[idx];
        let neighbors = [
            cell_index(ix - 1, iy, iz),
            cell_index(ix + 1, iy, iz),
            cell_index(ix, iy - 1, iz),
            cell_index(ix, iy + 1, iz),
            cell_index(ix, iy, iz - 1),
            cell_index(ix, iy, iz + 1),
        ];
        neighbors
            .iter()
            .any(|neighbor_idx| formation_idx_by_cell[*neighbor_idx] != fi)
    };

    let mut shell_mask = vec![false; total_cells];
    let mut shell_cell_count = 0usize;
    for iz in 0..nz {
        for iy in 0..ny {
            for ix in 0..nx {
                let idx = cell_index(ix, iy, iz);
                let keep = !shell_only || is_shell_cell(ix, iy, iz);
                shell_mask[idx] = keep;
                if keep {
                    shell_cell_count += 1;
                }
            }
        }
    }

    let mut display_blocks = Vec::<serde_json::Value>::new();
    let mut centers = Vec::<serde_json::Value>::new();
    for iy in 0..ny {
        for ix in 0..nx {
            let mut iz = 0usize;
            while iz < nz {
                let idx = cell_index(ix, iy, iz);
                if !shell_mask[idx] {
                    iz += 1;
                    continue;
                }
                let formation_index = formation_idx_by_cell[idx];
                let mut iz_end = iz + 1;
                if aggregate_vertical {
                    while iz_end < nz {
                        let next_idx = cell_index(ix, iy, iz_end);
                        if !shell_mask[next_idx]
                            || formation_idx_by_cell[next_idx] != formation_index
                        {
                            break;
                        }
                        iz_end += 1;
                    }
                }
                let formation = formations_top_to_bottom
                    .get(formation_index)
                    .cloned()
                    .unwrap_or_else(|| "unknown".into());
                let x = xmin + (ix as f64 + 0.5) * dx;
                let y = ymin + (iy as f64 + 0.5) * dy;
                let z = zmin + ((iz + iz_end) as f64 * 0.5) * dz;
                let merged_dz = dz * (iz_end - iz) as f64;
                let attrs = serde_json::json!({
                    "formation": formation,
                    "formation_index": formation_index as f64,
                    "display_role": if shell_only { "shell" } else { "full" },
                    "merged_cells": (iz_end - iz) as f64
                });
                display_blocks.push(serde_json::json!({
                    "x": x,
                    "y": y,
                    "z": z,
                    "dx": dx,
                    "dy": dy,
                    "dz": merged_dz,
                    "attributes": attrs
                }));
                centers.push(serde_json::json!({
                    "x": x,
                    "y": y,
                    "z": z,
                    "attributes": {
                        "formation": formation,
                        "formation_index": formation_index as f64,
                        "display_role": if shell_only { "shell" } else { "full" },
                        "merged_cells": (iz_end - iz) as f64
                    }
                }));
                iz = iz_end;
            }
        }
    }

    if display_blocks.len() > max_display_blocks {
        let stride = ((display_blocks.len() as f64) / (max_display_blocks as f64)).ceil() as usize;
        display_blocks = display_blocks
            .into_iter()
            .enumerate()
            .filter_map(|(idx, block)| (idx % stride == 0).then_some(block))
            .take(max_display_blocks)
            .collect();
        centers = centers
            .into_iter()
            .enumerate()
            .filter_map(|(idx, center)| (idx % stride == 0).then_some(center))
            .take(max_display_blocks)
            .collect();
    }

    if display_blocks.is_empty() {
        return Err(NodeError::InvalidConfig(
            "lith_block_model_build produced no blocks".into(),
        ));
    }
    let voxels_payload = serde_json::json!({
        "schema_id": "geology.lith_block_model.v1",
        "schema_version": 1,
        "type": "lith_block_model_display_voxels",
        "crs": crs,
        "display_contract": {
            "renderer": "block_voxels",
            "display_pointer": "scene3d.block_voxels",
            "editable": ["visible", "opacity", "measure", "palette"]
        },
        "measure_candidates": ["formation", "formation_index", "display_role", "merged_cells"],
        "style_defaults": {
            "palette": "earth"
        },
        "blocks": display_blocks,
        "stats": {
            "block_count": display_blocks.len(),
            "full_cell_count": total_cells,
            "shell_cell_count": shell_cell_count,
            "display_shell_only": shell_only,
            "display_aggregate_vertical": aggregate_vertical,
            "display_budget": max_display_blocks,
            "formations_top_to_bottom": formations_top_to_bottom
        },
        "provenance": {
            "node_kind": "lith_block_model_build",
            "node_id": job.node_id.to_string()
        }
    });
    let centers_payload = serde_json::json!({
        "type": "lith_block_model_centers",
        "display_contract": {
            "renderer": "sample_points",
            "display_pointer": "scene3d.sample_points",
            "editable": ["visible", "opacity", "size", "measure", "palette"]
        },
        "measure_candidates": ["formation", "formation_index", "display_role", "merged_cells"],
        "points": centers
    });
    let report = serde_json::json!({
        "schema_id": "report.geology_lith_block_model.v1",
        "schema_version": 1,
        "block_count": voxels_payload.get("blocks").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0),
        "full_cell_count": total_cells,
        "shell_cell_count": shell_cell_count,
        "display_shell_only": shell_only,
        "display_aggregate_vertical": aggregate_vertical,
        "display_budget": max_display_blocks,
        "formations_top_to_bottom": scalar.get("formations_top_to_bottom").cloned().unwrap_or_else(|| serde_json::json!([]))
    });
    let vox_key = format!(
        "graphs/{}/nodes/{}/lith_block_model_voxels.json",
        job.graph_id, job.node_id
    );
    let cen_key = format!(
        "graphs/{}/nodes/{}/lith_block_model_centers.json",
        job.graph_id, job.node_id
    );
    let rep_key = format!(
        "graphs/{}/nodes/{}/lith_block_model_report.json",
        job.graph_id, job.node_id
    );
    let vox_ref = super::runtime::write_artifact(
        ctx,
        &vox_key,
        &serde_json::to_vec(&voxels_payload)?,
        Some("application/json"),
    )
    .await?;
    let cen_ref = super::runtime::write_artifact(
        ctx,
        &cen_key,
        &serde_json::to_vec(&centers_payload)?,
        Some("application/json"),
    )
    .await?;
    let rep_ref = super::runtime::write_artifact(
        ctx,
        &rep_key,
        &serde_json::to_vec(&report)?,
        Some("application/json"),
    )
    .await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![vox_ref.clone(), cen_ref.clone(), rep_ref.clone()],
        content_hashes: vec![
            vox_ref.content_hash,
            cen_ref.content_hash,
            rep_ref.content_hash,
        ],
        error_message: None,
    })
}
