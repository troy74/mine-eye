use mine_eye_types::{
    CollarRecord, CrsRecord, IntervalSampleRecord, JobEnvelope, JobResult, JobStatus,
    SurveyStationRecord,
};

use crate::crs_transform::transform_xy;
use crate::executor::ExecutionContext;
use crate::NodeError;

/// Parses collars/surveys/assays from job config params JSON and writes canonical JSON artifact.
pub async fn run_drillhole_ingest(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payload = job
        .input_payload
        .as_ref()
        .ok_or_else(|| NodeError::InvalidConfig("missing input_payload for ingest".into()))?;
    let collars: Vec<CollarRecord> = serde_json::from_value(
        payload
            .pointer("/collars")
            .cloned()
            .unwrap_or(serde_json::json!([])),
    )
    .unwrap_or_default();
    let surveys: Vec<SurveyStationRecord> = serde_json::from_value(
        payload
            .pointer("/surveys")
            .cloned()
            .unwrap_or(serde_json::json!([])),
    )
    .unwrap_or_default();
    let assays: Vec<IntervalSampleRecord> = serde_json::from_value(
        payload
            .pointer("/assays")
            .cloned()
            .unwrap_or(serde_json::json!([])),
    )
    .unwrap_or_default();

    let payload = serde_json::json!({
        "collars": collars,
        "surveys": surveys,
        "assays": assays,
    });
    let bytes = serde_json::to_vec(&payload)?;
    let key = format!("graphs/{}/nodes/{}/ingest.json", job.graph_id, job.node_id);
    let artifact =
        super::runtime::write_artifact(ctx, &key, &bytes, Some("application/json")).await?;

    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

/// Single primitive: collars only.
pub async fn run_collar_ingest(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payload = job.input_payload.as_ref().ok_or_else(|| {
        NodeError::InvalidConfig("missing input_payload for collar_ingest".into())
    })?;
    let mut collars: Vec<CollarRecord> = serde_json::from_value(
        payload
            .pointer("/collars")
            .cloned()
            .unwrap_or(serde_json::json!([])),
    )
    .unwrap_or_default();

    if let Some(target) = super::runtime::collar_output_target_crs(job)? {
        let project_missing =
            super::runtime::collar_output_crs_mode(job) == "project" && job.project_crs.is_none();
        for c in &mut collars {
            if project_missing {
                c.qa_flags
                    .push("project_crs_missing_output_epsg_4326".into());
            }
            if c.crs == target {
                continue;
            }
            let (nx, ny) = transform_xy(&c.crs, &target, c.x, c.y)?;
            c.x = nx;
            c.y = ny;
            c.crs = target.clone();
            c.qa_flags.push("reprojected_xy".into());
        }
    }

    let out = serde_json::json!({ "collars": collars });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!("graphs/{}/nodes/{}/collars.json", job.graph_id, job.node_id);
    let artifact =
        super::runtime::write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

/// Single primitive: survey stations only.
pub async fn run_survey_ingest(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payload = job.input_payload.as_ref().ok_or_else(|| {
        NodeError::InvalidConfig("missing input_payload for survey_ingest".into())
    })?;
    let surveys: Vec<SurveyStationRecord> = serde_json::from_value(
        payload
            .pointer("/surveys")
            .cloned()
            .unwrap_or(serde_json::json!([])),
    )
    .unwrap_or_default();
    let out = serde_json::json!({ "surveys": surveys });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!("graphs/{}/nodes/{}/surveys.json", job.graph_id, job.node_id);
    let artifact =
        super::runtime::write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

/// Single primitive: interval assays only.
pub async fn run_assay_ingest(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payload = job
        .input_payload
        .as_ref()
        .ok_or_else(|| NodeError::InvalidConfig("missing input_payload for assay_ingest".into()))?;
    let assays: Vec<IntervalSampleRecord> = serde_json::from_value(
        payload
            .pointer("/assays")
            .cloned()
            .unwrap_or(serde_json::json!([])),
    )
    .unwrap_or_default();
    let out = serde_json::json!({ "assays": assays });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!("graphs/{}/nodes/{}/assays.json", job.graph_id, job.node_id);
    let artifact =
        super::runtime::write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

/// Single primitive: lithology / stratigraphic intervals only.
pub async fn run_lithology_ingest(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payload = job.input_payload.as_ref().ok_or_else(|| {
        NodeError::InvalidConfig("missing input_payload for lithology_ingest".into())
    })?;
    let intervals: Vec<IntervalSampleRecord> = serde_json::from_value(
        payload
            .pointer("/intervals")
            .cloned()
            .unwrap_or(serde_json::json!([])),
    )
    .unwrap_or_default();
    let crs: CrsRecord = serde_json::from_value(
        payload
            .pointer("/source_crs")
            .cloned()
            .unwrap_or_else(|| serde_json::json!(CrsRecord::epsg(4326))),
    )
    .unwrap_or_else(|_| CrsRecord::epsg(4326));

    if intervals.is_empty() {
        return Err(NodeError::InvalidConfig(
            "lithology_ingest requires interval rows".into(),
        ));
    }

    let mut formation_order: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::<String>::new();
    let interval_rows = intervals
        .iter()
        .filter_map(|it| {
            let formation = it
                .attributes
                .get("formation")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())?
                .to_string();
            if seen.insert(formation.clone()) {
                formation_order.push(formation.clone());
            }
            Some(serde_json::json!({
                "hole_id": it.hole_id,
                "from_m": it.from_m,
                "to_m": it.to_m,
                "formation": formation,
                "group": it.attributes.get("group").cloned().unwrap_or(serde_json::Value::Null),
                "lithology_code": it.attributes.get("lithology_code").cloned().unwrap_or(serde_json::Value::Null),
                "attributes": it.attributes,
                "qa_flags": it.qa_flags,
            }))
        })
        .collect::<Vec<_>>();

    if interval_rows.is_empty() {
        return Err(NodeError::InvalidConfig(
            "lithology_ingest requires a mapped 'formation' column".into(),
        ));
    }

    let out = serde_json::json!({
        "schema_id": "geology.lithology_intervals.v1",
        "schema_version": 1,
        "crs": crs,
        "holes": [],
        "intervals": interval_rows,
        "formation_order": formation_order,
        "provenance": {
            "node_kind": "lithology_ingest",
            "node_id": job.node_id.to_string(),
        }
    });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!(
        "graphs/{}/nodes/{}/lithology_intervals.json",
        job.graph_id, job.node_id
    );
    let artifact =
        super::runtime::write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

/// Single primitive: structural orientations only.
pub async fn run_orientation_ingest(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payload = job.input_payload.as_ref().ok_or_else(|| {
        NodeError::InvalidConfig("missing input_payload for orientation_ingest".into())
    })?;
    let rows = payload
        .get("rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let crs: CrsRecord = serde_json::from_value(
        payload
            .pointer("/source_crs")
            .cloned()
            .unwrap_or_else(|| serde_json::json!(CrsRecord::epsg(4326))),
    )
    .unwrap_or_else(|_| CrsRecord::epsg(4326));

    if rows.is_empty() {
        return Err(NodeError::InvalidConfig(
            "orientation_ingest requires row payloads".into(),
        ));
    }

    fn num(obj: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<f64> {
        obj.get(key).and_then(|v| v.as_f64())
    }

    fn pole_from_dip_azimuth(dip_deg: f64, azimuth_deg: f64) -> [f64; 3] {
        let dip = dip_deg.to_radians();
        let az = azimuth_deg.to_radians();
        let strike = az - std::f64::consts::FRAC_PI_2;
        let nx = -dip.sin() * strike.sin();
        let ny = dip.sin() * strike.cos();
        let nz = dip.cos();
        let len = (nx * nx + ny * ny + nz * nz).sqrt().max(1e-12);
        [nx / len, ny / len, nz / len]
    }

    let mut orientations = Vec::<serde_json::Value>::new();
    for (idx, row) in rows.iter().enumerate() {
        let Some(obj) = row.as_object() else { continue };
        let formation = obj
            .get("formation")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .unwrap_or("");
        if formation.is_empty() {
            continue;
        }
        let (Some(x), Some(y), Some(z)) = (num(obj, "x"), num(obj, "y"), num(obj, "z")) else {
            continue;
        };

        let dip_deg = num(obj, "dip_deg");
        let azimuth_deg = num(obj, "azimuth_deg");
        let pole_vector = obj
            .get("pole_vector")
            .and_then(|v| v.as_array())
            .filter(|a| a.len() >= 3)
            .and_then(|a| {
                Some([
                    a.first()?.as_f64()?,
                    a.get(1)?.as_f64()?,
                    a.get(2)?.as_f64()?,
                ])
            })
            .or_else(|| match (dip_deg, azimuth_deg) {
                (Some(dip), Some(az)) => Some(pole_from_dip_azimuth(dip, az)),
                _ => None,
            });

        let Some(pole_vector) = pole_vector else {
            continue;
        };
        let confidence = num(obj, "confidence").unwrap_or(1.0).clamp(0.0, 1.0);
        orientations.push(serde_json::json!({
            "orientation_id": obj.get("orientation_id").and_then(|v| v.as_str()).map(|s| s.to_string()).unwrap_or_else(|| format!("ori_{:04}", idx + 1)),
            "formation": formation,
            "group": obj.get("group").cloned().unwrap_or(serde_json::Value::Null),
            "x": x,
            "y": y,
            "z": z,
            "dip_deg": dip_deg,
            "azimuth_deg": azimuth_deg,
            "polarity": obj.get("polarity").and_then(|v| v.as_str()).unwrap_or("normal"),
            "pole_vector": pole_vector,
            "source_kind": obj.get("source_kind").and_then(|v| v.as_str()).unwrap_or("observed"),
            "confidence": confidence,
            "attributes": obj.get("attributes").cloned().unwrap_or_else(|| serde_json::json!({})),
            "qa_flags": obj.get("qa_flags").cloned().unwrap_or_else(|| serde_json::json!([])),
        }));
    }

    if orientations.is_empty() {
        return Err(NodeError::InvalidConfig(
            "orientation_ingest produced no usable orientation rows".into(),
        ));
    }

    let out = serde_json::json!({
        "schema_id": "geology.formation_orientations.v1",
        "schema_version": 1,
        "crs": crs,
        "orientations": orientations,
        "provenance": {
            "node_kind": "orientation_ingest",
            "node_id": job.node_id.to_string(),
        }
    });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!(
        "graphs/{}/nodes/{}/formation_orientations.json",
        job.graph_id, job.node_id
    );
    let artifact =
        super::runtime::write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}

/// Single primitive: surface samples as point rows for plan-view and downstream interpolation.
pub async fn run_surface_sample_ingest(
    ctx: &ExecutionContext<'_>,
    job: &JobEnvelope,
) -> Result<JobResult, NodeError> {
    let payload = job.input_payload.as_ref().ok_or_else(|| {
        NodeError::InvalidConfig("missing input_payload for surface_sample_ingest".into())
    })?;
    let mut points = payload
        .pointer("/points")
        .cloned()
        .unwrap_or(serde_json::json!([]));
    let mut terrain_grid_for_fill: Option<(usize, usize, f64, f64, f64, f64, Vec<Option<f64>>)> =
        None;
    for ar in &job.input_artifact_refs {
        let v = super::runtime::read_json_artifact(ctx, &ar.key).await?;
        let Some(g) = v.get("surface_grid").and_then(|x| x.as_object()) else {
            continue;
        };
        let nx = g
            .get("nx")
            .and_then(|x| x.as_u64())
            .map(|v| v as usize)
            .unwrap_or(0);
        let ny = g
            .get("ny")
            .and_then(|x| x.as_u64())
            .map(|v| v as usize)
            .unwrap_or(0);
        let xmin = g.get("xmin").and_then(|x| x.as_f64());
        let xmax = g.get("xmax").and_then(|x| x.as_f64());
        let ymin = g.get("ymin").and_then(|x| x.as_f64());
        let ymax = g.get("ymax").and_then(|x| x.as_f64());
        let vals = g.get("values").and_then(|x| x.as_array());
        let (Some(xmin), Some(xmax), Some(ymin), Some(ymax), Some(vals)) =
            (xmin, xmax, ymin, ymax, vals)
        else {
            continue;
        };
        if nx < 2 || ny < 2 || vals.len() != nx * ny {
            continue;
        }
        let values: Vec<Option<f64>> = vals.iter().map(|v| v.as_f64()).collect();
        terrain_grid_for_fill = Some((nx, ny, xmin, xmax, ymin, ymax, values));
        break;
    }

    if let Some(target) = super::runtime::collar_output_target_crs(job)? {
        let project_missing =
            super::runtime::collar_output_crs_mode(job) == "project" && job.project_crs.is_none();
        if let Some(arr) = points.as_array_mut() {
            for p in arr.iter_mut() {
                let Some(obj) = p.as_object_mut() else {
                    continue;
                };
                let (Some(x), Some(y)) = (
                    obj.get("x").and_then(|v| v.as_f64()),
                    obj.get("y").and_then(|v| v.as_f64()),
                ) else {
                    continue;
                };

                let src_crs = obj
                    .get("crs")
                    .cloned()
                    .and_then(|v| serde_json::from_value::<CrsRecord>(v).ok())
                    .unwrap_or_else(|| target.clone());
                let (nx, ny) = if src_crs == target {
                    (x, y)
                } else {
                    transform_xy(&src_crs, &target, x, y)?
                };
                obj.insert("x".into(), serde_json::json!(nx));
                obj.insert("y".into(), serde_json::json!(ny));
                obj.insert("crs".into(), serde_json::to_value(&target)?);

                let mut qa_vals: Vec<String> = obj
                    .get("qa_flags")
                    .and_then(|x| x.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default();
                if project_missing {
                    qa_vals.push("project_crs_missing_output_epsg_4326".into());
                }
                if src_crs != target {
                    qa_vals.push("reprojected_xy".into());
                }

                let has_z = obj.get("z").and_then(|v| v.as_f64()).is_some();
                if !has_z {
                    if let Some((gnx, gny, gxmin, gxmax, gymin, gymax, gvals)) =
                        terrain_grid_for_fill.as_ref()
                    {
                        if let Some(zv) = super::runtime::bilinear_from_grid(
                            *gnx, *gny, *gxmin, *gxmax, *gymin, *gymax, gvals, nx, ny,
                        ) {
                            obj.insert("z".into(), serde_json::json!(zv));
                            qa_vals.push("z_from_terrain_grid".into());
                        }
                    }
                }
                obj.insert("qa_flags".into(), serde_json::json!(qa_vals));
            }
        }
    }
    let out = serde_json::json!({ "points": points });
    let bytes = serde_json::to_vec(&out)?;
    let key = format!(
        "graphs/{}/nodes/{}/surface_samples.json",
        job.graph_id, job.node_id
    );
    let artifact =
        super::runtime::write_artifact(ctx, &key, &bytes, Some("application/json")).await?;
    Ok(JobResult {
        job_id: job.job_id,
        status: JobStatus::Succeeded,
        output_artifact_refs: vec![artifact.clone()],
        content_hashes: vec![artifact.content_hash],
        error_message: None,
    })
}
