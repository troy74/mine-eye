use std::path::Path;
use std::sync::Arc;

use mine_eye_graph::GraphSnapshot;
use mine_eye_store::PgStore;
use serde::Serialize;
use uuid::Uuid;

#[derive(Serialize)]
pub struct ViewerManifestLayer {
    pub source_node_id: Uuid,
    pub source_node_kind: String,
    pub edge_id: Uuid,
    pub semantic_type: String,
    pub from_port: String,
    pub to_port: String,
    pub artifact_key: String,
    pub artifact_url: String,
    pub content_hash: String,
    pub media_type: Option<String>,
    pub presentation: serde_json::Value,
}

#[derive(Serialize)]
pub struct ViewerManifest {
    pub graph_id: Uuid,
    pub viewer_node_id: Uuid,
    pub viewer_node_kind: String,
    pub manifest_version: u32,
    pub viewer_ui: serde_json::Value,
    pub layers: Vec<ViewerManifestLayer>,
}

pub async fn build_viewer_manifest(
    store: &Arc<PgStore>,
    artifact_root: &Path,
    snapshot: &GraphSnapshot,
    graph_id: Uuid,
    viewer_node_id: Uuid,
) -> Result<ViewerManifest, anyhow::Error> {
    let viewer = snapshot
        .nodes
        .get(&viewer_node_id)
        .ok_or_else(|| anyhow::anyhow!("viewer node not found: {}", viewer_node_id))?;
    let viewer_ui = viewer
        .config
        .params
        .get("ui")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));

    // Terrain-class node kinds whose tileserver_in port is traversed to inject imagery layers.
    const TERRAIN_PASSTHROUGH_KINDS: &[&str] = &["dem_fetch", "terrain_adjust", "dem_integrate"];

    let mut layers: Vec<ViewerManifestLayer> = Vec::new();
    for edge in snapshot
        .edges
        .iter()
        .filter(|e| e.to_node == viewer_node_id)
    {
        let source_kind = snapshot
            .nodes
            .get(&edge.from_node)
            .map(|n| n.config.kind.clone())
            .unwrap_or_else(|| "unknown".to_string());
        let rows = store.list_artifacts_for_node(edge.from_node).await?;
        for (key, hash, media_type) in rows {
            let lower = key.to_ascii_lowercase();
            if !(lower.ends_with(".json") || lower.ends_with(".geojson")) {
                continue;
            }
            let presentation =
                layer_presentation_from_artifact(artifact_root, &key, &source_kind).await;
            layers.push(ViewerManifestLayer {
                source_node_id: edge.from_node,
                source_node_kind: source_kind.clone(),
                edge_id: edge.id,
                semantic_type: edge.semantic_type.as_str().to_string(),
                from_port: edge.from_port.clone(),
                to_port: edge.to_port.clone(),
                artifact_key: key.clone(),
                artifact_url: format!("/files/{}?h={}", key, hash),
                content_hash: hash,
                media_type,
                presentation,
            });
        }

        // If this source node is a terrain node with a tileserver_in port wired, inject the
        // imagery provider's artifacts as drape layers (tileserver → dem_fetch → viewer pattern).
        if TERRAIN_PASSTHROUGH_KINDS.contains(&source_kind.as_str()) {
            for upstream in snapshot
                .edges
                .iter()
                .filter(|e| e.to_node == edge.from_node && e.to_port == "tileserver_in")
            {
                let upstream_kind = snapshot
                    .nodes
                    .get(&upstream.from_node)
                    .map(|n| n.config.kind.clone())
                    .unwrap_or_else(|| "unknown".to_string());
                let up_rows = store.list_artifacts_for_node(upstream.from_node).await?;
                for (key, hash, media_type) in up_rows {
                    let lower = key.to_ascii_lowercase();
                    if !(lower.ends_with(".json") || lower.ends_with(".geojson")) {
                        continue;
                    }
                    let presentation =
                        layer_presentation_from_artifact(artifact_root, &key, &upstream_kind).await;
                    layers.push(ViewerManifestLayer {
                        source_node_id: upstream.from_node,
                        source_node_kind: upstream_kind.clone(),
                        edge_id: upstream.id,
                        semantic_type: upstream.semantic_type.as_str().to_string(),
                        from_port: upstream.from_port.clone(),
                        // prefix with "00_" so drape sorts before terrain in the layer stack
                        to_port: format!("00_{}", edge.to_port),
                        artifact_key: key.clone(),
                        artifact_url: format!("/files/{}?h={}", key, hash),
                        content_hash: hash,
                        media_type,
                        presentation,
                    });
                }
            }
        }
    }
    layers.sort_by(|a, b| {
        a.to_port
            .cmp(&b.to_port)
            .then_with(|| a.source_node_kind.cmp(&b.source_node_kind))
            .then_with(|| a.artifact_key.cmp(&b.artifact_key))
    });

    Ok(ViewerManifest {
        graph_id,
        viewer_node_id,
        viewer_node_kind: viewer.config.kind.clone(),
        manifest_version: 1,
        viewer_ui,
        layers,
    })
}

async fn layer_presentation_from_artifact(
    artifact_root: &Path,
    key: &str,
    source_kind: &str,
) -> serde_json::Value {
    let mut out = serde_json::json!({
        "renderer": "generic",
        "display_pointer": "scene3d.generic",
        "editable": ["visible", "opacity", "style"],
        "has_surface_grid": false,
        "has_contours": key.to_ascii_lowercase().ends_with(".geojson"),
        "schema_id": null,
        "is_contract": false,
        "measure_candidates": [],
    });
    let lower = key.to_ascii_lowercase();
    let sk = source_kind.to_ascii_lowercase();
    if sk == "desurvey_trajectory" {
        out["renderer"] = serde_json::json!("trace_polyline");
        out["display_pointer"] = serde_json::json!("scene3d.trace_polyline");
        out["editable"] = serde_json::json!(["visible", "opacity", "width", "color"]);
    } else if sk == "block_grade_model" && lower.contains("voxels") {
        out["renderer"] = serde_json::json!("block_voxels");
        out["display_pointer"] = serde_json::json!("scene3d.block_voxels");
        out["editable"] = serde_json::json!(["visible", "opacity", "measure", "palette", "cutoff"]);
    } else if sk == "block_grade_model" && lower.contains("centers") {
        out["renderer"] = serde_json::json!("sample_points");
        out["display_pointer"] = serde_json::json!("scene3d.sample_points");
        out["editable"] = serde_json::json!(["visible", "opacity", "size", "measure", "palette"]);
    } else if sk == "drillhole_model" && lower.contains("drillhole_meshes") {
        out["renderer"] = serde_json::json!("grade_segments");
        out["display_pointer"] = serde_json::json!("scene3d.grade_segments");
        out["editable"] = serde_json::json!(["visible", "opacity", "width", "measure", "palette"]);
    } else if sk == "surface_iso_extract" || lower.contains("iso_contours") {
        out["renderer"] = serde_json::json!("contour_lines");
        out["display_pointer"] = serde_json::json!("scene3d.contour_lines");
        out["editable"] = serde_json::json!(["visible", "opacity", "width", "color"]);
    } else if lower.contains("assay_points") || sk == "assay_ingest" {
        out["renderer"] = serde_json::json!("sample_points");
        out["display_pointer"] = serde_json::json!("scene3d.sample_points");
        out["editable"] = serde_json::json!(["visible", "opacity", "size", "measure", "palette"]);
    } else if sk == "terrain_adjust"
        || sk == "dem_integrate"
        || sk == "dem_fetch"
        || sk == "xyz_to_surface"
        || lower.contains("dem")
        || lower.contains("terrain_adjusted")
        || lower.contains("xyz_surface")
    {
        out["renderer"] = serde_json::json!("terrain");
        out["display_pointer"] = serde_json::json!("scene3d.terrain");
        out["editable"] = serde_json::json!(["visible", "opacity"]);
    } else if sk == "imagery_provider" || sk == "tilebroker" {
        out["renderer"] = serde_json::json!("drape");
        out["display_pointer"] = serde_json::json!("scene3d.imagery_drape");
        out["editable"] = serde_json::json!(["visible", "opacity", "provider"]);
    } else if sk == "scene3d_layer_stack" {
        out["renderer"] = serde_json::json!("layer_stack");
        out["display_pointer"] = serde_json::json!("scene3d.layer_stack");
        out["editable"] = serde_json::json!(["visible", "opacity", "order", "style"]);
    } else if sk == "aoi" {
        out["renderer"] = serde_json::json!("aoi");
        out["display_pointer"] = serde_json::json!("spatial.aoi");
        out["editable"] = serde_json::json!(["visible"]);
    }
    if !lower.ends_with(".json") {
        return out;
    }
    let path = artifact_root.join(key);
    let Ok(bytes) = tokio::fs::read(path).await else {
        return out;
    };
    let Ok(root) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return out;
    };
    let Some(obj) = root.as_object() else {
        return out;
    };
    if let Some(schema_id) = obj.get("schema_id").and_then(|v| v.as_str()) {
        out["schema_id"] = serde_json::json!(schema_id);
        out["is_contract"] = serde_json::json!(schema_id.ends_with(".v1"));
        match schema_id {
            "scene3d.imagery_drape.v1" => {
                out["renderer"] = serde_json::json!("drape");
                out["display_pointer"] = serde_json::json!("scene3d.imagery_drape");
                out["editable"] = serde_json::json!(["visible", "opacity", "provider"]);
            }
            "scene3d.tilebroker_response.v1" => {
                out["renderer"] = serde_json::json!("drape");
                out["display_pointer"] = serde_json::json!("scene3d.imagery_drape");
                out["editable"] = serde_json::json!(["visible", "opacity", "provider"]);
            }
            "scene3d.layer_stack.v1" => {
                out["renderer"] = serde_json::json!("layer_stack");
                out["display_pointer"] = serde_json::json!("scene3d.layer_stack");
                out["editable"] = serde_json::json!(["visible", "opacity", "order", "style"]);
            }
            "spatial.aoi.v1" => {
                out["renderer"] = serde_json::json!("aoi");
                out["display_pointer"] = serde_json::json!("spatial.aoi");
                out["editable"] = serde_json::json!(["visible"]);
            }
            "terrain.surface_grid.v1" => {
                out["renderer"] = serde_json::json!("terrain");
                out["display_pointer"] = serde_json::json!("scene3d.terrain");
                out["editable"] = serde_json::json!(["visible", "opacity"]);
                out["has_surface_grid"] = serde_json::json!(true);
            }
            _ => {}
        }
    }
    if let Some(dc) = obj.get("display_contract").and_then(|v| v.as_object()) {
        if let Some(r) = dc.get("renderer").and_then(|v| v.as_str()) {
            out["renderer"] = serde_json::json!(r);
        }
        if let Some(dp) = dc.get("display_pointer").and_then(|v| v.as_str()) {
            out["display_pointer"] = serde_json::json!(dp);
        }
        if let Some(ed) = dc.get("editable").and_then(|v| v.as_array()) {
            out["editable"] = serde_json::Value::Array(
                ed.iter()
                    .filter_map(|x| x.as_str().map(|s| serde_json::json!(s)))
                    .collect(),
            );
        }
    }
    if let Some(mc) = obj.get("measure_candidates").and_then(|v| v.as_array()) {
        out["measure_candidates"] = serde_json::Value::Array(
            mc.iter()
                .filter_map(|x| x.as_str().map(|s| serde_json::json!(s)))
                .collect(),
        );
    }
    if let Some(cfg) = obj.get("heatmap_config").and_then(|v| v.as_object()) {
        out["heatmap_config"] = serde_json::Value::Object(cfg.clone());
    }
    if let Some(stats) = obj.get("stats").and_then(|v| v.as_object()) {
        out["stats"] = serde_json::Value::Object(stats.clone());
    }
    if let Some(ui_caps) = obj.get("ui_capabilities").and_then(|v| v.as_array()) {
        out["editable"] = serde_json::Value::Array(
            ui_caps
                .iter()
                .filter_map(|x| x.as_str().map(|s| serde_json::json!(s)))
                .collect(),
        );
    }
    if obj.get("surface_grid").is_some() {
        out["has_surface_grid"] = serde_json::json!(true);
    }
    out
}
