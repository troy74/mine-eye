//! Node-kind taxonomy surface.
//!
//! This module exposes grouped node execution entry points by domain while
//! preserving the existing public `run_*` API for compatibility.

pub(crate) mod colour;
pub(crate) mod parse_util;

pub mod acquisition;
pub mod chart_plot;
pub mod data_model;
pub mod drillhole;
pub mod electrical_ip;
pub mod heatmap_raster_tile_cache;
pub mod imagery_raster;
pub mod magnetic_depth;
pub mod magnetic_model;
pub mod markdown_viewer;
pub mod observation_ingest;
pub mod resource_model;
pub mod runtime;
pub mod scene_contract;
pub mod spatial;
pub mod stubs;
pub mod surface;
pub mod tile_cache;
pub mod trajectory;
pub mod visualization;

pub use acquisition::{
    run_assay_ingest, run_collar_ingest, run_drillhole_ingest, run_surface_sample_ingest,
    run_survey_ingest,
};
pub use chart_plot::run_plot_chart;
pub use data_model::run_data_model_transform;
pub use drillhole::{run_drillhole_merge, run_drillhole_model};
pub use electrical_ip::{
    run_ip_corridor_model, run_ip_inversion_mesh, run_ip_inversion_preview, run_ip_pseudosection,
    run_ip_qc_normalize, run_ip_survey_ingest,
};
pub use heatmap_raster_tile_cache::run_heatmap_raster_tile_cache;
pub use imagery_raster::{run_imagery_provider, run_tilebroker};
pub use magnetic_depth::run_magnetic_depth_model;
pub use magnetic_model::run_magnetic_model;
pub use markdown_viewer::run_md_viewer;
pub use observation_ingest::run_observation_ingest;
pub use resource_model::run_block_grade_model;
pub use scene_contract::run_scene3d_layer_stack;
pub use spatial::run_aoi;
pub use stubs::{run_block_model_stub, run_dem_integrate_stub};
pub use surface::{
    run_assay_heatmap, run_dem_fetch, run_surface_iso_extract, run_terrain_adjust,
    run_xyz_to_surface,
};
pub use trajectory::run_desurvey_trajectory;
pub use visualization::{run_plan_view_2d, run_plan_view_3d};
