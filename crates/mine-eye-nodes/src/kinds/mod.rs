//! Node-kind taxonomy surface.
//!
//! This module exposes grouped node execution entry points by domain while
//! preserving the existing public `run_*` API for compatibility.

pub mod acquisition;
pub mod artifact_ingest;
pub mod chart_plot;
pub mod tile_cache;
pub mod data_model;
pub mod drillhole;
pub mod imagery_raster;
pub mod magnetic_mapper;
pub mod markdown_viewer;
pub mod resource_model;
pub mod runtime;
pub mod scene_contract;
pub mod spatial;
pub mod stubs;
pub mod surface;
pub mod trajectory;
pub mod visualization;

pub use acquisition::{
    run_assay_ingest, run_collar_ingest, run_drillhole_ingest, run_surface_sample_ingest,
    run_survey_ingest,
};
pub use artifact_ingest::run_artifact_ingest;
pub use chart_plot::run_plot_chart;
pub use data_model::run_data_model_transform;
pub use drillhole::{run_drillhole_merge, run_drillhole_model};
pub use imagery_raster::{run_imagery_provider, run_tilebroker};
pub use magnetic_mapper::run_magnetic_mapper;
pub use markdown_viewer::run_md_viewer;
pub use resource_model::run_block_grade_model;
pub use scene_contract::run_scene3d_layer_stack;
pub use spatial::run_aoi;
pub use stubs::{run_block_model_stub, run_dem_integrate_stub};
pub use surface::{run_assay_heatmap, run_dem_fetch, run_surface_iso_extract, run_terrain_adjust, run_xyz_to_surface};
pub use trajectory::run_desurvey_trajectory;
pub use visualization::{run_plan_view_2d, run_plan_view_3d};
