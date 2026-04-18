//! Core node implementations behind `NodeExecutor`.

mod crs_transform;
mod error;
mod executor;
mod kinds;

pub use error::NodeError;
pub use executor::{ExecutionContext, NodeExecutor, NodeExecutorRegistry, ProgressUpdate};
pub use kinds::{
    run_block_grade_model, run_block_model_stub, run_constraint_merge, run_dem_integrate_stub,
    run_desurvey_trajectory, run_drillhole_ingest, run_drillhole_model,
    run_formation_catalog_build, run_formation_interface_extract, run_heatmap_raster_tile_cache,
    run_lith_block_model_build, run_lithology_ingest, run_magnetic_model, run_md_viewer,
    run_model_domain_define, run_observation_ingest, run_orientation_ingest, run_plot_chart,
    run_stratigraphic_interpolator, run_stratigraphic_order_define, run_structural_frame_builder,
    run_vertical_trajectory,
};
