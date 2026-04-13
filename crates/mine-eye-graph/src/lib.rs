//! Pure graph logic: DAG structure, edges, content hashing, staleness propagation.

mod dag;
mod error;
mod hash;

pub use dag::{propagate_stale, EdgeRef, GraphSnapshot};
pub use error::GraphError;
pub use hash::hash_node_config;
