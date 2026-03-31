//! Pure graph logic: DAG structure, edges, content hashing, staleness propagation.

mod dag;
mod error;
mod hash;

pub use dag::{EdgeRef, GraphSnapshot, propagate_stale};
pub use error::GraphError;
pub use hash::hash_node_config;
