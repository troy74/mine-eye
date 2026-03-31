//! Scheduler applies policies and produces job envelopes for dirty nodes.

mod plan;

pub use plan::{
    collect_dirty_nodes, expand_dirty, mark_dirty_from_hash_change, SchedulePlan, Scheduler,
};
