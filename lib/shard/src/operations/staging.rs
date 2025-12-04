//! Staging-only operations for testing and debugging purposes.
//!
//! This module contains operations that are only available when the `staging` feature is enabled.

use std::time::Duration;

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

/// Test operation that introduces an artificial delay.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, Hash)]
pub struct TestDelayUpsertPointsOperation {
    /// Duration of the delay in seconds.
    pub duration: OrderedFloat<f64>,
}

impl TestDelayUpsertPointsOperation {
    /// Execute the delay operation (blocking).
    pub fn execute(&self) {
        let duration_secs = self.duration.into_inner();
        log::debug!("TestDelayUpsertPoints: sleeping for {duration_secs}s");
        std::thread::sleep(Duration::from_secs_f64(duration_secs));
        log::debug!("TestDelayUpsertPoints: finished sleeping");
    }
}
