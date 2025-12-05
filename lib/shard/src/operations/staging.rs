//! Staging-only operations for testing and debugging purposes.
//!
//! This module contains operations that are only available when the `staging` feature is enabled.

use std::time::Duration;

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

/// Test operation that introduces an artificial delay.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, Hash)]
pub struct TestDelayOperation {
    /// Duration of the delay in seconds.
    pub duration: OrderedFloat<f64>,
}

impl TestDelayOperation {
    /// Create a new TestDelayOperation with the given duration in seconds.
    pub fn new(duration_secs: f64) -> Self {
        Self {
            duration: OrderedFloat(duration_secs),
        }
    }

    /// Execute the delay operation (blocking).
    pub fn execute(&self) {
        let duration_secs = self.duration.into_inner();
        if duration_secs < 0.0 {
            log::warn!("TestDelay: negative duration {duration_secs}s, skipping");
            return;
        }
        let delay = Duration::from_secs_f64(duration_secs);
        log::debug!("TestDelay: sleeping for {delay:?}");
        std::thread::sleep(delay);
        log::debug!("TestDelay: finished sleeping");
    }
}
