//! Staging-only operations for testing and debugging purposes.
//!
//! This module contains operations that are only available when the `staging` feature is enabled.

use std::time::Duration;

use ordered_float::OrderedFloat;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use validator::Validate;

use super::CollectionUpdateOperations;

/// Staging operations enum - all staging-only operations that can be sent to the staging endpoint.
/// New staging operations should be added here to be immediately usable via the API.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema, Hash)]
#[serde(rename_all = "snake_case")]
pub enum StagingOperations {
    /// Introduce an artificial delay for testing purposes
    Delay(TestDelayOperation),
}

impl Validate for StagingOperations {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            StagingOperations::Delay(op) => op.validate(),
        }
    }
}

impl From<StagingOperations> for CollectionUpdateOperations {
    fn from(op: StagingOperations) -> Self {
        CollectionUpdateOperations::StagingOperation(op)
    }
}

/// Test operation that introduces an artificial delay.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema, Validate, Hash)]
pub struct TestDelayOperation {
    /// Duration of the delay in seconds (0.0 to 300.0).
    #[validate(range(min = 0.0, max = 300.0))]
    pub duration_sec: OrderedFloat<f64>,
}

impl TestDelayOperation {
    /// Create a new TestDelayOperation with the given duration in seconds.
    pub fn new(duration_secs: f64) -> Self {
        Self {
            duration_sec: OrderedFloat(duration_secs),
        }
    }

    /// Execute the delay operation (blocking).
    pub fn execute(&self) {
        let duration_secs = self.duration_sec.into_inner();
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
