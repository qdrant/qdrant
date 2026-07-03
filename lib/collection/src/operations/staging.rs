//! Staging-only operations for testing and debugging purposes.
//!
//! This module contains operations that are only available when the `staging` feature is enabled.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::shards::shard::PeerId;

fn default_test_slow_down_duration() -> f64 {
    1.0
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct TestSlowDownOperation {
    #[validate(nested)]
    pub test_slow_down: TestSlowDown,
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct TestSlowDown {
    /// Target peer ID to execute the sleep on.
    /// If not specified, the operation will be executed on all peers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<PeerId>,
    /// Duration of the sleep in seconds (default: 1.0, max: 300.0).
    #[serde(default = "default_test_slow_down_duration")]
    #[validate(range(min = 0.0, max = 300.0))]
    pub duration: f64,
}

fn default_test_transient_error_probability() -> f64 {
    0.5
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct TestTransientErrorOperation {
    #[validate(nested)]
    pub test_transient_error: TestTransientError,
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct TestTransientError {
    /// Target peer ID to fail on.
    /// If not specified, the operation will be executed on all peers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<PeerId>,
    /// Probability that applying the operation fails on each targeted peer,
    /// stopping its consensus thread with a service error (default: 0.5, max: 1.0).
    ///
    /// A failed peer re-applies the operation when its consensus thread restarts,
    /// rolling the probability again. Probability 1.0 therefore simulates a
    /// permanently failing consensus operation.
    #[serde(default = "default_test_transient_error_probability")]
    #[validate(range(min = 0.0, max = 1.0))]
    pub failure_probability: f64,
}
