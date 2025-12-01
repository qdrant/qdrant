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
