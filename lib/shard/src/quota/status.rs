use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::config::QuotaConfig;

/// Quota configuration in effect, and how close the node is to it.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
pub struct QuotaStatus {
    pub config: QuotaConfig,
    pub usage: QuotaUsage,
}

/// Current utilization of the quota-managed resources. A field is `null` when
/// the platform does not expose the underlying stat.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
pub struct QuotaUsage {
    /// Process resident memory as a percentage of total system memory.
    pub resident_memory_percent: Option<u8>,
    /// Used space of the storage filesystem as a percentage of its capacity.
    pub disk_usage_percent: Option<u8>,
}
