use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::config::QuotaConfig;

/// Quota configuration in effect, and how close the node is to it.
///
/// The configuration is cluster-wide, the utilization is not: `usage` describes
/// the peer that served the request. Ask each peer separately to see where the
/// cluster stands.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
pub struct QuotaStatus {
    pub config: QuotaConfig,
    pub usage: QuotaUsage,
}

/// Utilization of the quota-managed resources **on this node alone** — memory
/// and disk are node-local, so a peer under its limit says nothing about the
/// rest of the cluster.
///
/// A field is `null` when the platform does not expose the underlying stat.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
pub struct QuotaUsage {
    /// Resident memory of this node's process, as a percentage of the memory
    /// available to it (cgroup limit if one applies, else total system memory).
    pub resident_memory_percent: Option<u8>,
    /// Used space of this node's storage filesystem, as a percentage of its
    /// capacity.
    pub disk_usage_percent: Option<u8>,
}
