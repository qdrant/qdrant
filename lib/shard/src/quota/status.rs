use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::config::QuotaConfig;
use crate::PeerId;

/// Quota configuration in effect, and how close each peer is to it.
///
/// The configuration is cluster-wide; the utilization is not. `usage` is the
/// node that served the request, and `peers` is what every peer that answered
/// reports about itself — memory and disk are node-local, so one peer being
/// under its limit says nothing about the others.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct QuotaStatus {
    pub config: QuotaConfig,
    /// Utilization of the node that served this request.
    pub usage: QuotaUsage,
    /// Utilization reported by each peer, keyed by peer ID, including the one
    /// that served the request.
    ///
    /// Only peers that answered are listed: a peer missing from the map could
    /// not be reached, which is itself worth seeing. Absent entirely outside
    /// distributed mode, where there are no peers to ask.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peers: Option<HashMap<PeerId, PeerQuotaUsage>>,
}

/// What one peer reports about the quota it is enforcing.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
pub struct PeerQuotaUsage {
    #[serde(flatten)]
    pub usage: QuotaUsage,
    /// Whether this peer is at or over one of the enforced limits, and so is
    /// currently refusing updates. Always false while the quota is disabled.
    pub exceeded: bool,
}

/// What a node reports about the quota it is enforcing.
///
/// Carries the verdict rather than the raw utilization, because the point of
/// reporting it is to know whether this node is currently refusing writes —
/// which depends on the limits as well as the readings.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
pub struct QuotaTelemetry {
    pub config: QuotaConfig,
    /// Whether this node is at or over one of the enforced limits. Always false
    /// while the quota is disabled.
    pub exceeded: bool,
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
