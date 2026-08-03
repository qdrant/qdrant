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
    pub exceeded: QuotaExceeded,
}

/// Which of the enforced limits a node is currently refusing work over.
///
/// Reported per resource because they are freed by different actions: disk by
/// deleting or optimizing, memory by unloading. A single flag would not say
/// which one to go and fix.
///
/// `true` outlasts the reading that caused it: a resource that reaches its limit
/// stays flagged until it has fallen a margin below, so that one resting near
/// the limit does not flip the node in and out of service. Expect to see it set
/// while the reported utilization is already back under the configured limit.
///
/// A field is `null` when the node is not enforcing that resource — the quota is
/// disabled, no limit is set for it, or it cannot be measured here. That is
/// deliberately distinct from `false`: a resource that can never trip must not
/// be reported as one that is within its limits, or it invites an alert that can
/// never fire.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
pub struct QuotaExceeded {
    pub resident_memory: Option<bool>,
    pub disk_usage: Option<bool>,
}

impl QuotaExceeded {
    /// Whether any enforced limit is reached, and so the node is refusing
    /// updates.
    pub fn any(&self) -> bool {
        let Self {
            resident_memory,
            disk_usage,
        } = self;

        resident_memory.unwrap_or(false) || disk_usage.unwrap_or(false)
    }
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
