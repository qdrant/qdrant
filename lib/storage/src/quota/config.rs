use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Cluster-wide limits on node resources.
///
/// An unset limit means the corresponding resource is not capped. Limits are
/// only enforced while `enabled` is true.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, Validate,
)]
pub struct QuotaConfig {
    /// Whether the limits below are enforced.
    #[serde(default)]
    pub enabled: bool,

    /// Reject memory-consuming updates once process resident memory reaches this
    /// percentage of total system memory (or of the cgroup limit, if one applies).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 1, max = 100))]
    pub max_resident_memory_percent: Option<u8>,

    /// Reject disk-consuming updates once the filesystem hosting the storage
    /// directory is filled to this percentage of its capacity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 1, max = 100))]
    pub max_disk_usage_percent: Option<u8>,
}

impl QuotaConfig {
    /// Limits that this config puts into effect, `None` for each resource left
    /// uncapped or while the quota is disabled.
    pub fn limits(&self) -> QuotaLimits {
        let Self {
            enabled,
            max_resident_memory_percent,
            max_disk_usage_percent,
        } = *self;

        if !enabled {
            return QuotaLimits::default();
        }

        QuotaLimits {
            max_resident_memory_percent,
            max_disk_usage_percent,
        }
    }
}

/// Limits in effect, with the `enabled` flag already resolved.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct QuotaLimits {
    pub max_resident_memory_percent: Option<u8>,
    pub max_disk_usage_percent: Option<u8>,
}
