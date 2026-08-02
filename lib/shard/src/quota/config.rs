use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use validator::Validate;

/// How far below its limit a resource has to fall before the node accepts work
/// again, when the config does not say.
pub const DEFAULT_RELEASE_MARGIN_PERCENT: u8 = 5;

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

    /// How many percentage points below its limit a resource has to fall before
    /// this node starts accepting work again.
    ///
    /// Without a margin, a resource resting on its limit crosses it in both
    /// directions on the noise between two readings, putting the node in and out
    /// of service each time — and restarting a shard recovery with it. Raise it
    /// where usage is volatile; `0` disables the margin and releases as soon as
    /// usage is back under the limit.
    ///
    /// Unset leaves the built-in default in force, so a config written today
    /// does not pin a number that a later release may want to revise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[validate(range(max = 100))]
    pub release_margin_percent: Option<u8>,
}

impl QuotaConfig {
    /// Limits that this config puts into effect, `None` for each resource left
    /// uncapped or while the quota is disabled.
    pub fn limits(&self) -> QuotaLimits {
        let Self {
            enabled,
            max_resident_memory_percent,
            max_disk_usage_percent,
            release_margin_percent,
        } = *self;

        if !enabled {
            return QuotaLimits::default();
        }

        QuotaLimits {
            max_resident_memory_percent,
            max_disk_usage_percent,
            // Resolved here, next to `enabled`, so enforcement never has to know
            // that the margin was left to us to pick.
            release_margin_percent: release_margin_percent
                .unwrap_or(DEFAULT_RELEASE_MARGIN_PERCENT),
        }
    }
}

/// Limits on the quota-managed resources, with the `enabled` flag already
/// resolved. `None` leaves the corresponding resource uncapped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaLimits {
    pub max_resident_memory_percent: Option<u8>,
    pub max_disk_usage_percent: Option<u8>,
    pub release_margin_percent: u8,
}

impl Default for QuotaLimits {
    fn default() -> Self {
        QuotaLimits {
            max_resident_memory_percent: None,
            max_disk_usage_percent: None,
            release_margin_percent: DEFAULT_RELEASE_MARGIN_PERCENT,
        }
    }
}
