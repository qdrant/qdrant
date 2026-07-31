use std::sync::OnceLock;

use super::error::QuotaError;

/// A resource that [`super::QuotaManager`] measures and caps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resource {
    ResidentMemory,
    DiskUsage,
}

impl Resource {
    /// The limit parameter that governs this resource, named the same way in the
    /// quota config and in a strict mode config.
    fn parameter(self) -> &'static str {
        match self {
            Resource::ResidentMemory => "max_resident_memory_percent",
            Resource::DiskUsage => "max_disk_usage_percent",
        }
    }

    /// Build the user-facing rejection, naming the exact condition that tripped
    /// and the parameter that governs it.
    pub(super) fn rejected(self, used_percent: u8, limit: EffectiveLimit) -> QuotaError {
        let EffectiveLimit { percent, source } = limit;

        let condition = match self {
            Resource::ResidentMemory => format!(
                "Resident memory usage is at {used_percent}% of total memory, \
                 exceeding the configured limit of {percent}%",
            ),
            Resource::DiskUsage => format!(
                "Disk usage is at {used_percent}% of total capacity, \
                 exceeding the configured limit of {percent}%",
            ),
        };

        let remedy = match self {
            Resource::ResidentMemory => {
                "Reduce memory usage (e.g. delete points or drop collections)"
            }
            Resource::DiskUsage => "Reduce disk usage (e.g. delete points or drop collections)",
        };

        QuotaError::LimitReached(format!(
            "{condition}. Help: {remedy}, or raise {}.",
            source.describe(self.parameter()),
        ))
    }
}

/// Where an effective limit was configured, so a rejected request can point at
/// the knob that has to change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitSource {
    /// A per-request override, i.e. the collection's `strict_mode_config`.
    Override,
    /// Cluster-wide quota config.
    Quota,
}

impl LimitSource {
    fn describe(self, parameter: &str) -> String {
        match self {
            LimitSource::Override => {
                format!("`{parameter}` in the strict mode config of this collection")
            }
            LimitSource::Quota => format!("`{parameter}` in the global quota config"),
        }
    }
}

/// A limit that is in effect, and where it came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectiveLimit {
    pub percent: u8,
    pub source: LimitSource,
}

impl EffectiveLimit {
    /// Resolve a single limit from the quota's own value and a caller-supplied
    /// override.
    ///
    /// An override can only ever tighten: the stricter of the two wins, so no
    /// caller can grant itself more of a resource than the cluster-wide quota
    /// allows. A resource the quota leaves uncapped is governed by the override
    /// alone.
    pub fn resolve(override_limit: Option<u8>, quota: Option<u8>) -> Option<Self> {
        let override_limit = override_limit.map(|percent| EffectiveLimit {
            percent,
            source: LimitSource::Override,
        });
        let quota = quota.map(|percent| EffectiveLimit {
            percent,
            source: LimitSource::Quota,
        });

        match (override_limit, quota) {
            (Some(override_limit), Some(quota)) if override_limit.percent < quota.percent => {
                Some(override_limit)
            }
            // Includes the tie: when both name the same percentage it is the
            // quota that binds, and raising the override would not lift the
            // rejection — so the message has to point at the quota.
            (_, Some(quota)) => Some(quota),
            (override_limit, None) => override_limit,
        }
    }
}

/// `used` as a percentage of `total`, clamped to 100 and `None` for a zero total.
pub fn percent_of(used: u64, total: u64) -> Option<u8> {
    if total == 0 {
        return None;
    }
    // Widened, not saturated: saturating the multiply would divide a capped
    // numerator by a huge total and report a *lower* utilization than the truth,
    // which is the one direction a quota must never round.
    let percent = u128::from(used) * 100 / u128::from(total);
    Some(percent.min(100) as u8)
}

/// Total system memory (or cgroup limit) in bytes. Cached once — total memory
/// is effectively constant for a running process.
pub fn total_memory_bytes() -> u64 {
    static TOTAL: OnceLock<u64> = OnceLock::new();
    *TOTAL.get_or_init(|| segment::utils::mem::Mem::new().total_memory_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quota(percent: u8) -> EffectiveLimit {
        EffectiveLimit {
            percent,
            source: LimitSource::Quota,
        }
    }

    fn override_limit(percent: u8) -> EffectiveLimit {
        EffectiveLimit {
            percent,
            source: LimitSource::Override,
        }
    }

    #[test]
    fn an_override_can_only_tighten_the_quota() {
        // Stricter than the quota: the override binds
        assert_eq!(
            EffectiveLimit::resolve(Some(70), Some(90)),
            Some(override_limit(70)),
        );

        // Laxer than the quota: the quota still binds. Anyone who can edit a
        // collection must not be able to buy it more of a node-wide resource
        // than the cluster-wide quota allows.
        assert_eq!(EffectiveLimit::resolve(Some(99), Some(90)), Some(quota(90)));
        assert_eq!(
            EffectiveLimit::resolve(Some(100), Some(90)),
            Some(quota(90))
        );

        // Equal: the quota is the one that has to be raised to lift it
        assert_eq!(EffectiveLimit::resolve(Some(90), Some(90)), Some(quota(90)));
    }

    #[test]
    fn a_limit_set_on_only_one_side_applies_on_its_own() {
        assert_eq!(EffectiveLimit::resolve(None, Some(90)), Some(quota(90)));
        // Nothing to tighten against — an uncapped resource takes the override
        assert_eq!(
            EffectiveLimit::resolve(Some(90), None),
            Some(override_limit(90))
        );
        assert_eq!(EffectiveLimit::resolve(None, None), None);
    }

    #[test]
    fn a_rejection_names_the_knob_that_has_to_change() {
        let message = Resource::DiskUsage
            .rejected(
                95,
                EffectiveLimit {
                    percent: 90,
                    source: LimitSource::Quota,
                },
            )
            .to_string();
        assert!(message.contains("Disk usage is at 95%"), "{message}");
        assert!(
            message.contains("`max_disk_usage_percent` in the global quota config"),
            "{message}",
        );

        let message = Resource::ResidentMemory
            .rejected(
                95,
                EffectiveLimit {
                    percent: 90,
                    source: LimitSource::Override,
                },
            )
            .to_string();
        assert!(
            message.contains("Resident memory usage is at 95%"),
            "{message}"
        );
        assert!(
            message.contains("`max_resident_memory_percent` in the strict mode config"),
            "{message}",
        );
    }

    #[test]
    fn utilization_is_a_clamped_percentage() {
        assert_eq!(percent_of(0, 100), Some(0));
        assert_eq!(percent_of(90, 100), Some(90));
        // Rounds down, so a limit is only reached once it is fully reached.
        assert_eq!(percent_of(999, 1_000), Some(99));
        // Filesystems that report `available > total` saturate rather than wrap.
        assert_eq!(percent_of(200, 100), Some(100));
        // A zero-sized filesystem is not a 100%-full one.
        assert_eq!(percent_of(0, 0), None);
        assert_eq!(percent_of(u64::MAX, u64::MAX), Some(100));
    }
}
