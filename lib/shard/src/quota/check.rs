use super::error::QuotaError;

/// A resource that [`super::QuotaManager`] measures and caps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resource {
    ResidentMemory,
    DiskUsage,
}

impl Resource {
    /// The parameter governing this resource in the quota config.
    fn parameter(self) -> &'static str {
        match self {
            Resource::ResidentMemory => "max_resident_memory_percent",
            Resource::DiskUsage => "max_disk_usage_percent",
        }
    }

    /// The user-facing rejection, naming the condition that tripped and the
    /// parameter governing it.
    pub(super) fn rejected(self, used_percent: u8, limit: u8) -> QuotaError {
        let condition = match self {
            Resource::ResidentMemory => format!(
                "Resident memory usage is at {used_percent}% of total memory, \
                 exceeding the configured limit of {limit}%",
            ),
            Resource::DiskUsage => format!(
                "Disk usage is at {used_percent}% of total capacity, \
                 exceeding the configured limit of {limit}%",
            ),
        };

        let remedy = match self {
            Resource::ResidentMemory => {
                "Reduce memory usage (e.g. delete points or drop collections)"
            }
            Resource::DiskUsage => "Reduce disk usage (e.g. delete points or drop collections)",
        };

        QuotaError::LimitReached(format!(
            "{condition}. Help: {remedy}, or raise `{}` in the global quota config.",
            self.parameter(),
        ))
    }
}

/// `used` as a percentage of `total`, clamped to 100 and `None` for a zero total.
pub fn percent_of(used: u64, total: u64) -> Option<u8> {
    if total == 0 {
        return None;
    }
    // Widened, not saturated: a saturating multiply divides a capped numerator by
    // a huge total and under-reports utilization — the one direction a quota must
    // never round.
    let percent = u128::from(used) * 100 / u128::from(total);
    Some(percent.min(100) as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utilization_is_a_clamped_percentage() {
        assert_eq!(percent_of(0, 100), Some(0));
        // Rounds down, so a limit is only reached once it is fully reached
        assert_eq!(percent_of(999, 1_000), Some(99));
        // Filesystems that report `available > total` saturate rather than wrap
        assert_eq!(percent_of(200, 100), Some(100));
        // A zero-sized filesystem is not a 100%-full one
        assert_eq!(percent_of(0, 0), None);
        assert_eq!(percent_of(u64::MAX, u64::MAX), Some(100));
    }
}
