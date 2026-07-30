use std::sync::OnceLock;

use ::common::disk_usage::DiskUsage;

use crate::content_manager::errors::{StorageError, StorageResult};

/// Where an effective limit was configured, so a rejected request can point at
/// the knob that has to change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitSource {
    /// Per-collection `strict_mode_config`.
    StrictMode,
    /// Cluster-wide quota config.
    Quota,
}

impl LimitSource {
    fn describe(self, parameter: &str) -> String {
        match self {
            LimitSource::StrictMode => {
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
    /// Resolve a single limit: a value set in an enabled strict mode config wins,
    /// the quota provides the default.
    pub fn resolve(strict_mode: Option<u8>, quota: Option<u8>) -> Option<Self> {
        strict_mode
            .map(|percent| EffectiveLimit {
                percent,
                source: LimitSource::StrictMode,
            })
            .or_else(|| {
                quota.map(|percent| EffectiveLimit {
                    percent,
                    source: LimitSource::Quota,
                })
            })
    }
}

/// Reject a memory-consuming update if process resident memory is at or above
/// `limit`.
///
/// `resident_reader` returns process resident memory in bytes, or `None` when
/// the platform does not expose the stat; in that case, and when no limit is in
/// effect, the update is allowed through.
pub fn check_resident_memory(
    limit: Option<EffectiveLimit>,
    resident_reader: impl FnOnce() -> Option<usize>,
) -> StorageResult<()> {
    let Some(EffectiveLimit { percent, source }) = limit else {
        return Ok(());
    };

    let Some(used_percent) = resident_memory_percent(resident_reader) else {
        return Ok(());
    };

    if used_percent < percent {
        return Ok(());
    }

    Err(rejected(
        format!(
            "Resident memory usage is at {used_percent}% of total memory, \
             exceeding the configured limit of {percent}%",
        ),
        "Reduce memory usage (e.g. delete points or drop collections)",
        source,
        "max_resident_memory_percent",
    ))
}

/// Reject a disk-consuming update if the filesystem hosting the storage
/// directory is filled to or above `limit`.
///
/// `usage_reader` returns a disk usage snapshot, or `None` when the stat call
/// failed (e.g. path missing, permission denied); in that case, and when no
/// limit is in effect, the update is allowed through.
pub fn check_disk_usage(
    limit: Option<EffectiveLimit>,
    usage_reader: impl FnOnce() -> Option<DiskUsage>,
) -> StorageResult<()> {
    let Some(EffectiveLimit { percent, source }) = limit else {
        return Ok(());
    };

    let Some(used_percent) = disk_usage_percent(usage_reader) else {
        return Ok(());
    };

    if used_percent < percent {
        return Ok(());
    }

    Err(rejected(
        format!(
            "Disk usage is at {used_percent}% of total capacity, \
             exceeding the configured limit of {percent}%",
        ),
        "Reduce disk usage (e.g. delete points or drop collections)",
        source,
        "max_disk_usage_percent",
    ))
}

/// Process resident memory as a percentage of total system memory, or `None`
/// when either number is unavailable.
pub fn resident_memory_percent(resident_reader: impl FnOnce() -> Option<usize>) -> Option<u8> {
    let resident = resident_reader()?;
    percent_of(resident as u64, total_memory_bytes())
}

/// Used space of a filesystem as a percentage of its capacity, or `None` when
/// the snapshot is unavailable or reports a zero-sized filesystem.
pub fn disk_usage_percent(usage_reader: impl FnOnce() -> Option<DiskUsage>) -> Option<u8> {
    let usage = usage_reader()?;
    percent_of(usage.used(), usage.total)
}

/// `used` as a percentage of `total`, clamped to 100 and `None` for a zero total.
fn percent_of(used: u64, total: u64) -> Option<u8> {
    if total == 0 {
        return None;
    }
    Some(used.saturating_mul(100).div_euclid(total).min(100) as u8)
}

/// Total system memory (or cgroup limit) in bytes. Cached once — total memory
/// is effectively constant for a running process.
fn total_memory_bytes() -> u64 {
    static TOTAL: OnceLock<u64> = OnceLock::new();
    *TOTAL.get_or_init(|| segment::utils::mem::Mem::new().total_memory_bytes())
}

/// Build the user-facing rejection, naming the exact condition that tripped and
/// the parameter that governs it.
fn rejected(condition: String, remedy: &str, source: LimitSource, parameter: &str) -> StorageError {
    StorageError::bad_request(format!(
        "{condition}. Help: {remedy}, or raise {}.",
        source.describe(parameter),
    ))
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

    #[test]
    fn unreadable_stats_allow_the_update() {
        check_resident_memory(Some(quota(1)), || None).unwrap();
        check_disk_usage(Some(quota(1)), || None).unwrap();
        check_disk_usage(Some(quota(1)), || {
            Some(DiskUsage {
                total: 0,
                available: 0,
            })
        })
        .unwrap();
    }

    #[test]
    fn disk_usage_is_rejected_at_the_limit() {
        let full = || {
            Some(DiskUsage {
                total: 100,
                available: 10,
            })
        };

        check_disk_usage(Some(quota(91)), full).unwrap();
        let err = check_disk_usage(Some(quota(90)), full).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("Disk usage is at 90%"), "{message}");
        assert!(message.contains("global quota config"), "{message}");
    }

    #[test]
    fn strict_mode_limit_wins_over_the_quota_default() {
        assert_eq!(
            EffectiveLimit::resolve(Some(70), Some(90)),
            Some(EffectiveLimit {
                percent: 70,
                source: LimitSource::StrictMode,
            }),
        );
        assert_eq!(
            EffectiveLimit::resolve(None, Some(90)),
            Some(EffectiveLimit {
                percent: 90,
                source: LimitSource::Quota,
            }),
        );
        assert_eq!(EffectiveLimit::resolve(None, None), None);
    }
}
