//! Filesystem disk usage reader.
//!
//! Returns `None` when the underlying call fails (e.g. the path does not exist
//! yet, permission denied). Callers should treat `None` as "disk check
//! unavailable on this invocation" and skip the check, matching the behaviour of
//! [`crate::memory_usage::resident_bytes`].
//!
//! Reading is not free — it is two `statvfs`/`GetDiskFreeSpaceEx` calls — and
//! this module deliberately does not cache. Deciding when a sample is stale
//! needs to know what the sample is compared against, so that policy lives with
//! the caller that owns the limits (`storage::quota::QuotaManager`).

use std::path::Path;

/// Snapshot of disk capacity for a single filesystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskUsage {
    /// Total capacity of the filesystem in bytes.
    pub total: u64,
    /// Bytes currently free (as reported by the OS — for unprivileged
    /// processes this is typically what `df` shows in the "Available"
    /// column).
    pub available: u64,
}

impl DiskUsage {
    /// Bytes currently used on the filesystem, saturating at 0 if the OS
    /// reports `available > total` (can happen on quota-backed filesystems).
    pub fn used(&self) -> u64 {
        self.total.saturating_sub(self.available)
    }
}

/// Current disk usage of the filesystem hosting `path`, read fresh on every
/// call.
pub fn disk_usage(path: &Path) -> Option<DiskUsage> {
    let total = fs4::total_space(path).ok()?;
    let available = fs4::available_space(path).ok()?;
    Some(DiskUsage { total, available })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_returns_sensible_values_for_tempdir() {
        let dir = tempfile::tempdir().unwrap();
        let usage = disk_usage(dir.path()).expect("disk usage should be readable");
        assert!(usage.total > 0, "total should be > 0");
        assert!(
            usage.available <= usage.total,
            "available ({}) must be <= total ({})",
            usage.available,
            usage.total,
        );
    }

    #[test]
    fn missing_path_does_not_panic() {
        // We intentionally don't assert `is_none()` here: on Windows
        // `GetDiskFreeSpaceEx` succeeds for non-existent paths by resolving
        // up to the containing drive, while on Unix `statvfs` returns
        // ENOENT. Both behaviours are fine — what matters is that the
        // reader never panics and the result is well-formed when present.
        let missing = Path::new("/this/path/should/not/exist/qdrant-disk-usage-test");
        if let Some(usage) = disk_usage(missing) {
            assert!(usage.total > 0);
            assert!(usage.available <= usage.total);
        }
    }
}
