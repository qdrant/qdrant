//! Filesystem disk usage reader with a small time-based cache.
//!
//! Strict mode uses this to reject disk-consuming updates when the volume
//! hosting Qdrant's storage is close to full. Each call would otherwise
//! invoke `statvfs`/`GetDiskFreeSpaceEx`, which — although cheap individually —
//! adds up under high request rates. We cache the most recently observed
//! value per path for [`CACHE_TTL`].
//!
//! Returns `None` when the underlying call fails (e.g. the path does not
//! exist yet, permission denied). Callers should treat `None` as "disk
//! check unavailable on this invocation" and skip the check, matching the
//! behaviour of [`crate::memory_usage::resident_bytes`].

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use ahash::AHashMap;

/// How long to reuse a previously observed `(total, available)` pair before
/// refreshing. Chosen to match the resident-memory cache so both strict-mode
/// checks have the same reaction latency.
const CACHE_TTL: Duration = Duration::from_secs(5);

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

static CACHE: Mutex<Option<AHashMap<PathBuf, (Instant, DiskUsage)>>> = Mutex::new(None);

/// Returns current disk usage for the filesystem hosting `path`, served from
/// a cached value that refreshes at most once per [`CACHE_TTL`] per path.
pub fn disk_usage(path: &Path) -> Option<DiskUsage> {
    let now = Instant::now();

    // Fast path: cache hit.
    if let Ok(mut guard) = CACHE.lock() {
        let map = guard.get_or_insert_with(AHashMap::new);
        if let Some((cached_at, value)) = map.get(path)
            && now.duration_since(*cached_at) < CACHE_TTL
        {
            return Some(*value);
        }
    }

    // Cache miss or stale — query the OS. Both `total_space` and
    // `available_space` are independent stat calls; keep them outside the
    // lock so concurrent callers for different paths don't serialise.
    let usage = read_disk_usage(path)?;

    if let Ok(mut guard) = CACHE.lock() {
        let map = guard.get_or_insert_with(AHashMap::new);
        map.insert(path.to_path_buf(), (now, usage));
    }

    Some(usage)
}

/// Direct, uncached read of disk usage. Exposed for callers that want to
/// bypass the cache (e.g. tests).
pub fn read_disk_usage(path: &Path) -> Option<DiskUsage> {
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
        let usage = read_disk_usage(dir.path()).expect("disk usage should be readable");
        assert!(usage.total > 0, "total should be > 0");
        assert!(
            usage.available <= usage.total,
            "available ({}) must be <= total ({})",
            usage.available,
            usage.total,
        );
    }

    #[test]
    fn cached_disk_usage_returns_value() {
        let dir = tempfile::tempdir().unwrap();
        let first = disk_usage(dir.path()).expect("first call should populate cache");
        let second = disk_usage(dir.path()).expect("second call should hit cache");
        // Total capacity should not change between two adjacent calls — this
        // also exercises the cache hot path.
        assert_eq!(first.total, second.total);
    }

    #[test]
    fn missing_path_does_not_panic() {
        // We intentionally don't assert `is_none()` here: on Windows
        // `GetDiskFreeSpaceEx` succeeds for non-existent paths by resolving
        // up to the containing drive, while on Unix `statvfs` returns
        // ENOENT. Both behaviours are fine — what matters is that the
        // reader never panics and the result is well-formed when present.
        let missing = Path::new("/this/path/should/not/exist/qdrant-disk-usage-test");
        if let Some(usage) = read_disk_usage(missing) {
            assert!(usage.total > 0);
            assert!(usage.available <= usage.total);
        }
    }
}
