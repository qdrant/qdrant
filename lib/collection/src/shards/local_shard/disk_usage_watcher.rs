use std::path::PathBuf;

use tokio_util::task::AbortOnDropHandle;

use crate::operations::types::{CollectionError, CollectionResult};

const MB: u64 = 1024 * 1024;

/// Below this much free space, ask for a fresh reading on every check instead of
/// reusing a cached one. The WAL threshold itself is only a few MB, which a disk
/// under load can cross well inside the caching interval — this is the margin
/// that keeps us from finding out too late.
const WATCH_BELOW_MB: u64 = 512;

/// Watches whether the shard's WAL still has room to write.
///
/// Measuring is delegated to [`shard::quota::QuotaManager`], the node's single
/// reader of disk usage, which caches readings above [`WATCH_BELOW_MB`] and takes
/// a fresh one below it.
pub struct DiskUsageWatcher {
    disk_path: PathBuf,
    /// Set when the disk cannot be read at all, so we stop asking.
    disabled: bool,
    min_free_disk_size_mb: usize,
}

impl DiskUsageWatcher {
    pub async fn new(disk_path: PathBuf, min_free_disk_size_mb: usize) -> Self {
        let mut watcher = Self {
            disk_path,
            disabled: false,
            min_free_disk_size_mb,
        };
        match watcher.is_disk_full().await {
            Ok(Some(_)) => {} // do nothing
            Ok(None) | Err(_) => watcher.disabled = true,
        };
        watcher
    }

    /// Returns true if the disk free space is less than the `disk_buffer_threshold_mb`,
    /// or `None` when free space cannot be determined.
    pub async fn is_disk_full(&self) -> CollectionResult<Option<bool>> {
        let free_space = self.get_free_space_bytes().await?;
        Ok(free_space.map(|free_space| free_space < self.min_free_bytes()))
    }

    /// Return current disk usage in bytes, if available
    pub async fn get_free_space_bytes(&self) -> CollectionResult<Option<u64>> {
        if self.disabled {
            return Ok(None);
        }

        let path = self.disk_path.clone();
        let watch_below = self.min_free_bytes().max(WATCH_BELOW_MB * MB);

        // Still off the async thread: a cache miss goes down to `statvfs`.
        let free_space = AbortOnDropHandle::new(tokio::task::spawn_blocking(move || {
            shard::quota::global().available_bytes(&path, watch_below)
        }))
        .await
        .map_err(|e| CollectionError::service_error(format!("Failed to join async task: {e}")))?;

        if free_space.is_none() {
            log::debug!(
                "Failed to get free space for path: {}",
                self.disk_path.as_path().display(),
            );
        }

        Ok(free_space)
    }

    /// Free space below which the WAL refuses to grow.
    fn min_free_bytes(&self) -> u64 {
        self.min_free_disk_size_mb as u64 * MB
    }
}
