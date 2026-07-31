use std::path::PathBuf;

use tokio_util::task::AbortOnDropHandle;

use crate::operations::types::{CollectionError, CollectionResult};

/// Watches whether the shard's WAL still has room to write.
///
/// Measuring is delegated to [`shard::quota::QuotaManager`], the node's single
/// reader of disk usage — which also means this no longer has to ration the
/// reads itself: the quota manager caches each measurement, and re-measures on
/// every call once the disk is close enough to full for that to matter.
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
        Ok(free_space
            .map(|free_space| (free_space as usize) < self.min_free_disk_size_mb * 1024 * 1024))
    }

    /// Return current disk usage in bytes, if available
    pub async fn get_free_space_bytes(&self) -> CollectionResult<Option<u64>> {
        if self.disabled {
            return Ok(None);
        }

        let path = self.disk_path.clone();

        // Still off the async thread: a cache miss goes down to `statvfs`.
        let free_space = AbortOnDropHandle::new(tokio::task::spawn_blocking(move || {
            shard::quota::global().available_bytes(&path)
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
}
