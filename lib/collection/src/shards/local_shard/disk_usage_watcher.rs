use std::path::PathBuf;

use crate::operations::types::{CollectionError, CollectionResult};

pub struct DiskUsageWatcher {
    disk_path: PathBuf,
    disk_free_space_bytes: u64,
    disk_buffer_threshold_mb: u64,
    update_count_threshold: u64,
    // The number of times the disk usage has been checked since the last update
    count_since_last_update: u64,
    // DiskUsageWatcher is disabled if it failed to create a new instance
    // This may happen if the platform or environment does not support the required APIs
    disabled: bool,
}

impl DiskUsageWatcher {
    pub async fn new(disk_path: PathBuf, disk_buffer_threshold_mb: u64) -> Self {
        let mut watcher = Self {
            disk_path,
            disk_free_space_bytes: 0,
            disk_buffer_threshold_mb,
            count_since_last_update: 0,
            update_count_threshold: 1000, //  <TODO> A thousand vectors with dimensionality 1500 are 6 megabytes. Fine tune later
            disabled: false,
        };
        match watcher.update_disk_usage().await {
            Ok(_) => watcher,
            Err(e) => {
                log::warn!("Failed to get disk usage: {}", e);
                watcher.disabled = true;
                watcher
            }
        }
    }
    /// Returns true if the disk free space is less than the `disk_buffer_threshold_mb`
    /// As the side effect, it updates the disk usage every `update_count_threshold` calls
    pub async fn is_disk_full(&mut self) -> bool {
        if self.disabled {
            return false;
        }
        let free_space = self.get_free_space_bytes().await;
        free_space < self.disk_buffer_threshold_mb * 1024 * 1024
    }
    /// Returns the number of bytes of free space on the disk
    /// As the side effect, it updates the disk usage every `update_count_threshold` calls
    pub async fn get_free_space_bytes(&mut self) -> u64 {
        if self.disabled {
            return u64::MAX;
        }
        self.count_since_last_update += 1;
        if self.count_since_last_update >= self.update_count_threshold {
            self.update_disk_usage().await.unwrap_or_else(|e| {
                log::warn!("Failed to update disk usage: {}", e);
            });
            self.count_since_last_update = 0;
        }
        self.disk_free_space_bytes
    }
    /// Updates the disk usage
    pub async fn update_disk_usage(&mut self) -> CollectionResult<()> {
        if self.disabled {
            return Ok(());
        }
        let path = self.disk_path.clone();
        let disk_free_space_bytes: u64 =
            tokio::task::spawn_blocking(move || fs4::available_space(path.as_path()))
                .await
                .map_err(|e| {
                    CollectionError::service_error(format!("Failed to join async task: {}", e))
                })?
                .map_err(|err| {
                    CollectionError::service_error(format!(
                        "Failed to get free space for path: {} due to: {}",
                        self.disk_path.as_path().display(),
                        err
                    ))
                })?;
        self.disk_free_space_bytes = disk_free_space_bytes;
        Ok(())
    }
}
