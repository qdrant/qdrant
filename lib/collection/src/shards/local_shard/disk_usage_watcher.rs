use std::path::PathBuf;

use crate::operations::types::{CollectionError, CollectionResult};

pub struct DiskUsageWatcher {
    pub(crate) disk_path: PathBuf,
    pub(crate) disk_free_space_bytes: u64,
    pub(crate) disk_buffer_threshold_mb: u64,
    // DiskUsageWathcer is disabled if it failed to create a new instance
    // This may happen if the platform or environment does not support the required APIs
    pub(crate) disabled: bool,
}

impl DiskUsageWatcher {
    pub async fn new(disk_path: PathBuf, disk_buffer_threshold_mb: u64) -> Self {
        let mut watcher = Self {
            disk_path,
            disk_free_space_bytes: 0,
            disk_buffer_threshold_mb,
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
    pub async fn is_disk_full(&self) -> bool {
        if self.disabled {
            return false;
        }
        let free_space = self.get_free_space_bytes().await;
        free_space < self.disk_buffer_threshold_mb * 1024 * 1024
    }
    pub async fn get_free_space_bytes(&self) -> u64 {
        if self.disabled {
            return u64::MAX;
        }
        self.disk_free_space_bytes
    }
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
