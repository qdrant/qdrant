use std::path::PathBuf;

use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::operations::types::{CollectionError, CollectionResult};

/// Defines how frequent the check of the disk usage should be, depending on the free space
///
/// The idea is that if we have a lot of disk space, it is unlikely that it will be out of space soon,
/// so we can check it less frequently. But if we have a little space, we should check it more often
/// and at some point, we should check it every time
const FREE_SPACE_TO_CHECK_FREQUENCY_HEURISTIC_MB: &[(usize, usize); 4] =
    &[(512, 0), (1024, 10), (2048, 20), (8096, 100)];

/// Even if there were no many updates, we still want to force the check of the disk usage
/// because some external process could have consumed the disk space
const MIN_DISK_CHECK_INTERVAL_MILLIS: usize = 2000;

#[derive(Default)]
struct LastCheck {
    last_check_time: Option<Instant>,
    next_check_count: usize,
}

pub struct DiskUsageWatcher {
    disk_path: PathBuf,
    disabled: bool,
    min_free_disk_size_mb: usize,
    last_check: Mutex<LastCheck>,
}

impl DiskUsageWatcher {
    pub async fn new(disk_path: PathBuf, min_free_disk_size_mb: usize) -> Self {
        let mut watcher = Self {
            disk_path,
            disabled: false,
            min_free_disk_size_mb,
            last_check: Default::default(),
        };
        match watcher.is_disk_full().await {
            Ok(Some(_)) => {} // do nothing
            Ok(None) => watcher.disabled = true,
            Err(_) => {
                watcher.disabled = true;
            }
        };
        watcher
    }
    /// Returns true if the disk free space is less than the `disk_buffer_threshold_mb`
    /// As the side effect, it updates the disk usage every `update_count_threshold` calls
    pub async fn is_disk_full(&self) -> CollectionResult<Option<bool>> {
        if self.disabled {
            return Ok(None);
        }
        let mut last_check_guard = self.last_check.lock().await;

        let since_last_check = last_check_guard
            .last_check_time
            .map(|x| x.elapsed().as_millis() as usize)
            .unwrap_or(usize::MAX);

        if last_check_guard.next_check_count == 0
            || since_last_check >= MIN_DISK_CHECK_INTERVAL_MILLIS
        {
            let free_space = self.get_free_space_bytes().await?;

            last_check_guard.last_check_time = Some(Instant::now());

            let is_full = match free_space {
                Some(free_space) => {
                    let free_space = free_space as usize;
                    let mut next_check = 0;
                    for (threshold_mb, interval) in FREE_SPACE_TO_CHECK_FREQUENCY_HEURISTIC_MB {
                        if free_space < (*threshold_mb * 1024 * 1024) {
                            next_check = *interval;
                            break;
                        }
                    }
                    last_check_guard.next_check_count = next_check;

                    Some(free_space < self.min_free_disk_size_mb * 1024 * 1024)
                }
                None => {
                    last_check_guard.next_check_count = 0;
                    None
                }
            };

            Ok(is_full)
        } else {
            last_check_guard.next_check_count = last_check_guard.next_check_count.saturating_sub(1);
            Ok(None)
        }
    }

    /// Return current disk usage in bytes, if available
    pub async fn get_free_space_bytes(&self) -> CollectionResult<Option<u64>> {
        if self.disabled {
            return Ok(None);
        }
        let path = self.disk_path.clone();
        let result = tokio::task::spawn_blocking(move || fs4::available_space(path.as_path()))
            .await
            .map_err(|e| {
                CollectionError::service_error(format!("Failed to join async task: {}", e))
            })?;

        let result = match result {
            Ok(result) => Some(result),
            Err(err) => {
                log::debug!(
                    "Failed to get free space for path: {} due to: {}",
                    self.disk_path.as_path().display(),
                    err
                );
                None
            }
        };
        Ok(result)
    }
}
