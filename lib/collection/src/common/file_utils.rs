use std::path::{Path, PathBuf};

use fs_extra::dir::CopyOptions;

use crate::operations::types::{CollectionError, CollectionResult};

/// Move directory from one location to another.
/// Handles the case when the source and destination are on different filesystems.
pub async fn move_dir(from: impl Into<PathBuf>, to: impl Into<PathBuf>) -> CollectionResult<()> {
    // Try to rename first and fallback to copy to prevent TOCTOU
    let from = from.into();
    let to = to.into();

    if let Err(_err) = tokio::fs::rename(&from, &to).await {
        // If rename failed, try to copy.
        // It is possible that the source and destination are on different filesystems.
        let task = tokio::task::spawn_blocking(move || {
            let options = CopyOptions::new().copy_inside(true);
            fs_extra::dir::move_dir(&from, &to, &options).map_err(|err| {
                CollectionError::service_error(format!(
                    "Can't move dir from {} to {} due to {}",
                    from.display(),
                    to.display(),
                    err
                ))
            })
        });
        task.await??;
    }
    Ok(())
}

pub async fn move_file(from: impl AsRef<Path>, to: impl AsRef<Path>) -> CollectionResult<()> {
    // Try to rename first and fallback to copy to prevent TOCTOU
    let from = from.as_ref();
    let to = to.as_ref();

    if let Err(_err) = tokio::fs::rename(from, to).await {
        // If rename failed, try to copy.
        // It is possible that the source and destination are on different filesystems.
        tokio::fs::copy(from, to).await.map_err(|err| {
            CollectionError::service_error(format!(
                "Can't move file from {} to {} due to {}",
                from.display(),
                to.display(),
                err
            ))
        })?;

        tokio::fs::remove_file(from).await.map_err(|err| {
            CollectionError::service_error(format!(
                "Can't remove file {} due to {}",
                from.display(),
                err
            ))
        })?;
    }
    Ok(())
}

/// Guard, that ensures that file will be deleted on drop.
pub struct FileCleaner {
    path: Option<PathBuf>,
}

impl FileCleaner {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: Some(path.into()),
        }
    }

    pub fn delete(&mut self) {
        if let Some(path) = &self.path {
            // Ignore errors, because file can be already deleted.
            if path.is_dir() {
                let _ = std::fs::remove_dir_all(path);
            } else {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    pub fn forget(&mut self) {
        self.path = None;
    }
}

impl Drop for FileCleaner {
    fn drop(&mut self) {
        self.delete();
    }
}
