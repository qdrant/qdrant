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

/// Guard, ensures that a file or directory is deleted on drop.
pub struct TempPath {
    path: Option<PathBuf>,
}

impl TempPath {
    /// Delete the file or directory now
    ///
    /// # Error
    ///
    /// Returns an error if the file or directory was already deleted, or if it failed to delete it
    /// now.
    ///
    /// To ignore errors, use `let _ = temp_path.delete();`.
    pub fn delete(mut self) -> std::io::Result<()> {
        match self.path.take() {
            Some(ref path) => Self::delete_path(path),
            None => Ok(()),
        }
    }

    /// Keep the file or directory, do not delete on drop
    pub fn keep(mut self) {
        self.path.take();
    }

    fn delete_path(path: &Path) -> std::io::Result<()> {
        if path.is_dir() {
            std::fs::remove_dir_all(path)
        } else {
            std::fs::remove_file(path)
        }
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        // Ignore errors, may have been deleted already
        if let Some(ref path) = self.path.take() {
            let _ = Self::delete_path(path);
        }
    }
}

impl<P> From<P> for TempPath
where
    P: Into<PathBuf>,
{
    fn from(path: P) -> Self {
        Self {
            path: Some(path.into())
        }
    }
}
