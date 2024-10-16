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

/// Move file from one location to another.
/// Handles the case when the source and destination are on different filesystems.
pub async fn move_file(from: impl AsRef<Path>, to: impl AsRef<Path>) -> CollectionResult<()> {
    let from = from.as_ref();
    let to = to.as_ref();

    // Try to rename first and fallback to copy to prevent TOCTOU.
    if let Ok(()) = tokio::fs::rename(from, to).await {
        return Ok(());
    }

    // If rename failed, try to copy.
    // It is possible that the source and destination are on different filesystems.
    if let Err(err) = tokio::fs::copy(from, to).await {
        cleanup_file(to).await;
        return Err(CollectionError::service_error(format!(
            "Can't move file from {} to {} due to {}",
            from.display(),
            to.display(),
            err
        )));
    }

    if let Err(err) = tokio::fs::remove_file(from).await {
        cleanup_file(to).await;
        return Err(CollectionError::service_error(format!(
            "Can't remove file {} due to {}",
            from.display(),
            err
        )));
    }

    Ok(())
}

/// Remove the file if it exists. Print a warning if the file can't be removed.
async fn cleanup_file(path: &Path) {
    if let Err(err) = tokio::fs::remove_file(path).await {
        if err.kind() != std::io::ErrorKind::NotFound {
            log::warn!("Failed to remove file {}: {err}", path.display());
        }
    }
}
