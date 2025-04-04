use std::path::{Path, PathBuf};

use fs_extra::dir::CopyOptions;

use crate::operations::types::{CollectionError, CollectionResult};

/// Move directory from one location to another.
/// Handles the case when the source and destination are on different filesystems.
pub async fn move_dir(from: impl Into<PathBuf>, to: impl Into<PathBuf>) -> CollectionResult<()> {
    let from = from.into();
    let to = to.into();

    log::trace!("Renaming directory {} to {}", from.display(), to.display());

    let Err(err) = tokio::fs::rename(&from, &to).await else {
        return Ok(());
    };

    log::trace!(
        "Failed to rename directory {} to {}: {err}",
        from.display(),
        to.display(),
    );

    // TODO: Only retry to move directory, if error kind is `CrossesDevices` or `AlreadyExists`?
    //
    // match err.kind() {
    //     io::ErrorKind::AlreadyExists | io::ErrorKind::CrossesDevices => (),
    //     _ => {
    //         return Err(CollectionError::service_error(format!(
    //             "failed to rename directory {} to {}: {err}",
    //             from.display(),
    //             to.display(),
    //         )));
    //     }
    // }

    if !to.exists() {
        log::trace!("Creating destination directory {}", to.display());

        tokio::fs::create_dir(&to).await.map_err(|err| {
            CollectionError::service_error(format!(
                "failed to move directory {} to {}: \
                 failed to create destination directory: \
                 {err}",
                from.display(),
                to.display(),
            ))
        })?;
    }

    tokio::task::spawn_blocking(move || {
        log::trace!("Moving directory {} to {}", from.display(), to.display());

        let opts = CopyOptions::new().content_only(true).overwrite(true);

        fs_extra::dir::move_dir(&from, &to, &opts).map_err(|err| {
            CollectionError::service_error(format!(
                "failed to move directory {} to {}: {err}",
                from.display(),
                to.display(),
            ))
        })
    })
    .await??;

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
