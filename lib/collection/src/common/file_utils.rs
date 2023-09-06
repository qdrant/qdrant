use std::fmt;
use std::path::{Path, PathBuf};

use crate::operations::types::{CollectionError, CollectionResult};

/// Move directory from one location to another.
/// Handles the case when the source and destination are on different filesystems.
pub async fn move_dir(from: impl Into<PathBuf>, to: impl Into<PathBuf>) -> CollectionResult<()> {
    let from = from.into();
    let to = to.into();

    // Try to rename first and fallback to copy to prevert TOCTOU
    let Err(err) = tokio::fs::rename(&from, &to).await else {
        return Ok(());
    };

    log_rename_error("directory", &from, &to, err);

    tokio::task::spawn_blocking(move || {
        fs_extra::dir::move_dir(&from, &to, &Default::default())
            .map_err(|err| move_error("directory", &from, &to, err))
    })
    .await??;

    Ok(())
}

pub async fn move_file(from: impl Into<PathBuf>, to: impl Into<PathBuf>) -> CollectionResult<()> {
    let from = from.into();
    let to = to.into();

    // Try to rename first and fallback to copy to prevert TOCTOU
    let Err(err) = tokio::fs::rename(&from, &to).await else {
        return Ok(());
    };

    log_rename_error("file", &from, &to, err);

    tokio::task::spawn_blocking(move || {
        fs_extra::file::move_file(&from, &to, &Default::default())
            .map_err(|err| move_error("file", &from, &to, err))
    })
    .await??;

    Ok(())
}

fn log_rename_error(resource: &str, from: &Path, to: &Path, err: impl fmt::Display) {
    log::error!(
        "Failed to rename {resource} {} to {}: {err}",
        from.display(),
        to.display()
    );
}

fn move_error(resource: &str, from: &Path, to: &Path, err: impl fmt::Display) -> CollectionError {
    CollectionError::service_error(format!(
        "failed to move {resource} {} to {}: {err}",
        from.display(),
        to.display()
    ))
}
