use std::path::{Path, PathBuf};

use common::service_error::Context as _;
use fs_extra::dir::CopyOptions;

use crate::operations::types::CollectionResult;

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
            fs_extra::dir::move_dir(&from, &to, &options).with_context(|| {
                format!("Can't move dir from {} to {}", from.display(), to.display())
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
        tokio::fs::copy(from, to).await.with_context(|| {
            format!(
                "Can't move file from {} to {}",
                from.display(),
                to.display(),
            )
        })?;

        tokio::fs::remove_file(from)
            .await
            .with_context(|| format!("Can't remove file {}", from.display()))?;
    }
    Ok(())
}
