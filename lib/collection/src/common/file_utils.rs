use std::path::PathBuf;

use fs_extra::dir::CopyOptions;

use crate::operations::types::{CollectionError, CollectionResult};

/// Move directory from one location to another.
/// Handles the case when the source and destination are on different filesystems.
pub async fn move_dir(from: impl Into<PathBuf>, to: impl Into<PathBuf>) -> CollectionResult<()> {
    // Try to rename first and fallback to copy to prevert TOCTOU
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
