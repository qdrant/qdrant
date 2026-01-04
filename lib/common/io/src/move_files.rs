use std::io;
use std::path::{Path, PathBuf};

use fs_err as fs;
use fs_extra::dir::CopyOptions;

/// Move directory from one location to another
///
/// Handles the case when the source and destination are on different filesystems.
pub fn move_dir(from: impl Into<PathBuf>, to: impl Into<PathBuf>) -> io::Result<()> {
    let from = from.into();
    let to = to.into();

    let Err(_err) = fs::rename(&from, &to) else {
        return Ok(());
    };

    if !to.exists() {
        fs::create_dir(&to)?;
    }

    let opts = CopyOptions::new().content_only(true).overwrite(true);

    fs_extra::dir::move_dir(&from, &to, &opts).map_err(|err| {
        io::Error::other(format!(
            "failed to move directory {} to {}: {err}",
            from.display(),
            to.display(),
        ))
    })?;
    Ok(())
}

/// Move file from one location to another.
/// Handles the case when the source and destination are on different filesystems.
pub fn move_file(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    let from = from.as_ref();
    let to = to.as_ref();

    // Try to rename first and fallback to copy to prevent TOCTOU.
    if let Ok(()) = fs::rename(from, to) {
        return Ok(());
    }

    // If rename failed, try to copy.
    // It is possible that the source and destination are on different filesystems.
    if let Err(err) = fs::copy(from, to) {
        let _res = cleanup_file(to);
        return Err(err);
    }

    if let Err(err) = fs::remove_file(from) {
        let _res = cleanup_file(to);
        return Err(err);
    }

    Ok(())
}

/// Remove the file if it exists. Print a warning if the file can't be removed.
fn cleanup_file(path: &Path) -> io::Result<()> {
    if let Err(err) = fs::remove_file(path)
        && err.kind() != std::io::ErrorKind::NotFound
    {
        return Err(err);
    }
    Ok(())
}
