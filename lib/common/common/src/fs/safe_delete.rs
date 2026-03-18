//! Functions to safely (atomically) delete directories.
//!
//! Consider this situation:
//!
//! 1. We call `fs::remove_dir_all(segment_path)`.
//! 2. It deletes some files, but not all.
//! 3. The process crashes.
//! 4. After restart, we read the partially deleted directory.
//!
//! This module provides functions to avoid this situation.
//! Before deleting a directory, these functions atomically mark the directory
//! as deleted, by either
//! - moving it into a temporary directory ([`safe_delete_in_tmp`]),
//! - or renaming it with a `.deleted` suffix ([`safe_delete_with_suffix`]).
//!
//! # Alternatives to this module
//!
//! In our code base we also have other ways to avoid the above situation, e.g.:
//! - Mark directories as incomplete by either creating or deleting marker file,
//!   e.g., `shard_{shard_id}.initializing` or `shard_config.json`.

use std::ffi::OsStr;
use std::io;
use std::path::Path;

use fs_err as fs;
use fs_err::tokio as tokio_fs;
use tempfile::TempDir;

/// Safe delete a directory by moving it into a temporary directory first.
///
/// Allows deleting in the background without blocking the main thread.
/// This function only does the first part. The caller should do the second
/// part by calling [`TempDir::close()`].
///
/// ```ignore
/// // Option 1: delete immediately in the current thread
/// safe_delete_in_tmp(&segment_path, &tmp)?.close()?;
///
/// // Option 2: delete in background thread
/// let to_delete = safe_delete_in_tmp(&segment_path, &tmp)?;
/// let handle = std::thread::spawn(move || to_delete.close());
/// handle.join()??;
/// ```
#[must_use = "You need to call TempDir::close() explicitly."]
pub fn safe_delete_in_tmp(path: &Path, temp_dir: &Path) -> io::Result<TempDir> {
    let (parent, _file_name) = split(path).ok_or_else(|| err_invalid_path(path))?;
    fs::create_dir_all(temp_dir)?;
    let to_delete = tempfile::Builder::new().prefix("").tempdir_in(temp_dir)?;
    fs::rename(path, &to_delete)?;
    if cfg!(unix) {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(to_delete)
}

/// Safe delete a directory by renaming it with a `.deleted` suffix within
/// the same parent directory.
///
/// Suitable for deleting segments since segments with `.deleted` suffix are
/// ignored.
pub fn safe_delete_with_suffix(path: &Path) -> io::Result<()> {
    let (parent, file_name) = split(path).ok_or_else(|| err_invalid_path(path))?;

    // Avoid `file_name.deleted.deleted.deleted`. We use to_string_lossy as we
    // don't mind if temp filename would not be exactly the same.
    let file_name = file_name.to_string_lossy();
    let file_name = file_name.strip_suffix(".deleted").unwrap_or(&file_name);

    let to_delete = tempfile::Builder::new()
        .prefix(file_name)
        .suffix(".deleted")
        .tempdir_in(parent)?;
    fs::rename(path, &to_delete)?;
    if cfg!(unix) {
        fs::File::open(parent)?.sync_all()?;
    }
    to_delete.close()
}

/// After creating or removing a file, it's not enough to call `sync_all()` on
/// this file. It's also necessary to call `sync_all()` on the containing
/// directory. See <https://man7.org/linux/man-pages/man2/fsync.2.html>.
pub fn sync_parent_dir(path: &Path) -> io::Result<()> {
    let (parent, _file_name) = split(path).ok_or_else(|| err_invalid_path(path))?;
    if cfg!(unix) {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

/// See [`sync_parent_dir()`].
pub async fn sync_parent_dir_async(path: &Path) -> io::Result<()> {
    let (parent, _file_name) = split(path).ok_or_else(|| err_invalid_path(path))?;
    if cfg!(unix) {
        tokio_fs::File::open(parent).await?.sync_all().await?;
    }
    Ok(())
}

/// Q: Why we couple [`parent`] and [`file_name`] into a single function?
/// A: [`file_name`] errors out on paths like `foo/..`. We want [`parent`] to
///    fail too even if we don't use [`file_name`] result.
///
/// [`parent`]: Path::parent
/// [`file_name`]: Path::file_name
fn split(path: &Path) -> Option<(&Path, &OsStr)> {
    let parent = match path.parent()? {
        p if p.as_os_str().is_empty() => Path::new("."),
        p => p,
    };
    Some((parent, path.file_name()?))
}

fn err_invalid_path(path: &Path) -> io::Error {
    let msg = format!("Invalid path to delete: {}", path.display());
    io::Error::new(io::ErrorKind::InvalidInput, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_safe_delete() -> io::Result<()> {
        let base = tempfile::tempdir()?;
        let segments = base.path().join("segments");
        let tmp = base.path().join("tmp");

        let make_segment = |name: &str| {
            let segment = segments.join(name);
            fs::create_dir_all(&segment)?;
            fs::write(segment.join("file"), b"test")?;
            io::Result::Ok(segment)
        };

        let segment1 = make_segment("segment1")?;
        let segment2 = make_segment("segment2")?;
        let segment3 = make_segment("segment3")?;
        let segment4 = make_segment("segment4")?;
        let segment4d = make_segment("segment4.deleted")?; // name collision test
        let nonexistent = segments.join("nonexistent");

        // Case 1: delete immediately in the current thread
        safe_delete_in_tmp(&segment1, &tmp)?.close()?;

        // Case 2: delete in background thread
        let to_delete = safe_delete_in_tmp(&segment2, &tmp)?;
        let handle = std::thread::spawn(move || to_delete.close());
        handle.join().unwrap()?;

        // Case 3: rename with suffix
        safe_delete_with_suffix(&segment3)?;
        safe_delete_with_suffix(&segment4)?;
        safe_delete_with_suffix(&segment4d)?;

        // Case 4: nonexistent, follow `fs::remove_dir_all` behavior
        assert!(safe_delete_in_tmp(&nonexistent, &tmp).is_err());
        assert!(safe_delete_with_suffix(&nonexistent).is_err());

        assert_eq!(fs::read_dir(&segments)?.count(), 0);
        assert_eq!(fs::read_dir(&tmp)?.count(), 0);

        Ok(())
    }
}
