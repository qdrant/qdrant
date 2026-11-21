use std::io;
use std::path::Path;

#[cfg(unix)]
use fs_err::File;

/// Commits filesystem caches for the given directory.
///
/// On Linux, it commits the entire filesystem containing the directory.
///
/// On non-unix platforms (e.g. Windows) this is a no-op: we can neither `fsync`
/// a read-only file handle (`FlushFileBuffers` requires write access and fails
/// with `ERROR_ACCESS_DENIED`) nor a directory handle. See #9132.
pub fn bulk_sync_dir(dir: &Path) -> io::Result<()> {
    // Matches all platforms that have `nix::unistd::syncfs` function.
    // https://github.com/nix-rust/nix/blob/v0.30.1/src/unistd.rs#L1679
    #[cfg(any(target_os = "linux", target_os = "android", target_os = "hurd"))]
    // If the directory contains a lot of small files, calling `syncfs` once
    // could be faster than calling `fsync` on each file individually.
    // See https://man7.org/linux/man-pages/man2/syncfs.2.html
    match nix::unistd::syncfs(File::open(dir)?) {
        Ok(()) => return Ok(()),
        // Don't return an error as it could be caused by issues outside our
        // control. Just log a warning.
        Err(e) => log::warn!("syncfs failed for {}: {e}", dir.display()),
    }

    // Fallback for the remaining unix platforms (e.g. macOS, *BSD).
    #[cfg(unix)]
    sync_dir_with_fsync(dir)?;

    // Nothing to sync on non-unix platforms (see the doc comment above).
    #[cfg(not(unix))]
    let _ = dir;

    Ok(())
}

/// Calls `fsync` recursively.
#[cfg(unix)]
fn sync_dir_with_fsync(dir: &Path) -> io::Result<()> {
    sync_dir_entries(dir, fs_err::read_dir(dir)?)
}

#[cfg(unix)]
fn sync_dir_entries(
    dir: &Path,
    entries: impl IntoIterator<Item = io::Result<fs_err::DirEntry>>,
) -> io::Result<()> {
    for entry in entries {
        let entry = entry?;
        let result = entry.file_type().and_then(|file_type| {
            let path = entry.path();
            if file_type.is_dir() {
                sync_dir_with_fsync(&path)
            } else {
                File::open(path)?.sync_all()
            }
        });
        // An atomic write can remove a temporary entry after it was listed.
        if let Err(err) = result
            && err.kind() != io::ErrorKind::NotFound
        {
            return Err(err);
        }
    }
    File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression test for #9132: syncing files opened read-only failed on
    /// Windows with `ERROR_ACCESS_DENIED (os error 5)`. `bulk_sync_dir` must
    /// succeed on every platform for a freshly written directory tree.
    #[test]
    fn bulk_sync_dir_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        fs_err::write(dir.path().join("applied_seq.json"), b"{}").unwrap();
        fs_err::create_dir(dir.path().join("sub")).unwrap();
        fs_err::write(dir.path().join("sub").join("data"), b"x").unwrap();
        bulk_sync_dir(dir.path()).unwrap();
        #[cfg(unix)]
        sync_dir_with_fsync(dir.path()).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn recursive_sync_tolerates_atomic_write_cleanup() {
        use std::io::Write as _;

        let shard = tempfile::tempdir().unwrap();
        let wal = shard.path().join("wal");
        fs_err::create_dir(&wal).unwrap();
        fs_err::write(wal.join("open-1"), b"wal segment").unwrap();
        let first_index = wal.join("first-index");
        fs_err::write(&first_index, b"0").unwrap();

        // Capture the entries seen by the walker before the atomic write commits
        // and removes its temporary directory.
        let mut entries = Vec::new();
        super::super::atomic_save(&first_index, |writer| -> io::Result<()> {
            entries.extend(fs_err::read_dir(&wal)?);
            writer.write_all(b"1")
        })
        .unwrap();

        let temporary_dir = entries
            .iter()
            .map(|entry| entry.as_ref().unwrap())
            .find(|entry| {
                entry
                    .file_name()
                    .as_encoded_bytes()
                    .starts_with(b".atomicwrite")
            })
            .unwrap()
            .path();
        assert!(!temporary_dir.exists());
        sync_dir_entries(&wal, entries).unwrap();
        assert_eq!(fs_err::read(first_index).unwrap(), b"1");
        assert_eq!(fs_err::read(wal.join("open-1")).unwrap(), b"wal segment");
    }

    #[cfg(unix)]
    #[test]
    fn recursive_sync_propagates_errors() {
        let dir = tempfile::tempdir().unwrap();
        let missing = sync_dir_with_fsync(&dir.path().join("missing")).unwrap_err();
        assert_eq!(missing.kind(), io::ErrorKind::NotFound);

        // Opening a symlink loop fails with an error other than NotFound.
        std::os::unix::fs::symlink("loop", dir.path().join("loop")).unwrap();
        let loop_error = sync_dir_with_fsync(dir.path()).unwrap_err();
        assert_ne!(loop_error.kind(), io::ErrorKind::NotFound);
    }
}
