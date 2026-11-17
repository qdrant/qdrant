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
    for entry in fs_err::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            sync_dir_with_fsync(&entry.path())?;
        } else {
            File::open(entry.path())?.sync_all()?;
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
    }
}
