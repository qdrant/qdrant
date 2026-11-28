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
///
/// Entries that vanish between listing and visiting are skipped with a
/// warning: the walked tree can be concurrently modified (e.g. atomic file
/// writes create and remove temporary directories in it), and there is
/// nothing left to fsync once an entry is gone. See #11563.
///
/// The root directory itself is expected to exist — every caller syncs a
/// directory it has just created or restored, so its disappearance is a real
/// error and is propagated to the caller.
#[cfg(unix)]
fn sync_dir_with_fsync(dir: &Path) -> io::Result<()> {
    for entry in fs_err::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let is_dir = match entry.file_type() {
            Ok(file_type) => file_type.is_dir(),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                log::warn!("Skipping vanished entry {}: {err}", path.display());
                continue;
            }
            Err(err) => return Err(err),
        };
        if is_dir {
            match sync_dir_with_fsync(&path) {
                Ok(()) => (),
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    log::warn!("Skipping vanished directory {}: {err}", path.display());
                }
                Err(err) => return Err(err),
            }
        } else {
            match File::open(&path) {
                Ok(file) => file.sync_all()?,
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    log::warn!("Skipping vanished file {}: {err}", path.display());
                }
                Err(err) => return Err(err),
            }
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

    /// Regression test for #11563: entries that vanish between listing and
    /// visiting must not fail the recursive sync.
    ///
    /// On macOS this walk raced with the WAL flush worker, whose atomic file
    /// writes create and remove `.atomicwrite*` subdirectories in the walked
    /// tree, making snapshot restores fail spuriously with `NotFound`. Here we
    /// simulate the same race: the walk runs against a tree with many
    /// temporary subdirectories while another thread removes them, starting
    /// right after the walk has listed the root (listing happens within the
    /// first microseconds of the walk) and finishing well before the walk
    /// does. Any removal landing between the listing of an entry and its
    /// visit makes the walk observe a vanished entry; the sync must complete
    /// either way.
    #[cfg(unix)]
    #[test]
    fn sync_dir_with_fsync_tolerates_vanished_entries() {
        use std::thread;
        use std::time::Duration;

        /// Temporary subdirectories imitating `atomicwrites` scratch dirs.
        const DOOMED_DIRS: usize = 64;
        /// Fillers keep the walk busy long enough for deletions to overlap it.
        const FILLER_FILES: usize = 256;

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();

        for i in 0..DOOMED_DIRS {
            let doomed = root.join(format!(".atomicwrite{i}"));
            fs_err::create_dir(&doomed).unwrap();
            fs_err::write(doomed.join("tmpfile"), b"data").unwrap();
        }
        for i in 0..FILLER_FILES {
            fs_err::write(root.join(format!("file{i:04}")), b"data").unwrap();
        }

        let deleter_root = root.clone();
        let deleter = thread::spawn(move || {
            thread::sleep(Duration::from_millis(1));
            for i in 0..DOOMED_DIRS {
                let _ = fs_err::remove_dir_all(deleter_root.join(format!(".atomicwrite{i}")));
            }
        });

        // Must not fail with `NotFound` even though entries vanish mid-walk.
        sync_dir_with_fsync(&root).unwrap();

        deleter.join().unwrap();
    }
}
