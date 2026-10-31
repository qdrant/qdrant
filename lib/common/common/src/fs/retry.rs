//! Retry renames on transient Windows errors.
//!
//! On Windows, renaming a file or directory fails with `ERROR_ACCESS_DENIED`
//! (or a sharing/lock violation) while another process briefly holds a handle
//! to it, or to a file inside it, without `FILE_SHARE_DELETE`. Antivirus/EDR
//! file system filters (e.g. Microsoft Defender for Endpoint) do this right
//! after files are written. The condition is transient, so a short retry makes
//! the rename succeed instead of failing the flush or the optimizer.
//!
//! On other platforms these functions do a single attempt.

use std::io;
use std::path::Path;

use fs_err as fs;
use tempfile::TempPath;

/// Delays between attempts on Windows (~1.2 s in total).
#[cfg(windows)]
const RETRY_DELAYS_MS: [u64; 7] = [10, 20, 50, 100, 200, 300, 500];

/// Rename `from` to `to` like [`fs::rename`], retrying on transient Windows
/// errors.
pub fn rename_with_retry(from: &Path, to: &Path) -> io::Result<()> {
    #[cfg(windows)]
    for delay_ms in RETRY_DELAYS_MS {
        #[expect(
            clippy::disallowed_methods,
            reason = "fs_err hides the raw OS error code needed to detect transient errors"
        )]
        let result = std::fs::rename(from, to);
        match result {
            Ok(()) => return Ok(()),
            Err(err) if is_transient_windows_error(&err) => backoff(from, to, delay_ms, &err),
            Err(_) => break,
        }
    }
    // Last attempt with `fs_err` for a descriptive error
    fs::rename(from, to)
}

/// Persist `temp_path` to `path` like [`TempPath::persist`], retrying on
/// transient Windows errors. Windows replacements use write-through semantics.
pub fn persist_with_retry(temp_path: TempPath, path: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        let mut delays = RETRY_DELAYS_MS.into_iter();
        let mut temp_path = temp_path;
        // Clear Windows temporary attributes, then restore cleanup ownership.
        let temp_path = loop {
            match temp_path.keep() {
                Ok(path) => break TempPath::try_from_path(path)?,
                Err(err) => {
                    if is_transient_windows_error(&err.error)
                        && let Some(delay_ms) = delays.next()
                    {
                        backoff(&err.path, path, delay_ms, &err.error);
                        temp_path = err.path;
                    } else {
                        return Err(err.error);
                    }
                }
            }
        };
        for delay_ms in delays {
            match atomicwrites::replace_atomic(&temp_path, path) {
                Ok(()) => return Ok(()),
                Err(err) if is_transient_windows_error(&err) => {
                    backoff(&temp_path, path, delay_ms, &err);
                }
                Err(err) => return Err(err),
            }
        }
        atomicwrites::replace_atomic(&temp_path, path)
    }
    #[cfg(not(windows))]
    {
        temp_path.persist(path).map_err(|err| err.error)
    }
}

#[cfg(windows)]
fn backoff(from: &Path, to: &Path, delay_ms: u64, err: &io::Error) {
    log::warn!(
        "Failed to rename {} to {}, retrying in {delay_ms} ms: {err}",
        from.display(),
        to.display(),
    );
    std::thread::sleep(std::time::Duration::from_millis(delay_ms));
}

#[cfg(windows)]
pub(super) fn is_transient_windows_error(err: &io::Error) -> bool {
    const ERROR_ACCESS_DENIED: i32 = 5;
    const ERROR_SHARING_VIOLATION: i32 = 32;
    const ERROR_LOCK_VIOLATION: i32 = 33;

    matches!(
        err.raw_os_error(),
        Some(ERROR_ACCESS_DENIED | ERROR_SHARING_VIOLATION | ERROR_LOCK_VIOLATION)
    )
}

#[cfg(all(test, windows))]
mod tests {
    use std::io::Write as _;
    use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
    use std::thread::JoinHandle;
    use std::time::Duration;

    use super::*;

    /// Open `path` like a scanner that does not allow deleting/renaming it
    /// (share mode without `FILE_SHARE_DELETE`), and close it after 150 ms.
    #[expect(clippy::disallowed_types, reason = "std::fs is needed for share_mode")]
    pub(crate) fn hold_briefly(path: &Path) -> io::Result<JoinHandle<()>> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .share_mode(0)
            .open(path)?;
        Ok(std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(150));
            drop(file);
        }))
    }

    #[test]
    #[expect(clippy::disallowed_methods, reason = "need the raw OS error code")]
    fn test_rename_with_retry_while_file_is_held() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let from = dir.path().join("from");
        let to = dir.path().join("to");
        fs::write(&from, b"test")?;

        // Without retry, the plain rename fails while the handle is held
        let handle = hold_briefly(&from)?;
        let err = std::fs::rename(&from, &to).unwrap_err();
        assert!(is_transient_windows_error(&err), "unexpected error: {err}");
        handle.join().unwrap();

        let handle = hold_briefly(&from)?;
        rename_with_retry(&from, &to)?;
        handle.join().unwrap();

        assert!(!from.exists());
        assert_eq!(fs::read(&to)?, b"test");
        Ok(())
    }

    #[test]
    fn test_persist_with_retry_while_target_is_held() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("file");
        fs::write(&path, b"old")?;

        let mut tmp = tempfile::NamedTempFile::new_in(dir.path())?;
        tmp.write_all(b"new")?;
        let temp_path = tmp.into_temp_path();

        let handle = hold_briefly(&path)?;
        persist_with_retry(temp_path, &path)?;
        handle.join().unwrap();

        assert_eq!(fs::read(&path)?, b"new");
        assert_eq!(fs::read_dir(dir.path())?.count(), 1);
        // FILE_ATTRIBUTE_TEMPORARY must not survive publication.
        assert_eq!(fs::metadata(&path)?.file_attributes() & 0x100, 0);
        Ok(())
    }

    #[test]
    fn test_persist_with_retry_creates_destination() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("file");
        let mut tmp = tempfile::NamedTempFile::new_in(dir.path())?;
        tmp.write_all(b"new")?;

        persist_with_retry(tmp.into_temp_path(), &path)?;

        assert_eq!(fs::read(&path)?, b"new");
        assert_eq!(fs::read_dir(dir.path())?.count(), 1);
        assert_eq!(fs::metadata(&path)?.file_attributes() & 0x100, 0);
        Ok(())
    }

    #[test]
    fn test_persist_with_retry_while_source_is_held() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("file");
        let mut tmp = tempfile::NamedTempFile::new_in(dir.path())?;
        tmp.write_all(b"new")?;
        let temp_path = tmp.into_temp_path();

        let handle = hold_briefly(&temp_path)?;
        persist_with_retry(temp_path, &path)?;
        handle.join().unwrap();

        assert_eq!(fs::read(&path)?, b"new");
        assert_eq!(fs::read_dir(dir.path())?.count(), 1);
        assert_eq!(fs::metadata(&path)?.file_attributes() & 0x100, 0);
        Ok(())
    }

    #[test]
    fn test_persist_with_retry_cleans_up_on_error() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("missing").join("file");
        let tmp = tempfile::NamedTempFile::new_in(dir.path())?;
        let temp_path = tmp.into_temp_path();
        let original_path = temp_path.to_path_buf();

        let err = persist_with_retry(temp_path, &path).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert!(!original_path.exists());
        assert_eq!(fs::read_dir(dir.path())?.count(), 0);
        Ok(())
    }

    #[test]
    #[expect(clippy::disallowed_types, reason = "std::fs is needed for share_mode")]
    fn test_persist_with_retry_exhausted_preserves_destination() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("file");
        fs::write(&path, b"old")?;
        let mut tmp = tempfile::NamedTempFile::new_in(dir.path())?;
        tmp.write_all(b"new")?;
        let temp_path = tmp.into_temp_path();
        let original_path = temp_path.to_path_buf();
        let handle = std::fs::OpenOptions::new()
            .read(true)
            .share_mode(0)
            .open(&path)?;

        let err = persist_with_retry(temp_path, &path).unwrap_err();
        drop(handle);

        assert!(is_transient_windows_error(&err), "unexpected error: {err}");
        assert_eq!(fs::read(&path)?, b"old");
        assert!(!original_path.exists());
        assert_eq!(fs::read_dir(dir.path())?.count(), 1);
        Ok(())
    }

    #[test]
    fn test_atomic_save_while_target_is_held() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("segment.json");
        super::super::atomic_save_json(&path, &1)?;

        let handle = hold_briefly(&path)?;
        super::super::atomic_save_json(&path, &2)?;
        handle.join().unwrap();

        assert_eq!(super::super::read_json::<u32>(&path)?, 2);
        // No temp files left behind
        assert_eq!(fs::read_dir(dir.path())?.count(), 1);
        Ok(())
    }
}
