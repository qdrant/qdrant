//! Retry file operations that another process briefly blocks on Windows.
//!
//! Windows fails a rename or replace while another process has the file open, or for a
//! directory, any file inside it. Antivirus and EDR scanners (e.g. Microsoft Defender for
//! Endpoint) open files right after they are written, so such operations can fail with access
//! denied for a short moment. See <https://github.com/qdrant/qdrant/issues/10806>.
//!
//! On other platforms these functions do the plain operation, without any retry.

use std::io;
use std::path::Path;

use tempfile::TempPath;

/// Delays between attempts, about 1.2 seconds in total.
#[cfg(windows)]
const RETRY_DELAYS_MS: [u64; 7] = [10, 20, 50, 100, 200, 300, 500];

/// Run `operation` on `path`, retrying while another process briefly holds the file.
///
/// `operation` must return the raw OS error: `fs_err` errors hide the error code.
#[cfg(windows)]
pub(super) fn retry_while_held<T>(
    path: &Path,
    mut operation: impl FnMut() -> io::Result<T>,
) -> io::Result<T> {
    for delay_ms in RETRY_DELAYS_MS {
        match operation() {
            Err(err) if is_held_by_other_process(&err) => {
                log::warn!(
                    "{} is held by another process, retrying in {delay_ms} ms: {err}",
                    path.display(),
                );
                std::thread::sleep(std::time::Duration::from_millis(delay_ms));
            }
            result => return result,
        }
    }
    operation()
}

#[cfg(windows)]
fn is_held_by_other_process(err: &io::Error) -> bool {
    const ERROR_ACCESS_DENIED: i32 = 5;
    const ERROR_SHARING_VIOLATION: i32 = 32;
    const ERROR_LOCK_VIOLATION: i32 = 33;

    match err.raw_os_error() {
        Some(ERROR_ACCESS_DENIED | ERROR_SHARING_VIOLATION | ERROR_LOCK_VIOLATION) => true,
        Some(_) | None => false,
    }
}

/// [`fs_err::rename`], retried on Windows while another process briefly holds the files.
pub fn rename_while_held(from: &Path, to: &Path) -> io::Result<()> {
    #[cfg(not(windows))]
    {
        fs_err::rename(from, to)
    }
    #[cfg(windows)]
    {
        #[expect(
            clippy::disallowed_methods,
            reason = "`fs_err` hides the OS error code we retry on"
        )]
        let result = retry_while_held(from, || std::fs::rename(from, to));
        result.map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "failed to rename {} to {}: {err}",
                    from.display(),
                    to.display(),
                ),
            )
        })
    }
}

/// [`TempPath::persist`], retried on Windows while another process briefly holds the files.
pub fn persist_while_held(temp_path: TempPath, path: &Path) -> io::Result<()> {
    #[cfg(not(windows))]
    {
        temp_path.persist(path).map_err(io::Error::from)
    }
    #[cfg(windows)]
    {
        let mut temp_path = Some(temp_path);
        retry_while_held(path, || {
            let temp = temp_path.take().expect("not retried after success");
            temp.persist(path)
                .map_err(|tempfile::PathPersistError { error, path: temp }| {
                    temp_path = Some(temp);
                    error
                })
        })
    }
}

#[cfg(all(test, windows))]
mod tests {
    use std::io::Write as _;
    use std::os::windows::fs::OpenOptionsExt as _;
    use std::thread::JoinHandle;
    use std::time::Duration;

    use fs_err as fs;

    use super::*;

    /// Open `path` like a scanner: nobody else can replace or rename it while the handle is open.
    #[expect(clippy::disallowed_types, reason = "`share_mode` needs `std::fs`")]
    fn hold(path: &Path) -> std::fs::File {
        std::fs::OpenOptions::new()
            .read(true)
            .share_mode(0)
            .open(path)
            .unwrap()
    }

    /// Hold `path` like [`hold`], and release it after 100 ms.
    fn hold_briefly(path: &Path) -> JoinHandle<()> {
        let file = hold(path);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            drop(file);
        })
    }

    fn temp_path_with(dir: &Path, content: &[u8]) -> TempPath {
        let mut file = tempfile::NamedTempFile::new_in(dir).unwrap();
        file.write_all(content).unwrap();
        file.into_temp_path()
    }

    /// Like a scanner holding a file inside a directory while we rename the directory.
    #[test]
    fn test_rename_dir_while_file_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("from");
        let to = dir.path().join("to");
        fs::create_dir(&from).unwrap();
        fs::write(from.join("file"), b"test").unwrap();

        let file = fs::File::open(from.join("file")).unwrap();
        // A plain rename fails while the file is held
        fs::rename(&from, &to).unwrap_err();

        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            drop(file);
        });
        rename_while_held(&from, &to).unwrap();
        handle.join().unwrap();

        assert_eq!(fs::read(to.join("file")).unwrap(), b"test");
    }

    #[test]
    fn test_atomic_save_while_target_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file.json");
        crate::fs::atomic_save_json(&path, &1).unwrap();

        let handle = hold_briefly(&path);
        crate::fs::atomic_save_json(&path, &2).unwrap();
        handle.join().unwrap();

        assert_eq!(crate::fs::read_json::<u32>(&path).unwrap(), 2);
        // The temporary directory is gone
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn test_persist_while_target_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        fs::write(&path, b"old").unwrap();
        let temp_path = temp_path_with(dir.path(), b"new");

        let handle = hold_briefly(&path);
        persist_while_held(temp_path, &path).unwrap();
        handle.join().unwrap();

        assert_eq!(fs::read(&path).unwrap(), b"new");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    /// The file is held for longer than we retry: the error is returned, the old file is kept
    /// and the temporary file is removed.
    #[test]
    fn test_persist_fails_when_target_stays_held() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        fs::write(&path, b"old").unwrap();
        let temp_path = temp_path_with(dir.path(), b"new");

        let file = hold(&path);
        let err = persist_while_held(temp_path, &path).unwrap_err();
        drop(file);

        assert!(is_held_by_other_process(&err), "unexpected error: {err}");
        assert_eq!(fs::read(&path).unwrap(), b"old");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}
