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
    use std::time::Duration;

    use fs_err as fs;

    use super::*;

    /// Like a scanner holding a file inside a directory while we rename the directory.
    #[test]
    fn test_rename_dir_while_file_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("from");
        let to = dir.path().join("to");
        fs::create_dir(&from).unwrap();
        fs::write(from.join("file"), b"test").unwrap();

        let file = fs::File::open(from.join("file")).unwrap();
        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            drop(file);
        });
        rename_while_held(&from, &to).unwrap();
        handle.join().unwrap();

        assert_eq!(fs::read(to.join("file")).unwrap(), b"test");
    }
}
