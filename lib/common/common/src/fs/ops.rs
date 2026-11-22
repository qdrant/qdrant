use std::io::{BufReader, BufWriter, Error, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::thread::sleep;
use std::time::Duration;

use atomicwrites::replace_atomic;
use fs_err::File;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tempfile::Builder;

type Result<T, E = Error> = std::result::Result<T, E>;

/// How many attempts [`atomic_save`] makes on the final replace when Windows
/// reports a transient `PermissionDenied`, and the base of the exponential
/// backoff between attempts (10 ms x 2^attempt, capped by the attempt count).
const ATOMIC_SAVE_ATTEMPTS: usize = 8;
const ATOMIC_SAVE_BACKOFF_MS: u64 = 10;

#[allow(
    clippy::disallowed_types,
    clippy::disallowed_methods,
    reason = "the writer handed to `write` is a `&mut std::fs::File`, so the temp file \
              has to be created and synced as a raw `std::fs::File`; error context \
              comes from the `path` in the messages below"
)]
pub fn atomic_save<E, F>(path: &Path, write: F) -> Result<(), E>
where
    E: From<Error>,
    F: FnOnce(&mut BufWriter<&mut std::fs::File>) -> Result<(), E>,
{
    // Same scheme as `atomicwrites::AtomicFile::write` with
    // `OverwriteBehavior::AllowOverwrite`: a `.atomicwrite*` directory next to
    // the target, one synced temp file in it, then an atomic replace. The
    // prefix is part of the deal — directory-sync walkers tolerate exactly
    // these vanishing `.atomicwrite*` directories.
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let tmpdir = Builder::new().prefix(".atomicwrite").tempdir_in(parent)?;
    let tmppath: PathBuf = tmpdir.path().join("tmpfile.tmp");

    let mut tmpfile = std::fs::File::create(&tmppath)?;
    {
        let mut writer = BufWriter::new(&mut tmpfile);
        write(&mut writer)?;
        writer.flush().map_err(|e| {
            E::from(Error::new(
                e.kind(),
                format!("Failed to flush {}: {e}", path.display()),
            ))
        })?;
    }
    tmpfile.sync_all()?;

    // On Windows the replace (`MoveFileExW(MOVEFILE_REPLACE_EXISTING)`) fails
    // with `PermissionDenied` while an antivirus scanner or a backup tool
    // holds the destination open without `FILE_SHARE_DELETE`. Those holds are
    // transient — typically well under a second — so wait them out instead of
    // failing a durable write that is otherwise perfectly fine (#11123).
    // Other platforms have no such sharing semantics; a `PermissionDenied`
    // there is a real permissions problem and is returned as before.
    for attempt in 0..ATOMIC_SAVE_ATTEMPTS {
        match replace_atomic(&tmppath, path) {
            Ok(()) => return Ok(()),
            Err(err) => {
                let transient = cfg!(windows) && err.kind() == ErrorKind::PermissionDenied;
                if !transient || attempt + 1 == ATOMIC_SAVE_ATTEMPTS {
                    return Err(E::from(err));
                }
                sleep(Duration::from_millis(ATOMIC_SAVE_BACKOFF_MS << attempt));
            }
        }
    }
    unreachable!("the loop returns on its last attempt")
}

pub fn atomic_save_bin<T: Serialize>(path: &Path, object: &T) -> Result<()> {
    atomic_save(path, |writer| {
        bincode::serialize_into(writer, object)
            .map_err(|e| Error::other(format!("Failed to serialize {}: {e}", path.display())))
    })
}

pub fn atomic_save_json<T: Serialize>(path: &Path, object: &T) -> Result<()> {
    atomic_save(path, |writer| {
        serde_json::to_writer(writer, object)
            .map_err(|e| Error::other(format!("Failed to serialize {}: {e}", path.display())))
    })
}

pub fn read_bin<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let file = File::open(path)?;
    bincode::deserialize_from(BufReader::new(file))
        .map_err(|e| Error::other(format!("Failed to deserialize {}: {e}", path.display())))
}

pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let file = File::open(path)?;
    serde_json::from_reader(BufReader::new(file))
        .map_err(|e| Error::other(format!("Failed to deserialize {}: {e}", path.display())))
}

#[cfg(all(test, windows))]
mod windows_tests {
    use std::io::{Error as IoError, Write as _};
    use std::time::{Duration, Instant};

    use fs_err::os::windows::fs::OpenOptionsExt as _;

    use super::atomic_save;

    /// Replacing a file on Windows (`MoveFileExW(MOVEFILE_REPLACE_EXISTING)`,
    /// what `atomic_save` commits with) fails with `ERROR_ACCESS_DENIED` while
    /// any handle keeps the destination open without `FILE_SHARE_DELETE`.
    /// Antivirus scanners and backup tools briefly hold freshly written files
    /// in exactly that way, which makes atomic saves flake on Windows CI
    /// (#11123): the panic there is
    /// `Custom { kind: PermissionDenied, error: PathError { path: "...\\.tmpXXXX", err: Os { code: 5, kind: PermissionDenied } } }`
    /// from `StoredBitSlice::atomic_update`, which routes through
    /// `fs::atomic_save`. A save that hits that window must not fail — it has
    /// to wait for the foreign handle to close and finish.
    #[test]
    fn atomic_save_waits_out_transient_access_denied_on_windows() {
        let dir = tempfile::Builder::new()
            .prefix("atomic-save-retry")
            .tempdir()
            .unwrap();
        let path = dir.path().join("target");
        fs_err::write(&path, b"old").unwrap();

        // Hold the destination open with FILE_SHARE_READ only: no
        // FILE_SHARE_DELETE, so replacing it is denied while it is held —
        // the same denial an AV scanner produces.
        let mut options = fs_err::OpenOptions::new();
        options.read(true).share_mode(1); // FILE_SHARE_READ
        let blocker = options.open(&path).unwrap();

        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            drop(blocker);
        });

        let started = Instant::now();
        atomic_save(&path, |writer| {
            writer.write_all(b"new").map_err(IoError::other)
        })
        .unwrap();
        let elapsed = started.elapsed();

        assert!(
            elapsed >= Duration::from_millis(200),
            "the save must have waited for the blocking handle instead of failing \
             early, took {elapsed:?}",
        );
        assert_eq!(fs_err::read(&path).unwrap(), b"new");
        releaser.join().unwrap();
    }
}
