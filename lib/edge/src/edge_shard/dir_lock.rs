//! Exclusive lock on an edge shard directory.
//!
//! The WAL's `flock` on `<path>/wal` used to be the only exclusion between two handles of one
//! shard. It is taken late in [`EdgeShard::load`](super::EdgeShard::load), after the segments
//! are scanned and normalized (which deletes segment directories without a completeness marker,
//! such as one an optimizer of the first handle is still building), and a shard without a WAL
//! takes it not at all, so a second load opened a second engine over the same files.
//!
//! The directory lock is taken before the segments are read or changed (`load` reads only
//! `edge_config.json` before it) and held for the shard's lifetime.

use std::io;
use std::path::Path;

use fs_err::File;
use segment::common::operation_error::{OperationError, OperationResult};

/// Prefix of the error a locked directory produces; matched by [`is_shard_locked`].
const SHARD_LOCKED: &str = "edge shard is already open by another handle";

/// Lock file used on Windows, where a directory cannot be opened as a file and locked. Mirrors
/// the `wal` crate's `.wal` proxy file.
#[cfg(target_os = "windows")]
const WINDOWS_LOCK_FILE: &str = ".edge-shard.lock";

/// An exclusive advisory lock on a shard directory, released on drop.
#[derive(Debug)]
pub(super) struct ShardDirLock {
    _file: File,
}

impl ShardDirLock {
    /// Takes the lock on the existing directory `path` without blocking.
    pub(super) fn acquire(path: &Path) -> OperationResult<Self> {
        let file = open_lock_target(path).map_err(|err| {
            OperationError::service_error(format!(
                "cannot open edge shard directory {} for locking: {err}",
                path.display(),
            ))
        })?;
        // `fs4`'s `flock(2)` rather than `File::try_lock`, which returns `Unsupported` on
        // Android; called via UFCS because the trait method collides with the inherent one
        // (same as the `wal` crate).
        match fs4::FileExt::try_lock(file.file()) {
            Ok(()) => Ok(Self { _file: file }),
            Err(fs4::TryLockError::WouldBlock) => Err(OperationError::service_error(format!(
                "{SHARD_LOCKED}: {}",
                path.display(),
            ))),
            Err(fs4::TryLockError::Error(err)) => Err(OperationError::service_error(format!(
                "cannot lock edge shard directory {}: {err}",
                path.display(),
            ))),
        }
    }
}

/// True if `err` means another handle holds the shard: its directory lock, or the WAL lock of a
/// holder that takes only that one. A recoverable condition, not a damaged shard.
pub fn is_shard_locked(err: &OperationError) -> bool {
    let OperationError::ServiceError { description, .. } = err else {
        return false;
    };
    if description.starts_with(SHARD_LOCKED) {
        return true;
    }
    // The `wal` crate reports a held lock as an io error it stringifies:
    // "Can't init WAL: Kind(WouldBlock)" after the WAL path.
    description
        .split_once("Can't init WAL")
        .is_some_and(|(_, tail)| tail.contains("WouldBlock"))
}

#[cfg(not(target_os = "windows"))]
fn open_lock_target(path: &Path) -> io::Result<File> {
    File::open(path)
}

#[cfg(target_os = "windows")]
fn open_lock_target(path: &Path) -> io::Result<File> {
    File::options()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path.join(WINDOWS_LOCK_FILE))
}
