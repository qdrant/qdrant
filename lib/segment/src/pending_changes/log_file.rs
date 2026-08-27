//! Append-only log file storing the pending changes of a proxy segment.
//!
//! ## File format
//!
//! The file is a sequence of length-prefixed entries, simply concatenated:
//!
//! +---------------------+------------------------------------+
//! | entry length: u32   | JSON-serialized [`PendingChange`]  |
//! +---------------------+------------------------------------+
//!
//! Entries are only ever appended. Each append serializes all new entries into a single buffer
//! and writes it with a single write call on a file opened in append mode, so growing the file
//! and writing the data happen in one operation. A crash can therefore only ever leave a
//! partially written entry at the very end of the file, which [`load_changes`] detects and
//! truncates — the operations it held were never durable, so they were never acknowledged in the
//! WAL and will simply be replayed from there.
//!
//! This mirrors the mutable ID tracker mappings storage.

use std::cmp::Ordering;
use std::io::{BufReader, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;

use byteorder::{LittleEndian, ReadBytesExt as _, WriteBytesExt as _};
use common::fs::{OneshotFile, sync_parent_dir};
use fs_err::File;
use parking_lot::Mutex;

use super::change::PendingChange;
use crate::common::operation_error::{OperationError, OperationResult};

/// File name of the pending changes log of the first (inner most) proxy layer.
///
/// Additional proxy layers append a numeric suffix: the second layer writes to
/// `pending_changes.log.1`, the third to `pending_changes.log.2`, and so on. See
/// [`pending_changes_log_path`].
pub const PENDING_CHANGES_LOG_FILE: &str = "pending_changes.log";

/// Sanity limit for a single log entry, to not trust a corrupted length prefix.
const MAX_ENTRY_SIZE: u32 = 32 * 1024 * 1024;

/// Path of the pending changes log file for the given proxy `level` inside `segment_path`.
///
/// The first (inner most) proxy layer gets no suffix, each further layer gets its level as a
/// numeric suffix.
pub fn pending_changes_log_path(segment_path: &Path, level: usize) -> PathBuf {
    if level == 0 {
        segment_path.join(PENDING_CHANGES_LOG_FILE)
    } else {
        segment_path.join(format!("{PENDING_CHANGES_LOG_FILE}.{level}"))
    }
}

/// List all pending changes log files inside the given segment directory, ordered by proxy level
/// (inner most first).
///
/// Levels are not necessarily contiguous: a proxy layer that never persisted any change does not
/// create a file, while a layer above it may have. The listing is therefore based on the actual
/// directory contents rather than probing levels until the first gap.
pub fn list_pending_changes_log_files(segment_path: &Path) -> Vec<PathBuf> {
    let Ok(dir) = fs_err::read_dir(segment_path) else {
        return Vec::new();
    };

    let mut files: Vec<(usize, PathBuf)> = dir
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let file_name = entry.file_name();
            let level = parse_log_file_level(file_name.to_str()?)?;
            entry.path().is_file().then(|| (level, entry.path()))
        })
        .collect();
    files.sort_unstable_by_key(|(level, _)| *level);
    files.into_iter().map(|(_, path)| path).collect()
}

/// Parse the proxy level from a pending changes log file name, if it is one.
fn parse_log_file_level(file_name: &str) -> Option<usize> {
    if file_name == PENDING_CHANGES_LOG_FILE {
        return Some(0);
    }
    let suffix = file_name
        .strip_prefix(PENDING_CHANGES_LOG_FILE)?
        .strip_prefix('.')?;
    // Higher levels always carry an explicit non-zero suffix, level 0 never does
    let level: usize = suffix.parse().ok()?;
    (level > 0).then_some(level)
}

/// Store new pending changes, appending them to the given log file.
///
/// `expected_len` is the shared valid length of the file in bytes; everything past it is not
/// durable and may be garbage from an earlier failed append. It is read fresh on every call and
/// bumped after the appended entries have been fsynced, so a later append (or a retry after a
/// failure) starts exactly after the last durable entry.
pub(super) fn store_changes(
    path: &Path,
    changes: &[PendingChange],
    expected_len: &AtomicU64,
) -> OperationResult<()> {
    let is_new_file = !path.exists();

    // Create or open file in append mode to write new changes to the end
    let file = File::options().create(true).append(true).open(path)?;

    // Ensure correct file length to not corrupt entries when appending
    let file_len = file
        .metadata()
        .map_err(|err| {
            OperationError::service_error(format!(
                "Failed to get pending changes log file size: {err}"
            ))
        })?
        .len();
    let file_start_appending = expected_len.load(std::sync::atomic::Ordering::Relaxed);
    match file_len.cmp(&file_start_appending) {
        // File size is what we expect, continue normally
        Ordering::Equal => {}
        // File is larger than expected, previous append might not have completed properly
        // Clean up by truncating to what we expect, then append
        // May happen if system is out of disk space and the file cannot be grown
        Ordering::Greater => {
            file.set_len(file_start_appending).map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to truncate pending changes log file that is too large: {err}"
                ))
            })?;
        }
        // File is smaller than expected, indicates a bug we cannot recover from
        Ordering::Less => {
            return Err(OperationError::service_error(format!(
                "Pending changes log file size is less than expected, cannot append new changes (file size: {file_len}, expected: {file_start_appending})",
            )));
        }
    }

    // Serialize all entries into a single buffer so appending them is a single call that both
    // grows the file and writes into it, protecting against torn writes in the middle
    let mut buffer = Vec::new();
    for change in changes {
        write_entry(&mut buffer, change)?;
    }

    let mut file = file;
    file.write_all(&buffer).map_err(|err| {
        OperationError::service_error(format!(
            "Failed to persist pending changes log ({}): {err}",
            path.display(),
        ))
    })?;

    // Explicitly fsync file contents to ensure durability
    file.sync_all().map_err(|err| {
        OperationError::service_error(format!("Failed to fsync pending changes log: {err}"))
    })?;

    // Make sure a newly created file has its directory entry persisted as well
    if is_new_file {
        sync_parent_dir(path)?;
    }

    // Update expected file length to append after these entries next time
    expected_len.store(
        file_start_appending + buffer.len() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );

    Ok(())
}

/// Pending changes loaded from a log file.
pub(super) struct LoadedChanges {
    /// All entries of the file, in append order.
    pub changes: Vec<PendingChange>,
    /// Length of the valid part of the file in bytes.
    ///
    /// If the file held a partially written entry at the end it has been truncated away, and this
    /// reflects the truncated length.
    pub valid_len: u64,
}

/// Load all pending changes from the given log file.
///
/// If the file ends with a partially written entry — the result of a crash in the middle of an
/// append — that entry is truncated from the file. The operations it held were never durable, so
/// they were never acknowledged in the WAL, and are recovered by regular WAL replay instead.
///
/// A malformed entry that is *not* at the end of the file cannot be explained by a torn append
/// and is reported as a hard error: entries after it may have been acknowledged in the WAL, so
/// silently dropping them would lose acknowledged operations.
pub(super) fn load_changes(path: &Path) -> OperationResult<LoadedChanges> {
    let file = OneshotFile::open(path)?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);

    let mut changes = Vec::new();
    let mut valid_len: u64 = 0;

    loop {
        match read_entry(&mut reader, file_len - valid_len) {
            ReadEntry::Change(change, entry_bytes) => {
                valid_len += entry_bytes;
                changes.push(change);
            }
            ReadEntry::EndOfFile => break,
            // A torn entry at the end of the file, truncate it below
            ReadEntry::Truncated => break,
            ReadEntry::Err(err) => {
                return Err(OperationError::service_error(format!(
                    "Malformed entry in pending changes log at offset {valid_len} ({}): {err}",
                    path.display(),
                )));
            }
        }
    }

    reader.into_inner().drop_cache()?;

    // If the file holds a partial entry at the end, truncate the file
    // It can happen on crash while appending. We must truncate the file here to not corrupt new
    // entries we append after it
    debug_assert!(
        valid_len <= file_len,
        "cannot read past the end of the file"
    );
    if valid_len < file_len {
        log::warn!(
            "Pending changes log ends with incomplete entry, removing last {} bytes and assuming automatic recovery by WAL ({})",
            file_len - valid_len,
            path.display(),
        );
        let file = File::options().write(true).truncate(false).open(path)?;
        file.set_len(valid_len)?;
        file.sync_all()?;
    }

    Ok(LoadedChanges { changes, valid_len })
}

/// Result of reading a single entry from a pending changes log.
enum ReadEntry {
    /// A complete, valid entry, and the number of bytes it takes in the file.
    Change(PendingChange, u64),
    /// The reader was exactly at the end of the file.
    EndOfFile,
    /// The file ends with a partially written entry.
    Truncated,
    /// The entry is malformed even though the file holds all its bytes.
    Err(std::io::Error),
}

/// Read a single entry from the given reader, with `remaining` bytes left in the file.
fn read_entry(reader: &mut impl std::io::Read, remaining: u64) -> ReadEntry {
    if remaining == 0 {
        return ReadEntry::EndOfFile;
    }
    if remaining < size_of::<u32>() as u64 {
        return ReadEntry::Truncated;
    }

    let entry_len = match reader.read_u32::<LittleEndian>() {
        Ok(entry_len) => entry_len,
        Err(err) => return ReadEntry::Err(err),
    };

    // If the length prefix points past the end of the file the entry is torn, no matter whether
    // the prefix itself is garbage or the payload just did not make it to disk
    if u64::from(entry_len) > remaining - size_of::<u32>() as u64 {
        return ReadEntry::Truncated;
    }
    if entry_len > MAX_ENTRY_SIZE {
        return ReadEntry::Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("entry length {entry_len} exceeds sanity limit {MAX_ENTRY_SIZE}"),
        ));
    }

    let mut payload = vec![0; entry_len as usize];
    if let Err(err) = reader.read_exact(&mut payload) {
        return ReadEntry::Err(err);
    }

    let entry_bytes = size_of::<u32>() as u64 + u64::from(entry_len);

    match serde_json::from_slice(&payload) {
        Ok(change) => ReadEntry::Change(change, entry_bytes),
        // The payload bytes are all present but malformed; only a torn write of the very last
        // entry can explain that, anything before it is corruption
        Err(err) if entry_bytes == remaining => {
            log::warn!("Pending changes log ends with malformed entry, truncating: {err}");
            ReadEntry::Truncated
        }
        Err(err) => ReadEntry::Err(err.into()),
    }
}

/// Serialize a single entry and write it into the given writer.
fn write_entry(writer: &mut Vec<u8>, change: &PendingChange) -> OperationResult<()> {
    let payload = serde_json::to_vec(change).map_err(|err| {
        OperationError::service_error(format!("Failed to serialize pending change: {err}"))
    })?;
    debug_assert!(payload.len() <= MAX_ENTRY_SIZE as usize);
    writer.write_u32::<LittleEndian>(payload.len() as u32)?;
    writer.extend_from_slice(&payload);
    Ok(())
}

/// Drop persisted entries from the pending buffer after they have been stored.
///
/// Counts how many entries at the front of `pending` match the just-persisted `changes` and
/// drains them. With concurrent flushers it is possible that the beginning of the lists doesn't
/// match. Since replaying an entry twice is harmless (all operations are version gated) it is not
/// a problem, and we can store everything again in the next iteration.
pub(super) fn reconcile_persisted_changes(
    pending: &Mutex<Vec<PendingChange>>,
    changes: &[PendingChange],
) {
    let mut pending = pending.lock();

    let count = pending
        .iter()
        .zip(changes)
        .take_while(|(pending, persisted)| pending == persisted)
        .count();

    pending.drain(0..count);
}
