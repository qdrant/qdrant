use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;

use common::bitvec::BitVec;
use common::fs::OneshotFile;
use common::types::PointOffsetType;
use fs_err::File;
use itertools::Itertools;
use parking_lot::Mutex;

use super::change::{MappingChange, read_entry, write_entry};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::point_mappings::PointMappings;
use crate::types::PointIdType;

const FILE_MAPPINGS: &str = "mutable_id_tracker.mappings";

pub(crate) fn mappings_path(segment_path: &Path) -> PathBuf {
    segment_path.join(FILE_MAPPINGS)
}

/// Store new mapping changes, appending them to the given file
pub(super) fn store_mapping_changes(
    mappings_path: &Path,
    changes: &Vec<MappingChange>,
    persisted_mappings_size: &AtomicU64,
) -> OperationResult<()> {
    // Create or open file in append mode to write new changes to the end
    let file = File::options()
        .create(true)
        .append(true)
        .open(mappings_path)?;

    // Ensure correct file length to not corrupt mappings when appending
    let file_len = file
        .metadata()
        .map_err(|err| {
            OperationError::service_error(format!(
                "Failed to get ID tracker mappings file size: {err}"
            ))
        })?
        .len();
    let file_start_appending = persisted_mappings_size.load(std::sync::atomic::Ordering::Relaxed);
    match file_len.cmp(&file_start_appending) {
        // File size is what we expect, continue normally
        Ordering::Equal => {}
        // File is larger than expected, previous flush might not have completed properly
        // Clean up by truncating to what we expect, then append
        // May happen if system is out of disk space and the file cannot be grown
        Ordering::Greater => {
            file.set_len(file_start_appending)
                .map_err(|err| OperationError::service_error(
                    format!("Failed to truncate mutable ID tracker mappings file that is too large, ignoring: {err}"),
                ))?;
        }
        // File is smaller than expected, indicates a bug we cannot recover from
        Ordering::Less => {
            return Err(OperationError::service_error(format!(
                "Mutable ID tracker mappings file size is less than persisted mappings size, cannot append new mappings (file size: {file_len}, persisted mappings size: {file_start_appending})",
            )));
        }
    }

    let mut writer = BufWriter::new(file);

    log::trace!("writing mapping changes to {mappings_path:?}: {changes:?}");

    write_mapping_changes(&mut writer, changes).map_err(|err| {
        OperationError::service_error(format!(
            "Failed to persist ID tracker point mappings ({}): {err}",
            mappings_path.display(),
        ))
    })?;

    writer.flush()?;
    let mut file = writer.into_inner().map_err(|err| {
        OperationError::service_error(format!(
            "Failed to flush ID tracker point mappings write buffer: {err}"
        ))
    })?;

    // Get new persisted size as a position after writing all changes.
    // Stream position is used here to handle cases where the pending changes are shorter than non-persisted part.
    let new_persisted_size = file.stream_position().map_err(|err| {
        OperationError::service_error(format!(
            "Failed to get new persisted size of ID tracker mappings: {err}"
        ))
    })?;

    // Explicitly fsync file contents to ensure durability
    file.sync_all().map_err(|err| {
        OperationError::service_error(format!("Failed to fsync ID tracker point mappings: {err}"))
    })?;

    // Update persisted mappings size.
    persisted_mappings_size.store(new_persisted_size, std::sync::atomic::Ordering::Relaxed);

    Ok(())
}

/// Serializes pending point mapping changes into the given writer
///
/// ## File format
///
/// All entries have a variable size and are simply concatenated. Each entry has a 1-byte header
/// which specifies the change type and implies the length of the entry.
///
/// See [`read_entry`] and [`write_entry`] for more details.
fn write_mapping_changes<W: Write>(
    mut writer: W,
    changes: &Vec<MappingChange>,
) -> OperationResult<()> {
    for &change in changes {
        write_entry(&mut writer, change)?;
    }

    // Explicitly flush writer to catch IO errors
    writer.flush()?;

    Ok(())
}

/// Mappings loaded from the mappings log, see [`load_mappings`].
pub(super) struct LoadedMappings {
    pub mappings: PointMappings,
    /// Points whose last insert in the log was abandoned and that stay on their previous copy,
    /// see [`replay_mappings`]. Ordered by external id.
    pub abandoned_relocations: Vec<PointIdType>,
    /// Byte offset just past the last complete entry, the expected length of the log.
    pub read_to: u64,
}

/// Load point mappings from the given file.
/// Returns loaded point mappings and the number of bytes read from the file.
///
/// If the file ends with an incomplete entry, it is truncated from the file.
pub(super) fn load_mappings(
    mappings_path: &Path,
    deferred_internal_id: Option<PointOffsetType>,
    has_version: impl Fn(PointOffsetType) -> bool,
) -> OperationResult<LoadedMappings> {
    let file = OneshotFile::open(mappings_path)?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);

    let (mappings, abandoned_relocations) =
        replay_mappings(&mut reader, deferred_internal_id, has_version)?;

    let read_to = reader.stream_position()?;
    reader.into_inner().drop_cache()?;

    // If reader is not fully exhausted, there's an incomplete entry at the end, truncate the file
    // It can happen on crash while flushing. We must truncate the file here to not corrupt new
    // entries we append to the file
    debug_assert!(read_to <= file_len, "cannot read past the end of the file");
    if read_to < file_len {
        log::warn!(
            "Mutable ID tracker mappings file ends with incomplete entry, removing last {} bytes and assuming automatic recovery by WAL",
            file_len - read_to,
        );
        let file = File::options()
            .write(true)
            .truncate(false)
            .open(mappings_path)?;
        file.set_len(read_to)?;
        file.sync_all()?;
    }

    Ok(LoadedMappings {
        mappings,
        abandoned_relocations,
        read_to,
    })
}

/// Iterate over mapping changes from the given reader
///
/// Each non-errorous item is a tuple of the mapping change and the number of bytes read so far.
///
/// The iterator ends when the end of the file is reached, or when an error occurred.
///
/// ## Error
///
/// An error item is returned if reading a mapping change fails due to malformed data. Then the
/// iterator will not produce any more items.
pub(super) fn read_mappings_iter<R>(
    mut reader: R,
) -> impl Iterator<Item = OperationResult<MappingChange>>
where
    R: Read + Seek,
{
    let mut position = reader.stream_position().unwrap_or(0);

    // Keep reading until end of file or error
    std::iter::from_fn(move || match read_entry(&mut reader) {
        Ok((entry, read_bytes)) => {
            position += read_bytes;
            Some(Ok(entry))
        }
        // Done reading if end of file is reached
        // Explicitly seek to after last read entry, allows to detect if full file was read
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
            match reader.seek(io::SeekFrom::Start(position)) {
                Ok(_) => None,
                Err(err) => Some(Err(err.into())),
            }
        }
        // Propagate deserialization error
        Err(err) => Some(Err(err.into())),
    })
    // Can't read any more data reliably after first error
    .take_while_inclusive(|item| item.is_ok())
}

/// Read point mappings from the given reader
///
/// Returns loaded point mappings, see [`replay_mappings`].
#[cfg(test)]
pub(super) fn read_mappings<R>(
    reader: R,
    deferred_internal_id: Option<PointOffsetType>,
    has_version: impl Fn(PointOffsetType) -> bool,
) -> OperationResult<PointMappings>
where
    R: Read + Seek,
{
    Ok(replay_mappings(reader, deferred_internal_id, has_version)?.0)
}

/// Replay the mappings log from the given reader.
///
/// `has_version` tells whether a slot has a persisted version. An insert into a slot without one
/// is abandoned: its flush stopped between mappings and versions. For a point still linked
/// elsewhere, which is how an append-only mutation relocates a point to a fresh slot of the same
/// segment, the abandoned insert is not applied and the point stays on its previous copy, the
/// last one fully persisted. Linking the fresh slot would tombstone the previous copy, and the
/// inconsistency fix would then drop the point altogether. A point linked nowhere else is linked
/// as before, and the inconsistency fix drops it.
///
/// Returns the mappings, and the points whose last insert was abandoned and that stay on their
/// previous copy. The log still names the abandoned slot as theirs: the caller has to relink them
/// to their previous copy in the log, see
/// [`MutableIdTracker::open`](super::MutableIdTracker::open).
fn replay_mappings<R>(
    reader: R,
    deferred_internal_id: Option<PointOffsetType>,
    has_version: impl Fn(PointOffsetType) -> bool,
) -> OperationResult<(PointMappings, Vec<PointIdType>)>
where
    R: Read + Seek,
{
    // Replay the persisted change log through the canonical mutators rather
    // than accumulating into a single flat map per id-type. A flat
    // `external -> internal` map can only hold one head per external id, so it
    // collapses the active+deferred coexistence case (the same external id
    // linked first to an active slot, then to a deferred one via sequential
    // `set_link`) down to the last write, silently dropping the other head and
    // orphaning its slot. The log *is* the sequence of `set_link`/`drop` calls
    // that produced the live in-memory state, so replaying it through those
    // same functions reconstructs that state exactly — both heads, the
    // shadowed bit, and `deferred_deleted_count` — with no logic duplication.
    let mut mappings = PointMappings::new(
        BitVec::new(),
        Vec::new(),
        BTreeMap::new(),
        BTreeMap::new(),
        deferred_internal_id,
    );

    // Points whose last insert so far was abandoned while they stayed on their previous copy
    let mut abandoned = HashSet::new();

    for change in read_mappings_iter(reader) {
        match change? {
            MappingChange::Insert(external_id, internal_id)
                if !has_version(internal_id) && mappings.heads(&external_id).next().is_some() =>
            {
                // Keep the slot allocated: its storages may already hold data written for it
                mappings.reserve_slot(internal_id);
                abandoned.insert(external_id);
            }
            MappingChange::Insert(external_id, internal_id) => {
                mappings.set_link(external_id, internal_id);
                abandoned.remove(&external_id);
            }
            MappingChange::Delete(external_id) | MappingChange::DeleteVersioned(external_id, _) => {
                mappings.drop(external_id);
                abandoned.remove(&external_id);
            }
        }
    }

    let mut abandoned = abandoned.into_iter().collect_vec();
    abandoned.sort_unstable();

    Ok((mappings, abandoned))
}

pub(super) fn reconcile_persisted_mapping_changes(
    pending: &Mutex<Vec<MappingChange>>,
    changes: &[MappingChange],
) {
    let mut pending = pending.lock();

    // Count how many entries are equal in both lists
    // With concurrent flushers it is possible that the beginning of the lists doesn't match. Since
    // each event is idempotent it is not a problem, and we can store everything again in the next
    // iteration.
    let count = pending
        .iter()
        .zip(changes)
        .take_while(|(pending, persisted)| pending == persisted)
        .count();

    pending.drain(0..count);
}
