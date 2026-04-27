use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;

use common::bitvec::BitVec;
use common::fs::OneshotFile;
use common::types::PointOffsetType;
use fs_err::File;
use itertools::Itertools;
use parking_lot::Mutex;
use uuid::Uuid;

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

/// Load point mappings from the given file.
/// Returns loaded point mappings and the number of bytes read from the file.
///
/// If the file ends with an incomplete entry, it is truncated from the file.
pub(super) fn load_mappings(mappings_path: &Path) -> OperationResult<(PointMappings, u64)> {
    let file = OneshotFile::open(mappings_path)?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);

    let mappings = read_mappings(&mut reader)?;

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

    Ok((mappings, read_to))
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
fn read_mappings_iter<R>(mut reader: R) -> impl Iterator<Item = OperationResult<MappingChange>>
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
/// Returns loaded point mappings.
pub(super) fn read_mappings<R>(reader: R) -> OperationResult<PointMappings>
where
    R: Read + Seek,
{
    let mut deleted = BitVec::new();
    let mut internal_to_external: Vec<PointIdType> = Default::default();
    let mut external_to_internal_num: BTreeMap<u64, PointOffsetType> = Default::default();
    let mut external_to_internal_uuid: BTreeMap<Uuid, PointOffsetType> = Default::default();

    for change in read_mappings_iter(reader) {
        match change? {
            MappingChange::Insert(external_id, internal_id) => {
                // Update internal to external mapping
                if internal_id as usize >= internal_to_external.len() {
                    internal_to_external
                        .resize(internal_id as usize + 1, PointIdType::NumId(u64::MAX));
                }
                let replaced_external_id = internal_to_external[internal_id as usize];
                internal_to_external[internal_id as usize] = external_id;

                // If point already exists, drop existing mapping
                if deleted
                    .get(internal_id as usize)
                    .is_some_and(|deleted| !deleted)
                {
                    // Fixing corrupted mapping - this id should be recovered from WAL
                    // This should not happen in normal operation, but it can happen if
                    // the database is corrupted.
                    log::warn!(
                        "removing duplicated external id {external_id} in internal id {replaced_external_id}",
                    );
                    debug_assert!(false, "should never have to remove");
                    match replaced_external_id {
                        PointIdType::NumId(num) => {
                            external_to_internal_num.remove(&num);
                        }
                        PointIdType::Uuid(uuid) => {
                            external_to_internal_uuid.remove(&uuid);
                        }
                    }
                }

                // Mark point entry as not deleted
                if internal_id as usize >= deleted.len() {
                    deleted.resize(internal_id as usize + 1, true);
                }
                deleted.set(internal_id as usize, false);

                // Set external to internal mapping
                match external_id {
                    PointIdType::NumId(num) => {
                        external_to_internal_num.insert(num, internal_id);
                    }
                    PointIdType::Uuid(uuid) => {
                        external_to_internal_uuid.insert(uuid, internal_id);
                    }
                }
            }
            MappingChange::Delete(external_id) => {
                // Remove external to internal mapping
                let internal_id = match external_id {
                    PointIdType::NumId(idx) => external_to_internal_num.remove(&idx),
                    PointIdType::Uuid(uuid) => external_to_internal_uuid.remove(&uuid),
                };
                let Some(internal_id) = internal_id else {
                    continue;
                };

                // Set internal to external mapping back to max int
                if (internal_id as usize) < internal_to_external.len() {
                    internal_to_external[internal_id as usize] = PointIdType::NumId(u64::MAX);
                }

                // Mark internal point as deleted
                if internal_id as usize >= deleted.len() {
                    deleted.resize(internal_id as usize + 1, true);
                }
                deleted.set(internal_id as usize, true);
            }
        }
    }

    let mappings = PointMappings::new(
        deleted,
        internal_to_external,
        external_to_internal_num,
        external_to_internal_uuid,
    );

    Ok(mappings)
}

pub(super) fn reconcile_persisted_mapping_changes(
    pending: &Mutex<Vec<MappingChange>>,
    changes: &Vec<MappingChange>,
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
