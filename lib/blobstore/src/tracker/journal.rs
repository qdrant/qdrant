//! Journal of the pointer writes of the tracker, to repair torn tracker writes.
//!
//! The tracker file is written in place, so a crash during a flush can tear pointer writes and
//! leave pointers to invalid data. A flush therefore first durably appends its pointer writes to
//! this journal, and only then writes them to the tracker. Opening a writable storage replays the
//! journal onto the tracker, which repairs torn writes, and then removes it.
//!
//! Once the tracker is durable the journal isn't needed anymore. It is removed after a flush once
//! it grew past [`MAX_SIZE`], and created again by a next flush.
//!
//! ## File format
//!
//! The file is a sequence of entries, appended by flushes:
//!
//! +-------------+-------------+-------------------------------------------------+
//! | length: u32 | CRC32C: u32 | records: [(point offset: u32, slot: 16 bytes)]  |
//! +-------------+-------------+-------------------------------------------------+
//!
//! Entries are only appended, so a crash can only leave an incomplete entry at the end. Replay
//! stops at the first incomplete or invalid entry.

use std::cmp::Ordering;
use std::io::{ErrorKind, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{self, AtomicU64};

use ahash::AHashMap;
use common::fs::sync_parent_dir;
use fs_err::File;

use super::{OptionalPointer, PointOffset, PointerUpdates};
use crate::Result;
use crate::error::BlobstoreError;

pub(super) const FILE_NAME: &str = "tracker_journal.dat";

/// Size past which the journal is removed once the tracker is durable, about 1k pointer writes.
const MAX_SIZE: u64 = 1024 * size_of::<Record>() as u64;

/// Maximum number of records in a single entry, keeps the entry length within `u32`.
const MAX_ENTRY_RECORDS: usize = 1024 * 1024;

/// Size of the length and checksum in front of each entry.
const ENTRY_HEADER_SIZE: usize = 2 * size_of::<u32>();

/// A journaled pointer write: the slot to write at a point offset.
#[derive(Debug, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C)]
pub(super) struct Record {
    pub point_offset: PointOffset,
    pub slot: OptionalPointer,
}

#[derive(Debug)]
pub(crate) struct Journal {
    path: PathBuf,
    /// Length of the file up to the last durable entry.
    ///
    /// A failed append may leave an incomplete entry behind it, which is cut off before appending
    /// again.
    len: AtomicU64,
}

impl Journal {
    pub(super) fn new(dir: &Path) -> Self {
        Self {
            path: dir.join(FILE_NAME),
            len: AtomicU64::new(0),
        }
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    /// Durably append the pointer writes of the given updates, before writing them to the
    /// tracker.
    pub(crate) fn append(
        &self,
        pending_updates: &AHashMap<PointOffset, PointerUpdates>,
    ) -> Result<()> {
        if pending_updates.is_empty() {
            return Ok(());
        }

        let records: Vec<Record> = pending_updates
            .iter()
            .map(|(&point_offset, updates)| Record {
                point_offset,
                slot: OptionalPointer::from(updates.current),
            })
            .collect();

        let mut buffer = Vec::new();
        for chunk in records.chunks(MAX_ENTRY_RECORDS) {
            let payload: &[u8] = bytemuck::cast_slice(chunk);
            buffer.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            buffer.extend_from_slice(&crc32c::crc32c(payload).to_le_bytes());
            buffer.extend_from_slice(payload);
        }

        let len = self.len.load(atomic::Ordering::Relaxed);
        let mut file = File::options().create(true).append(true).open(&self.path)?;

        // Cut off an incomplete entry left behind by a failed append, replay would stop at it
        match file.metadata()?.len().cmp(&len) {
            Ordering::Equal => {}
            // An append-only handle cannot truncate on Windows, use a separate one
            Ordering::Greater => File::options().write(true).open(&self.path)?.set_len(len)?,
            Ordering::Less => {
                return Err(BlobstoreError::service_error(format!(
                    "Tracker journal {} is shorter than expected",
                    self.path.display(),
                )));
            }
        }

        file.write_all(&buffer)?;
        file.sync_all()?;

        // Persist the directory entry of a new file as well
        if len == 0 {
            sync_parent_dir(&self.path)?;
        }

        self.len
            .store(len + buffer.len() as u64, atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Read the records of all entries in order, `None` if there is no journal.
    ///
    /// Stops at the first incomplete or invalid entry, as left behind by a crash during an
    /// append.
    pub(super) fn read(&self) -> Result<Option<Vec<Record>>> {
        let bytes = match fs_err::read(&self.path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };

        let mut records = Vec::new();
        let mut rest = bytes.as_slice();
        while let Some((payload, tail)) = split_entry(rest) {
            records.extend(bytemuck::pod_collect_to_vec::<u8, Record>(payload));
            rest = tail;
        }

        if !rest.is_empty() {
            log::warn!(
                "Ignoring incomplete or invalid entries in the last {} bytes of tracker journal {}",
                rest.len(),
                self.path.display(),
            );
        }

        Ok(Some(records))
    }

    /// Remove the journal if it grew past [`MAX_SIZE`], the tracker must be durable.
    pub(crate) fn remove_if_large(&self) -> Result<()> {
        if self.len.load(atomic::Ordering::Relaxed) > MAX_SIZE {
            self.remove()?;
        }
        Ok(())
    }

    /// Remove the journal, the tracker must be durable.
    pub(super) fn remove(&self) -> Result<()> {
        match fs_err::remove_file(&self.path) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
        self.len.store(0, atomic::Ordering::Relaxed);
        Ok(())
    }
}

/// Split the payload of the first entry off `bytes`, `None` if it is incomplete or invalid.
fn split_entry(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    let (header, rest) = bytes.split_at_checked(ENTRY_HEADER_SIZE)?;
    let (length, crc) = header.split_at(size_of::<u32>());
    let length = u32::from_le_bytes(length.try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(crc.try_into().unwrap());

    let (payload, rest) = rest.split_at_checked(length)?;
    // Entries are never empty, a zeroed tail would read as one
    let is_valid =
        length > 0 && length.is_multiple_of(size_of::<Record>()) && crc32c::crc32c(payload) == crc;
    is_valid.then_some((payload, rest))
}
