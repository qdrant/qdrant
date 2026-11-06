use std::path::Path;
use std::sync::Arc;

use ahash::AHashSet;
use common::is_alive_lock::IsAliveLock;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{MmapFs, UniversalWrite};
use parking_lot::Mutex;

use super::{DeleteLogEntry, DeleteLogWriter, DeleteVersioning};
use crate::common::Flusher;
use crate::common::buffered_update_bitslice::BufferedUpdateBitSlice;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::DELETED_POINT_VERSION;
use crate::types::SeqNumberType;

/// The deletes of a regular (read-write) immutable or disk-resident id tracker, buffered until
/// flushed. They go to one place, as [`DeleteVersioning`] says: the delete log with their
/// versions, or the deleted mask. See [`delete_versions`](super).
///
/// Regular trackers work on local files, so the log is written through [`MmapFs`].
#[derive(Debug)]
pub enum BufferedDeletes<S> {
    /// Deletes set in the deleted mask.
    Unversioned { mask: BufferedUpdateBitSlice<S> },
    /// Deletes appended to the delete log with their versions. The mask is left as it is; only
    /// its length is kept, to bound the slots.
    Versioned {
        mask_len: u64,
        log: Arc<Mutex<DeleteLogWriter>>,
        /// Deletes made since the last flush that reached the log, with their versions.
        unlogged: Arc<Mutex<Vec<DeleteLogEntry>>>,
        /// Lock to prevent concurrent flush and drop
        is_alive_flush_lock: IsAliveLock,
    },
}

impl<S: UniversalWrite + Send + Sync + 'static> BufferedDeletes<S> {
    pub fn new(mask: StoredBitSlice<S>, segment_path: &Path, versioning: DeleteVersioning) -> Self {
        match versioning {
            DeleteVersioning::Unversioned => Self::Unversioned {
                mask: BufferedUpdateBitSlice::new(mask),
            },
            // The tracker read the mask whole at open and versioned deletes never write it, so
            // the handle drops here.
            DeleteVersioning::Versioned => Self::Versioned {
                mask_len: mask.bit_len(),
                log: Arc::new(Mutex::new(DeleteLogWriter::new(segment_path))),
                unlogged: Arc::new(Mutex::new(Vec::new())),
                is_alive_flush_lock: IsAliveLock::new(),
            },
        }
    }

    /// Mark `slot` deleted, buffered until the next flush. `version` is the version of the
    /// operation that deleted it, recorded when deletes go to the log; `None`, for a delete no
    /// operation made, is recorded as [`DELETED_POINT_VERSION`].
    ///
    /// ## Panics
    /// Panics if the slot is beyond the mask.
    pub fn delete(&self, slot: PointOffsetType, version: Option<SeqNumberType>) {
        match self {
            Self::Unversioned { mask } => mask.set(slot as usize, true),
            Self::Versioned {
                mask_len,
                log: _,
                unlogged,
                is_alive_flush_lock: _,
            } => {
                assert!(
                    u64::from(slot) < *mask_len,
                    "slot {slot} out of range: {mask_len}",
                );
                unlogged.lock().push(DeleteLogEntry {
                    slot,
                    version: version.unwrap_or(DELETED_POINT_VERSION),
                });
            }
        }
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            Self::Unversioned { mask } => mask.clear_cache(),
            Self::Versioned {
                mask_len: _,
                log: _,
                unlogged: _,
                is_alive_flush_lock: _,
            } => Ok(()),
        }
    }

    /// Flush the buffered deletes, to the log or the mask.
    pub fn flusher(&self) -> Flusher {
        match self {
            Self::Unversioned { mask } => mask.flusher(),
            Self::Versioned {
                mask_len: _,
                log,
                unlogged,
                is_alive_flush_lock,
            } => log_flusher(log, unlogged, is_alive_flush_lock),
        }
    }
}

fn log_flusher(
    log: &Arc<Mutex<DeleteLogWriter>>,
    unlogged: &Arc<Mutex<Vec<DeleteLogEntry>>>,
    is_alive_flush_lock: &IsAliveLock,
) -> Flusher {
    let mut entries: Vec<DeleteLogEntry> = unlogged.lock().clone();
    if entries.is_empty() {
        return Box::new(|| Ok(()));
    }
    // A slot deleted twice keeps its first delete: stable sort, then dedup keeps the first.
    entries.sort_by_key(|entry| entry.slot);
    entries.dedup_by_key(|entry| entry.slot);

    let log = Arc::clone(log);
    let unlogged = Arc::downgrade(unlogged);
    let is_alive_flush_lock = is_alive_flush_lock.handle();
    Box::new(move || {
        let (Some(is_alive_flush_guard), Some(unlogged)) =
            (is_alive_flush_lock.lock_if_alive(), unlogged.upgrade())
        else {
            return Err(OperationError::cancelled(
                "Aborted flushing on a dropped BufferedDeletes instance",
            ));
        };

        log.lock().append(&MmapFs, &entries)?;
        drop(is_alive_flush_guard);

        let logged: AHashSet<PointOffsetType> = entries.iter().map(|entry| entry.slot).collect();
        unlogged
            .lock()
            .retain(|entry| !logged.contains(&entry.slot));
        Ok(())
    })
}
