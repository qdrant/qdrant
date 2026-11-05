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
use crate::types::SeqNumberType;

/// The deletes of a regular (read-write) immutable or disk-resident id tracker, buffered until
/// flushed. They go to one place, as [`DeleteVersioning`] says: the delete log with their
/// versions, or the deleted mask. See [`delete_versions`](super).
///
/// Regular trackers work on local files, so the log is written through [`MmapFs`].
#[derive(Debug)]
pub struct BufferedDeletes<S> {
    versioning: DeleteVersioning,
    /// The deleted mask, written by [`DeleteVersioning::Unversioned`] flushes only.
    mask: BufferedUpdateBitSlice<S>,
    /// The delete log, written by [`DeleteVersioning::Versioned`] flushes only.
    log: Arc<Mutex<DeleteLogWriter>>,
    /// Slots deleted since the last flush that reached the log.
    unlogged: Arc<Mutex<Vec<PointOffsetType>>>,
    /// Lock to prevent concurrent flush and drop
    is_alive_flush_lock: IsAliveLock,
}

impl<S: UniversalWrite + Send + Sync + 'static> BufferedDeletes<S> {
    pub fn new(mask: StoredBitSlice<S>, segment_path: &Path, versioning: DeleteVersioning) -> Self {
        Self {
            versioning,
            mask: BufferedUpdateBitSlice::new(mask),
            log: Arc::new(Mutex::new(DeleteLogWriter::new(segment_path))),
            unlogged: Arc::new(Mutex::new(Vec::new())),
            is_alive_flush_lock: IsAliveLock::new(),
        }
    }

    /// Mark `slot` deleted, buffered until the next flush.
    ///
    /// ## Panics
    /// Panics if the slot is beyond the mask.
    pub fn delete(&self, slot: PointOffsetType) {
        match self.versioning {
            DeleteVersioning::Versioned => {
                assert!(
                    (slot as usize) < self.mask.len(),
                    "slot {slot} out of range: {}",
                    self.mask.len(),
                );
                self.unlogged.lock().push(slot);
            }
            DeleteVersioning::Unversioned => self.mask.set(slot as usize, true),
        }
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        self.mask.clear_cache()
    }

    /// Flush the buffered deletes. `version_of` gives the version of each slot deleted since the
    /// last flush that reached the log; it is only asked when deletes go to the log.
    pub fn flusher(&self, version_of: impl Fn(PointOffsetType) -> SeqNumberType) -> Flusher {
        match self.versioning {
            DeleteVersioning::Unversioned => self.mask.flusher(),
            DeleteVersioning::Versioned => self.log_flusher(version_of),
        }
    }

    fn log_flusher(&self, version_of: impl Fn(PointOffsetType) -> SeqNumberType) -> Flusher {
        let mut entries: Vec<DeleteLogEntry> = self
            .unlogged
            .lock()
            .iter()
            .map(|&slot| DeleteLogEntry {
                slot,
                version: version_of(slot),
            })
            .collect();
        if entries.is_empty() {
            return Box::new(|| Ok(()));
        }
        entries.sort_unstable_by_key(|entry| entry.slot);
        entries.dedup_by_key(|entry| entry.slot);

        let log = Arc::clone(&self.log);
        let unlogged = Arc::downgrade(&self.unlogged);
        let is_alive_flush_lock = self.is_alive_flush_lock.handle();
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

            let logged: AHashSet<PointOffsetType> =
                entries.iter().map(|entry| entry.slot).collect();
            unlogged.lock().retain(|slot| !logged.contains(slot));
            Ok(())
        })
    }
}
