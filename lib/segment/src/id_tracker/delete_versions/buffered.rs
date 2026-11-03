use std::path::Path;
use std::sync::Arc;

use ahash::AHashSet;
use common::bitvec::BitVec;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{MmapFs, UniversalWrite};
use parking_lot::{Mutex, RwLock};

use super::{DeleteLogEntry, DeleteLogWriter, DeleteVersioning};
use crate::common::Flusher;
use crate::common::buffered_update_bitslice::BufferedUpdateBitSlice;
use crate::common::operation_error::OperationResult;

/// The deleted mask of a regular (read-write) immutable or disk-resident id tracker, buffered
/// until flushed like a [`BufferedUpdateBitSlice`], together with the segment's delete log, which
/// a flush writes ahead of the mask. So a regular tracker deleting in a segment that has a delete
/// log keeps the log holding every delete, see [`delete_versions`](super).
///
/// Regular trackers work on local files, so the log is written through [`MmapFs`].
#[derive(Debug)]
pub struct BufferedDeletedSlots<S> {
    /// The deleted slots, as regular trackers and plain readers see them.
    mask: BufferedUpdateBitSlice<S>,
    /// The same deletes as `mask`, but with the version of each, which serverless followers need
    /// to gate deletes. Only written once the log has started.
    log: Arc<Mutex<DeleteLogWriter>>,
    /// Slots deleted since the last flush that reached the log.
    unlogged: Arc<Mutex<Vec<PointOffsetType>>>,
}

impl<S: UniversalWrite + Send + Sync + 'static> BufferedDeletedSlots<S> {
    pub fn new(mask: StoredBitSlice<S>, segment_path: &Path) -> Self {
        Self {
            mask: BufferedUpdateBitSlice::new(mask),
            log: Arc::new(Mutex::new(DeleteLogWriter::new(segment_path))),
            unlogged: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Mark `slot` deleted, buffered until the next flush.
    ///
    /// ## Panics
    /// Panics if the slot is beyond the mask.
    pub fn delete(&self, slot: PointOffsetType) {
        self.mask.set(slot as usize, true);
        self.unlogged.lock().push(slot);
    }

    /// The slots deleted since the last flush that reached the log, for the caller to give each its
    /// version in [`flusher`](Self::flusher).
    pub fn unlogged(&self) -> Vec<PointOffsetType> {
        self.unlogged.lock().clone()
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        self.mask.clear_cache()
    }

    /// Start the delete log of a freshly built segment whose mask is flushed, so its deletes come
    /// from the log from the start. Does nothing with [`DeleteVersioning::Legacy`].
    pub fn start_log(&self, versioning: DeleteVersioning) -> OperationResult<()> {
        match versioning {
            DeleteVersioning::Record => {}
            DeleteVersioning::Legacy => return Ok(()),
        }
        let stored = self.mask.stored();
        append_log(&self.log, &stored, versioning, &[])
    }

    /// Flush the deletes: `entries`, the [`unlogged`](Self::unlogged) slots with their versions, go
    /// to the delete log first, then the mask.
    pub fn flusher(&self, entries: Vec<DeleteLogEntry>, versioning: DeleteVersioning) -> Flusher {
        let mask_flusher = self.mask.flusher();
        if entries.is_empty() {
            return mask_flusher;
        }

        let log = Arc::clone(&self.log);
        let stored = self.mask.stored();
        let unlogged = Arc::clone(&self.unlogged);
        Box::new(move || {
            append_log(&log, &stored, versioning, &entries)?;
            mask_flusher()?;

            let logged: AHashSet<PointOffsetType> =
                entries.iter().map(|entry| entry.slot).collect();
            unlogged.lock().retain(|slot| !logged.contains(slot));
            Ok(())
        })
    }
}

/// Append `entries` to the log. A log started here begins with the deletes the stored mask holds,
/// the ones flushed before these.
fn append_log<S: UniversalWrite>(
    log: &Mutex<DeleteLogWriter>,
    stored: &RwLock<StoredBitSlice<S>>,
    versioning: DeleteVersioning,
    entries: &[DeleteLogEntry],
) -> OperationResult<()> {
    let mut log = log.lock();
    let baseline = match versioning {
        DeleteVersioning::Record if !log.is_started(&MmapFs)? => {
            stored.read().read_all()?.into_owned()
        }
        DeleteVersioning::Record | DeleteVersioning::Legacy => BitVec::new(),
    };
    log.append(&MmapFs, versioning, &baseline, entries)
}
