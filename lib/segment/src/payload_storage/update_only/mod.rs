#[cfg(test)]
mod tests;

use std::path::Path;

use blobstore::config::LogstoreConfig;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{UniversalAppend, UniversalAppendFs};

use crate::common::operation_error::OperationResult;
use crate::common::update_only_blobstore::UpdateOnlyBlobstore;
use crate::payload_storage::payload_storage_impl::storage_dir;
use crate::types::Payload;

/// The write half of the payload storage for an update-only segment: a
/// short-lived writer opened for one batch and dropped with it.
///
/// The payload half of the same storage [`PayloadStorageImpl`][1] writes, in
/// its append-only mode, so a slot's payload is written once and never
/// rewritten. [`append_many`](Self::append_many) flushes what it wrote, so a
/// batch is durable when it returns and there is nothing to flush across calls.
///
/// [1]: crate::payload_storage::payload_storage_impl::PayloadStorageImpl
pub struct UpdateOnlyPayloadStorage<S: UniversalAppend + 'static> {
    storage: UpdateOnlyBlobstore<Payload, S>,
}

impl<S: UniversalAppend + 'static> UpdateOnlyPayloadStorage<S> {
    /// Open the payload storage of the segment at `segment_path` for appending,
    /// creating it if the segment has none yet — the append-only counterpart of
    /// [`PayloadStorageImpl::open_or_create`][1].
    ///
    /// A segment whose payload storage was created in mutable mode is rejected:
    /// its file format is not one this writer can append to.
    ///
    /// [1]: crate::payload_storage::payload_storage_impl::PayloadStorageImpl::open_or_create
    pub fn open(
        fs: &impl UniversalAppendFs<File = S>,
        segment_path: &Path,
    ) -> OperationResult<Self> {
        let storage =
            UpdateOnlyBlobstore::open(fs, &storage_dir(segment_path), LogstoreConfig::DEFAULT)?;

        Ok(Self { storage })
    }

    /// Store one payload per point of a batch, each at the slot the ID tracker
    /// claimed for it, and persist them.
    ///
    /// The slots must come in increasing order and must all be above every slot
    /// this storage already holds a payload for: it is append-only, so a slot
    /// that was written cannot be written again. Slots skipped in between stay
    /// empty for good.
    ///
    /// A point with an empty payload is not stored at all, its slot is skipped:
    /// an unwritten slot already reads back as an empty payload.
    pub fn append_many<'a>(
        &mut self,
        fs: &S::Fs,
        payloads: impl IntoIterator<Item = (PointOffsetType, &'a Payload)>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        for (internal_id, payload) in payloads {
            if payload.is_empty() {
                continue;
            }

            self.storage.put(
                fs,
                internal_id,
                payload,
                hw_counter.ref_payload_io_write_counter(),
            )?;
        }

        // Puts only buffer, the flush is what makes them durable.
        self.storage.flush()
    }
}
