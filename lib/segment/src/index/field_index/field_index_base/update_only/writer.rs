//! The storage every appendable field index writer shares, and the seam that
//! is all they differ by.

use std::path::Path;

use blobstore::Blob;
use blobstore::config::{Compression, DEFAULT_PAGE_SIZE_BYTES, LogstoreConfig};
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalAppend;
use serde_json::Value;

use crate::common::operation_error::OperationResult;
use crate::common::update_only_blobstore::UpdateOnlyBlobstore;

/// Storage layout for an appendable field index this writer creates.
///
/// Uncompressed, as every appendable index is on the mutable side. Unlike that
/// side the page size is not scaled down: an append-only page starts empty and
/// grows, so there is no preallocation overhead to bound.
const INDEX_LOGSTORE_CONFIG: LogstoreConfig = LogstoreConfig {
    page_capacity_bytes: DEFAULT_PAGE_SIZE_BYTES,
    compression: Compression::None,
};

/// How one appendable field index turns a point's payload values into what it
/// persists for that point.
pub trait UpdateOnlyIndexKind {
    /// The blob the index stores for a single point.
    type Stored: Blob;

    /// What to store for a point whose indexed field holds `values`, or `None`
    /// when there is nothing to index — no value of the type this index accepts,
    /// or no value at all.
    fn extract(&self, values: &[&Value]) -> OperationResult<Option<Self::Stored>>;
}

/// The write half of one appendable field index of an update-only segment: a
/// short-lived writer opened for one batch and dropped with it.
///
/// Writes only what the index persists; whoever opens the index next rebuilds
/// the in-memory structure from these very values.
pub struct UpdateOnlyValueIndex<K: UpdateOnlyIndexKind, S: UniversalAppend + 'static> {
    kind: K,
    storage: UpdateOnlyBlobstore<K::Stored, S>,
}

impl<K: UpdateOnlyIndexKind, S: UniversalAppend + 'static> UpdateOnlyValueIndex<K, S> {
    /// Open the index storage directory at `dir` for appending, creating it if
    /// the field has no index there yet.
    pub fn open(fs: S::Fs, dir: &Path, kind: K) -> OperationResult<Self> {
        let storage = UpdateOnlyBlobstore::open(fs, dir, INDEX_LOGSTORE_CONFIG)?;
        Ok(Self { kind, storage })
    }

    /// Index `values`, the point's values for this index's field, at the slot
    /// the ID tracker claimed for it.
    ///
    /// Buffers only; [`flush`](Self::flush) is what persists the batch. Slots
    /// must come in increasing order and must all be above every slot this
    /// index already holds values for.
    ///
    /// A point with nothing to index is skipped rather than stored empty: an
    /// empty slot reads back as a point this index does not cover.
    pub fn add_point(
        &mut self,
        slot: PointOffsetType,
        values: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let Some(stored) = self.kind.extract(values)? else {
            return Ok(());
        };

        self.storage.put(
            slot,
            &stored,
            hw_counter.ref_payload_index_io_write_counter(),
        )
    }

    /// Persist everything buffered since the last flush.
    pub fn flush(&mut self) -> OperationResult<()> {
        self.storage.flush()
    }
}
