//! The write half of the appendable payload indexes, for update-only segments.
//!
//! An appendable field index keeps two things: the values it persists per point,
//! and the in-memory structure it answers queries from. Only the first is state
//! — the second is rebuilt from it on every open, by the mutable index and by
//! its read-only counterpart alike. So a writer that never answers a query has
//! nothing to hold: it turns a point's payload into the values its index would
//! persist, appends them at the point's slot, and is done.
//!
//! What differs between index types is only that translation, which is what
//! [`UpdateOnlyIndexKind`] captures; [`UpdateOnlyValueIndex`] is the storage
//! around it, the same for all of them. Each kind lives next to the index it
//! writes for, as the read-only counterparts do.

use std::path::Path;

use blobstore::Blob;
use blobstore::config::{Compression, DEFAULT_PAGE_SIZE_BYTES, LogstoreConfig};
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalAppend;
use serde_json::Value;

use crate::common::operation_error::OperationResult;
use crate::common::update_only_blobstore::UpdateOnlyBlobstore;
use crate::index::field_index::ValueIndexer;

/// Storage layout for an appendable field index this writer creates.
///
/// Uncompressed, as every appendable index is on the mutable side: the values
/// are numbers, ids and token lists, which compress poorly for the CPU it
/// costs. The mutable side additionally scales its page size down with its
/// block size, to bound the overhead of preallocating the first page; an
/// append-only page starts empty and grows, so there is nothing to bound.
pub const INDEX_LOGSTORE_CONFIG: LogstoreConfig = LogstoreConfig {
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

/// The values a batch indexes, flattened the way [`ValueIndexer::add_point`]
/// flattens them: arrays contribute their elements, and a value the index
/// cannot accept contributes nothing.
///
/// Takes the extraction from the index type `I` itself rather than restating
/// it, so the two sides cannot drift apart.
pub fn extracted_values<I: ValueIndexer>(values: &[&Value]) -> Vec<I::ValueType> {
    values
        .iter()
        .flat_map(|value| I::get_values(value))
        .collect()
}

/// The write half of one appendable field index of an update-only segment: a
/// short-lived writer opened for one batch and dropped with it.
///
/// Writes only what the index persists. The in-memory structure that answers
/// queries is not built here and not kept: whoever opens this index next
/// rebuilds it from these very values.
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
    /// A point with nothing to index is skipped rather than stored empty, as on
    /// the mutable side: a slot with no values of its own reads back as a point
    /// this index does not cover.
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
    pub fn flush(&self) -> OperationResult<()> {
        self.storage.flush()
    }
}
