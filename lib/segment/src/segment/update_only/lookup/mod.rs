//! The read phase of an update: the components a batch resolves its points
//! against, and nothing that writes.

mod lifecycle;
mod resolve;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::universal_io::UniversalRead;

use super::{AppendableIdTrackerState, DeleteOnlyIdTrackerState, WriterIdTrackerState};
use crate::id_tracker::IdTrackerRead as _;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::payload_storage::read_only::ReadOnlyPayloadStorage;
use crate::types::{SegmentConfig, VectorNameBuf};
use crate::vector_storage::read_only::VectorStorageReadEnum;

/// A segment opened to resolve updates against: read components only. Generic
/// over the backend `S`, like `ReadOnlySegment`, and requiring no more of it
/// than reads — every segment of a shard is opened this way, including those a
/// batch never writes to.
pub struct LookupSegment<S: UniversalRead + 'static> {
    /// Path to the segment directory.
    pub segment_path: PathBuf,

    pub id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
    pub payload_storage: Arc<AtomicRefCell<ReadOnlyPayloadStorage<S>>>,
    /// One storage per named vector — no vector index, no quantized vectors.
    pub vector_data: HashMap<VectorNameBuf, Arc<AtomicRefCell<VectorStorageReadEnum<S>>>>,

    pub segment_config: SegmentConfig,
    /// Whether this segment accepts appends, and can therefore be the target
    /// of a write.
    pub appendable: bool,
}

impl<S: UniversalRead + 'static> LookupSegment<S> {
    /// The id-tracker state a writer resuming this segment picks up from, for
    /// [`UpdateOnlySegmentEnum::open`](super::UpdateOnlySegmentEnum::open).
    ///
    /// Taken from this segment's own reads rather than re-read by the writer:
    /// a second read costs another round-trip on a remote backend and may
    /// land on a different state than the batch resolved against. The deleted
    /// mask is handed over only when already in memory — the disk-resident
    /// tracker deliberately avoids materializing it.
    pub fn writer_state(&self) -> WriterIdTrackerState {
        match &*self.id_tracker.borrow() {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => {
                WriterIdTrackerState::Appendable(AppendableIdTrackerState {
                    max_claimed_internal_id: id_tracker.max_claimed_internal_id(),
                    pending_inserts: id_tracker.pending_inserts().collect(),
                    mappings_end: id_tracker.mappings_read_to(),
                })
            }
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => {
                WriterIdTrackerState::DeleteOnly(DeleteOnlyIdTrackerState::Immutable(Some(
                    id_tracker.deleted_point_bitslice().to_bitvec(),
                )))
            }
            ReadOnlyIdTrackerEnum::DiskResident(id_tracker) => {
                WriterIdTrackerState::DeleteOnly(DeleteOnlyIdTrackerState::DiskResident(
                    id_tracker.deleted_full_if_materialized().cloned(),
                ))
            }
        }
    }
}
