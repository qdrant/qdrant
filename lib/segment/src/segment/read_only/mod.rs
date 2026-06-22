use std::collections::HashMap;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

use atomic_refcell::{AtomicRef, AtomicRefCell};
use common::universal_io::UniversalRead;
use uuid::Uuid;

use crate::id_tracker::mutable_id_tracker::read_only::LiveReloadResult;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::index::field_index::ReadOnlyFieldIndex;
use crate::index::read_only::VectorIndexReadEnum;
use crate::index::struct_payload_index::StructPayloadIndexReadView;
use crate::index::struct_payload_index::read_only::ReadOnlyStructPayloadIndex;
use crate::payload_storage::read_only::ReadOnlyPayloadStorage;
use crate::segment::read_view::SegmentReadView;
use crate::segment::vector_data_read::VectorDataRead;
use crate::types::{SegmentConfig, SegmentType, VectorNameBuf};
use crate::vector_storage::quantized::quantized_vectors::ReadOnlyQuantizedVectors;
use crate::vector_storage::read_only::VectorStorageReadEnum;

mod config_reload;
mod lifecycle;
mod live_reload;

pub use config_reload::SegmentConfigReloadDiff;
mod read_entry;
#[cfg(test)]
mod tests;

pub struct ReadOnlySegment<S: UniversalRead + 'static> {
    pub uuid: Uuid,
    /// Path to the segment directory
    pub segment_path: PathBuf,

    pub id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
    pub vector_data: HashMap<VectorNameBuf, ReadOnlyVectorData<S>>,
    pub payload_index: Arc<AtomicRefCell<ReadOnlyStructPayloadIndex<S>>>,
    pub payload_storage: Arc<AtomicRefCell<ReadOnlyPayloadStorage<S>>>,

    /// Id-tracker delta consumed but not yet fully applied to every component.
    ///
    /// [`live_reload`](ReadOnlySegment::live_reload) drains the id-tracker delta
    /// (which advances tracker state and cannot be replayed) and fans it out to
    /// all components. If any component fails mid-way, the delta is kept here so
    /// the next reload folds in fresh changes and replays the union — components
    /// can't drift out of sync. Empty once everything is in sync.
    pub pending_reload: AtomicRefCell<LiveReloadResult>,

    /// Shows what kind of indexes and storages are used in this segment
    pub segment_type: SegmentType,
    pub segment_config: SegmentConfig,
}

pub struct ReadOnlyVectorData<S: UniversalRead + 'static> {
    pub vector_index: Arc<AtomicRefCell<VectorIndexReadEnum<S>>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
    pub quantized_vectors: Arc<AtomicRefCell<Option<ReadOnlyQuantizedVectors<S>>>>,
}

impl<S: UniversalRead + 'static> VectorDataRead for ReadOnlyVectorData<S> {
    type IndexRef<'a>
        = AtomicRef<'a, VectorIndexReadEnum<S>>
    where
        Self: 'a;

    type StorageRef<'a>
        = AtomicRef<'a, VectorStorageReadEnum<S>>
    where
        Self: 'a;

    fn vector_index(&self) -> Self::IndexRef<'_> {
        self.vector_index.borrow()
    }

    fn vector_storage(&self) -> Self::StorageRef<'_> {
        self.vector_storage.borrow()
    }
}

/// Concrete [`SegmentReadView`] instantiation that wraps a [`ReadOnlySegment`].
pub type ReadOnlySegmentReadViewFor<'s, S> = SegmentReadView<
    's,
    ReadOnlyIdTrackerEnum<S>,
    StructPayloadIndexReadView<
        's,
        ReadOnlyPayloadStorage<S>,
        ReadOnlyIdTrackerEnum<S>,
        VectorStorageReadEnum<S>,
        ReadOnlyFieldIndex<S>,
    >,
    ReadOnlyPayloadStorage<S>,
    ReadOnlyVectorData<S>,
>;

impl<S: UniversalRead + 'static> ReadOnlySegment<S> {
    pub fn with_view<T>(&self, f: impl FnOnce(ReadOnlySegmentReadViewFor<'_, S>) -> T) -> T {
        let id_tracker = self.id_tracker.borrow();
        let payload_index = self.payload_index.borrow();
        let payload_storage = self.payload_storage.borrow();

        // Mirror `Segment::with_view`: nest the payload-index view so segment
        // reads go through `StructPayloadIndexReadView`'s `PayloadIndexRead`
        // impl. The inner `with_view` borrows the index's `id_tracker` cell for
        // the closure scope.
        payload_index.with_view(|payload_index_view| {
            let view = SegmentReadView {
                id_tracker: id_tracker.deref(),
                payload_index: &payload_index_view,
                payload_storage: payload_storage.deref(),
                vector_data: &self.vector_data,
                segment_config: &self.segment_config,
                // A read-only segment never accepts appends.
                appendable_flag: false,
            };

            f(view)
        })
    }
}
