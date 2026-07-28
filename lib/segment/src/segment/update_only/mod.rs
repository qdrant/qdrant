//! Update-only segment: the write-side counterpart of
//! [`ReadOnlySegment`](crate::segment::read_only::ReadOnlySegment).
//!
//! An [`UpdateOnlySegment`] exposes no read API at all — its public surface is
//! storing points and tombstoning slots. Internally it still *reads*: an
//! operation like `set_payload` names a point but not its vectors, so the point
//! has to be resolved against what is stored before it can be appended in full.
//! Those reads go through the same read-only components a follower uses, which
//! is why the segment carries them.
//!
//! What it deliberately does **not** carry is just as important. No vector
//! index (an appendable segment never persists one), no quantized vectors, and
//! no payload index of the segments it only reads from — the writer never
//! updates the payload index of an immutable segment, not even to record a
//! deletion, so on a remote backend those files are never fetched. That is the
//! whole point of a separate segment type rather than a mode on
//! `ReadOnlySegment`: the component set is narrower, and the open path proves
//! it.

mod append;
mod lifecycle;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use atomic_refcell::{AtomicRef, AtomicRefCell};
use common::universal_io::UniversalRead;
use uuid::Uuid;

use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::payload_storage::read_only::ReadOnlyPayloadStorage;
use crate::segment::update_view::SegmentUpdateView;
use crate::segment::vector_data_storage::VectorDataStorageRead;
use crate::types::{SegmentConfig, VectorNameBuf};
use crate::vector_storage::read_only::VectorStorageReadEnum;

/// A segment a batch writer updates: read components to resolve points with,
/// append-only components to store them through.
///
/// Generic over the backend `S`, like `ReadOnlySegment` — the serverless
/// updater runs against object storage, a local process against memory-mapped
/// files.
pub struct UpdateOnlySegment<S: UniversalRead + 'static> {
    pub uuid: Uuid,
    /// Path to the segment directory.
    pub segment_path: PathBuf,
    /// Backend the segment was opened on. Retained because appends need a
    /// filesystem handle of their own: the caching wrapper an open may go
    /// through only lives for that open.
    pub fs: S::Fs,

    pub id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
    pub payload_storage: Arc<AtomicRefCell<ReadOnlyPayloadStorage<S>>>,
    pub vector_data: HashMap<VectorNameBuf, UpdateOnlyVectorData<S>>,

    pub segment_config: SegmentConfig,
    /// Whether this segment can accept appends. Only an appendable segment is
    /// a write target; the rest are read (and tombstoned) but never grown.
    appendable: bool,
}

/// A single named vector of an [`UpdateOnlySegment`]: storage only.
///
/// Contrast [`ReadOnlyVectorData`], which also carries the vector index and the
/// quantized vectors. Neither is of any use to a writer — an appendable segment
/// does not persist a vector index, and quantized vectors are derived data — so
/// neither is opened.
///
/// [`ReadOnlyVectorData`]: crate::segment::read_only::ReadOnlyVectorData
pub struct UpdateOnlyVectorData<S: UniversalRead + 'static> {
    pub vector_storage: Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
}

impl<S: UniversalRead + 'static> VectorDataStorageRead for UpdateOnlyVectorData<S> {
    type StorageRef<'a>
        = AtomicRef<'a, VectorStorageReadEnum<S>>
    where
        Self: 'a;

    fn vector_storage(&self) -> Self::StorageRef<'_> {
        self.vector_storage.borrow()
    }
}

/// Concrete [`SegmentUpdateView`] instantiation over an [`UpdateOnlySegment`].
pub type UpdateOnlySegmentUpdateViewFor<'s, S> = SegmentUpdateView<
    's,
    ReadOnlyIdTrackerEnum<S>,
    ReadOnlyPayloadStorage<S>,
    UpdateOnlyVectorData<S>,
>;

impl<S: UniversalRead + 'static> UpdateOnlySegment<S> {
    /// Whether this segment accepts appends, and can therefore be the target of
    /// a write.
    pub fn is_appendable(&self) -> bool {
        self.appendable
    }

    /// Run `f` against the segment's data as a [`SegmentUpdateView`] — the one
    /// place batch resolution logic lives, shared with any other segment kind
    /// that can produce the same view.
    pub fn with_update_view<T>(
        &self,
        f: impl FnOnce(UpdateOnlySegmentUpdateViewFor<'_, S>) -> T,
    ) -> T {
        let id_tracker = self.id_tracker.borrow();
        let payload_storage = self.payload_storage.borrow();

        f(SegmentUpdateView::new(
            &id_tracker,
            &payload_storage,
            &self.vector_data,
            &self.segment_config,
        ))
    }
}
