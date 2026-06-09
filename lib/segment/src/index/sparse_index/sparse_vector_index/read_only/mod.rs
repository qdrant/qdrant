mod read;

use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::universal_io::UniversalRead;
use sparse::SearchScratchPool;
use sparse::index::inverted_index::InvertedIndex;

use crate::id_tracker::IdTrackerEnum;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::index::field_index::FieldIndex;
use crate::index::sparse_index::indices_tracker::IndicesTracker;
use crate::index::sparse_index::sparse_index_config::SparseIndexConfig;
use crate::index::sparse_index::sparse_search_telemetry::SparseSearchesTelemetry;
use crate::index::sparse_index::sparse_vector_index::read_view::SparseVectorIndexReadView;
use crate::index::struct_payload_index::{StructPayloadIndex, StructPayloadIndexReadView};
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::vector_storage::VectorStorageEnum;
use crate::vector_storage::read_only::VectorStorageReadEnum;

/// Read-only, generic-over-storage counterpart of [`SparseVectorIndex`].
///
/// The id tracker and vector storage are parameterized by the backing storage
/// `S`, while the inverted index (`TInvertedIndex`) is whichever persisted layout
/// was loaded — an immutable-ram or an `S`-backed mmap variant.
///
/// [`SparseVectorIndex`]: super::SparseVectorIndex
pub struct ReadOnlySparseVectorIndex<S: UniversalRead, TInvertedIndex: InvertedIndex> {
    config: SparseIndexConfig,
    id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    inverted_index: TInvertedIndex,
    searches_telemetry: SparseSearchesTelemetry,
    indices_tracker: IndicesTracker,
    search_scratch_pool: SearchScratchPool,
}

/// Read-only view over a [`ReadOnlySparseVectorIndex`].
///
/// The top-level backends are read-only ([`ReadOnlyIdTrackerEnum`] /
/// [`VectorStorageReadEnum`]), while the payload index view is still built over
/// the in-memory enums of [`StructPayloadIndex`].
type ReadView<'a, S, TInvertedIndex> = SparseVectorIndexReadView<
    'a,
    ReadOnlyIdTrackerEnum<S>,
    VectorStorageReadEnum<S>,
    StructPayloadIndexReadView<
        'a,
        PayloadStorageEnum,
        IdTrackerEnum,
        VectorStorageEnum,
        FieldIndex,
    >,
    TInvertedIndex,
>;

impl<S: UniversalRead, TInvertedIndex: InvertedIndex> ReadOnlySparseVectorIndex<S, TInvertedIndex> {
    pub fn inverted_index(&self) -> &TInvertedIndex {
        &self.inverted_index
    }

    /// Borrow all backing storages and hand a read view to `f`, mirroring
    /// [`SparseVectorIndex::with_view`].
    ///
    /// [`SparseVectorIndex::with_view`]: super::SparseVectorIndex::with_view
    pub fn with_view<R>(&self, f: impl FnOnce(ReadView<'_, S, TInvertedIndex>) -> R) -> R {
        let id_tracker = self.id_tracker.borrow();
        let vector_storage = self.vector_storage.borrow();
        let payload_index = self.payload_index.borrow();

        payload_index.with_view(|payload_index_view| {
            let read_view = SparseVectorIndexReadView {
                config: self.config,
                id_tracker: &*id_tracker,
                vector_storage: &*vector_storage,
                payload_index: payload_index_view,
                inverted_index: &self.inverted_index,
                searches_telemetry: &self.searches_telemetry,
                indices_tracker: &self.indices_tracker,
                search_scratch_pool: &self.search_scratch_pool,
            };
            f(read_view)
        })
    }
}
