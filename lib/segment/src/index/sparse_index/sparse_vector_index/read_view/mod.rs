mod search;

use sparse::SearchScratchPool;
use sparse::index::inverted_index::InvertedIndex;

use crate::id_tracker::{IdTrackerEnum, IdTrackerRead};
use crate::index::PayloadIndexRead;
use crate::index::field_index::FieldIndex;
use crate::index::sparse_index::indices_tracker::IndicesTracker;
use crate::index::sparse_index::sparse_index_config::SparseIndexConfig;
use crate::index::sparse_index::sparse_search_telemetry::SparseSearchesTelemetry;
use crate::index::struct_payload_index::StructPayloadIndexReadView;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

/// Read-only view over a sparse vector index.
///
/// The top-level id tracker / vector storage / inverted index are decoupled from
/// the payload index view (`P`), so read-only backends can be combined with a
/// payload index view built over different (e.g. still-mutable) backends.
pub struct SparseVectorIndexReadView<'a, I, V, P, TInvertedIndex>
where
    I: IdTrackerRead,
    V: VectorStorageRead,
    P: PayloadIndexRead,
    TInvertedIndex: InvertedIndex,
{
    pub(crate) config: SparseIndexConfig,
    pub(crate) id_tracker: &'a I,
    pub(crate) vector_storage: &'a V,
    pub(crate) payload_index: P,
    pub(crate) inverted_index: &'a TInvertedIndex,
    pub(crate) searches_telemetry: &'a SparseSearchesTelemetry,
    pub(crate) indices_tracker: &'a IndicesTracker,
    pub(crate) search_scratch_pool: &'a SearchScratchPool,
}

/// Read-only view over a [`SparseVectorIndex`] backed by the in-memory
/// [`IdTrackerEnum`] / [`VectorStorageEnum`] / [`PayloadStorageEnum`] enums.
///
/// [`SparseVectorIndex`]: super::SparseVectorIndex
pub(crate) type SparseVectorIndexReadViewEnum<'a, TInvertedIndex> = SparseVectorIndexReadView<
    'a,
    IdTrackerEnum,
    VectorStorageEnum,
    StructPayloadIndexReadView<
        'a,
        PayloadStorageEnum,
        IdTrackerEnum,
        VectorStorageEnum,
        FieldIndex,
    >,
    TInvertedIndex,
>;
