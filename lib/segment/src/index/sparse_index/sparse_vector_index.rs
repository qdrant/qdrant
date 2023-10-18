use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::types::ScoredPointOffset;
use sparse::common::sparse_vector::SparseVector;
use sparse::index::inverted_index::InvertedIndex;
use sparse::index::search_context::SearchContext;

use crate::id_tracker::IdTrackerSS;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::vector_storage::VectorStorageEnum;

pub struct SparseVectorIndex<TInvertedIndex: InvertedIndex> {
    id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    path: PathBuf,
    inverted_index: TInvertedIndex,
}

impl<TInvertedIndex: InvertedIndex> SparseVectorIndex<TInvertedIndex> {
    /// Create new sparse vector index
    pub fn new(
        id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
        vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
        payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
        path: PathBuf,
        inverted_index: TInvertedIndex,
    ) -> Self {
        Self {
            id_tracker,
            vector_storage,
            payload_index,
            path,
            inverted_index,
        }
    }

    /// Search index using sparse vector query
    pub fn search_sparse(
        &self,
        query: SparseVector,
        top: usize,
        is_stopped: &AtomicBool,
    ) -> Vec<ScoredPointOffset> {
        let mut search_context = SearchContext::new(query, top, &self.inverted_index, is_stopped);
        search_context.search()
    }
}

// TODO impl VectorIndex for SparseVectorIndex
