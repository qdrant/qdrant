use std::path::PathBuf;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use sparse::index::inverted_index::InvertedIndex;

use crate::id_tracker::IdTrackerSS;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::vector_storage::VectorStorageEnum;

pub struct SparseVectorIndex<T: InvertedIndex> {
    id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    path: PathBuf,
    inverted_index: T,
}

impl<T: InvertedIndex> SparseVectorIndex<T> {
    pub fn new(
        id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
        vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
        payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
        path: PathBuf,
        inverted_index: T,
    ) -> Self {
        Self {
            id_tracker,
            vector_storage,
            payload_index,
            path,
            inverted_index,
        }
    }
}

// TODO impl VectorIndex for SparseVectorIndex
