use std::fmt::Debug;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::types::PointOffsetType;
use rand::Rng;
use sparse::common::sparse_vector::SparseVector;
use sparse::common::sparse_vector_fixture::random_sparse_vector;
use sparse::index::inverted_index::InvertedIndex;

use crate::common::operation_error::OperationResult;
use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
use crate::fixtures::payload_context_fixture::FixtureIdTracker;
use crate::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use crate::index::sparse_index::sparse_vector_index::{
    SparseVectorIndex, SparseVectorIndexOpenArgs,
};
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::VectorIndex;
use crate::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use crate::vector_storage::sparse::simple_sparse_vector_storage::open_simple_sparse_vector_storage;
use crate::vector_storage::VectorStorage;

/// Prepares a sparse vector index with a given iterator of sparse vectors
pub fn fixture_sparse_index_from_iter<I: InvertedIndex>(
    data_dir: &Path,
    vectors: impl ExactSizeIterator<Item = SparseVector>,
    full_scan_threshold: usize,
    index_type: SparseIndexType,
) -> OperationResult<SparseVectorIndex<I>> {
    let stopped = AtomicBool::new(false);

    // directories
    let index_dir = &data_dir.join("index");
    let payload_dir = &data_dir.join("payload");
    let storage_dir = &data_dir.join("storage");

    // setup
    let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(vectors.len())));
    let payload_storage = InMemoryPayloadStorage::default();
    let wrapped_payload_storage = Arc::new(AtomicRefCell::new(payload_storage.into()));
    let payload_index = StructPayloadIndex::open(
        wrapped_payload_storage,
        id_tracker.clone(),
        std::collections::HashMap::new(),
        payload_dir,
        true,
    )?;
    let wrapped_payload_index = Arc::new(AtomicRefCell::new(payload_index));

    let db = open_db(storage_dir, &[DB_VECTOR_CF]).unwrap();
    let vector_storage = Arc::new(AtomicRefCell::new(open_simple_sparse_vector_storage(
        db,
        DB_VECTOR_CF,
        &stopped,
    )?));
    let mut borrowed_storage = vector_storage.borrow_mut();

    let num_vectors = vectors.len();
    let mut num_vectors_not_empty = 0;
    for (idx, vec) in vectors.enumerate() {
        borrowed_storage
            .insert_vector(idx as PointOffsetType, (&vec).into())
            .unwrap();
        num_vectors_not_empty += usize::from(!vec.is_empty());
    }
    drop(borrowed_storage);

    // assert all empty points are in storage
    assert_eq!(
        vector_storage.borrow().available_vector_count(),
        num_vectors,
    );

    let sparse_index_config = SparseIndexConfig::new(Some(full_scan_threshold), index_type, None);
    let sparse_vector_index: SparseVectorIndex<I> =
        SparseVectorIndex::open(SparseVectorIndexOpenArgs {
            config: sparse_index_config,
            id_tracker,
            vector_storage: vector_storage.clone(),
            payload_index: wrapped_payload_index,
            path: index_dir,
            stopped: &stopped,
            tick_progress: || (),
        })?;

    assert_eq!(
        sparse_vector_index.indexed_vector_count(),
        num_vectors_not_empty
    );

    Ok(sparse_vector_index)
}

/// Prepares a sparse vector index with random sparse vectors
pub fn fixture_sparse_index<I: InvertedIndex + Debug, R: Rng + ?Sized>(
    rnd: &mut R,
    num_vectors: usize,
    max_dim: usize,
    full_scan_threshold: usize,
    data_dir: &Path,
) -> SparseVectorIndex<I> {
    fixture_sparse_index_from_iter(
        data_dir,
        (0..num_vectors).map(|_| random_sparse_vector(rnd, max_dim)),
        full_scan_threshold,
        SparseIndexType::ImmutableRam,
    )
    .unwrap()
}

#[macro_export]
macro_rules! fixture_for_all_indices {
    ($test:ident::<_>($($args:tt)*)) => {
        eprintln!("InvertedIndexCompressedImmutableRam<f32>");
        $test::<
            ::sparse::index::inverted_index::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam<f32>
        >($($args)*);

        eprintln!("InvertedIndexCompressedMmap<f32>");
        $test::<
            ::sparse::index::inverted_index::inverted_index_compressed_mmap::InvertedIndexCompressedMmap<f32>
        >($($args)*);

        eprintln!("InvertedIndexImmutableRam");
        $test::<
            ::sparse::index::inverted_index::inverted_index_immutable_ram::InvertedIndexImmutableRam
        >($($args)*);

        eprintln!("InvertedIndexMmap");
        $test::<
            ::sparse::index::inverted_index::inverted_index_mmap::InvertedIndexMmap
        >($($args)*);
    };
}
