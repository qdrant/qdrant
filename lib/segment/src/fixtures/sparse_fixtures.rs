use std::borrow::Cow;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs};
use rand::Rng;
use sparse::common::sparse_vector::SparseVector;
use sparse::common::sparse_vector_fixture::random_sparse_vector;
use sparse::common::types::Weight;
use sparse::index::inverted_index::InvertedIndex;
use sparse::index::inverted_index::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;
use sparse::index::inverted_index::inverted_index_compressed_mmap::InvertedIndexCompressedMmap;
use sparse::index::inverted_index::inverted_index_ram::InvertedIndexRam;

use crate::common::operation_error::OperationResult;
use crate::fixtures::payload_context_fixture::create_id_tracker_fixture;
use crate::index::VectorIndexRead;
use crate::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use crate::index::sparse_index::sparse_vector_index::{
    SparseOpenPlan, SparseVectorIndex, SparseVectorIndexOpenArgs,
};
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use crate::vector_storage::sparse::mmap_sparse_vector_storage::MmapSparseVectorStorage;
use crate::vector_storage::{VectorStorage, VectorStorageEnum, VectorStorageRead};

// Per-type constructors over the local mmap filesystem, passed to the fixtures
// and `SparseVectorIndex::open` in place of trait-level construction.
pub fn ram_open(path: &Path) -> common::universal_io::Result<InvertedIndexRam> {
    InvertedIndexRam::open(&MmapFs, path)
}
pub fn ram_from_ram(
    ram: Cow<InvertedIndexRam>,
    path: &Path,
) -> common::universal_io::Result<InvertedIndexRam> {
    InvertedIndexRam::from_ram_index(&MmapFs, ram, path)
}
pub fn iram_open<W: Weight + 'static>(
    path: &Path,
) -> common::universal_io::Result<InvertedIndexCompressedImmutableRam<W>> {
    InvertedIndexCompressedImmutableRam::open(&MmapFs, path)
}
pub fn iram_from_ram<W: Weight + 'static>(
    ram: Cow<InvertedIndexRam>,
    path: &Path,
) -> common::universal_io::Result<InvertedIndexCompressedImmutableRam<W>> {
    InvertedIndexCompressedImmutableRam::from_ram_index(&MmapFs, ram, path)
}
pub fn mmap_open<W: Weight + 'static>(
    path: &Path,
) -> common::universal_io::Result<InvertedIndexCompressedMmap<W, MmapFile>> {
    InvertedIndexCompressedMmap::open(&MmapFs, path)
}
pub fn mmap_from_ram<W: Weight + 'static>(
    ram: Cow<InvertedIndexRam>,
    path: &Path,
) -> common::universal_io::Result<InvertedIndexCompressedMmap<W, MmapFile>> {
    InvertedIndexCompressedMmap::from_ram_index(&MmapFs, ram, path)
}

/// Test/bench convenience that runs the load-or-build orchestration and
/// assembles a [`SparseVectorIndex`], given the concrete inverted-index
/// constructors. Production code (`create_sparse_vector_index`) inlines the same
/// `plan` → construct → `finish` steps per type without these callbacks.
pub fn open_sparse_index<I: InvertedIndex>(
    args: SparseVectorIndexOpenArgs<impl FnMut()>,
    open_index: impl FnOnce(&Path) -> common::universal_io::Result<I>,
    from_ram_index: impl FnOnce(Cow<InvertedIndexRam>, &Path) -> common::universal_io::Result<I>,
) -> OperationResult<SparseVectorIndex<I>> {
    let SparseVectorIndexOpenArgs {
        config,
        id_tracker,
        vector_storage,
        payload_index,
        path,
        stopped,
        mut tick_progress,
    } = args;
    let plan = SparseVectorIndex::<I>::plan(
        config,
        &id_tracker,
        &vector_storage,
        path,
        stopped,
        &mut tick_progress,
    )?;
    let (inverted_index, config, indices_tracker, persist) = match plan {
        SparseOpenPlan::Load {
            config,
            indices_tracker,
        } => (open_index(path)?, config, indices_tracker, false),
        SparseOpenPlan::Build {
            config,
            ram_index,
            indices_tracker,
            persist,
        } => (
            from_ram_index(Cow::Owned(ram_index), path)?,
            config,
            indices_tracker,
            persist,
        ),
    };
    SparseVectorIndex::finish(
        config,
        id_tracker,
        vector_storage,
        payload_index,
        path,
        inverted_index,
        indices_tracker,
        persist,
    )
}

/// Prepares a sparse vector index with a given iterator of sparse vectors.
///
/// `open_index` / `from_ram_index` build the concrete inverted index (the caller
/// knows the type and filesystem); see `SparseVectorIndex::open`.
pub fn fixture_sparse_index_from_iter<I: InvertedIndex>(
    data_dir: &Path,
    vectors: impl ExactSizeIterator<Item = SparseVector>,
    full_scan_threshold: usize,
    index_type: SparseIndexType,
    open_index: impl FnOnce(&Path) -> common::universal_io::Result<I>,
    from_ram_index: impl FnOnce(Cow<InvertedIndexRam>, &Path) -> common::universal_io::Result<I>,
) -> OperationResult<SparseVectorIndex<I>> {
    let stopped = AtomicBool::new(false);

    // directories
    let index_dir = &data_dir.join("index");
    let payload_dir = &data_dir.join("payload");
    let storage_dir = &data_dir.join("storage");

    // setup
    let id_tracker = Arc::new(AtomicRefCell::new(create_id_tracker_fixture(vectors.len())));
    let payload_storage = InMemoryPayloadStorage::default();
    let wrapped_payload_storage = Arc::new(AtomicRefCell::new(payload_storage.into()));
    let payload_index = StructPayloadIndex::open(
        wrapped_payload_storage,
        id_tracker.clone(),
        std::collections::HashMap::new(),
        payload_dir,
        true,
        true,
    )?;
    let wrapped_payload_index = Arc::new(AtomicRefCell::new(payload_index));

    let vector_storage = Arc::new(AtomicRefCell::new(VectorStorageEnum::SparseMmap(
        MmapSparseVectorStorage::open_or_create(storage_dir)?,
    )));
    let mut borrowed_storage = vector_storage.borrow_mut();

    let num_vectors = vectors.len();
    let mut num_vectors_not_empty = 0;
    let hw_counter = HardwareCounterCell::new();
    for (idx, vec) in vectors.enumerate() {
        borrowed_storage
            .insert_vector(idx as PointOffsetType, (&vec).into(), &hw_counter)
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
    let sparse_vector_index: SparseVectorIndex<I> = open_sparse_index(
        SparseVectorIndexOpenArgs {
            config: sparse_index_config,
            id_tracker,
            vector_storage: vector_storage.clone(),
            payload_index: wrapped_payload_index,
            path: index_dir,
            stopped: &stopped,
            tick_progress: || (),
        },
        open_index,
        from_ram_index,
    )?;

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
    open_index: impl FnOnce(&Path) -> common::universal_io::Result<I>,
    from_ram_index: impl FnOnce(Cow<InvertedIndexRam>, &Path) -> common::universal_io::Result<I>,
) -> SparseVectorIndex<I> {
    fixture_sparse_index_from_iter(
        data_dir,
        (0..num_vectors).map(|_| random_sparse_vector(rnd, max_dim)),
        full_scan_threshold,
        SparseIndexType::ImmutableRam,
        open_index,
        from_ram_index,
    )
    .unwrap()
}
