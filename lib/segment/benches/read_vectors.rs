use std::cell::LazyCell;
use std::hint::black_box;
use std::ops::Deref as _;
use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::{AccessPattern, Random, Sequential};
use common::mmap::AdviceSetting;
use common::types::PointOffsetType;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::prelude::{SliceRandom, SmallRng};
use rand::{RngExt, SeedableRng};
use segment::data_types::vectors::{TypedMultiDenseVectorRef, VectorElementType, VectorRef};
use segment::types::{Distance, MultiVectorConfig};
use segment::vector_storage::dense::appendable_dense_vector_storage::{
    AppendableMmapDenseVectorStorage, open_appendable_memmap_vector_storage_impl,
};
use segment::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::{
    AppendableMmapMultiDenseVectorStorage, open_appendable_memmap_multi_vector_storage_impl,
};
use segment::vector_storage::sparse::mmap_sparse_vector_storage::MmapSparseVectorStorage;
use segment::vector_storage::{VectorStorage, VectorStorageRead};
use sparse::common::sparse_vector_fixture::random_sparse_vector;
use tempfile::{Builder, TempDir};

const RNG_SEED: u64 = 42;
const POINT_COUNT: usize = 10_000;
const MAX_VECTORS_PER_POINT: usize = 8;
const VECTOR_DIM: usize = 128;
const SPARSE_MAX_DIM: usize = 1000;
const READ_BATCH_SIZE: usize = 1024;

criterion_main!(benches);

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench
}

fn bench(c: &mut Criterion) {
    let sequential_keys: Vec<_> = (0..READ_BATCH_SIZE as PointOffsetType).collect();

    let mut random_keys: Vec<_> = (0..POINT_COUNT as PointOffsetType).collect();
    random_keys.shuffle(&mut SmallRng::seed_from_u64(RNG_SEED));
    random_keys.truncate(READ_BATCH_SIZE);

    let mut group = c.benchmark_group("read-vectors");

    {
        let tmpdir = tmpdir();
        let storage = LazyCell::new(|| storage_dense(tmpdir.path()));

        group.bench_function("appendable-mmap-dense/sequential", |b| {
            b.iter(|| read_vectors::<Sequential, _>(storage.deref(), &sequential_keys));
        });

        group.bench_function("appendable-mmap-dense/random", |b| {
            b.iter(|| read_vectors::<Random, _>(storage.deref(), &random_keys));
        });
    }

    {
        let tmpdir = tmpdir();
        let storage = LazyCell::new(|| storage_multi(tmpdir.path()));

        group.bench_function("appendable-mmap-multi-dense/sequential", |b| {
            b.iter(|| read_vectors::<Sequential, _>(storage.deref(), &sequential_keys));
        });

        group.bench_function("appendable-mmap-multi-dense/random", |b| {
            b.iter(|| read_vectors::<Random, _>(storage.deref(), &random_keys));
        });
    }

    {
        let tmpdir = tmpdir();
        let storage = LazyCell::new(|| storage_sparse(tmpdir.path()));

        group.bench_function("mmap-sparse/sequential", |b| {
            b.iter(|| read_vectors::<Sequential, _>(storage.deref(), &sequential_keys));
        });

        group.bench_function("mmap-sparse/random", |b| {
            b.iter(|| read_vectors::<Random, _>(storage.deref(), &random_keys));
        });
    }

    group.finish();
}

fn tmpdir() -> TempDir {
    Builder::new()
        .prefix("read-vectors-bench")
        .tempdir()
        .expect("tempdir created")
}

fn storage_dense(path: &Path) -> AppendableMmapDenseVectorStorage<VectorElementType> {
    let mut storage = open_appendable_memmap_vector_storage_impl(
        path,
        VECTOR_DIM,
        Distance::Dot,
        AdviceSetting::Global,
        true,
    )
    .expect("appendable mmap dense storage opened");

    let mut rng = SmallRng::seed_from_u64(RNG_SEED);
    let mut vector_buffer = vec![0.0; VECTOR_DIM];
    let hw_counter = HardwareCounterCell::disposable();

    for point_id in 0..POINT_COUNT as PointOffsetType {
        let vector = random_vector(&mut rng, &mut vector_buffer, VECTOR_DIM);

        storage
            .insert_vector(point_id, VectorRef::from(vector), &hw_counter)
            .expect("vector inserted");
    }

    storage.flusher()().expect("storage flushed");
    storage.populate().expect("storage populated");

    storage
}

fn storage_multi(path: &Path) -> AppendableMmapMultiDenseVectorStorage<VectorElementType> {
    let mut storage = open_appendable_memmap_multi_vector_storage_impl(
        path,
        VECTOR_DIM,
        Distance::Dot,
        MultiVectorConfig::default(),
        AdviceSetting::Global,
        true,
    )
    .expect("appendable mmap multi dense storage opened");

    let mut rng = SmallRng::seed_from_u64(RNG_SEED);
    let mut vector_buffer = vec![0.0; MAX_VECTORS_PER_POINT * VECTOR_DIM];
    let hw_counter = HardwareCounterCell::disposable();

    for point_id in 0..POINT_COUNT as PointOffsetType {
        let vectors_count = rng.random_range(1..=MAX_VECTORS_PER_POINT);
        let vector = random_vector(&mut rng, &mut vector_buffer, vectors_count * VECTOR_DIM);
        let vector = TypedMultiDenseVectorRef::new(vector, VECTOR_DIM);

        storage
            .insert_vector(point_id, VectorRef::from(vector), &hw_counter)
            .expect("vector inserted");
    }

    storage.flusher()().expect("storage flushed");
    storage.populate().expect("storage populated");

    storage
}

fn storage_sparse(path: &Path) -> MmapSparseVectorStorage {
    let mut storage =
        MmapSparseVectorStorage::open_or_create(path).expect("mmap sparse storage opened");

    let mut rng = SmallRng::seed_from_u64(RNG_SEED);
    let hw_counter = HardwareCounterCell::disposable();

    for point_id in 0..POINT_COUNT as PointOffsetType {
        let vector = random_sparse_vector(&mut rng, SPARSE_MAX_DIM);

        storage
            .insert_vector(point_id, VectorRef::from(&vector), &hw_counter)
            .expect("vector inserted");
    }

    storage.flusher()().expect("storage flushed");
    storage.populate().expect("storage populated");

    storage
}

fn random_vector<'a>(
    rng: &mut SmallRng,
    buffer: &'a mut [VectorElementType],
    dim: usize,
) -> &'a [VectorElementType] {
    let buffer = &mut buffer[..dim];
    buffer.fill_with(|| rng.random());
    buffer
}

fn read_vectors<P, S>(storage: &S, point_offsets: &[PointOffsetType]) -> usize
where
    P: AccessPattern,
    S: VectorStorageRead,
{
    let point_offsets = point_offsets.iter().map(|&point_offset| ((), point_offset));

    let mut bytes_read = 0;
    storage.read_vectors::<P, _>(point_offsets, |_, _, vector| {
        bytes_read += vector.estimate_size_in_bytes();
    });

    black_box(bytes_read)
}
