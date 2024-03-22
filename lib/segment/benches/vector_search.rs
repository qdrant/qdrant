use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::types::PointOffsetType;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::distributions::Standard;
use rand::Rng;
use segment::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
use segment::data_types::vectors::{DenseVector, Vector, VectorRef};
use segment::fixtures::payload_context_fixture::FixtureIdTracker;
use segment::id_tracker::IdTrackerSS;
use segment::types::Distance;
use segment::vector_storage::simple_dense_vector_storage::open_simple_vector_storage;
use segment::vector_storage::{new_raw_scorer, VectorStorageEnum, VectorStorageUpdater};
use tempfile::Builder;

const NUM_VECTORS: usize = 100000;
const DIM: usize = 1024; // Larger dimensionality - greater the SIMD advantage

fn random_vector(size: usize) -> DenseVector {
    let rng = rand::thread_rng();

    rng.sample_iter(Standard).take(size).collect()
}

fn init_vector_storage(
    path: &Path,
    dim: usize,
    num: usize,
    dist: Distance,
) -> (
    Arc<AtomicRefCell<VectorStorageEnum>>,
    Arc<AtomicRefCell<IdTrackerSS>>,
) {
    let db = open_db(path, &[DB_VECTOR_CF]).unwrap();
    let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(num)));
    let storage =
        open_simple_vector_storage(db, DB_VECTOR_CF, dim, dist, &AtomicBool::new(false)).unwrap();
    {
        let mut borrowed_storage = storage.borrow_mut();
        for i in 0..num {
            let vector: Vector = random_vector(dim).into();
            borrowed_storage
                .insert_vector(i as PointOffsetType, VectorRef::from(&vector))
                .unwrap();
        }
    }

    (storage, id_tracker)
}

fn benchmark_naive(c: &mut Criterion) {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    let dist = Distance::Dot;
    let (storage, id_tracker) = init_vector_storage(dir.path(), DIM, NUM_VECTORS, dist);
    let borrowed_storage = storage.borrow();
    let borrowed_id_tracker = id_tracker.borrow();

    let mut group = c.benchmark_group("storage-score-all");

    group.bench_function("storage vector search", |b| {
        b.iter(|| {
            let vector = random_vector(DIM);
            let vector = vector.as_slice().into();
            new_raw_scorer(
                vector,
                &borrowed_storage,
                borrowed_id_tracker.deleted_point_bitslice(),
            )
            .unwrap()
            .peek_top_all(10)
        })
    });
}

fn random_access_benchmark(c: &mut Criterion) {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    let dist = Distance::Dot;
    let (storage, id_tracker) = init_vector_storage(dir.path(), DIM, NUM_VECTORS, dist);
    let borrowed_storage = storage.borrow();
    let borrowed_id_tracker = id_tracker.borrow();

    let mut group = c.benchmark_group("storage-score-random");

    let vector = random_vector(DIM);
    let vector = vector.as_slice().into();

    let scorer = new_raw_scorer(
        vector,
        &borrowed_storage,
        borrowed_id_tracker.deleted_point_bitslice(),
    )
    .unwrap();

    let mut total_score = 0.;
    group.bench_function("storage vector search", |b| {
        b.iter(|| {
            let random_id = rand::thread_rng().gen_range(0..NUM_VECTORS) as PointOffsetType;
            total_score += scorer.score_point(random_id);
        })
    });
    eprintln!("total_score = {:?}", total_score);
}

criterion_group!(benches, benchmark_naive, random_access_benchmark);
criterion_main!(benches);
