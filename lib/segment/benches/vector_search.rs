use std::path::Path;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::distributions::Standard;
use rand::Rng;
use segment::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
use segment::data_types::vectors::VectorElementType;
use segment::types::Distance;
use segment::vector_storage::simple_vector_storage::open_simple_vector_storage;
use segment::vector_storage::{VectorStorage, VectorStorageEnum};
use tempfile::Builder;

const NUM_VECTORS: usize = 50000;
const DIM: usize = 1000; // Larger dimensionality - greater the SIMD advantage

fn random_vector(size: usize) -> Vec<VectorElementType> {
    let rng = rand::thread_rng();

    rng.sample_iter(Standard).take(size).collect()
}

fn init_vector_storage(
    path: &Path,
    dim: usize,
    num: usize,
    dist: Distance,
) -> Arc<AtomicRefCell<VectorStorageEnum>> {
    let db = open_db(path, &[DB_VECTOR_CF]).unwrap();
    let storage = open_simple_vector_storage(db, DB_VECTOR_CF, dim, dist).unwrap();
    {
        let mut borrowed_storage = storage.borrow_mut();
        for _i in 0..num {
            let vector: Vec<VectorElementType> = random_vector(dim);
            borrowed_storage.put_vector(vector).unwrap();
        }
    }

    storage
}

fn benchmark_naive(c: &mut Criterion) {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    let dist = Distance::Dot;
    let storage = init_vector_storage(dir.path(), DIM, NUM_VECTORS, dist);
    let borrowed_storage = storage.borrow();
    let scorer_builder = borrowed_storage.scorer_builder();

    let mut group = c.benchmark_group("storage-score-all");
    group.sample_size(1000);

    group.bench_function("storage vector search", |b| {
        b.iter(|| {
            let vector = random_vector(DIM);
            scorer_builder.score_all(&vector, 10)
        })
    });
}

criterion_group!(benches, benchmark_naive);
criterion_main!(benches);
