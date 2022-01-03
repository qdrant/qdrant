use atomic_refcell::AtomicRefCell;
use criterion::{criterion_group, criterion_main, Criterion};
use ndarray::{Array, Array2};
use rand::distributions::Standard;
use rand::Rng;
use std::sync::Arc;
use tempdir::TempDir;

use segment::spaces::tools::peek_top_scores_iterable;
use segment::types::{Distance, PointOffsetType, VectorElementType};
use segment::vector_storage::simple_vector_storage::open_simple_vector_storage;
use segment::vector_storage::{ScoredPointOffset, VectorStorage};

const NUM_VECTORS: usize = 50000;
const DIM: usize = 1000; // Larger dimensionality - greater the SIMD advantage

fn random_vector(size: usize) -> Vec<VectorElementType> {
    let rng = rand::thread_rng();

    rng.sample_iter(Standard).take(size).collect()
}

fn init_vector_storage(
    dir: &TempDir,
    dim: usize,
    num: usize,
    dist: Distance,
) -> Arc<AtomicRefCell<dyn VectorStorage>> {
    let storage = open_simple_vector_storage(dir.path(), dim, dist).unwrap();
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
    let dir = TempDir::new("storage_dir").unwrap();

    let dist = Distance::Dot;
    let storage = init_vector_storage(&dir, DIM, NUM_VECTORS, dist);
    let borrowed_storage = storage.borrow();

    let mut group = c.benchmark_group("storage-score-all");
    group.sample_size(1000);

    group.bench_function("storage vector search", |b| {
        b.iter(|| {
            let vector = random_vector(DIM);
            borrowed_storage.score_all(&vector, 10)
        })
    });
}

fn benchmark_ndarray(c: &mut Criterion) {
    let mut matrix = Array2::<f32>::zeros((NUM_VECTORS, DIM));

    for i in 0..NUM_VECTORS {
        let vector = Array::from(random_vector(DIM));
        matrix.row_mut(i).assign(&vector);
    }

    eprintln!("matrix.shape() = {:#?}", matrix.shape());

    c.bench_function("ndarray SIMD dot production", |b| {
        b.iter(|| {
            let vector = Array::from(random_vector(DIM));
            let production_result = matrix.dot(&vector);

            peek_top_scores_iterable(
                production_result
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(idx, score)| ScoredPointOffset {
                        idx: idx as PointOffsetType,
                        score,
                    }),
                10,
            );
        })
    });
}

criterion_group!(benches, benchmark_ndarray, benchmark_naive);
criterion_main!(benches);
