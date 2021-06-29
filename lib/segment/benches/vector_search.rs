use criterion::{criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use ndarray::{Array, Array1, Array2, ArrayBase, Axis, ShapeBuilder};
use tempdir::TempDir;

use segment::spaces::tools::{peek_top_scores, peek_top_scores_iterable};
use segment::types::{Distance, PointOffsetType, VectorElementType};
use segment::vector_storage::simple_vector_storage::SimpleVectorStorage;
use segment::vector_storage::vector_storage::{ScoredPointOffset, VectorStorage};

const NUM_VECTORS: usize = 50000;
const DIM: usize = 1000; // Larger dimensionality - greater the BLAS advantage

fn random_vector(size: usize) -> Vec<VectorElementType> {
    let mut vec: Vec<VectorElementType> = Vec::with_capacity(size);
    for _ in 0..vec.capacity() {
        vec.push(rand::random());
    }
    return vec;
}

fn init_vector_storage(
    dir: &TempDir,
    dim: usize,
    num: usize,
    dist: Distance,
) -> SimpleVectorStorage {
    let mut storage = SimpleVectorStorage::open(dir.path(), dim, dist).unwrap();

    for _i in 0..num {
        let vector: Vec<VectorElementType> = random_vector(dim);
        storage.put_vector(vector).unwrap();
    }

    storage
}

fn benchmark_naive(c: &mut Criterion) {
    let dir = TempDir::new("storage_dir").unwrap();

    let dist = Distance::Dot;
    let storage = init_vector_storage(&dir, DIM, NUM_VECTORS, dist);

    c.bench_function("storage vector search", |b| {
        b.iter(|| {
            let vector = random_vector(DIM);
            storage.score_all(&vector, 10)
        })
    });
}

fn benchmark_ndarray(c: &mut Criterion) {
    let mut matrix: Array2<f32> = Array::<f32, _>::zeros((NUM_VECTORS, DIM));

    for i in 0..NUM_VECTORS {
        let vector = Array::from(random_vector(DIM));
        matrix.row_mut(i).assign(&vector);
    }

    eprintln!("matrix.shape() = {:#?}", matrix.shape());

    c.bench_function("ndarray BLAS dot production", |b| {
        b.iter(|| {
            let vector = Array::from(random_vector(DIM));
            let mut production_result = matrix.dot(&vector);

            let top = peek_top_scores_iterable(
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
