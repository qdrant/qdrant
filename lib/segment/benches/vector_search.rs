use criterion::{Criterion, criterion_group, criterion_main};
use itertools::Itertools;
use ndarray::{Array, Array1, Array2, ArrayBase, ShapeBuilder};
use tempdir::TempDir;

use segment::spaces::tools::{peek_top_scores, peek_top_scores_iterable};
use segment::types::Distance;
use segment::vector_storage::simple_vector_storage::SimpleVectorStorage;
use segment::vector_storage::vector_storage::{ScoredPointOffset, VectorStorage};

const NUM_VECTORS: usize = 50000;
const DIM: usize = 1000;

fn random_vector(size: usize) -> Vec<f32> {
    let mut vec: Vec<f32> = Vec::with_capacity(size);
    for _ in 0..vec.capacity() {
        vec.push(rand::random());
    };
    return vec;
}

fn init_vector_storage(dir: &TempDir, dim: usize, num: usize) -> SimpleVectorStorage {
    let mut storage = SimpleVectorStorage::open(dir.path(), dim).unwrap();

    for _i in 0..num {
        let vector: Vec<f32> = random_vector(dim);
        storage.put_vector(&vector).unwrap();
    }

    storage
}

fn benchmark_naive(c: &mut Criterion) {
    let dir = TempDir::new("storage_dir").unwrap();

    let dist = Distance::Dot;
    let storage = init_vector_storage(&dir, DIM, NUM_VECTORS);

    c.bench_function("naive vector search",
                     |b| b.iter(|| {
                         let vector = random_vector(DIM);
                         storage.score_all(&vector, 10, &dist)
                     }));
}

fn benchmark_ndarray(c: &mut Criterion) {
    let mut matrix: Array2<f32> = Array::<f32, _>::zeros((NUM_VECTORS, DIM));

    for i in 0..NUM_VECTORS {
        let vector = Array::from(random_vector(DIM));
        matrix.row_mut(i).assign(&vector);
    }

    eprintln!("matrix.shape() = {:#?}", matrix.shape());


    c.bench_function("ndarray BLAS dot production",
                     |b| b.iter(|| {
                         let vector = Array::from(random_vector(DIM));
                         let production_result: Array1<f32> = matrix.dot(&vector);
                         let top = peek_top_scores_iterable(
                             production_result.iter()
                                 .cloned()
                                 .enumerate()
                                 .map(
                                     |(idx, score)| ScoredPointOffset { idx, score }),
                             10,
                             &Distance::Dot,
                         );
                     }));
}

criterion_group!(benches, benchmark_ndarray, benchmark_naive);
criterion_main!(benches);
