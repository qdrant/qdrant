#[cfg(not(target_os = "windows"))]
mod prof;

use std::hint::black_box;

use common::types::PointOffsetType;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::SeedableRng;
use rand::rngs::StdRng;
use segment::fixtures::index_fixtures::random_vector;
use segment::index::hnsw_index::graph_layers::SearchAlgorithm;
use segment::spaces::simple::CosineMetric;
use segment::vector_storage::DEFAULT_STOPPED;

const NUM_VECTORS: usize = 1_000_000;
const DIM: usize = 64;
const M: usize = 16;
const TOP: usize = 10;
const EF_CONSTRUCT: usize = 100;
const EF: usize = 100;
const USE_HEURISTIC: bool = true;

mod fixture;

type Metric = CosineMetric;

fn hnsw_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw-search-graph");

    let (vector_holder, mut graph_layers) =
        fixture::make_cached_graph::<Metric>(NUM_VECTORS, DIM, M, EF_CONSTRUCT, USE_HEURISTIC);

    let mut rng = StdRng::seed_from_u64(42);
    group.bench_function("uncompressed", |b| {
        b.iter(|| {
            let query = random_vector(&mut rng, DIM);

            let scorer = vector_holder.scorer(query);

            black_box(
                graph_layers
                    .search(
                        TOP,
                        EF,
                        SearchAlgorithm::Hnsw,
                        scorer,
                        None,
                        &DEFAULT_STOPPED,
                    )
                    .unwrap(),
            );
        })
    });

    graph_layers.compress_ram();
    let mut rng = StdRng::seed_from_u64(42);
    group.bench_function("compressed", |b| {
        b.iter(|| {
            let query = random_vector(&mut rng, DIM);

            let scorer = vector_holder.scorer(query);

            black_box(
                graph_layers
                    .search(
                        TOP,
                        EF,
                        SearchAlgorithm::Hnsw,
                        scorer,
                        None,
                        &DEFAULT_STOPPED,
                    )
                    .unwrap(),
            );
        })
    });

    let mut plain_search_range: Vec<PointOffsetType> =
        (0..NUM_VECTORS as PointOffsetType).collect();
    let mut rng = StdRng::seed_from_u64(42);
    group.bench_function("plain", |b| {
        b.iter(|| {
            let query = random_vector(&mut rng, DIM);

            let mut scorer = vector_holder.scorer(query);

            let mut top_score = 0.;
            let scores = scorer.score_points(&mut plain_search_range, NUM_VECTORS);
            scores.for_each(|score| {
                if score.score > top_score {
                    top_score = score.score
                }
            });
        })
    });

    group.finish();
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = hnsw_benchmark
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = hnsw_benchmark
}

criterion_main!(benches);
