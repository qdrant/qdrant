mod prof;

use criterion::{criterion_group, criterion_main, Criterion};
use rand::rngs::StdRng;
use rand::{thread_rng, SeedableRng};
use segment::fixtures::index_fixtures::{random_vector, FakeFilterContext, TestRawScorerProducer};
use segment::index::hnsw_index::graph_layers::GraphLayers;
use segment::index::hnsw_index::point_scorer::FilteredScorer;
use segment::spaces::simple::CosineMetric;
use segment::types::PointOffsetType;

const NUM_POINTS: usize = 10000;


fn conditional_search_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let mut group = c.benchmark_group("conditional-search-group");

    group.bench_function("conditional-search", |b| {
        b.iter(|| {

        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = conditional_search_benchmark
}

criterion_main!(benches);
