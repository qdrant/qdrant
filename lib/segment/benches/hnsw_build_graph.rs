#[cfg(not(target_os = "windows"))]
mod prof;

use common::types::PointOffsetType;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::rngs::StdRng;
use rand::{thread_rng, SeedableRng};
use segment::fixtures::index_fixtures::{FakeFilterContext, TestRawScorerProducer};
use segment::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use segment::index::hnsw_index::point_scorer::FilteredScorer;
use segment::spaces::simple::CosineMetric;

const NUM_VECTORS: usize = 10000;
const DIM: usize = 32;
const M: usize = 16;
const EF_CONSTRUCT: usize = 64;
const USE_HEURISTIC: bool = true;

fn hnsw_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let vector_holder = TestRawScorerProducer::<CosineMetric>::new(DIM, NUM_VECTORS, &mut rng);
    let mut group = c.benchmark_group("hnsw-index-build-group");
    group.sample_size(10);
    group.bench_function("hnsw_index", |b| {
        b.iter(|| {
            let mut rng = thread_rng();
            let mut graph_layers_builder =
                GraphLayersBuilder::new(NUM_VECTORS, M, M * 2, EF_CONSTRUCT, 10, USE_HEURISTIC);
            let fake_filter_context = FakeFilterContext {};
            for idx in 0..(NUM_VECTORS as PointOffsetType) {
                let added_vector = vector_holder.vectors.get(idx).to_vec();
                let raw_scorer = vector_holder.get_raw_scorer(added_vector).unwrap();
                let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
                let level = graph_layers_builder.get_random_layer(&mut rng);
                graph_layers_builder.set_levels(idx, level);
                graph_layers_builder
                    .link_new_point(idx, scorer, &false.into())
                    .unwrap();
            }
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
