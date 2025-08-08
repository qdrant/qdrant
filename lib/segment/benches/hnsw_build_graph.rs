#[cfg(not(target_os = "windows"))]
mod prof;

use common::types::PointOffsetType;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::SeedableRng;
use rand::rngs::StdRng;
use segment::fixtures::index_fixtures::TestRawScorerProducer;
use segment::index::hnsw_index::HnswM;
use segment::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use segment::types::Distance;

const NUM_VECTORS: usize = 10000;
const DIM: usize = 32;
const M: usize = 16;
const EF_CONSTRUCT: usize = 64;
const USE_HEURISTIC: bool = true;

fn hnsw_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let vector_holder =
        TestRawScorerProducer::new(DIM, Distance::Cosine, NUM_VECTORS, false, &mut rng);
    let mut group = c.benchmark_group("hnsw-index-build-group");
    group.sample_size(10);
    group.bench_function("hnsw_index", |b| {
        b.iter(|| {
            let mut rng = rand::rng();
            let mut graph_layers_builder = GraphLayersBuilder::new(
                NUM_VECTORS,
                HnswM::new2(M),
                EF_CONSTRUCT,
                10,
                USE_HEURISTIC,
            );
            for idx in 0..(NUM_VECTORS as PointOffsetType) {
                let level = graph_layers_builder.get_random_layer(&mut rng);
                graph_layers_builder.set_levels(idx, level);
                graph_layers_builder.link_new_point(idx, vector_holder.internal_scorer(idx));
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
