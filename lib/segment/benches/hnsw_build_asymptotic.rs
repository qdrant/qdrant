mod prof;

use criterion::{criterion_group, criterion_main, Bencher, BenchmarkId, Criterion};
use rand::rngs::StdRng;
use rand::{thread_rng, SeedableRng};
use segment::fixtures::index_fixtures::{
    random_vector, FakeConditionChecker, TestRawScorerProducer,
};
use segment::index::hnsw_index::graph_layers::GraphLayers;
use segment::index::hnsw_index::point_scorer::FilteredScorer;
use segment::spaces::simple::CosineMetric;
use segment::types::PointOffsetType;

const NUM_VECTORS: usize = 10_000;
const DIM: usize = 32;
const M: usize = 16;
const TOP: usize = 10;
const EF_CONSTRUCT: usize = 64;
const EF: usize = 64;
const USE_HEURISTIC: bool = true;

fn build_index(num_vectors: usize) -> (TestRawScorerProducer<CosineMetric>, GraphLayers) {
    let mut rng = thread_rng();

    let vector_holder = TestRawScorerProducer::new(DIM, num_vectors, CosineMetric {}, &mut rng);
    let mut graph_layers = GraphLayers::new(num_vectors, M, M * 2, EF_CONSTRUCT, 10, USE_HEURISTIC);
    let fake_condition_checker = FakeConditionChecker {};
    for idx in 0..(num_vectors as PointOffsetType) {
        let added_vector = vector_holder.vectors[idx as usize].to_vec();
        let raw_scorer = vector_holder.get_raw_scorer(added_vector);
        let scorer = FilteredScorer {
            raw_scorer: &raw_scorer,
            condition_checker: &fake_condition_checker,
            filter: None,
        };
        let level = graph_layers.get_random_layer(&mut rng);
        graph_layers.link_new_point(idx, level, &scorer);
    }
    (vector_holder, graph_layers)
}

fn hnsw_build_asymptotic(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw-index-build-asymptotic");

    let mut rng = thread_rng();

    let (vector_holder, graph_layers) = build_index(NUM_VECTORS);

    group.bench_function("build-n-search-hnsw", |b| {
        b.iter(|| {
            let fake_condition_checker = FakeConditionChecker {};
            let query = random_vector(&mut rng, DIM);
            let raw_scorer = vector_holder.get_raw_scorer(query);
            let scorer = FilteredScorer {
                raw_scorer: &raw_scorer,
                condition_checker: &fake_condition_checker,
                filter: None,
            };
            graph_layers.search(TOP, EF, &scorer);
        })
    });

    let (vector_holder, graph_layers) = build_index(NUM_VECTORS * 5);

    group.bench_function("build-n-search-hnsw-5x", |b| {
        b.iter(|| {
            let fake_condition_checker = FakeConditionChecker {};
            let query = random_vector(&mut rng, DIM);
            let raw_scorer = vector_holder.get_raw_scorer(query);
            let scorer = FilteredScorer {
                raw_scorer: &raw_scorer,
                condition_checker: &fake_condition_checker,
                filter: None,
            };
            graph_layers.search(TOP, EF, &scorer);
        })
    });


    let (vector_holder, graph_layers) = build_index(NUM_VECTORS * 10);

    group.bench_function("build-n-search-hnsw-10x", |b| {
        b.iter(|| {
            let fake_condition_checker = FakeConditionChecker {};
            let query = random_vector(&mut rng, DIM);
            let raw_scorer = vector_holder.get_raw_scorer(query);
            let scorer = FilteredScorer {
                raw_scorer: &raw_scorer,
                condition_checker: &fake_condition_checker,
                filter: None,
            };
            graph_layers.search(TOP, EF, &scorer);
        })
    });
}


criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = hnsw_build_asymptotic
}

criterion_main!(benches);
