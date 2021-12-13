mod prof;

use criterion::{criterion_group, criterion_main, Criterion};
use rand::rngs::StdRng;
use rand::{thread_rng, SeedableRng};
use segment::fixtures::index_fixtures::{
    random_vector, FakeConditionChecker, TestRawScorerProducer,
};
use segment::index::hnsw_index::graph_layers::GraphLayers;
use segment::index::hnsw_index::point_scorer::FilteredScorer;
use segment::types::{Distance, PointOffsetType};

const NUM_VECTORS: usize = 100000;
const DIM: usize = 64;
const M: usize = 16;
const TOP: usize = 10;
const EF_CONSTRUCT: usize = 100;
const EF: usize = 100;
const USE_HEURISTIC: bool = true;

fn hnsw_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let vector_holder = TestRawScorerProducer::new(DIM, NUM_VECTORS, Distance::Cosine, &mut rng);
    let mut group = c.benchmark_group("hnsw-index-build-group");
    let mut rng = thread_rng();
    let fake_condition_checker = FakeConditionChecker {};

    let mut graph_layers = GraphLayers::new(NUM_VECTORS, M, M * 2, EF_CONSTRUCT, 10, USE_HEURISTIC);
    for idx in 0..(NUM_VECTORS as PointOffsetType) {
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

    group.bench_function("hnsw_search", |b| {
        b.iter(|| {
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

    group.bench_function("plain_search", |b| {
        b.iter(|| {
            let query = random_vector(&mut rng, DIM);

            let raw_scorer = vector_holder.get_raw_scorer(query);
            let scorer = FilteredScorer {
                raw_scorer: &raw_scorer,
                condition_checker: &fake_condition_checker,
                filter: None,
            };

            let mut iter = (0..NUM_VECTORS as PointOffsetType).into_iter();
            let mut top_score = 0.;
            scorer.score_iterable_points(&mut iter, NUM_VECTORS, |score| {
                if score.score > top_score {
                    top_score = score.score
                }
            });
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = hnsw_benchmark
}

criterion_main!(benches);
