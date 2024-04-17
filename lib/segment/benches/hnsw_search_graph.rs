#[cfg(not(target_os = "windows"))]
mod prof;

use common::types::PointOffsetType;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::rngs::StdRng;
use rand::{thread_rng, SeedableRng};
use segment::fixtures::index_fixtures::{random_vector, FakeFilterContext, TestRawScorerProducer};
use segment::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use segment::index::hnsw_index::graph_links::GraphLinksRam;
use segment::index::hnsw_index::point_scorer::FilteredScorer;
use segment::spaces::simple::CosineMetric;

const NUM_VECTORS: usize = 100000;
const DIM: usize = 64;
const M: usize = 16;
const TOP: usize = 10;
const EF_CONSTRUCT: usize = 100;
const EF: usize = 100;
const USE_HEURISTIC: bool = true;

fn hnsw_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let vector_holder = TestRawScorerProducer::<CosineMetric>::new(DIM, NUM_VECTORS, &mut rng);
    let mut group = c.benchmark_group("hnsw-index-search-group");
    let mut rng = thread_rng();
    let fake_filter_context = FakeFilterContext {};

    let mut graph_layers_builder =
        GraphLayersBuilder::new(NUM_VECTORS, M, M * 2, EF_CONSTRUCT, 10, USE_HEURISTIC);
    for idx in 0..(NUM_VECTORS as PointOffsetType) {
        let added_vector = vector_holder.vectors.get(idx);
        #[cfg(not(feature = "f16"))]
        let added_vector = added_vector.to_vec();
        #[cfg(feature = "f16")]
        let added_vector = half::slice::HalfFloatSliceExt::to_f32_vec(added_vector);
        let raw_scorer = vector_holder.get_raw_scorer(added_vector).unwrap();
        let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
        let level = graph_layers_builder.get_random_layer(&mut rng);
        graph_layers_builder.set_levels(idx, level);
        graph_layers_builder.link_new_point(idx, scorer);
    }
    let graph_layers = graph_layers_builder
        .into_graph_layers::<GraphLinksRam>(None)
        .unwrap();

    group.bench_function("hnsw_search", |b| {
        b.iter(|| {
            let query = random_vector(&mut rng, DIM);

            let raw_scorer = vector_holder.get_raw_scorer(query).unwrap();
            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));

            graph_layers.search(TOP, EF, scorer, None);
        })
    });

    let mut plain_search_range: Vec<PointOffsetType> =
        (0..NUM_VECTORS as PointOffsetType).collect();
    group.bench_function("plain_search", |b| {
        b.iter(|| {
            let query = random_vector(&mut rng, DIM);

            let raw_scorer = vector_holder.get_raw_scorer(query).unwrap();
            let mut scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));

            let mut top_score = 0.;
            let scores = scorer.score_points(&mut plain_search_range, NUM_VECTORS);
            scores.iter().copied().for_each(|score| {
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
