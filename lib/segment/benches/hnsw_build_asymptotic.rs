#[cfg(not(target_os = "windows"))]
mod prof;

use common::types::PointOffsetType;
use criterion::{criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use rand::{thread_rng, Rng};
use segment::data_types::vectors::VectorElementType;
use segment::fixtures::index_fixtures::{random_vector, FakeFilterContext, TestRawScorerProducer};
use segment::index::hnsw_index::graph_layers::GraphLayers;
use segment::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use segment::index::hnsw_index::graph_links::GraphLinksRam;
use segment::index::hnsw_index::point_scorer::FilteredScorer;
use segment::spaces::metric::Metric;
use segment::spaces::simple::{CosineMetric, DotProductMetric};

const NUM_VECTORS: usize = 5_000;
const DIM: usize = 16;
const M: usize = 16;
const TOP: usize = 10;
const EF_CONSTRUCT: usize = 64;
const EF: usize = 64;
const USE_HEURISTIC: bool = true;

fn build_index<TMetric: Metric<VectorElementType>>(
    num_vectors: usize,
) -> (TestRawScorerProducer<TMetric>, GraphLayers<GraphLinksRam>) {
    let mut rng = thread_rng();

    let vector_holder = TestRawScorerProducer::<TMetric>::new(DIM, num_vectors, &mut rng);
    let mut graph_layers_builder =
        GraphLayersBuilder::new(num_vectors, M, M * 2, EF_CONSTRUCT, 10, USE_HEURISTIC);
    let fake_filter_context = FakeFilterContext {};
    for idx in 0..(num_vectors as PointOffsetType) {
        let added_vector = vector_holder.vectors.get(idx).to_vec();
        let raw_scorer = vector_holder.get_raw_scorer(added_vector).unwrap();
        let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
        let level = graph_layers_builder.get_random_layer(&mut rng);
        graph_layers_builder.set_levels(idx, level);
        graph_layers_builder
            .link_new_point(idx, scorer, &false.into())
            .unwrap();
    }
    (
        vector_holder,
        graph_layers_builder.into_graph_layers(None).unwrap(),
    )
}

fn hnsw_build_asymptotic(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw-index-build-asymptotic");

    let mut rng = thread_rng();

    let (vector_holder, graph_layers) = build_index::<CosineMetric>(NUM_VECTORS);

    group.bench_function("build-n-search-hnsw", |b| {
        b.iter(|| {
            let fake_filter_context = FakeFilterContext {};
            let query = random_vector(&mut rng, DIM);
            let raw_scorer = vector_holder.get_raw_scorer(query).unwrap();
            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
            graph_layers.search(TOP, EF, scorer, None);
        })
    });

    for _ in 0..10 {
        let fake_filter_context = FakeFilterContext {};
        let query = random_vector(&mut rng, DIM);
        let raw_scorer = vector_holder.get_raw_scorer(query).unwrap();
        let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
        graph_layers.search(TOP, EF, scorer, None);
    }

    let (vector_holder, graph_layers) = build_index::<CosineMetric>(NUM_VECTORS * 10);

    group.bench_function("build-n-search-hnsw-10x", |b| {
        b.iter(|| {
            let fake_filter_context = FakeFilterContext {};
            let query = random_vector(&mut rng, DIM);
            let raw_scorer = vector_holder.get_raw_scorer(query).unwrap();
            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
            graph_layers.search(TOP, EF, scorer, None);
        })
    });

    group.bench_function("build-n-search-hnsw-10x-score-point", |b| {
        b.iter(|| {
            let fake_filter_context = FakeFilterContext {};
            let query = random_vector(&mut rng, DIM);
            let raw_scorer = vector_holder.get_raw_scorer(query).unwrap();
            let mut scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));

            let mut points_to_score = (0..1500)
                .map(|_| rng.gen_range(0..(NUM_VECTORS * 10)) as u32)
                .collect_vec();
            scorer.score_points(&mut points_to_score, 1000);
        })
    });

    for _ in 0..10 {
        let fake_filter_context = FakeFilterContext {};
        let query = random_vector(&mut rng, DIM);
        let raw_scorer = vector_holder.get_raw_scorer(query).unwrap();
        let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
        graph_layers.search(TOP, EF, scorer, None);
    }
}

fn scoring_vectors(c: &mut Criterion) {
    let mut group = c.benchmark_group("scoring-vector");
    let mut rng = thread_rng();
    let points_per_cycle = 1000;
    let base_num_vectors = 10_000;

    let num_vectors = base_num_vectors;
    let vector_holder = TestRawScorerProducer::<DotProductMetric>::new(DIM, num_vectors, &mut rng);

    group.bench_function("score-point", |b| {
        b.iter(|| {
            let fake_filter_context = FakeFilterContext {};
            let query = random_vector(&mut rng, DIM);
            let raw_scorer = vector_holder.get_raw_scorer(query).unwrap();
            let mut scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));

            let mut points_to_score = (0..points_per_cycle)
                .map(|_| rng.gen_range(0..num_vectors) as u32)
                .collect_vec();
            scorer.score_points(&mut points_to_score, points_per_cycle);
        })
    });

    let num_vectors = base_num_vectors * 10;
    let vector_holder = TestRawScorerProducer::<DotProductMetric>::new(DIM, num_vectors, &mut rng);

    group.bench_function("score-point-10x", |b| {
        b.iter(|| {
            let fake_filter_context = FakeFilterContext {};
            let query = random_vector(&mut rng, DIM);
            let raw_scorer = vector_holder.get_raw_scorer(query).unwrap();
            let mut scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));

            let mut points_to_score = (0..points_per_cycle)
                .map(|_| rng.gen_range(0..num_vectors) as u32)
                .collect_vec();
            scorer.score_points(&mut points_to_score, points_per_cycle);
        })
    });

    let num_vectors = base_num_vectors * 50;
    let vector_holder = TestRawScorerProducer::<DotProductMetric>::new(DIM, num_vectors, &mut rng);

    group.bench_function("score-point-50x", |b| {
        b.iter(|| {
            let fake_filter_context = FakeFilterContext {};
            let query = random_vector(&mut rng, DIM);
            let raw_scorer = vector_holder.get_raw_scorer(query).unwrap();
            let mut scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));

            let mut points_to_score = (0..points_per_cycle)
                .map(|_| rng.gen_range(0..num_vectors) as u32)
                .collect_vec();
            scorer.score_points(&mut points_to_score, points_per_cycle);
        })
    });
}

fn basic_scoring_vectors(c: &mut Criterion) {
    let mut group = c.benchmark_group("scoring-vector");
    let mut rng = thread_rng();
    let points_per_cycle = 1000;
    let base_num_vectors = 10_000_000;

    let num_vectors = base_num_vectors;

    let vectors = (0..num_vectors)
        .map(|_| random_vector(&mut rng, DIM))
        .collect_vec();

    group.bench_function("basic-score-point", |b| {
        b.iter(|| {
            let query = random_vector(&mut rng, DIM);
            let points_to_score = (0..points_per_cycle).map(|_| rng.gen_range(0..num_vectors));

            let _s: f32 = points_to_score
                .map(|x| DotProductMetric::similarity(&vectors[x], &query))
                .sum();
        })
    });

    let num_vectors = base_num_vectors * 2;

    let vectors = (0..num_vectors)
        .map(|_| random_vector(&mut rng, DIM))
        .collect_vec();

    group.bench_function("basic-score-point-10x", |b| {
        b.iter(|| {
            let query = random_vector(&mut rng, DIM);
            let points_to_score = (0..points_per_cycle).map(|_| rng.gen_range(0..num_vectors));

            let _s: f32 = points_to_score
                .map(|x| DotProductMetric::similarity(&vectors[x], &query))
                .sum();
        })
    });
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = hnsw_build_asymptotic, scoring_vectors, basic_scoring_vectors
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = hnsw_build_asymptotic, scoring_vectors, basic_scoring_vectors
}

criterion_main!(benches);
