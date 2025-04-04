#[cfg(not(target_os = "windows"))]
mod prof;

use std::cell::LazyCell;

use criterion::{Criterion, criterion_group, criterion_main};
use itertools::Itertools;
use rand::{Rng, rng};
use segment::fixtures::index_fixtures::{FakeFilterContext, TestRawScorerProducer, random_vector};
use segment::index::hnsw_index::point_scorer::FilteredScorer;
use segment::spaces::metric::Metric;
use segment::spaces::simple::{CosineMetric, DotProductMetric};
use segment::vector_storage::DEFAULT_STOPPED;

const DIM: usize = 16;
const M: usize = 16;
const TOP: usize = 10;
const EF_CONSTRUCT: usize = 64;
const EF: usize = 64;
const USE_HEURISTIC: bool = true;

mod fixture;

fn hnsw_build_asymptotic(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw-index-build-asymptotic");

    let mut rng = rng();

    let setup_5k = LazyCell::new(|| {
        eprintln!();
        fixture::make_cached_graph::<CosineMetric>(5_000, DIM, M, EF_CONSTRUCT, USE_HEURISTIC)
    });

    group.bench_function("build-n-search-hnsw-5k", |b| {
        let (vector_holder, graph_layers) = &*setup_5k;
        b.iter(|| {
            let fake_filter_context = FakeFilterContext {};
            let query = random_vector(&mut rng, DIM);
            let raw_scorer = vector_holder.get_raw_scorer(query).unwrap();
            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
            graph_layers
                .search(TOP, EF, scorer, None, &DEFAULT_STOPPED)
                .unwrap();
        })
    });

    drop(setup_5k);

    const NUM_VECTORS: usize = 1_000_000;
    let setup_1m = LazyCell::new(|| {
        eprintln!();
        fixture::make_cached_graph::<CosineMetric>(NUM_VECTORS, DIM, M, EF_CONSTRUCT, USE_HEURISTIC)
    });

    group.bench_function("build-n-search-hnsw-1M", |b| {
        let (vector_holder, graph_layers) = &*setup_1m;
        b.iter(|| {
            let fake_filter_context = FakeFilterContext {};
            let query = random_vector(&mut rng, DIM);
            let raw_scorer = vector_holder.get_raw_scorer(query).unwrap();
            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
            graph_layers
                .search(TOP, EF, scorer, None, &DEFAULT_STOPPED)
                .unwrap();
        })
    });

    group.bench_function("build-n-search-hnsw-1M-score-point", |b| {
        let (vector_holder, _graph_layers) = &*setup_1m;
        b.iter(|| {
            let fake_filter_context = FakeFilterContext {};
            let query = random_vector(&mut rng, DIM);
            let raw_scorer = vector_holder.get_raw_scorer(query).unwrap();
            let mut scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));

            let mut points_to_score = (0..1500)
                .map(|_| rng.random_range(0..NUM_VECTORS) as u32)
                .collect_vec();
            scorer.score_points(&mut points_to_score, 1000);
        })
    });

    drop(setup_1m);
}

fn scoring_vectors(c: &mut Criterion) {
    let mut group = c.benchmark_group("scoring-vector");
    let mut rng = rng();
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
                .map(|_| rng.random_range(0..num_vectors) as u32)
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
                .map(|_| rng.random_range(0..num_vectors) as u32)
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
                .map(|_| rng.random_range(0..num_vectors) as u32)
                .collect_vec();
            scorer.score_points(&mut points_to_score, points_per_cycle);
        })
    });
}

fn basic_scoring_vectors(c: &mut Criterion) {
    let mut group = c.benchmark_group("scoring-vector");
    let points_per_cycle = 1000;
    let base_num_vectors = 10_000_000;

    let num_vectors = base_num_vectors;
    let setup = LazyCell::new(|| {
        let mut rng = rng();
        (0..num_vectors)
            .map(|_| random_vector(&mut rng, DIM))
            .collect_vec()
    });
    group.bench_function("basic-score-point", |b| {
        let vectors = &*setup;
        let mut rng = rng();
        b.iter(|| {
            let query = random_vector(&mut rng, DIM);
            let points_to_score = (0..points_per_cycle).map(|_| rng.random_range(0..num_vectors));

            let _s: f32 = points_to_score
                .map(|x| DotProductMetric::similarity(&vectors[x], &query))
                .sum();
        })
    });
    drop(setup);

    let num_vectors = base_num_vectors * 2;
    let setup = LazyCell::new(|| {
        let mut rng = rng();
        (0..num_vectors)
            .map(|_| random_vector(&mut rng, DIM))
            .collect_vec()
    });
    group.bench_function("basic-score-point-10x", |b| {
        let vectors = &*setup;
        let mut rng = rng();
        b.iter(|| {
            let query = random_vector(&mut rng, DIM);
            let points_to_score = (0..points_per_cycle).map(|_| rng.random_range(0..num_vectors));

            let _s: f32 = points_to_score
                .map(|x| DotProductMetric::similarity(&vectors[x], &query))
                .sum();
        })
    });
    drop(setup);
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
