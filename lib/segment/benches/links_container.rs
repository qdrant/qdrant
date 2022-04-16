mod prof;

use criterion::{criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use rand::rngs::StdRng;
use rand::{thread_rng, Rng, SeedableRng};
use segment::fixtures::index_fixtures::{random_vector, FakeFilterContext, TestRawScorerProducer};
use segment::index::hnsw_index::base_links_container::BaseLinksContainer;
use segment::index::hnsw_index::graph_layers::GraphLayers;
use segment::index::hnsw_index::links_container::LinksContainer;
use segment::index::hnsw_index::point_scorer::FilteredScorer;
use segment::index::hnsw_index::simple_links_container::SimpleLinksContainer;
use segment::spaces::simple::DotProductMetric;
use segment::types::PointOffsetType;

const NUM_VECTORS: usize = 200_000;
const DIM: usize = 16;
const M: usize = 32;
const SAMPLE: usize = 100;

fn links_container_bench_base(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let vector_holder = TestRawScorerProducer::new(DIM, NUM_VECTORS, DotProductMetric {}, &mut rng);
    let mut group = c.benchmark_group("links-container-bench");

    let mut link_container = BaseLinksContainer::new(M, NUM_VECTORS);

    for i in 0..NUM_VECTORS {
        let links = (0..M)
            .map(|_| rng.gen_range(0..NUM_VECTORS) as PointOffsetType)
            .collect_vec();
        link_container.set_links(i as PointOffsetType, &links);
    }

    group.bench_function("base-level", |b| {
        b.iter(|| {
            let query = random_vector(&mut rng, DIM);
            let raw_scorer = vector_holder.get_raw_scorer(query);
            let mut scorer = FilteredScorer::new(&raw_scorer, None);

            let mut top_score = 0.;
            let mut buffer = vec![];
            for _ in 0..SAMPLE {
                buffer.clear();
                let point_id = rng.gen_range(0..NUM_VECTORS) as PointOffsetType;
                buffer.extend_from_slice(link_container.get_links(point_id));
                let scores = scorer.score_points(&mut buffer, M);
                scores.iter().copied().for_each(|score| {
                    if score.score > top_score {
                        top_score = score.score
                    }
                });
            }
        })
    });

    group.finish();
}

fn links_container_bench_simple(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let vector_holder = TestRawScorerProducer::new(DIM, NUM_VECTORS, DotProductMetric {}, &mut rng);
    let mut group = c.benchmark_group("links-container-bench");

    let mut link_container = SimpleLinksContainer::new(NUM_VECTORS);

    for i in 0..NUM_VECTORS {
        let links = (0..M)
            .map(|_| rng.gen_range(0..NUM_VECTORS) as PointOffsetType)
            .collect_vec();
        link_container.set_links(i as PointOffsetType, 0, &links);
    }

    group.bench_function("simple", |b| {
        b.iter(|| {
            let query = random_vector(&mut rng, DIM);
            let raw_scorer = vector_holder.get_raw_scorer(query);
            let mut scorer = FilteredScorer::new(&raw_scorer, None);

            let mut top_score = 0.;
            let mut buffer = vec![];
            for _ in 0..SAMPLE {
                buffer.clear();
                let point_id = rng.gen_range(0..NUM_VECTORS) as PointOffsetType;
                buffer.extend_from_slice(link_container.get_links(point_id, 0));
                let scores = scorer.score_points(&mut buffer, M);
                scores.iter().copied().for_each(|score| {
                    if score.score > top_score {
                        top_score = score.score
                    }
                });
            }
        })
    });

    group.finish();
}

fn links_container_bench_with_levels(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let vector_holder = TestRawScorerProducer::new(DIM, NUM_VECTORS, DotProductMetric {}, &mut rng);
    let mut group = c.benchmark_group("links-container-bench");

    let mut link_container = LinksContainer::new();

    link_container.reserve(NUM_VECTORS, M);

    for i in 0..NUM_VECTORS {
        let links = (0..M)
            .map(|_| rng.gen_range(0..NUM_VECTORS) as PointOffsetType)
            .collect_vec();
        link_container.set_links(i as PointOffsetType, 0, &links);
    }

    group.bench_function("full-levels", |b| {
        b.iter(|| {
            let query = random_vector(&mut rng, DIM);
            let raw_scorer = vector_holder.get_raw_scorer(query);
            let mut scorer = FilteredScorer::new(&raw_scorer, None);

            let mut top_score = 0.;
            let mut buffer = vec![];
            for _ in 0..SAMPLE {
                buffer.clear();
                let point_id = rng.gen_range(0..NUM_VECTORS) as PointOffsetType;
                buffer.extend_from_slice(link_container.get_links(point_id, 0));
                let scores = scorer.score_points(&mut buffer, M);
                scores.iter().copied().for_each(|score| {
                    if score.score > top_score {
                        top_score = score.score
                    }
                });
            }
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = links_container_bench_base, links_container_bench_simple, links_container_bench_with_levels
}

criterion_main!(benches);
