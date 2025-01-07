#[cfg(not(target_os = "windows"))]
mod prof;

use std::hint::black_box;
use std::path::Path;

use common::types::PointOffsetType;
use criterion::{criterion_group, criterion_main, Criterion};
use indicatif::{ParallelProgressIterator, ProgressStyle};
use rand::rngs::StdRng;
use rand::SeedableRng;
use rayon::iter::{IntoParallelIterator as _, ParallelIterator as _};
use segment::fixtures::index_fixtures::{random_vector, FakeFilterContext, TestRawScorerProducer};
use segment::index::hnsw_index::graph_layers::GraphLayers;
use segment::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use segment::index::hnsw_index::graph_links::GraphLinksFormat;
use segment::index::hnsw_index::point_scorer::FilteredScorer;
use segment::spaces::simple::CosineMetric;

const NUM_VECTORS: usize = 1_000_000;
const DIM: usize = 64;
const M: usize = 16;
const TOP: usize = 10;
const EF_CONSTRUCT: usize = 100;
const EF: usize = 100;
const USE_HEURISTIC: bool = true;

type Metric = CosineMetric;

fn hnsw_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw-search-graph");

    let fake_filter_context = FakeFilterContext {};

    // Note: make sure that vector generation is deterministic.
    let vector_holder =
        TestRawScorerProducer::<Metric>::new(DIM, NUM_VECTORS, &mut StdRng::seed_from_u64(42));

    // Building HNSW index is expensive, so it's cached between runs.
    let mut graph_layers;
    {
        let path = Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(env!("CARGO_PKG_NAME"))
            .join(env!("CARGO_CRATE_NAME"))
            .join(format!(
                "{NUM_VECTORS}-{DIM}-{M}-{EF_CONSTRUCT}-{USE_HEURISTIC}-{:?}",
                <Metric as segment::spaces::metric::Metric<f32>>::distance(),
            ));

        if GraphLayers::get_path(&path).exists() {
            eprintln!("Loading cached links from {path:?}");
            graph_layers = GraphLayers::load(&path, false, false).unwrap();
        } else {
            let mut graph_layers_builder =
                GraphLayersBuilder::new(NUM_VECTORS, M, M * 2, EF_CONSTRUCT, 10, USE_HEURISTIC);

            let mut rng = StdRng::seed_from_u64(42);
            for idx in 0..NUM_VECTORS {
                let level = graph_layers_builder.get_random_layer(&mut rng);
                graph_layers_builder.set_levels(idx as PointOffsetType, level);
            }

            (0..NUM_VECTORS)
                .into_par_iter()
                .progress_with_style(
                    ProgressStyle::with_template("{percent:>3}% Buildng HNSW {wide_bar}").unwrap(),
                )
                .for_each(|idx| {
                    let added_vector = vector_holder.vectors.get(idx).to_vec();
                    let raw_scorer = vector_holder.get_raw_scorer(added_vector).unwrap();
                    let scorer =
                        FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
                    graph_layers_builder.link_new_point(idx as PointOffsetType, scorer);
                });

            std::fs::create_dir_all(&path).unwrap();
            graph_layers = graph_layers_builder
                .into_graph_layers(&path, GraphLinksFormat::Plain, false)
                .unwrap();
        }
    }

    let mut rng = StdRng::seed_from_u64(42);
    group.bench_function("uncompressed", |b| {
        b.iter(|| {
            let query = random_vector(&mut rng, DIM);

            let raw_scorer = vector_holder.get_raw_scorer(query).unwrap();
            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));

            black_box(graph_layers.search(TOP, EF, scorer, None));
        })
    });

    graph_layers.compress_ram();
    let mut rng = StdRng::seed_from_u64(42);
    group.bench_function("compressed", |b| {
        b.iter(|| {
            let query = random_vector(&mut rng, DIM);

            let raw_scorer = vector_holder.get_raw_scorer(query).unwrap();
            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));

            black_box(graph_layers.search(TOP, EF, scorer, None));
        })
    });

    let mut plain_search_range: Vec<PointOffsetType> =
        (0..NUM_VECTORS as PointOffsetType).collect();
    let mut rng = StdRng::seed_from_u64(42);
    group.bench_function("plain", |b| {
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
