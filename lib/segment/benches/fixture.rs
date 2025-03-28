use std::path::Path;

use common::types::PointOffsetType;
use rand::SeedableRng as _;
use rand::rngs::StdRng;
use rayon::iter::{IntoParallelIterator as _, ParallelIterator as _};
use segment::fixtures::index_fixtures::{FakeFilterContext, TestRawScorerProducer};
use segment::index::hnsw_index::graph_layers::GraphLayers;
use segment::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use segment::index::hnsw_index::graph_links::GraphLinksFormat;
use segment::index::hnsw_index::hnsw::SINGLE_THREADED_HNSW_BUILD_THRESHOLD;
use segment::index::hnsw_index::point_scorer::FilteredScorer;
use segment::spaces::metric::Metric;

pub fn make_cached_graph<METRIC>(
    num_vectors: usize,
    dim: usize,
    m: usize,
    ef_construct: usize,
    use_heuristic: bool,
) -> (TestRawScorerProducer<METRIC>, GraphLayers)
where
    METRIC: Metric<f32> + Sync + Send,
{
    use indicatif::{ParallelProgressIterator as _, ProgressStyle};

    let path = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(env!("CARGO_PKG_NAME"))
        .join(env!("CARGO_CRATE_NAME"))
        .join(format!(
            "{num_vectors}-{dim}-{m}-{ef_construct}-{use_heuristic}-{:?}",
            METRIC::distance(),
        ));

    let fake_filter_context = FakeFilterContext {};

    // Note: make sure that vector generation is deterministic.
    let vector_holder =
        TestRawScorerProducer::<METRIC>::new(dim, num_vectors, &mut StdRng::seed_from_u64(42));

    let graph_layers = if GraphLayers::get_path(&path).exists() {
        eprintln!("Loading cached links from {path:?}");
        GraphLayers::load(&path, false, false).unwrap()
    } else {
        let mut graph_layers_builder =
            GraphLayersBuilder::new(num_vectors, m, m * 2, ef_construct, 10, use_heuristic);

        let mut rng = StdRng::seed_from_u64(42);
        for idx in 0..num_vectors {
            let level = graph_layers_builder.get_random_layer(&mut rng);
            graph_layers_builder.set_levels(idx as PointOffsetType, level);
        }

        let add_point = |idx| {
            let added_vector = vector_holder.vectors.get(idx).to_vec();
            let raw_scorer = vector_holder.get_raw_scorer(added_vector).unwrap();
            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
            graph_layers_builder.link_new_point(idx as PointOffsetType, scorer);
        };

        (0..SINGLE_THREADED_HNSW_BUILD_THRESHOLD.min(num_vectors)).for_each(add_point);
        (SINGLE_THREADED_HNSW_BUILD_THRESHOLD..num_vectors)
            .into_par_iter()
            .progress_with_style(
                ProgressStyle::with_template("{percent:>3}% Buildng HNSW {wide_bar}").unwrap(),
            )
            .for_each(add_point);

        std::fs::create_dir_all(&path).unwrap();
        graph_layers_builder
            .into_graph_layers(&path, GraphLinksFormat::Plain, false)
            .unwrap()
    };

    (vector_holder, graph_layers)
}
