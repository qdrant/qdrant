use common::bench_cache::{build_once, cache_path};
use common::types::PointOffsetType;
use fs_err as fs;
use rand::SeedableRng as _;
use rand::rngs::SmallRng;
use rayon::iter::{IntoParallelIterator as _, ParallelIterator as _};
use segment::fixtures::index_fixtures::TestRawScorerProducer;
use segment::index::hnsw_index::HnswM;
use segment::index::hnsw_index::graph_layers::GraphLayers;
use segment::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use segment::index::hnsw_index::graph_links::{GraphLinksFormatParam, GraphLinksResidency};
use segment::index::hnsw_index::hnsw::SINGLE_THREADED_HNSW_BUILD_THRESHOLD;
use segment::spaces::metric::Metric;

/// Generate vectors and HNSW graph to be used in benchmarks.
///
/// Graph layers are cached on disk to avoid wait times across repeated
/// benchmark runs.
/// Vectors values are not saved on disk, but generated deterministically using
/// the same seed.
pub fn make_cached_graph<METRIC>(
    num_vectors: usize,
    dim: usize,
    m: usize,
    ef_construct: usize,
    use_heuristic: bool,
) -> (TestRawScorerProducer, GraphLayers)
where
    METRIC: Metric<f32> + Sync + Send,
{
    use indicatif::{ParallelProgressIterator as _, ProgressStyle};

    // Note: make sure that vector generation is deterministic.
    let vector_holder = TestRawScorerProducer::new(
        dim,
        METRIC::distance(),
        num_vectors,
        false,
        &mut SmallRng::seed_from_u64(42),
    );

    let path = cache_path!(
        "graph-{num_vectors}-{dim}-{m}-{ef_construct}-{use_heuristic}-{:?}",
        METRIC::distance(),
    );

    build_once(&path, |path| {
        let mut graph_layers_builder =
            GraphLayersBuilder::new(num_vectors, HnswM::new2(m), ef_construct, 10, use_heuristic);

        let mut rng = SmallRng::seed_from_u64(42);
        for idx in 0..num_vectors {
            let level = graph_layers_builder.get_random_layer(&mut rng);
            graph_layers_builder.set_levels(idx as PointOffsetType, level);
        }

        let add_point = |idx| {
            let scorer = vector_holder.internal_scorer(idx as PointOffsetType);
            graph_layers_builder.link_new_point(idx as PointOffsetType, scorer);
        };

        (0..SINGLE_THREADED_HNSW_BUILD_THRESHOLD.min(num_vectors)).for_each(add_point);
        (SINGLE_THREADED_HNSW_BUILD_THRESHOLD..num_vectors)
            .into_par_iter()
            .progress_with_style(
                ProgressStyle::with_template("{percent:>3}% Buildng HNSW {wide_bar}").unwrap(),
            )
            .for_each(add_point);

        fs::create_dir_all(path).unwrap();
        graph_layers_builder
            .into_graph_layers(path, GraphLinksFormatParam::Plain, false)
            .unwrap();
    });

    let graph_layers = GraphLayers::load(&path, GraphLinksResidency::Cached, false).unwrap();

    (vector_holder, graph_layers)
}
