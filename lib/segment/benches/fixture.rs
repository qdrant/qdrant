use std::path::Path;
use std::time::Duration;

use common::types::PointOffsetType;
use fs_err as fs;
use rand::SeedableRng as _;
use rand::rngs::SmallRng;
use rayon::iter::{IntoParallelIterator as _, ParallelIterator as _};
use segment::data_types::vectors::DenseVector;
use segment::fixtures::index_fixtures::{TestRawScorerProducer, preprocessed_random_vectors};
use segment::index::hnsw_index::HnswM;
use segment::index::hnsw_index::graph_layers::GraphLayers;
use segment::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use segment::index::hnsw_index::graph_links::{GraphLinksFormatParam, GraphLinksResidency};
use segment::index::hnsw_index::hnsw::SINGLE_THREADED_HNSW_BUILD_THRESHOLD;
use segment::spaces::metric::Metric;
use segment::types::Distance;
use segment::vector_storage::dense::dense_vector_storage::open_dense_vector_storage;
use segment::vector_storage::{DEFAULT_STOPPED, DenseVectorStorage, VectorStorageEnum};
use tempfile::TempDir;

/// Generate vectors and HNSW graph to be used in benchmarks.
///
/// Graph layers are cached on disk to avoid wait times across repeated
/// benchmark runs.
/// Vectors values are not saved on disk, but generated deterministically using
/// the same seed.
// Shared bench module: not every bench that includes `fixture` uses this.
#[allow(dead_code)]
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
    // The "smallrng" suffix keys the cache by RNG algorithm: the cached
    // graph must match the vectors regenerated below.
    let cache_key = format!(
        "{num_vectors}-{dim}-{m}-{ef_construct}-{use_heuristic}-{:?}-smallrng",
        METRIC::distance(),
    );

    // Note: make sure that vector generation is deterministic.
    let vector_holder = TestRawScorerProducer::new(
        dim,
        METRIC::distance(),
        num_vectors,
        false,
        &mut SmallRng::seed_from_u64(42),
    );

    let graph_layers = build_or_load_graph(
        &cache_key,
        &vector_holder,
        num_vectors,
        m,
        ef_construct,
        use_heuristic,
    );

    (vector_holder, graph_layers)
}

/// Build an HNSW graph over the vectors held by `vector_holder`, or load it
/// from the on-disk cache keyed by `cache_key`. The caller is responsible for
/// making `cache_key` unique per vector set and build parameters.
pub fn build_or_load_graph(
    cache_key: &str,
    vector_holder: &TestRawScorerProducer,
    num_vectors: usize,
    m: usize,
    ef_construct: usize,
    use_heuristic: bool,
) -> GraphLayers {
    use indicatif::{ParallelProgressIterator as _, ProgressStyle};

    let path = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(env!("CARGO_PKG_NAME"))
        .join(env!("CARGO_CRATE_NAME"))
        .join(cache_key);

    let graph_layers_path = GraphLayers::get_path(&path);
    if graph_layers_path.exists() {
        let updated_ago = updated_ago(&graph_layers_path).unwrap_or_else(|_| "???".to_string());
        eprintln!("Loading cached links (built {updated_ago} ago) from {graph_layers_path:?}.");
        eprintln!("Delete the directory above if code related to HNSW graph building is changed");
        GraphLayers::load(&path, GraphLinksResidency::Cached, false).unwrap()
    } else {
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

        fs::create_dir_all(&path).unwrap();
        graph_layers_builder
            .into_graph_layers(&path, GraphLinksFormatParam::Plain, false)
            .unwrap()
    }
}

/// Build an mmap-backed dense vector storage holding the same seed-42 vectors
/// as [`make_cached_graph`], so the cached graph is valid against it.
///
/// Returns the [`TempDir`] (which must be kept alive for the mmap to stay open)
/// and a [`TestRawScorerProducer`] whose `scorer` reads from the mmap storage.
// Shared bench module: not every bench that includes `fixture` uses this.
#[allow(dead_code)]
pub fn make_memmap_producer<METRIC>(
    num_vectors: usize,
    dim: usize,
) -> (TempDir, TestRawScorerProducer)
where
    METRIC: Metric<f32>,
{
    let distance = METRIC::distance();

    // Force the plain mmap variant (not io_uring), so prefetch is exercised.
    #[cfg(target_os = "linux")]
    segment::vector_storage::common::set_async_scorer(false);

    let tmp = tempfile::Builder::new()
        .prefix("hnsw-mmap-bench")
        .tempdir()
        .unwrap();
    let mut storage = open_dense_vector_storage(tmp.path(), dim, distance, false).unwrap();

    // Regenerate the exact vectors from `make_cached_graph` (same seed and
    // generation sequence, via the shared `preprocessed_random_vectors`).
    let mut rng = SmallRng::seed_from_u64(42);
    let mut vectors = preprocessed_random_vectors(&mut rng, dim, distance, num_vectors)
        .map(|v| (std::borrow::Cow::Owned(v), false));
    let VectorStorageEnum::DenseMemmap(mmap_storage) = &mut storage else {
        panic!("expected DenseMemmap storage");
    };
    mmap_storage
        .update_from(&mut vectors, &DEFAULT_STOPPED)
        .unwrap();

    let producer = TestRawScorerProducer::from_storage(storage, num_vectors);
    (tmp, producer)
}

/// Like [`make_memmap_producer`], but for the given (already preprocessed)
/// vectors instead of regenerated random ones.
#[allow(dead_code)]
pub fn make_memmap_producer_from_vectors(
    vectors: &[DenseVector],
    dim: usize,
    distance: Distance,
) -> (TempDir, TestRawScorerProducer) {
    // Force the plain mmap variant (not io_uring), so prefetch is exercised.
    #[cfg(target_os = "linux")]
    segment::vector_storage::common::set_async_scorer(false);

    let tmp = tempfile::Builder::new()
        .prefix("hnsw-mmap-bench")
        .tempdir()
        .unwrap();
    let mut storage = open_dense_vector_storage(tmp.path(), dim, distance, false).unwrap();

    let mut vectors_iter = vectors
        .iter()
        .map(|v| (std::borrow::Cow::Borrowed(v.as_slice()), false));
    let VectorStorageEnum::DenseMemmap(mmap_storage) = &mut storage else {
        panic!("expected DenseMemmap storage");
    };
    mmap_storage
        .update_from(&mut vectors_iter, &DEFAULT_STOPPED)
        .unwrap();

    let producer = TestRawScorerProducer::from_storage(storage, vectors.len());
    (tmp, producer)
}

/// Read a TEXMEX `.fvecs` file: per vector, a little-endian `i32` dimension
/// followed by that many little-endian `f32` components.
/// See <http://corpus-texmex.irisa.fr/>.
#[allow(dead_code)]
pub fn read_fvecs(path: &Path, limit: usize) -> Vec<DenseVector> {
    let bytes = fs::read(path).unwrap();
    let mut vectors = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() && vectors.len() < limit {
        let dim = i32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let end = offset + dim * size_of::<f32>();
        let vector = bytes[offset..end]
            .chunks_exact(size_of::<f32>())
            .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
            .collect();
        offset = end;
        vectors.push(vector);
    }
    vectors
}

fn updated_ago(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let elapsed = fs::metadata(path)?.modified()?.elapsed()?;
    let secs_rounded = elapsed.as_secs().next_multiple_of(60);
    Ok(humantime::format_duration(Duration::from_secs(secs_rounded)).to_string())
}
