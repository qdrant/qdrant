//! End-to-end differential tests for the two build routes.
//!
//! Runs the whole pipeline — scatter, plan, build, assemble, assemble-verify — twice over identical
//! input, once by each route, and compares what came out. Unit tests in [`crate::build`] cover the
//! pieces; this covers the thing that actually matters, which is that swapping the route does not
//! change the artifact.
//!
//! The comparison loads the built segments through Qdrant's own `load_segment`, so it sees the
//! segments exactly as a serving node would, and compares ids, dense vectors, sparse vectors,
//! payloads and the recorded segment config. A route that silently dropped preprocessing, or
//! shifted internal ids against their external ones, or resolved a different segment config, fails
//! here.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::Result;
use segment::entry::entry_point::ReadSegmentEntry;
use segment::types::PointIdType;
use tempfile::TempDir;

use crate::build::{BuildLayout, BuildOptions, BuildRoute};
use crate::config::LoadedConfig;
use crate::payload_index::PayloadIndexSchema;
use crate::plan::PlanOptions;
use crate::ring::ShardRouter;
use crate::scatter::{ScatterLayout, ScatterOptions};
use crate::store::LocalStore;

/// Small enough to build in seconds, large enough to exercise batching and multi-part segments.
const DIM: usize = 8;

/// A generated corpus plus the config it was generated for.
struct Corpus {
    _dir: TempDir,
    root: PathBuf,
    input: PathBuf,
    config: LoadedConfig,
    router: ShardRouter,
    payload_index: PayloadIndexSchema,
}

impl Corpus {
    /// The input layer's view of this corpus. Tests scatter through the store seam, exactly as
    /// `main.rs` does, so the seam itself is under test too.
    fn store(&self) -> LocalStore {
        LocalStore::new(&self.input)
    }
}

/// One point of generated input, deterministic in `id`.
///
/// Vectors are deliberately not unit length, so Cosine preprocessing has an observable effect, and
/// sparse term counts vary so posting lists differ in length.
fn point_json(id: u64) -> serde_json::Value {
    let dense: Vec<f32> = (0..DIM)
        .map(|d| (id as f32 + d as f32 + 1.0) * 0.5)
        .collect();

    let term_count = (id % 5) + 1;
    let indices: Vec<u32> = (0..term_count).map(|t| (t * 7 + id % 11) as u32).collect();
    let values: Vec<f32> = indices.iter().map(|t| 1.0 + *t as f32 * 0.25).collect();

    serde_json::json!({
        "id": id,
        "vector": {
            "dense": dense,
            "sparse": { "indices": indices, "values": values },
        },
        "payload": {
            "dump": if id.is_multiple_of(3) { "CC-MAIN-A" } else { "CC-MAIN-B" },
            "language_score": (id % 100) as f64 / 100.0,
            "text": format!("document {id} with some body text to give the payload weight"),
        },
    })
}

/// Write `files` input files of `points_per_file` points each, and resolve the matching config.
fn corpus(shard_number: u32, files: usize, points_per_file: u64) -> Corpus {
    corpus_with(shard_number, files, points_per_file, None)
}

/// As [`corpus`], with `max_segment_size` in KB pinned so a test can force several segments.
///
/// Overridden here rather than by editing an already-loaded config: `LoadedConfig` serialises
/// unset optional fields as absent, and the loader deliberately rejects absent fields, so a
/// round-trip through JSON does not survive reloading.
fn corpus_with(
    shard_number: u32,
    files: usize,
    points_per_file: u64,
    max_segment_size_kb: Option<u64>,
) -> Corpus {
    let dir = TempDir::with_prefix("build_e2e").unwrap();
    let root = dir.path().to_path_buf();
    let input = root.join("input");
    fs_err::create_dir_all(&input).unwrap();

    let mut id = 0u64;
    for file in 0..files {
        let mut body = String::new();
        for _ in 0..points_per_file {
            body.push_str(&point_json(id).to_string());
            body.push('\n');
            id += 1;
        }
        fs_err::write(input.join(format!("data-{file:04}.jsonl")), body).unwrap();
    }

    let mut value = crate::config::tests::valid_config_json();
    value["params"]["shard_number"] = serde_json::json!(shard_number);
    value["params"]["vectors"]["dense"]["size"] = serde_json::json!(DIM);
    // Turbo quantization needs far more points than a test can afford to build, and it is not what
    // these tests are about.
    value["quantization_config"] = serde_json::Value::Null;
    // Keep HNSW cheap; the graph's shape is not under test, only that it is built at all.
    value["hnsw_config"]["m"] = serde_json::json!(8);
    value["hnsw_config"]["ef_construct"] = serde_json::json!(32);
    // Force the indexed, on-disk shape a production segment gets, so both routes are compared on
    // the config that actually ships rather than on a below-threshold plain segment.
    value["optimizer_config"]["indexing_threshold"] = serde_json::json!(1);
    if let Some(kb) = max_segment_size_kb {
        value["optimizer_config"]["max_segment_size"] = serde_json::json!(kb);
    }

    let config = crate::config::from_str(&value.to_string()).unwrap();
    let router = ShardRouter::new(&config).unwrap();

    let payload_index = PayloadIndexSchema::from_fields(BTreeMap::from([
        (
            "dump".parse().unwrap(),
            serde_json::from_value(serde_json::json!({"type": "keyword", "on_disk": true}))
                .unwrap(),
        ),
        (
            "language_score".parse().unwrap(),
            serde_json::from_value(serde_json::json!({"type": "float", "on_disk": true})).unwrap(),
        ),
    ]));

    Corpus {
        _dir: dir,
        root,
        input,
        config,
        router,
        payload_index,
    }
}

/// Everything about one built segment that a serving node can observe.
#[derive(Debug, PartialEq)]
struct SegmentSnapshot {
    points: Vec<PointRow>,
    /// The recorded config, which is what `ConfigMismatchOptimizer` compares on load.
    config: segment::types::SegmentConfig,
    /// Field indexes actually present on the segment holding the points.
    ///
    /// Compared separately from `config`, which does not carry them: the two routes create these
    /// by different means — the bulk route declares them on the builder so `build` constructs
    /// them, the edge route applies them as operations after `optimize` — so an unindexed field
    /// would otherwise pass every other comparison here and only show up as a slow filtered query
    /// on the serving cluster.
    indexed_fields: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, PartialEq)]
struct PointRow {
    id: PointIdType,
    dense: Vec<f32>,
    sparse: Option<(Vec<u32>, Vec<f32>)>,
    payload: String,
}

/// Load every segment under a shard directory and snapshot their contents, merged and sorted.
///
/// Loaded through `load_segment` rather than by reading files, so this observes the segments the
/// way the server does — including that they load at all, which is itself part of the contract.
fn snapshot_shard(segments_dir: &Path) -> Result<SegmentSnapshot> {
    use segment::segment_constructor::load_segment;

    let hw_counter = common::counter::hardware_counter::HardwareCounterCell::disposable();
    let stopped = std::sync::atomic::AtomicBool::new(false);

    let mut points = Vec::new();
    let mut config = None;
    let mut indexed_fields = std::collections::BTreeMap::new();

    let mut dirs: Vec<PathBuf> = fs_err::read_dir(segments_dir)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_dir())
        .filter(|path| {
            !path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .starts_with('.')
        })
        .collect();
    dirs.sort();

    for dir in dirs {
        let uuid = uuid::Uuid::parse_str(&dir.file_name().unwrap().to_string_lossy())
            .expect("segment directories are named by uuid");
        let segment = load_segment(&dir, uuid, None, &stopped)?;

        // The appendable segment `assemble` adds is empty; skip its config so the comparison is
        // about the built segment. An empty one contributes no points either way.
        if segment.available_point_count() > 0 {
            config = Some(segment.segment_config.clone());
            indexed_fields = segment
                .get_indexed_fields()
                .into_iter()
                .map(|(field, schema)| (field.to_string(), format!("{schema:?}")))
                .collect();
        }

        for id in segment.iter_points() {
            let vectors = segment.all_vectors(id, &hw_counter)?;

            let dense = match vectors.get("dense") {
                Some(segment::data_types::vectors::VectorRef::Dense(values)) => values.to_vec(),
                other => anyhow::bail!("point {id} has no dense vector: {other:?}"),
            };

            let sparse = match vectors.get("sparse") {
                Some(segment::data_types::vectors::VectorRef::Sparse(vector)) => {
                    Some((vector.indices.clone(), vector.values.clone()))
                }
                None => None,
                other => anyhow::bail!("point {id} has an unexpected sparse vector: {other:?}"),
            };

            // Compared as canonical JSON so key ordering cannot make two equal payloads differ.
            let payload = serde_json::to_string(&segment.payload(id, &hw_counter)?)?;

            points.push(PointRow {
                id,
                dense,
                sparse,
                payload,
            });
        }
    }

    points.sort_by_key(|row| row.id);

    Ok(SegmentSnapshot {
        points,
        config: config.expect("a shard must have at least one segment holding points"),
        indexed_fields,
    })
}

/// Run scatter, plan, build and assemble for one route, returning the shard output root.
fn run_pipeline(corpus: &Corpus, route: BuildRoute, tag: &str) -> Result<PathBuf> {
    run_pipeline_with_indexing_threads(corpus, route, tag, None)
}

/// Test-only entry point for measuring a real segment build with a fixed sparse-index permit.
fn run_pipeline_with_indexing_threads(
    corpus: &Corpus,
    route: BuildRoute,
    tag: &str,
    indexing_threads: Option<usize>,
) -> Result<PathBuf> {
    let work = corpus.root.join(format!("work_{tag}"));
    let out = corpus.root.join(format!("out_{tag}"));
    let staging = out.join("staging");
    fs_err::create_dir_all(&staging)?;

    let scatter_layout = ScatterLayout::new(&work);
    let store = corpus.store();
    let inputs = crate::scatter::discover_inputs(&store, None)?;
    crate::scatter::run(
        &corpus.config,
        &corpus.router,
        &store,
        &inputs,
        &scatter_layout,
        &ScatterOptions {
            workers: 2,
            mapping: None,
            slice: None,
            max_failures: 0,
        },
    )?;

    let planned = crate::plan::build(
        &corpus.config,
        &corpus.router,
        &scatter_layout,
        &work,
        &PlanOptions {
            workers: 2,
            slice: None,
            replan: false,
        },
    )?;

    let build_layout = BuildLayout::new(&out, &staging);
    let build_started = std::time::Instant::now();
    let stats = crate::build::run_all(
        &corpus.config,
        &planned.plans,
        &scatter_layout,
        &build_layout,
        &BuildOptions {
            concurrency: 2,
            batch_points: 64,
            payload_index: Some(&corpus.payload_index),
            slice: None,
            route,
            indexing_threads,
            mapping: None,
        },
    )?;
    assert!(stats.segments_built > 0, "{tag}: nothing was built");
    if let Some(threads) = indexing_threads {
        eprintln!(
            "sparse streaming profile: threads={threads}, build={:.3?}",
            build_started.elapsed()
        );
    }

    for (shard_id, shard_dir) in crate::assemble::discover_shards(&out)? {
        crate::assemble::assemble_shard(
            &corpus.config,
            &shard_dir,
            shard_id,
            Some(&corpus.payload_index),
        )?;
        crate::assemble::verify_assembled(&shard_dir)?;
    }

    Ok(out)
}

/// Whole shard-builder smoke benchmark, including dense HNSW and payload indexes.
/// This does not isolate sparse performance. Use segment's `profile_sparse_storage_build`
/// release benchmark for sparse timing, memory, and output comparisons.
///
/// Run with:
/// `cargo test -p shard-builder profile_sparse_streaming_build -- --ignored --nocapture`
#[test]
#[ignore]
fn profile_sparse_streaming_build() {
    let threads = std::env::var("SPARSE_STREAMING_PROFILE_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .map(|threads| vec![threads])
        .unwrap_or_else(|| vec![1, 4, 8, 16, 32]);
    for threads in threads {
        // One shard and one segment isolate intra-segment sparse indexing from build concurrency.
        let corpus = corpus(1, 1, 200_000);
        let started = std::time::Instant::now();
        run_pipeline_with_indexing_threads(
            &corpus,
            BuildRoute::Bulk,
            &format!("sparse-streaming-{threads}"),
            Some(threads),
        )
        .expect("streaming build must complete");
        eprintln!(
            "sparse streaming profile: threads={threads}, total={:.3?}",
            started.elapsed()
        );
    }
}

/// The headline test: both routes must produce the same artifact.
#[test]
fn both_routes_produce_the_same_shards() {
    let corpus = corpus(2, 3, 200);

    let bulk = run_pipeline(&corpus, BuildRoute::Bulk, "bulk").expect("bulk pipeline");
    let edge = run_pipeline(&corpus, BuildRoute::Edge, "edge").expect("edge pipeline");

    let shards = crate::assemble::discover_shards(&bulk).unwrap();
    assert_eq!(shards.len(), 2, "both shards should have been built");

    let mut total_points = 0;
    for (shard_id, bulk_shard) in shards {
        let edge_shard = edge.join(format!("shard_{shard_id}"));

        let bulk_snapshot =
            snapshot_shard(&bulk_shard.join(shard::files::SEGMENTS_PATH)).expect("bulk snapshot");
        let edge_snapshot =
            snapshot_shard(&edge_shard.join(shard::files::SEGMENTS_PATH)).expect("edge snapshot");

        assert_eq!(
            bulk_snapshot.points.len(),
            edge_snapshot.points.len(),
            "shard {shard_id}: point counts differ",
        );
        assert_eq!(
            bulk_snapshot.points, edge_snapshot.points,
            "shard {shard_id}: stored points differ between the two routes",
        );

        // The recorded config is what decides whether the serving cluster accepts the segment as
        // already-optimized or rebuilds it, so it has to match too — not just the data.
        assert_eq!(
            bulk_snapshot.config, edge_snapshot.config,
            "shard {shard_id}: the two routes resolved different segment configs",
        );

        assert_eq!(
            bulk_snapshot.indexed_fields, edge_snapshot.indexed_fields,
            "shard {shard_id}: the two routes built different payload field indexes",
        );
        assert_eq!(
            bulk_snapshot.indexed_fields.len(),
            2,
            "shard {shard_id}: both declared fields should be indexed, got {:?}",
            bulk_snapshot.indexed_fields,
        );

        total_points += bulk_snapshot.points.len();
    }

    assert_eq!(total_points, 600, "every input point must be present once");
}

/// The bulk route must reach the indexed, on-disk shape rather than leaving a plain segment.
///
/// Without this the differential test above could pass with both routes producing unindexed
/// segments, which is not what ships.
#[test]
fn the_bulk_route_builds_an_indexed_segment() {
    use segment::index::sparse_index::sparse_index_config::SparseIndexType;
    use segment::types::{Indexes, VectorStorageType};

    let corpus = corpus(1, 1, 300);
    let out = run_pipeline(&corpus, BuildRoute::Bulk, "indexed").expect("bulk pipeline");

    let (_, shard_dir) = crate::assemble::discover_shards(&out).unwrap().remove(0);
    let snapshot = snapshot_shard(&shard_dir.join(shard::files::SEGMENTS_PATH)).unwrap();

    let dense = snapshot.config.vector_data.get("dense").expect("dense");
    assert!(
        matches!(dense.index, Indexes::Hnsw(_)),
        "expected an HNSW index, got {:?}",
        dense.index,
    );
    assert_eq!(
        dense.storage_type,
        VectorStorageType::Mmap,
        "an indexed on-disk vector should land in single-file mmap storage",
    );

    let sparse = snapshot
        .config
        .sparse_vector_data
        .get("sparse")
        .expect("sparse");
    assert_eq!(
        sparse.index.index_type,
        SparseIndexType::Mmap,
        "the sparse index should be the immutable mmap form, not MutableRam",
    );
}

/// A segment built by the bulk route must be searchable on both of its vectors.
///
/// Counts and stored vectors can all be right while retrieval is broken, because the indexes are
/// built by a different step than the storages. This closes that gap.
#[test]
fn a_bulk_built_segment_is_searchable() {
    use segment::data_types::query_context::QueryContext;
    use segment::segment_constructor::load_segment;

    let corpus = corpus(1, 1, 300);
    let out = run_pipeline(&corpus, BuildRoute::Bulk, "search").expect("bulk pipeline");
    let query_context = QueryContext::new(
        usize::MAX,
        common::counter::hardware_accumulator::HwMeasurementAcc::disposable(),
    );
    let segment_query_context = query_context.get_segment_query_context();

    let (_, shard_dir) = crate::assemble::discover_shards(&out).unwrap().remove(0);
    let segments_dir = shard_dir.join(shard::files::SEGMENTS_PATH);
    let stopped = std::sync::atomic::AtomicBool::new(false);

    let mut searched_dense = false;
    let mut searched_sparse = false;

    for entry in fs_err::read_dir(&segments_dir).unwrap() {
        let dir = entry.unwrap().path();
        if !dir.is_dir() {
            continue;
        }
        let Ok(uuid) = uuid::Uuid::parse_str(&dir.file_name().unwrap().to_string_lossy()) else {
            continue;
        };
        let segment = load_segment(&dir, uuid, None, &stopped).unwrap();
        if segment.available_point_count() == 0 {
            continue;
        }

        let dense_query: Vec<f32> = (0..DIM).map(|d| (d as f32 + 1.0) * 0.5).collect();
        let dense_query = segment::data_types::vectors::VectorInternal::Dense(dense_query).into();
        let results = segment
            .search_batch(
                "dense",
                &[&dense_query],
                &Default::default(),
                &Default::default(),
                None,
                5,
                None,
                &segment_query_context,
            )
            .unwrap();
        assert!(
            results.iter().any(|hits| !hits.is_empty()),
            "dense search returned nothing",
        );
        searched_dense = true;

        // Term 0 is carried by every point whose id is a multiple of 11.
        let sparse_query =
            sparse::common::sparse_vector::SparseVector::new(vec![0], vec![1.0]).unwrap();
        let sparse_query =
            segment::data_types::vectors::VectorInternal::Sparse(sparse_query).into();
        let results = segment
            .search_batch(
                "sparse",
                &[&sparse_query],
                &Default::default(),
                &Default::default(),
                None,
                5,
                None,
                &segment_query_context,
            )
            .unwrap();
        assert!(
            results.iter().any(|hits| !hits.is_empty()),
            "sparse search returned nothing",
        );
        searched_sparse = true;
    }

    assert!(
        searched_dense && searched_sparse,
        "no segment holding points was found to search",
    );
}

/// Many segments built concurrently must all complete, and must not deadlock on the permit swap.
///
/// The bulk route acquires an IO permit, then calls `ResourceBudget::replace_with` to trade it for
/// a CPU permit before building indexes. `replace_with` acquires the new resource *while still
/// holding* the old one (`budget.rs:164`), which is the shape a hold-and-wait deadlock takes. The
/// budget is sized so the demand fits exactly, so it should not deadlock — but "should not" by
/// arithmetic is worth checking against the real thing, because the failure mode is a hang rather
/// than an error, and a hang partway through a multi-hour production build is expensive.
#[test]
fn many_segments_build_concurrently_without_deadlocking() {
    // Force several small segments per shard rather than one large one, so the workers actually
    // contend. bytes_per_point is DIM * 4 = 32, so 6 KB caps a segment at ~192 points.
    let corpus = corpus_with(2, 4, 400, Some(6));

    let work = corpus.root.join("work_concurrent");
    let out = corpus.root.join("out_concurrent");
    let staging = out.join("staging");
    fs_err::create_dir_all(&staging).unwrap();

    let scatter_layout = ScatterLayout::new(&work);
    let store = corpus.store();
    let inputs = crate::scatter::discover_inputs(&store, None).unwrap();
    crate::scatter::run(
        &corpus.config,
        &corpus.router,
        &store,
        &inputs,
        &scatter_layout,
        &ScatterOptions {
            workers: 4,
            mapping: None,
            slice: None,
            max_failures: 0,
        },
    )
    .unwrap();

    let planned = crate::plan::build(
        &corpus.config,
        &corpus.router,
        &scatter_layout,
        &work,
        &PlanOptions {
            workers: 2,
            slice: None,
            replan: false,
        },
    )
    .unwrap();

    let total_segments: usize = planned.plans.iter().map(|plan| plan.segments.len()).sum();
    assert!(
        total_segments >= 6,
        "the test needs several segments to contend, got {total_segments}",
    );

    let stats = crate::build::run_all(
        &corpus.config,
        &planned.plans,
        &scatter_layout,
        &BuildLayout::new(&out, &staging),
        &BuildOptions {
            concurrency: 8,
            batch_points: 32,
            payload_index: Some(&corpus.payload_index),
            slice: None,
            route: BuildRoute::Bulk,
            indexing_threads: None,
            mapping: None,
        },
    )
    .unwrap();

    assert_eq!(stats.segments_built as usize, total_segments);
    assert_eq!(stats.points, 1600, "every point must land somewhere");
}

/// A corrupt part file must fail the build loudly, not produce a short segment.
#[test]
fn a_truncated_part_file_fails_the_build() {
    let corpus = corpus(1, 1, 200);
    let work = corpus.root.join("work_corrupt");
    let out = corpus.root.join("out_corrupt");
    let staging = out.join("staging");
    fs_err::create_dir_all(&staging).unwrap();

    let scatter_layout = ScatterLayout::new(&work);
    let store = corpus.store();
    let inputs = crate::scatter::discover_inputs(&store, None).unwrap();
    crate::scatter::run(
        &corpus.config,
        &corpus.router,
        &store,
        &inputs,
        &scatter_layout,
        &ScatterOptions {
            workers: 1,
            mapping: None,
            slice: None,
            max_failures: 0,
        },
    )
    .unwrap();

    let planned = crate::plan::build(
        &corpus.config,
        &corpus.router,
        &scatter_layout,
        &work,
        &PlanOptions {
            workers: 1,
            slice: None,
            replan: false,
        },
    )
    .unwrap();

    // Chop the tail off the shard's only part file, after the plan has recorded its length.
    let shard_dir = scatter_layout.shard_dir(0);
    let part = fs_err::read_dir(&shard_dir)
        .unwrap()
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .find(|path| {
            path.file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .starts_with("part_")
                && path.extension().is_none()
        })
        .expect("a committed part file");
    let bytes = fs_err::read(&part).unwrap();
    fs_err::write(&part, &bytes[..bytes.len() * 2 / 3]).unwrap();

    let err = crate::build::run_all(
        &corpus.config,
        &planned.plans,
        &scatter_layout,
        &BuildLayout::new(&out, &staging),
        &BuildOptions {
            concurrency: 1,
            batch_points: 64,
            payload_index: None,
            slice: None,
            route: BuildRoute::Bulk,
            indexing_threads: None,
            mapping: None,
        },
    )
    .expect_err("a truncated part must not build successfully");

    let message = format!("{err:#}");
    assert!(
        message.contains("truncated")
            || message.contains("ended")
            || message.contains("cannot read"),
        "the failure should name the corruption, got: {message}",
    );

    // And nothing must be published under the final segment name.
    let segments = out.join("shard_0").join(shard::files::SEGMENTS_PATH);
    let published: Vec<_> = fs_err::read_dir(&segments)
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.file_name().to_string_lossy().to_string())
                .collect()
        })
        .unwrap_or_default();
    assert!(
        published.is_empty(),
        "a failed build must publish no segment, found {published:?}",
    );
}

/// Resume must still work: a second run rebuilds nothing and leaves the artifact alone.
#[test]
fn the_bulk_route_is_resumable() {
    let corpus = corpus(1, 1, 200);
    let work = corpus.root.join("work_resume");
    let out = corpus.root.join("out_resume");
    let staging = out.join("staging");
    fs_err::create_dir_all(&staging).unwrap();

    let scatter_layout = ScatterLayout::new(&work);
    let store = corpus.store();
    let inputs = crate::scatter::discover_inputs(&store, None).unwrap();
    crate::scatter::run(
        &corpus.config,
        &corpus.router,
        &store,
        &inputs,
        &scatter_layout,
        &ScatterOptions {
            workers: 1,
            mapping: None,
            slice: None,
            max_failures: 0,
        },
    )
    .unwrap();

    let planned = crate::plan::build(
        &corpus.config,
        &corpus.router,
        &scatter_layout,
        &work,
        &PlanOptions {
            workers: 1,
            slice: None,
            replan: false,
        },
    )
    .unwrap();

    let build_layout = BuildLayout::new(&out, &staging);
    let options = BuildOptions {
        concurrency: 1,
        batch_points: 64,
        payload_index: Some(&corpus.payload_index),
        slice: None,
        route: BuildRoute::Bulk,
        indexing_threads: None,
        mapping: None,
    };

    let first = crate::build::run_all(
        &corpus.config,
        &planned.plans,
        &scatter_layout,
        &build_layout,
        &options,
    )
    .unwrap();
    assert!(first.segments_built > 0);
    assert_eq!(first.segments_skipped, 0);

    let second = crate::build::run_all(
        &corpus.config,
        &planned.plans,
        &scatter_layout,
        &build_layout,
        &options,
    )
    .unwrap();
    assert_eq!(second.segments_built, 0, "a resumed run must build nothing");
    assert_eq!(second.segments_skipped, first.segments_built);

    // And no staging debris is left behind.
    let debris: Vec<_> = fs_err::read_dir(&staging)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        debris.is_empty(),
        "staging should be clean, found {debris:?}"
    );
}

/// Re-planning into an `--out` that already holds a *different* plan's segments must be
/// refused, not silently skipped.
///
/// The trap the content-addressed UUID + stale-output check close: build under config A, then
/// retune HNSW and re-plan into the same `--out`. With position-only segment names the retuned
/// build found every directory already present and skipped all of them — printing success while
/// the artifact still carried config A's graph. Now the retuned plan names its segments
/// differently (the fingerprint is folded into the UUID), so config A's directories are orphans,
/// and the build refuses rather than mixing two plans.
#[test]
fn a_retune_into_a_dirty_out_is_refused_not_silently_skipped() {
    let corpus = corpus(1, 2, 150);
    let work = corpus.root.join("work_dirty");
    let out = corpus.root.join("out_dirty");
    let staging = out.join("staging");
    fs_err::create_dir_all(&staging).unwrap();

    let scatter_layout = ScatterLayout::new(&work);
    let store = corpus.store();
    let inputs = crate::scatter::discover_inputs(&store, None).unwrap();
    crate::scatter::run(
        &corpus.config,
        &corpus.router,
        &store,
        &inputs,
        &scatter_layout,
        &ScatterOptions {
            workers: 1,
            mapping: None,
            slice: None,
            max_failures: 0,
        },
    )
    .unwrap();

    let build_layout = BuildLayout::new(&out, &staging);
    let options = BuildOptions {
        concurrency: 1,
        batch_points: 64,
        payload_index: Some(&corpus.payload_index),
        slice: None,
        route: BuildRoute::Bulk,
        indexing_threads: None,
        mapping: None,
    };

    // Build under config A (m = 8).
    let first_plan = crate::plan::build(
        &corpus.config,
        &corpus.router,
        &scatter_layout,
        &work,
        &PlanOptions {
            workers: 1,
            slice: None,
            replan: false,
        },
    )
    .unwrap();
    let first = crate::build::run_all(
        &corpus.config,
        &first_plan.plans,
        &scatter_layout,
        &build_layout,
        &options,
    )
    .unwrap();
    assert!(first.segments_built > 0);
    let built_uuids: std::collections::BTreeSet<String> = first_plan.plans[0]
        .segments
        .iter()
        .map(|s| s.uuid.to_string())
        .collect();

    // Retune HNSW and re-plan against the same scatter.
    let mut value = crate::config::tests::valid_config_json();
    value["params"]["shard_number"] = serde_json::json!(1);
    value["params"]["vectors"]["dense"]["size"] = serde_json::json!(DIM);
    value["quantization_config"] = serde_json::Value::Null;
    value["optimizer_config"]["indexing_threshold"] = serde_json::json!(1);
    value["hnsw_config"]["m"] = serde_json::json!(32);
    value["hnsw_config"]["ef_construct"] = serde_json::json!(64);
    let retuned = crate::config::from_str(&value.to_string()).unwrap();
    let retuned_router = ShardRouter::new(&retuned).unwrap();

    let second_plan = crate::plan::build(
        &retuned,
        &retuned_router,
        &scatter_layout,
        &work,
        &PlanOptions {
            workers: 1,
            slice: None,
            replan: true,
        },
    )
    .unwrap();

    // The retune moved the segment names, so config A's directories are now orphans.
    let retuned_uuids: std::collections::BTreeSet<String> = second_plan.plans[0]
        .segments
        .iter()
        .map(|s| s.uuid.to_string())
        .collect();
    assert!(
        retuned_uuids.is_disjoint(&built_uuids),
        "a retune must move every segment's directory name",
    );

    // Building the retuned plan into the same dirty --out must be refused, naming the stale
    // directories — not silently skipped, and not silently mixed.
    let err = crate::build::run_all(
        &retuned,
        &second_plan.plans,
        &scatter_layout,
        &build_layout,
        &options,
    )
    .expect_err("building a new plan into a dirty --out must be refused");
    let message = format!("{err:#}");
    assert!(
        message.contains("different plan") && message.contains("fresh --out"),
        "the refusal must explain the stale output, got: {message}",
    );

    // A fresh --out builds the retuned plan cleanly, and carries the new parameters.
    let clean_out = corpus.root.join("out_dirty_clean");
    let clean_staging = clean_out.join("staging");
    fs_err::create_dir_all(&clean_staging).unwrap();
    let clean_layout = BuildLayout::new(&clean_out, &clean_staging);
    let report = crate::build::run_all(
        &retuned,
        &second_plan.plans,
        &scatter_layout,
        &clean_layout,
        &options,
    )
    .expect("a fresh --out must build the retuned plan");
    assert_eq!(report.points, 300);
    for segment_dir in segment_dirs_of(&clean_layout.shard_segments_dir(0)) {
        let hnsw: serde_json::Value = serde_json::from_slice(
            &fs_err::read(
                segment_dir
                    .join("vector_index-dense")
                    .join("hnsw_config.json"),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(hnsw["m"], serde_json::json!(32), "segment kept the old m");
    }
}

/// The contract the two-fingerprint split encodes, end to end.
///
/// Retuning HNSW must cost a re-`plan` and nothing more — the scatter is reused, not re-read from
/// scratch and never re-written. And the config must still be frozen from `plan` onward, so a
/// document edited after planning is a hard error rather than a shard of mismatched segments.
///
/// Both halves matter. Without the first, changing `ef_construct` means re-scattering tens of
/// terabytes. Without the second, ten nodes can silently build one collection two different ways.
#[test]
fn hnsw_can_be_retuned_by_replanning_without_rescattering() {
    let corpus = corpus(1, 2, 150);
    let work = corpus.root.join("work_retune");
    let out = corpus.root.join("out_retune");
    let staging = out.join("staging");
    fs_err::create_dir_all(&staging).unwrap();

    let scatter_layout = ScatterLayout::new(&work);
    let store = corpus.store();
    let inputs = crate::scatter::discover_inputs(&store, None).unwrap();
    let scattered = crate::scatter::run(
        &corpus.config,
        &corpus.router,
        &store,
        &inputs,
        &scatter_layout,
        &ScatterOptions {
            workers: 1,
            mapping: None,
            slice: None,
            max_failures: 0,
        },
    )
    .unwrap();
    assert_eq!(scattered.files_processed, 2);

    // Record the scatter output so we can prove later that nothing rewrote it.
    let part_state_before = part_files_with_mtimes(&work);
    assert!(!part_state_before.is_empty(), "scatter produced no parts");

    let plan_options = PlanOptions {
        workers: 1,
        slice: None,
        replan: false,
    };
    let first_plan = crate::plan::build(
        &corpus.config,
        &corpus.router,
        &scatter_layout,
        &work,
        &plan_options,
    )
    .unwrap();

    // Retune the index. Nothing here changes the corpus, the ring, or the element width.
    let mut value = crate::config::tests::valid_config_json();
    value["params"]["shard_number"] = serde_json::json!(1);
    value["params"]["vectors"]["dense"]["size"] = serde_json::json!(DIM);
    value["quantization_config"] = serde_json::Value::Null;
    value["optimizer_config"]["indexing_threshold"] = serde_json::json!(1);
    value["hnsw_config"]["m"] = serde_json::json!(16);
    value["hnsw_config"]["ef_construct"] = serde_json::json!(64);
    value["hnsw_config"]["payload_m"] = serde_json::json!(0);
    let retuned = crate::config::from_str(&value.to_string()).unwrap();
    let retuned_router = ShardRouter::new(&retuned).unwrap();

    assert_ne!(
        retuned.fingerprint, corpus.config.fingerprint,
        "the retune must move the full fingerprint, or this test proves nothing",
    );
    assert_eq!(
        retuned.part_fingerprint, corpus.config.part_fingerprint,
        "the retune must not move the part fingerprint",
    );

    let build_layout = BuildLayout::new(&out, &staging);
    let options = BuildOptions {
        concurrency: 1,
        batch_points: 64,
        payload_index: Some(&corpus.payload_index),
        slice: None,
        route: BuildRoute::Bulk,
        indexing_threads: None,
        mapping: None,
    };

    // Building the *stale* plan under the retuned config must be refused: this is the freeze.
    let frozen = crate::build::run_all(
        &retuned,
        &first_plan.plans,
        &scatter_layout,
        &build_layout,
        &options,
    )
    .expect_err("a plan made under a different config must not be built");
    let message = format!("{frozen:#}");
    assert!(
        message.contains("frozen at `plan` time"),
        "the error must say how to proceed, got: {message}"
    );

    // Re-planning against the same scatter is all that is required.
    let second_plan = crate::plan::build(
        &retuned,
        &retuned_router,
        &scatter_layout,
        &work,
        &PlanOptions {
            workers: 1,
            slice: None,
            replan: true,
        },
    )
    .unwrap();

    let report = crate::build::run_all(
        &retuned,
        &second_plan.plans,
        &scatter_layout,
        &build_layout,
        &options,
    )
    .expect("re-planning must be enough to build under a retuned config");
    assert!(report.segments_built > 0);
    assert_eq!(report.points, 300);

    // The scatter was read, never rewritten: same files, same sizes, same mtimes.
    assert_eq!(
        part_files_with_mtimes(&work),
        part_state_before,
        "re-planning and rebuilding must not touch the scatter output",
    );

    // And the built segments really carry the retuned parameters.
    for segment_dir in segment_dirs_of(&build_layout.shard_segments_dir(0)) {
        let hnsw: serde_json::Value = serde_json::from_slice(
            &fs_err::read(
                segment_dir
                    .join("vector_index-dense")
                    .join("hnsw_config.json"),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(hnsw["m"], serde_json::json!(16), "segment kept the old m");
        assert_eq!(hnsw["ef_construct"], serde_json::json!(64));
        assert_eq!(
            hnsw["payload_m"],
            serde_json::json!(0),
            "payload_m: 0 must reach the segment, or the payload-subgraph stage still runs",
        );
    }
}

/// The retarget contract, end to end: rewriting placements on a finished artifact must produce
/// exactly the artifact a fresh build under the retargeted document produces.
///
/// This is the proof the design note demanded before retarget could exist. The serving
/// cluster's `ConfigMismatchOptimizer` queues a rebuild whenever a segment's recorded config
/// disagrees with what its own optimizer would resolve — and the bulk route resolves configs
/// with that same code (`optimized_segment_config`). So "retargeted == freshly built" is
/// precisely "the cluster loads the rewritten segments and queues zero optimizations".
#[test]
fn retargeting_placements_matches_a_fresh_build() {
    let corpus = corpus(1, 2, 150);
    let work = corpus.root.join("work_retarget");
    let scatter_layout = ScatterLayout::new(&work);
    let store = corpus.store();
    let inputs = crate::scatter::discover_inputs(&store, None).unwrap();
    crate::scatter::run(
        &corpus.config,
        &corpus.router,
        &store,
        &inputs,
        &scatter_layout,
        &ScatterOptions {
            workers: 1,
            mapping: None,
            slice: None,
            max_failures: 0,
        },
    )
    .unwrap();

    // The retargeted document: identical to the corpus config in everything structural, with
    // every placement flipped from the fixture's cold to cached.
    let mut value = crate::config::tests::valid_config_json();
    value["params"]["shard_number"] = serde_json::json!(1);
    value["params"]["vectors"]["dense"]["size"] = serde_json::json!(DIM);
    value["quantization_config"] = serde_json::Value::Null;
    value["optimizer_config"]["indexing_threshold"] = serde_json::json!(1);
    value["hnsw_config"]["m"] = serde_json::json!(8);
    value["hnsw_config"]["ef_construct"] = serde_json::json!(32);
    value["params"]["vectors"]["dense"]["memory"] = serde_json::json!("cached");
    value["params"]["sparse_vectors"]["sparse"]["index"]["memory"] = serde_json::json!("cached");
    value["params"]["payload"]["memory"] = serde_json::json!("cached");
    value["hnsw_config"]["memory"] = serde_json::json!("cached");
    let retargeted = crate::config::from_str(&value.to_string()).unwrap();
    let retargeted_router = ShardRouter::new(&retargeted).unwrap();

    assert_eq!(
        retargeted.part_fingerprint, corpus.config.part_fingerprint,
        "placement flips must not move the part fingerprint",
    );
    assert_ne!(
        retargeted.fingerprint, corpus.config.fingerprint,
        "placement flips move the full fingerprint — that is what retarget exists to bypass",
    );

    // Build and assemble under the original (cold) document.
    let out_cold = corpus.root.join("out_retarget_cold");
    {
        let staging = out_cold.join("staging");
        fs_err::create_dir_all(&staging).unwrap();
        let planned = crate::plan::build(
            &corpus.config,
            &corpus.router,
            &scatter_layout,
            &work,
            &PlanOptions {
                workers: 1,
                slice: None,
                replan: false,
            },
        )
        .unwrap();
        crate::build::run_all(
            &corpus.config,
            &planned.plans,
            &scatter_layout,
            &BuildLayout::new(&out_cold, &staging),
            &BuildOptions {
                concurrency: 1,
                batch_points: 64,
                payload_index: Some(&corpus.payload_index),
                slice: None,
                route: BuildRoute::Bulk,
                indexing_threads: None,
                mapping: None,
            },
        )
        .unwrap();
        for (shard_id, shard_dir) in crate::assemble::discover_shards(&out_cold).unwrap() {
            crate::assemble::assemble_shard(
                &corpus.config,
                &shard_dir,
                shard_id,
                Some(&corpus.payload_index),
            )
            .unwrap();
        }
    }

    // Build and assemble fresh under the retargeted (cached) document, off the same scatter.
    let out_fresh = corpus.root.join("out_retarget_fresh");
    {
        let staging = out_fresh.join("staging");
        fs_err::create_dir_all(&staging).unwrap();
        let planned = crate::plan::build(
            &retargeted,
            &retargeted_router,
            &scatter_layout,
            &work,
            &PlanOptions {
                workers: 1,
                slice: None,
                replan: true,
            },
        )
        .unwrap();
        crate::build::run_all(
            &retargeted,
            &planned.plans,
            &scatter_layout,
            &BuildLayout::new(&out_fresh, &staging),
            &BuildOptions {
                concurrency: 1,
                batch_points: 64,
                payload_index: Some(&corpus.payload_index),
                slice: None,
                route: BuildRoute::Bulk,
                indexing_threads: None,
                mapping: None,
            },
        )
        .unwrap();
        for (shard_id, shard_dir) in crate::assemble::discover_shards(&out_fresh).unwrap() {
            crate::assemble::assemble_shard(
                &retargeted,
                &shard_dir,
                shard_id,
                Some(&corpus.payload_index),
            )
            .unwrap();
        }
    }

    // Retarget the cold artifact to the cached document — after assemble, so the empty
    // appendable segment is covered too.
    let report = crate::retarget::run(
        &retargeted,
        &out_cold,
        &crate::retarget::RetargetOptions { dry_run: false },
    )
    .expect("a placement-only document must retarget cleanly");
    assert!(
        report.segments_rewritten > 0,
        "the flip must actually rewrite something",
    );
    let again = crate::retarget::run(
        &retargeted,
        &out_cold,
        &crate::retarget::RetargetOptions { dry_run: false },
    )
    .unwrap();
    assert_eq!(again.segments_rewritten, 0, "retarget must be idempotent");

    // The retargeted artifact is still a valid restorable shard.
    for (_, shard_dir) in crate::assemble::discover_shards(&out_cold).unwrap() {
        crate::assemble::verify_assembled(&shard_dir).unwrap();
    }

    // And it now equals the fresh build: same points, same recorded config — checked
    // through Qdrant's own loader, so the rewritten segments demonstrably still load.
    let retargeted_snapshot =
        snapshot_shard(&out_cold.join("shard_0").join(shard::files::SEGMENTS_PATH)).unwrap();
    let fresh_snapshot =
        snapshot_shard(&out_fresh.join("shard_0").join(shard::files::SEGMENTS_PATH)).unwrap();
    assert_eq!(
        retargeted_snapshot.config, fresh_snapshot.config,
        "a retargeted segment must record exactly what a fresh build records",
    );
    assert_eq!(retargeted_snapshot.points, fresh_snapshot.points);
    assert_eq!(retargeted_snapshot.points.len(), 300);
    assert_eq!(
        retargeted_snapshot.indexed_fields,
        fresh_snapshot.indexed_fields
    );

    // The empty appendable segment `assemble` added — invisible to the snapshot, which skips
    // segments holding no points — must carry the retargeted placements too.
    let appendable_config = |out: &Path| {
        let segments_dir = out.join("shard_0").join(shard::files::SEGMENTS_PATH);
        crate::assemble::segment_dirs(&segments_dir)
            .unwrap()
            .into_iter()
            .map(|dir| segment::segment::Segment::load_state(&dir).unwrap())
            // The appendable segment is the one whose vectors are unindexed — the built
            // segments all carry HNSW.
            .find(|state| {
                state
                    .config
                    .vector_data
                    .values()
                    .all(|data| matches!(data.index, segment::types::Indexes::Plain {}))
            })
            .expect("assemble must have added an appendable segment")
            .config
    };
    assert_eq!(
        appendable_config(&out_cold),
        appendable_config(&out_fresh),
        "the appendable segment's recorded config must be retargeted too",
    );
}

/// Every part file under a work directory, with size and mtime, for proving non-mutation.
fn part_files_with_mtimes(
    work: &std::path::Path,
) -> BTreeMap<String, (u64, std::time::SystemTime)> {
    let mut out = BTreeMap::new();
    let mut stack = vec![work.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs_err::read_dir(&dir).unwrap().filter_map(Result::ok) {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            if !name.starts_with("part_") {
                continue;
            }
            let meta = fs_err::metadata(&path).unwrap();
            let key = path.strip_prefix(work).unwrap().display().to_string();
            out.insert(key, (meta.len(), meta.modified().unwrap()));
        }
    }
    out
}

fn segment_dirs_of(segments_dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    fs_err::read_dir(segments_dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect()
}

/// Drop a sparse vector at build time, reusing the scatter that captured it.
///
/// This is the capability the manifest exists for. The corpus carries dense *and* sparse; the
/// second config declares dense only; the parts are read, never rewritten; and the resulting
/// segment has no sparse vector data at all.
#[test]
fn a_sparse_vector_can_be_dropped_without_rescattering() {
    let corpus = corpus(1, 2, 150);
    let work = corpus.root.join("work_drop");
    let scatter_layout = ScatterLayout::new(&work);
    let store = corpus.store();
    let inputs = crate::scatter::discover_inputs(&store, None).unwrap();
    crate::scatter::run(
        &corpus.config,
        &corpus.router,
        &store,
        &inputs,
        &scatter_layout,
        &ScatterOptions {
            workers: 1,
            mapping: None,
            slice: None,
            max_failures: 0,
        },
    )
    .unwrap();

    let parts_before = part_files_with_mtimes(&work);
    assert!(!parts_before.is_empty());

    // Sanity: the scatter really did capture a sparse vector.
    let part = work
        .join("shard_0")
        .join(format!("part_{}", inputs[0].file_id));
    let header = crate::partfile::PartReader::open(&part, &corpus.config.part_fingerprint)
        .unwrap()
        .header()
        .clone();
    assert!(
        header.manifest.sparse.contains_key("sparse"),
        "fixture must scatter a sparse vector for this test to mean anything",
    );
    assert!(header.manifest.dense.contains_key("dense"));

    // Now declare dense only.
    let mut value = crate::config::tests::valid_config_json();
    value["params"]["shard_number"] = serde_json::json!(1);
    value["params"]["vectors"]["dense"]["size"] = serde_json::json!(DIM);
    value["params"]["sparse_vectors"] = serde_json::Value::Null;
    value["quantization_config"] = serde_json::Value::Null;
    value["optimizer_config"]["indexing_threshold"] = serde_json::json!(1);
    value["hnsw_config"]["m"] = serde_json::json!(8);
    value["hnsw_config"]["ef_construct"] = serde_json::json!(32);
    let dense_only = crate::config::from_str(&value.to_string()).unwrap();
    let router = ShardRouter::new(&dense_only).unwrap();

    assert_eq!(
        dense_only.part_fingerprint, corpus.config.part_fingerprint,
        "dropping a vector must not move the part fingerprint",
    );

    let planned = crate::plan::build(
        &dense_only,
        &router,
        &scatter_layout,
        &work,
        &PlanOptions {
            workers: 1,
            slice: None,
            replan: true,
        },
    )
    .unwrap();

    let out = corpus.root.join("out_drop");
    let staging = out.join("staging");
    fs_err::create_dir_all(&staging).unwrap();
    let build_layout = BuildLayout::new(&out, &staging);

    let report = crate::build::run_all(
        &dense_only,
        &planned.plans,
        &scatter_layout,
        &build_layout,
        &BuildOptions {
            concurrency: 1,
            batch_points: 64,
            payload_index: Some(&corpus.payload_index),
            slice: None,
            route: BuildRoute::Bulk,
            indexing_threads: None,
            mapping: None,
        },
    )
    .expect("a dense-only build must succeed against a scatter that captured sparse too");
    assert_eq!(report.points, 300);

    // The scatter was read, not rewritten.
    assert_eq!(part_files_with_mtimes(&work), parts_before);

    // The segments carry dense and no sparse — checked through Qdrant's own loader.
    let snapshot = snapshot_shard(&build_layout.shard_segments_dir(0)).unwrap();
    assert_eq!(snapshot.points.len(), 300);
    assert!(
        snapshot.config.sparse_vector_data.is_empty(),
        "segment still declares sparse vectors: {:?}",
        snapshot.config.sparse_vector_data,
    );
    assert!(snapshot.config.vector_data.contains_key("dense"));
    for row in &snapshot.points {
        assert!(
            row.sparse.is_none(),
            "point {:?} still carries a sparse vector",
            row.id,
        );
        assert_eq!(row.dense.len(), DIM);
    }
}

/// Both routes must project identically, or the differential test stops meaning anything.
#[test]
fn both_routes_drop_the_same_vector() {
    let corpus = corpus(1, 1, 120);
    let work = corpus.root.join("work_drop_both");
    let scatter_layout = ScatterLayout::new(&work);
    let store = corpus.store();
    let inputs = crate::scatter::discover_inputs(&store, None).unwrap();
    crate::scatter::run(
        &corpus.config,
        &corpus.router,
        &store,
        &inputs,
        &scatter_layout,
        &ScatterOptions {
            workers: 1,
            mapping: None,
            slice: None,
            max_failures: 0,
        },
    )
    .unwrap();

    let mut value = crate::config::tests::valid_config_json();
    value["params"]["shard_number"] = serde_json::json!(1);
    value["params"]["vectors"]["dense"]["size"] = serde_json::json!(DIM);
    value["params"]["sparse_vectors"] = serde_json::Value::Null;
    value["quantization_config"] = serde_json::Value::Null;
    value["optimizer_config"]["indexing_threshold"] = serde_json::json!(1);
    value["hnsw_config"]["m"] = serde_json::json!(8);
    value["hnsw_config"]["ef_construct"] = serde_json::json!(32);
    let dense_only = crate::config::from_str(&value.to_string()).unwrap();
    let router = ShardRouter::new(&dense_only).unwrap();

    let planned = crate::plan::build(
        &dense_only,
        &router,
        &scatter_layout,
        &work,
        &PlanOptions {
            workers: 1,
            slice: None,
            replan: true,
        },
    )
    .unwrap();

    let mut snapshots = Vec::new();
    for (tag, route) in [("bulk", BuildRoute::Bulk), ("edge", BuildRoute::Edge)] {
        let out = corpus.root.join(format!("out_both_{tag}"));
        let staging = out.join("staging");
        fs_err::create_dir_all(&staging).unwrap();
        let build_layout = BuildLayout::new(&out, &staging);

        crate::build::run_all(
            &dense_only,
            &planned.plans,
            &scatter_layout,
            &build_layout,
            &BuildOptions {
                concurrency: 1,
                batch_points: 64,
                payload_index: Some(&corpus.payload_index),
                slice: None,
                route,
                indexing_threads: None,
                mapping: None,
            },
        )
        .unwrap_or_else(|err| panic!("{tag} route failed: {err:#}"));

        snapshots.push((
            tag,
            snapshot_shard(&build_layout.shard_segments_dir(0)).unwrap(),
        ));
    }

    let (_, bulk) = &snapshots[0];
    let (_, edge) = &snapshots[1];
    assert_eq!(bulk.points, edge.points, "routes disagree after projection");
    assert!(bulk.config.sparse_vector_data.is_empty());
    assert!(edge.config.sparse_vector_data.is_empty());
}

/// Staging and output on different filesystems, which is the `/dev/shm` + shared-storage case.
///
/// The point of that configuration is to keep the index phase's random reads off the network: HNSW
/// re-reads the dense and quantized vectors in graph order, and on a shared filesystem every cache
/// miss is a network round trip. Staging in RAM turns those into memory accesses and pays one
/// sequential write at the end.
///
/// Skipped rather than failed when `/dev/shm` is absent or turns out to be the same device, since a
/// container without it should not fail the suite.
#[test]
fn staging_and_out_can_be_on_different_filesystems() {
    use std::os::unix::fs::MetadataExt as _;

    let shm = Path::new("/dev/shm");
    if !shm.is_dir() {
        eprintln!("skipping: /dev/shm is not available");
        return;
    }

    let corpus = corpus(1, 1, 150);
    let out = corpus.root.join("out_xdev");
    fs_err::create_dir_all(&out).unwrap();

    let Ok(shm_meta) = fs_err::metadata(shm) else {
        eprintln!("skipping: cannot stat /dev/shm");
        return;
    };
    if shm_meta.dev() == fs_err::metadata(&out).unwrap().dev() {
        eprintln!("skipping: /dev/shm is the same device as the test directory");
        return;
    }

    // A unique staging root under tmpfs, cleaned up however the test ends.
    let staging = tempfile::Builder::new()
        .prefix("sb_xdev_staging")
        .tempdir_in(shm)
        .unwrap();

    let work = corpus.root.join("work_xdev");
    let scatter_layout = ScatterLayout::new(&work);
    let store = corpus.store();
    let inputs = crate::scatter::discover_inputs(&store, None).unwrap();
    crate::scatter::run(
        &corpus.config,
        &corpus.router,
        &store,
        &inputs,
        &scatter_layout,
        &ScatterOptions {
            workers: 1,
            mapping: None,
            slice: None,
            max_failures: 0,
        },
    )
    .unwrap();

    let planned = crate::plan::build(
        &corpus.config,
        &corpus.router,
        &scatter_layout,
        &work,
        &PlanOptions {
            workers: 1,
            slice: None,
            replan: false,
        },
    )
    .unwrap();

    // Both routes, because both publish through the same helper and both used to fail here.
    for (tag, route) in [("bulk", BuildRoute::Bulk), ("edge", BuildRoute::Edge)] {
        let route_out = out.join(tag);
        fs_err::create_dir_all(&route_out).unwrap();
        let build_layout = BuildLayout::new(&route_out, staging.path());

        let report = crate::build::run_all(
            &corpus.config,
            &planned.plans,
            &scatter_layout,
            &build_layout,
            &BuildOptions {
                concurrency: 1,
                batch_points: 64,
                payload_index: Some(&corpus.payload_index),
                slice: None,
                route,
                indexing_threads: None,
                mapping: None,
            },
        )
        .unwrap_or_else(|err| panic!("{tag} route failed across filesystems: {err:#}"));
        assert_eq!(report.points, 150, "{tag}");

        // The segment is really there and really loads.
        let snapshot = snapshot_shard(&build_layout.shard_segments_dir(0)).unwrap();
        assert_eq!(snapshot.points.len(), 150, "{tag}");

        // No `.incomplete` debris, and nothing hidden left behind that a later run would trip on.
        let leftovers: Vec<String> = fs_err::read_dir(build_layout.shard_segments_dir(0))
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|name| name.starts_with('.'))
            .collect();
        assert!(leftovers.is_empty(), "{tag}: hidden debris {leftovers:?}");

        // Staging is emptied, which matters more here than usual: tmpfs debris is unreclaimable RAM.
        let staged: Vec<String> = fs_err::read_dir(staging.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert!(
            staged.is_empty(),
            "{tag}: staging not cleaned, found {staged:?}"
        );
    }

    // And the two routes agree, across filesystems as they do within one.
    let bulk =
        snapshot_shard(&BuildLayout::new(out.join("bulk"), staging.path()).shard_segments_dir(0))
            .unwrap();
    let edge =
        snapshot_shard(&BuildLayout::new(out.join("edge"), staging.path()).shard_segments_dir(0))
            .unwrap();
    assert_eq!(bulk.points, edge.points);
}

/// A skipped segment must still have its staging swept.
///
/// Reproduces the one window where staging outlives a run: the publish succeeds, then the process
/// dies before the staging directory is removed. On the next run the segment counts as built and is
/// skipped, so the routes' own "clear stale staging before rebuilding" never fires — and on tmpfs
/// the leftover is RAM that survives into whatever runs next on the node.
#[test]
fn a_resumed_run_sweeps_staging_left_by_a_skipped_segment() {
    let corpus = corpus(1, 1, 120);
    let work = corpus.root.join("work_sweep");
    let out = corpus.root.join("out_sweep");
    let staging = out.join("staging");
    fs_err::create_dir_all(&staging).unwrap();

    let scatter_layout = ScatterLayout::new(&work);
    let store = corpus.store();
    let inputs = crate::scatter::discover_inputs(&store, None).unwrap();
    crate::scatter::run(
        &corpus.config,
        &corpus.router,
        &store,
        &inputs,
        &scatter_layout,
        &ScatterOptions {
            workers: 1,
            mapping: None,
            slice: None,
            max_failures: 0,
        },
    )
    .unwrap();

    let planned = crate::plan::build(
        &corpus.config,
        &corpus.router,
        &scatter_layout,
        &work,
        &PlanOptions {
            workers: 1,
            slice: None,
            replan: false,
        },
    )
    .unwrap();

    let build_layout = BuildLayout::new(&out, &staging);
    let options = BuildOptions {
        concurrency: 1,
        batch_points: 64,
        payload_index: Some(&corpus.payload_index),
        slice: None,
        route: BuildRoute::Bulk,
        indexing_threads: None,
        mapping: None,
    };

    let first = crate::build::run_all(
        &corpus.config,
        &planned.plans,
        &scatter_layout,
        &build_layout,
        &options,
    )
    .unwrap();
    assert!(first.segments_built > 0);

    // Stand in for a crash between publish and cleanup: the segment is built, and staging for it
    // exists again with something in it.
    let uuid = planned.plans[0].segments[0].uuid;
    let leaked = staging.join(format!("shard_0__segment_{uuid}"));
    fs_err::create_dir_all(leaked.join("segments")).unwrap();
    fs_err::write(leaked.join("segments").join("ghost.dat"), b"leaked").unwrap();
    assert!(leaked.exists());

    let second = crate::build::run_all(
        &corpus.config,
        &planned.plans,
        &scatter_layout,
        &build_layout,
        &options,
    )
    .unwrap();
    assert_eq!(
        second.segments_built, 0,
        "the segment must be skipped, not rebuilt"
    );
    assert_eq!(second.segments_skipped, first.segments_built);

    assert!(
        !leaked.exists(),
        "staging for a skipped segment was not swept: {}",
        leaked.display(),
    );
    let debris: Vec<String> = fs_err::read_dir(&staging)
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert!(debris.is_empty(), "staging not clean, found {debris:?}");
}

/// An interrupted cross-filesystem publish must not leak a segment-sized hidden directory.
///
/// `publish_segment` cleans up when it *fails*, but a killed process leaves its temp behind, and the
/// temp name is unique per attempt so a retry never reuses it. At production segment size that is
/// tens of gigabytes of invisible space on the output filesystem per crash. Both the build path and
/// the resume-skip path must reclaim it.
#[test]
fn abandoned_publish_directories_are_reclaimed() {
    let corpus = corpus(1, 1, 120);
    let work = corpus.root.join("work_orphan");
    let out = corpus.root.join("out_orphan");
    let staging = out.join("staging");
    fs_err::create_dir_all(&staging).unwrap();

    let scatter_layout = ScatterLayout::new(&work);
    let store = corpus.store();
    let inputs = crate::scatter::discover_inputs(&store, None).unwrap();
    crate::scatter::run(
        &corpus.config,
        &corpus.router,
        &store,
        &inputs,
        &scatter_layout,
        &ScatterOptions {
            workers: 1,
            mapping: None,
            slice: None,
            max_failures: 0,
        },
    )
    .unwrap();
    let planned = crate::plan::build(
        &corpus.config,
        &corpus.router,
        &scatter_layout,
        &work,
        &PlanOptions {
            workers: 1,
            slice: None,
            replan: false,
        },
    )
    .unwrap();

    let build_layout = BuildLayout::new(&out, &staging);
    let options = BuildOptions {
        concurrency: 1,
        batch_points: 64,
        payload_index: Some(&corpus.payload_index),
        slice: None,
        route: BuildRoute::Bulk,
        indexing_threads: None,
        mapping: None,
    };
    let uuid = planned.plans[0].segments[0].uuid;
    let segments_dir = build_layout.shard_segments_dir(0);
    fs_err::create_dir_all(&segments_dir).unwrap();

    // Case 1: orphan present before the segment is built. Stand in for a process killed mid-copy.
    let orphan_before = segments_dir.join(format!(".{uuid}.999999.0.incomplete"));
    fs_err::create_dir_all(orphan_before.join("vector_storage-dense")).unwrap();
    fs_err::write(
        orphan_before
            .join("vector_storage-dense")
            .join("matrix.dat"),
        vec![3u8; 8192],
    )
    .unwrap();

    let first = crate::build::run_all(
        &corpus.config,
        &planned.plans,
        &scatter_layout,
        &build_layout,
        &options,
    )
    .unwrap();
    assert!(first.segments_built > 0);
    assert!(
        !orphan_before.exists(),
        "orphan survived the build that owned its segment: {}",
        orphan_before.display(),
    );

    // Case 2: orphan appears after the segment is published, so the next run *skips* the segment.
    let orphan_after = segments_dir.join(format!(".{uuid}.999998.0.incomplete"));
    fs_err::create_dir_all(&orphan_after).unwrap();
    fs_err::write(orphan_after.join("junk.dat"), vec![4u8; 8192]).unwrap();

    let second = crate::build::run_all(
        &corpus.config,
        &planned.plans,
        &scatter_layout,
        &build_layout,
        &options,
    )
    .unwrap();
    assert_eq!(second.segments_built, 0, "the segment must be skipped");
    assert!(
        !orphan_after.exists(),
        "orphan survived a resumed run that skipped its segment: {}",
        orphan_after.display(),
    );

    // A temp belonging to a *different* segment must be left alone: this run does not own it.
    let other = segments_dir.join(".11111111-2222-3333-4444-555555555555.1.0.incomplete");
    fs_err::create_dir_all(&other).unwrap();
    crate::build::run_all(
        &corpus.config,
        &planned.plans,
        &scatter_layout,
        &build_layout,
        &options,
    )
    .unwrap();
    assert!(
        other.exists(),
        "swept a publish directory for a segment this run does not own",
    );

    // And the real segment still loads.
    let snapshot = snapshot_shard(&segments_dir).unwrap();
    assert_eq!(snapshot.points.len(), 120);
}

/// Everything the Aurora shape combines, in one test: staging on another filesystem, several
/// segments building at once, a projection dropping a vector and the payload, multiple shards, an
/// abandoned publish directory to reclaim, then assemble and load.
///
/// Each of these is covered alone elsewhere. This exists because the interactions are where the bugs
/// were: the publish temp name only collides under concurrency, the sweep only matters when a prior
/// run died, and the projection changes what `assemble` should find.
#[test]
fn the_production_shape_builds_end_to_end() {
    use std::os::unix::fs::MetadataExt as _;

    let shm = Path::new("/dev/shm");
    if !shm.is_dir() {
        eprintln!("skipping: /dev/shm unavailable");
        return;
    }

    const SHARDS: u32 = 3;
    const FILES: usize = 3;
    const PER_FILE: u64 = 90;
    let corpus = corpus(SHARDS, FILES, PER_FILE);
    let total = FILES as u64 * PER_FILE;

    let out = corpus.root.join("out_prod");
    fs_err::create_dir_all(&out).unwrap();
    if fs_err::metadata(shm).unwrap().dev() == fs_err::metadata(&out).unwrap().dev() {
        eprintln!("skipping: /dev/shm is the same device as the test directory");
        return;
    }
    let staging = tempfile::Builder::new()
        .prefix("sb_prod_staging")
        .tempdir_in(shm)
        .unwrap();

    // Scatter everything the corpus has.
    let work = corpus.root.join("work_prod");
    let scatter_layout = ScatterLayout::new(&work);
    let store = corpus.store();
    let inputs = crate::scatter::discover_inputs(&store, None).unwrap();
    crate::scatter::run(
        &corpus.config,
        &corpus.router,
        &store,
        &inputs,
        &scatter_layout,
        &ScatterOptions {
            workers: 2,
            mapping: None,
            slice: None,
            max_failures: 0,
        },
    )
    .unwrap();
    let parts_before = part_files_with_mtimes(&work);

    // Build a dense-only collection off it, forcing several segments per shard.
    let mut value = crate::config::tests::valid_config_json();
    value["params"]["shard_number"] = serde_json::json!(SHARDS);
    value["params"]["vectors"]["dense"]["size"] = serde_json::json!(DIM);
    value["params"]["sparse_vectors"] = serde_json::Value::Null;
    value["quantization_config"] = serde_json::Value::Null;
    value["optimizer_config"]["indexing_threshold"] = serde_json::json!(1);
    // Small enough to force several segments per shard, large enough that the bulk route's
    // dense-only sizing still lands on the same side of the indexing thresholds -- a 1 KB budget
    // trips the tool's own "this segment is too small to size from dense vectors" guard.
    value["optimizer_config"]["max_segment_size"] = serde_json::json!(6);
    value["hnsw_config"]["m"] = serde_json::json!(8);
    value["hnsw_config"]["ef_construct"] = serde_json::json!(32);
    value["hnsw_config"]["payload_m"] = serde_json::json!(0);
    let dense_only = crate::config::from_str(&value.to_string()).unwrap();
    let router = ShardRouter::new(&dense_only).unwrap();
    assert_eq!(dense_only.part_fingerprint, corpus.config.part_fingerprint);

    let planned = crate::plan::build(
        &dense_only,
        &router,
        &scatter_layout,
        &work,
        &PlanOptions {
            workers: 2,
            slice: None,
            replan: true,
        },
    )
    .unwrap();
    let segment_count: usize = planned.plans.iter().map(|p| p.segments.len()).sum();
    assert!(
        segment_count >= SHARDS as usize,
        "expected at least one segment per shard"
    );

    // Wreckage from a notional earlier run, one per shard, to be reclaimed under concurrency.
    for plan in &planned.plans {
        let dir = BuildLayout::new(&out, staging.path()).shard_segments_dir(plan.shard_id);
        fs_err::create_dir_all(&dir).unwrap();
        let orphan = dir.join(format!(".{}.424242.0.incomplete", plan.segments[0].uuid));
        fs_err::create_dir_all(&orphan).unwrap();
        fs_err::write(orphan.join("junk.dat"), vec![9u8; 4096]).unwrap();
    }

    let build_layout = BuildLayout::new(&out, staging.path());
    let report = crate::build::run_all(
        &dense_only,
        &planned.plans,
        &scatter_layout,
        &build_layout,
        &BuildOptions {
            concurrency: 4,
            batch_points: 16,
            payload_index: None,
            slice: None,
            route: BuildRoute::Bulk,
            indexing_threads: None,
            mapping: None,
        },
    )
    .expect("cross-filesystem concurrent build with a projection must succeed");
    assert_eq!(report.points, total);
    assert_eq!(report.segments_built as usize, segment_count);

    // The scatter was read, never rewritten.
    assert_eq!(part_files_with_mtimes(&work), parts_before);

    // Staging is empty, and no hidden debris survives anywhere under out.
    let staged: Vec<String> = fs_err::read_dir(staging.path())
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert!(staged.is_empty(), "staging not cleaned: {staged:?}");

    let mut seen = 0u64;
    for plan in &planned.plans {
        let dir = build_layout.shard_segments_dir(plan.shard_id);
        let hidden: Vec<String> = fs_err::read_dir(&dir)
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.starts_with('.'))
            .collect();
        assert!(
            hidden.is_empty(),
            "shard {}: hidden debris {hidden:?}",
            plan.shard_id
        );

        let snapshot = snapshot_shard(&dir).unwrap();
        assert!(
            snapshot.config.sparse_vector_data.is_empty(),
            "shard {} still declares sparse vectors",
            plan.shard_id,
        );
        for row in &snapshot.points {
            assert!(
                row.sparse.is_none(),
                "sparse vector survived the projection"
            );
            assert_eq!(row.dense.len(), DIM);
        }
        seen += snapshot.points.len() as u64;
    }
    assert_eq!(seen, total, "points lost across shards");

    // And the artifacts assemble into restorable shards. No payload index is declared, matching a
    // dense-only collection, so assemble must not expect one either.
    for (shard_id, shard_dir) in crate::assemble::discover_shards(&out).unwrap() {
        crate::assemble::assemble_shard(&dense_only, &shard_dir, shard_id, None).unwrap();
        crate::assemble::verify_assembled(&shard_dir).unwrap();
    }
}
