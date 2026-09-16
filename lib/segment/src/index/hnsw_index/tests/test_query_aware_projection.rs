//! End-to-end tests of the query-aware projection edges through the real segment build path.
//!
//! The unit tests of the algorithm itself live in
//! [`crate::index::hnsw_index::query_aware_edges`]; these cover the *plumbing*: the
//! `hnsw_config.projection` block, the collection-level training vectors file, and the hook in
//! [`HNSWIndex::build`].

// Deprecated storage placement params are still set here, like every other HNSW test.
#![allow(deprecated)]

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::bitvec::BitVec;
use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::FeatureFlags;
use common::progress_tracker::ProgressTracker;
use common::types::PointOffsetType;
use rand::SeedableRng;
use rand::rngs::StdRng;
use tempfile::{Builder, TempDir};

use crate::data_types::vectors::{
    DEFAULT_VECTOR_NAME, QueryVector, VectorRef, only_default_vector,
};
use crate::entry::entry_point::SegmentEntry;
use crate::fixtures::index_fixtures::random_vector;
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::hnsw_index::query_aware_edges::{
    ProjectionParams, add_query_aware_projection_edges,
};
use crate::index::hnsw_index::training_vectors::HnswTrainingVectors;
use crate::segment_constructor::VectorIndexBuildArgs;
use crate::segment_constructor::simple_segment_constructor::build_simple_segment;
use crate::types::{Distance, HnswConfig, HnswGlobalConfig, HnswProjectionConfig, SeqNumberType};
use crate::vector_storage::VectorStorage;
use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;

const DIM: usize = 16;
const NUM_VECTORS: u64 = 600;
const NUM_TRAINING: usize = 128;
const M: usize = 8;
const EF_CONSTRUCT: usize = 32;

/// A directory with training vectors for the default vector name, plus the vectors themselves.
fn make_training_vectors(seed: u64) -> (TempDir, Vec<Vec<f32>>) {
    let dir = Builder::new().prefix("training_vectors").tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(seed);
    let rows: Vec<Vec<f32>> = (0..NUM_TRAINING)
        .map(|_| random_vector(&mut rng, DIM))
        .collect();
    let flat: Vec<f32> = rows.iter().flatten().copied().collect();
    HnswTrainingVectors::store(dir.path(), DEFAULT_VECTOR_NAME, DIM, &flat, false).unwrap();
    (dir, rows)
}

/// Build an HNSW index over `NUM_VECTORS` seeded random vectors and return its level-0 links.
///
/// `training_dir` is the collection's training-vectors directory to read from, if any.
fn build_level0_links(
    projection: Option<HnswProjectionConfig>,
    training_dir: Option<&Path>,
) -> (Vec<Vec<PointOffsetType>>, HnswGraphConfig) {
    let stopped = AtomicBool::new(false);
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    // Same seed for the points and for the level draw, so two builds only differ in the
    // projection pass.
    let mut rng = StdRng::seed_from_u64(42);
    let mut segment = build_simple_segment(dir.path(), DIM, Distance::Dot).unwrap();
    for n in 0..NUM_VECTORS {
        let vector = random_vector(&mut rng, DIM);
        segment
            .upsert_point(
                n as SeqNumberType,
                n.into(),
                only_default_vector(&vector),
                &hw_counter,
            )
            .unwrap();
    }

    let hnsw_config = HnswConfig {
        memory: None,
        m: M,
        ef_construct: EF_CONSTRUCT,
        full_scan_threshold: 10_000,
        max_indexing_threads: 1,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
        projection,
    };

    // A single indexing thread keeps the insertion order — and therefore the graph — the same
    // across builds, so the control comparison below is exact.
    let permit = Arc::new(ResourcePermit::dummy(1));
    let mut build_rng = StdRng::seed_from_u64(7);

    let index = HNSWIndex::build(
        HnswIndexOpenArgs {
            path: hnsw_dir.path(),
            id_tracker: segment.id_tracker.clone(),
            vector_storage: segment.vector_data[DEFAULT_VECTOR_NAME]
                .vector_storage
                .clone(),
            quantized_vectors: Default::default(),
            payload_index: segment.payload_index.clone(),
            hnsw_config,
        },
        VectorIndexBuildArgs {
            permit,
            old_indices: &[],
            gpu_device: None,
            rng: &mut build_rng,
            stopped: &stopped,
            hnsw_global_config: &HnswGlobalConfig::default(),
            feature_flags: FeatureFlags::default(),
            progress: ProgressTracker::new_for_test(),
            hnsw_training_vectors: training_dir.map(|dir| {
                crate::index::hnsw_index::training_vectors::HnswTrainingVectorsSource {
                    dir,
                    vector_name: DEFAULT_VECTOR_NAME,
                }
            }),
        },
    )
    .unwrap();

    let links = (0..NUM_VECTORS)
        .map(|point_id| {
            index
                .graph()
                .links
                .links(point_id as PointOffsetType, 0)
                .collect()
        })
        .collect();

    let config = HnswGraphConfig::load_universal(
        &common::universal_io::MmapFs,
        &HnswGraphConfig::get_config_path(hnsw_dir.path()),
    )
    .unwrap()
    .unwrap();

    (links, config)
}

/// Without the config block the build path is skipped entirely and the graph is the control one.
/// With the block but no training vectors it is skipped too. With both, the level-0 links change.
#[test]
fn projection_rewrites_level0_links_only_when_configured_and_trained() {
    let (training_dir, _) = make_training_vectors(1234);

    let (control_links, control_config) = build_level0_links(None, None);
    assert!(control_config.projection.is_none());

    // Config block present, but no training vectors wired in: nothing happens.
    let projection = HnswProjectionConfig::default();
    let (no_training_links, no_training_config) = build_level0_links(Some(projection), None);
    assert!(
        no_training_config.projection.is_none(),
        "the projection must not be recorded when it did not run",
    );
    assert_eq!(
        control_links, no_training_links,
        "a configured but untrained projection must leave the graph untouched",
    );

    // Config block present and training vectors uploaded, but for a *different* vector name:
    // still nothing happens.
    let empty_dir = Builder::new().prefix("empty_training").tempdir().unwrap();
    let (empty_links, empty_config) = build_level0_links(Some(projection), Some(empty_dir.path()));
    assert!(empty_config.projection.is_none());
    assert_eq!(control_links, empty_links);

    // The real thing.
    let (projected_links, projected_config) =
        build_level0_links(Some(projection), Some(training_dir.path()));
    assert_eq!(
        projected_config.projection,
        Some(projection),
        "the built index must record the projection it was built with",
    );

    let changed = control_links
        .iter()
        .zip(&projected_links)
        .filter(|(a, b)| a != b)
        .count();
    assert!(
        changed > 0,
        "the projection pass must rewrite at least one point's level-0 links",
    );

    // The degree cap is what keeps search cost unchanged; it must still hold.
    let m0 = HnswM::new(M, M * 2).m0;
    for (point_id, links) in projected_links.iter().enumerate() {
        assert!(
            links.len() <= m0,
            "point {point_id} has {} level-0 links, over the cap of {m0}",
            links.len(),
        );
    }
}

/// The post-pass reports the edges it placed.
#[test]
fn projection_stats_report_placed_edges() {
    let num_points = 200usize;
    let mut rng = StdRng::seed_from_u64(99);
    let hw_counter = HardwareCounterCell::new();

    let mut storage = new_volatile_dense_vector_storage(DIM, Distance::Dot);
    let points: Vec<Vec<f32>> = (0..num_points)
        .map(|_| random_vector(&mut rng, DIM))
        .collect();
    for (i, vector) in points.iter().enumerate() {
        storage
            .insert_vector(i as PointOffsetType, VectorRef::from(vector), &hw_counter)
            .unwrap();
    }
    let deleted = BitVec::repeat(false, num_points);

    let mut builder =
        GraphLayersBuilder::new(num_points, HnswM::new(M, M * 2), EF_CONSTRUCT, 10, true);
    for i in 0..num_points as PointOffsetType {
        builder.set_levels(i, builder.get_random_layer(&mut rng));
    }
    for i in 0..num_points as PointOffsetType {
        let scorer = FilteredScorer::new_for_test(
            QueryVector::from(&points[i as usize][..]),
            &storage,
            &deleted,
        );
        builder.link_new_point(i, scorer);
    }

    let training: Vec<Vec<f32>> = (0..NUM_TRAINING)
        .map(|_| random_vector(&mut rng, DIM))
        .collect();

    let stats = add_query_aware_projection_edges(
        &builder,
        &ProjectionParams::default(),
        training.len(),
        |i| {
            Ok(FilteredScorer::new_for_test(
                QueryVector::from(&training[i][..]),
                &storage,
                &deleted,
            ))
        },
        || {
            Ok(FilteredScorer::new_for_test(
                QueryVector::from(&points[0][..]),
                &storage,
                &deleted,
            ))
        },
        &AtomicBool::new(false),
    )
    .unwrap();

    assert_eq!(stats.num_points, num_points);
    assert_eq!(stats.num_train_queries, training.len());
    assert!(
        stats.projected_edges > 0,
        "the pass placed no projected edges at all",
    );
    assert!(stats.points_with_candidates > 0);
    assert!(stats.mean_level0_degree() <= (M * 2) as f64);
}
