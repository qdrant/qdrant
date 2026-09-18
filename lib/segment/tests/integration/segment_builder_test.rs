use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::FeatureFlags;
use common::progress_tracker::{ProgressTracker, ProgressTree, ProgressView, new_progress_tracker};
use common::storage_version::VERSION_FILE;
use fs_err as fs;
use itertools::Itertools;
use rand::SeedableRng;
use rand::rngs::StdRng;
use segment::common::operation_error::OperationError;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, VectorRef, only_default_vector};
use segment::entry::entry_point::{NonAppendableSegmentEntry, ReadSegmentEntry, SegmentEntry};
use segment::fixtures::payload_fixtures::random_vector;
use segment::id_tracker::IdTrackerRead;
use segment::index::hnsw_index::get_num_indexing_threads;
use segment::json_path::JsonPath;
use segment::segment::Segment;
use segment::segment_constructor::normalize_segment_dir;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment_with_payload_storage;
use segment::types::{
    Distance, HnswGlobalConfig, Indexes, PayloadContainer, PayloadFieldSchema, PayloadKeyType,
    PayloadSchemaType, PayloadStorageType, SegmentConfig, VectorDataConfig, VectorStorageType,
};
use serde_json::Value;
use sparse::common::sparse_vector::SparseVector;
use tempfile::{Builder, TempDir};
use uuid::Uuid;

use crate::fixtures::segment::{
    PAYLOAD_KEY, SPARSE_VECTOR_NAME, build_segment_1, build_segment_2, build_segment_sparse_1,
    build_segment_sparse_2, empty_segment,
};

#[test]
fn test_building_new_segment() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let stopped = AtomicBool::new(false);

    let segment1 = build_segment_1(dir.path());
    let mut segment2 = build_segment_2(dir.path());

    let mut builder = SegmentBuilder::new(
        temp_dir.path(),
        &segment1.segment_config,
        &HnswGlobalConfig::default(),
        FeatureFlags::default(),
    )
    .unwrap();

    let hw_counter = HardwareCounterCell::new();

    // Include overlapping with segment1 to check the
    segment2
        .upsert_point(
            100,
            3.into(),
            only_default_vector(&[0., 0., 0., 0.]),
            &hw_counter,
        )
        .unwrap();

    builder
        .update(&[&segment1, &segment2, &segment2], &stopped, &hw_counter)
        .unwrap();

    // Check what happens if segment building fails here

    let segment_count = fs::read_dir(dir.path()).unwrap().count();

    assert_eq!(segment_count, 2);

    let temp_segment_count = fs::read_dir(temp_dir.path()).unwrap().count();

    assert_eq!(temp_segment_count, 1);

    // Now we finalize building

    let merged_segment: Segment = builder.build_for_test(dir.path());

    let new_segment_count = fs::read_dir(dir.path()).unwrap().count();

    assert_eq!(new_segment_count, 3);

    assert_eq!(
        merged_segment.iter_points().count(),
        merged_segment.available_point_count(),
    );
    assert_eq!(
        merged_segment.available_point_count(),
        segment1
            .iter_points()
            .chain(segment2.iter_points())
            .unique()
            .count(),
    );

    assert_eq!(merged_segment.point_version(3.into()), Some(100));
}

#[test]
fn test_building_new_defragmented_segment() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let stopped = AtomicBool::new(false);

    let defragment_key = JsonPath::from_str(PAYLOAD_KEY).unwrap();

    let hw_counter = HardwareCounterCell::new();

    let payload_schema = PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword);

    let mut segment1 = build_segment_1(dir.path());
    segment1
        .create_field_index(7, &defragment_key, Some(&payload_schema), &hw_counter)
        .unwrap();

    let mut segment2 = build_segment_2(dir.path());
    segment2
        .create_field_index(17, &defragment_key, Some(&payload_schema), &hw_counter)
        .unwrap();

    let mut builder = SegmentBuilder::new(
        temp_dir.path(),
        &segment1.segment_config,
        &HnswGlobalConfig::default(),
        FeatureFlags::default(),
    )
    .unwrap();

    // Include overlapping with segment1 to check the
    segment2
        .upsert_point(
            100,
            3.into(),
            only_default_vector(&[0., 0., 0., 0.]),
            &hw_counter,
        )
        .unwrap();

    builder.set_defragment_keys(vec![defragment_key.clone()]);

    builder
        .update(&[&segment1, &segment2], &stopped, &hw_counter)
        .unwrap();

    // Check what happens if segment building fails here

    let segment_count = fs::read_dir(dir.path()).unwrap().count();

    assert_eq!(segment_count, 2);

    let temp_segment_count = fs::read_dir(temp_dir.path()).unwrap().count();

    assert_eq!(temp_segment_count, 1);

    // Now we finalize building

    let merged_segment = builder.build_for_test(dir.path());

    let new_segment_count = fs::read_dir(dir.path()).unwrap().count();

    assert_eq!(new_segment_count, 3);

    assert_eq!(
        merged_segment.iter_points().count(),
        merged_segment.available_point_count(),
    );
    assert_eq!(
        merged_segment.available_point_count(),
        segment1
            .iter_points()
            .chain(segment2.iter_points())
            .unique()
            .count(),
    );

    assert_eq!(merged_segment.point_version(3.into()), Some(100));

    if let Err(err) = check_points_defragmented(&merged_segment, &defragment_key) {
        panic!("{err}");
    }
}

/// Iterates over the internal point ids of the merged segment and checks that the
/// points are grouped by the payload value.
fn check_points_defragmented(
    segment: &Segment,
    defragment_key: &PayloadKeyType,
) -> Result<(), &'static str> {
    let id_tracker = segment.id_tracker.borrow();

    // Previously seen group/value.
    let mut previous_value: Option<Value> = None;

    // keeps track of groups/values that have already been seen while iterating
    let mut seen_values: Vec<Value> = vec![];

    let hw_counter = HardwareCounterCell::new();

    for internal_id in id_tracker.point_mappings().iter_internal() {
        let external_id = id_tracker.external_id(internal_id).unwrap();
        let payload = segment.payload(external_id, &hw_counter).unwrap();
        let values = payload.get_value(defragment_key);

        if values.is_empty() {
            if !seen_values.is_empty() {
                return Err(
                    "In a defragmented segment, points without a payload value should come first!",
                );
            }

            continue;
        }

        let value = values[0].clone();

        let Some(prev) = previous_value.as_ref() else {
            previous_value = Some(value);
            continue;
        };

        if *prev == value {
            continue;
        }

        if seen_values.contains(&value) {
            return Err("Segment not defragmented");
        }

        seen_values.push(value.clone());
        previous_value = Some(value);
    }

    Ok(())
}

#[test]
fn test_building_new_sparse_segment() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let stopped = AtomicBool::new(false);

    let hw_counter = HardwareCounterCell::new();

    let segment1 = build_segment_sparse_1(dir.path());
    let mut segment2 = build_segment_sparse_2(dir.path());

    let mut builder = SegmentBuilder::new(
        temp_dir.path(),
        &segment1.segment_config,
        &HnswGlobalConfig::default(),
        FeatureFlags::default(),
    )
    .unwrap();

    // Include overlapping with segment1 to check the
    let vec = SparseVector::new(vec![0, 1, 2, 3], vec![0.0, 0.0, 0.0, 0.0]).unwrap();
    segment2
        .upsert_point(
            100,
            3.into(),
            NamedVectors::from_ref(SPARSE_VECTOR_NAME, VectorRef::Sparse(&vec)),
            &hw_counter,
        )
        .unwrap();

    builder
        .update(&[&segment1, &segment2, &segment2], &stopped, &hw_counter)
        .unwrap();

    // Check what happens if segment building fails here

    let segment_count = fs::read_dir(dir.path()).unwrap().count();

    assert_eq!(segment_count, 2);

    let temp_segment_count = fs::read_dir(temp_dir.path()).unwrap().count();

    assert_eq!(temp_segment_count, 1);

    // Now we finalize building

    let merged_segment = builder.build_for_test(dir.path());

    let new_segment_count = fs::read_dir(dir.path()).unwrap().count();

    assert_eq!(new_segment_count, 3);

    assert_eq!(
        merged_segment.iter_points().count(),
        merged_segment.available_point_count(),
    );
    assert_eq!(
        merged_segment.available_point_count(),
        segment1
            .iter_points()
            .chain(segment2.iter_points())
            .unique()
            .count(),
    );

    assert_eq!(merged_segment.point_version(3.into()), Some(100));
}

#[test]
fn test_build_not_ready_defers_version_file() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let stopped = AtomicBool::new(false);
    let segment1 = build_segment_1(dir.path());

    let mut builder = SegmentBuilder::new(
        temp_dir.path(),
        &segment1.segment_config,
        &HnswGlobalConfig::default(),
        FeatureFlags::default(),
    )
    .unwrap();

    let hw_counter = HardwareCounterCell::new();
    builder.update(&[&segment1], &stopped, &hw_counter).unwrap();

    let permit = ResourcePermit::dummy(get_num_indexing_threads(0) as u32);
    let mut rng = rand::rng();

    let built_segment = builder
        .build(
            dir.path(),
            Uuid::new_v4(),
            None,
            false, // ready
            permit,
            &stopped,
            &mut rng,
            &hw_counter,
            ProgressTracker::new_for_test(),
        )
        .unwrap();

    let segment_path = built_segment.segment_path.clone();
    drop(built_segment);

    // Version file must not be written while not `ready`.
    assert!(!segment_path.join(VERSION_FILE).is_file());

    // A restart (or snapshot recovery) must not pick this segment up before it is marked
    // ready; `normalize_segment_dir` discards it, same as any other half-built segment.
    assert!(normalize_segment_dir(&segment_path).unwrap().is_none());
    assert!(!segment_path.exists());
}

/// Unit test for a specific bug we caught before.
///
/// See: <https://github.com/qdrant/qdrant/pull/5614>
#[test]
fn test_building_new_segment_bug_5614() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let stopped = AtomicBool::new(false);

    let mut segment1 = build_segment_1(dir.path());
    let mut segment2 = build_segment_2(dir.path());

    let mut builder = SegmentBuilder::new(
        temp_dir.path(),
        &segment1.segment_config,
        &HnswGlobalConfig::default(),
        FeatureFlags::default(),
    )
    .unwrap();

    let vector_100_low = only_default_vector(&[1., 1., 0., 0.]);
    let vector_101_low = only_default_vector(&[2., 2., 0., 0.]);
    let vector_100_high = only_default_vector(&[3., 3., 0., 0.]);
    let vector_101_high = only_default_vector(&[4., 4., 0., 0.]);

    let hw_counter = HardwareCounterCell::new();

    // Insert point 100 and 101 in both segments
    // Do this in a specific order so that:
    // - the latter segment has a higher point version
    // - the internal point IDs don't match across segments
    segment1
        .upsert_point(123, 100.into(), vector_100_low, &hw_counter)
        .unwrap();
    segment1
        .upsert_point(123, 101.into(), vector_101_low, &hw_counter)
        .unwrap();

    segment2
        .upsert_point(124, 101.into(), vector_101_high.clone(), &hw_counter)
        .unwrap();
    segment2
        .upsert_point(124, 100.into(), vector_100_high.clone(), &hw_counter)
        .unwrap();

    builder
        .update(&[&segment1, &segment2], &stopped, &hw_counter)
        .unwrap();

    let hw_counter = HardwareCounterCell::new();

    let merged_segment: Segment = builder.build_for_test(dir.path());

    // Assert correct point versions - must have latest
    assert_eq!(merged_segment.point_version(100.into()), Some(124));
    assert_eq!(merged_segment.point_version(101.into()), Some(124));

    // Assert correct vectors still belong to the point
    // This was broken before <https://github.com/qdrant/qdrant/pull/5543>
    assert_eq!(
        merged_segment.all_vectors(100.into(), &hw_counter).unwrap(),
        vector_100_high,
    );
    assert_eq!(
        merged_segment.all_vectors(101.into(), &hw_counter).unwrap(),
        vector_101_high,
    );
}

const CANCELLATION_TEST_POINTS: u64 = 10_000;

/// Number of main-graph insertions after which the build gets cancelled.
/// Above `SINGLE_THREADED_HNSW_BUILD_THRESHOLD`, so this hits the parallel phase.
const CANCEL_AT_MAIN_GRAPH_POINTS: u64 = 1_000;

/// Segment with random (non-degenerate) vectors, big enough for a meaningful HNSW build.
fn cancellation_test_segment(path: &Path) -> Segment {
    let mut rng = StdRng::seed_from_u64(42);
    let mut segment = empty_segment(path);
    let hw_counter = HardwareCounterCell::new();
    for idx in 0..CANCELLATION_TEST_POINTS {
        let vector = random_vector(&mut rng, 4);
        segment
            .upsert_point(1, idx.into(), only_default_vector(&vector), &hw_counter)
            .unwrap();
    }
    segment
}

/// Builder that turns `source` into a segment with an HNSW index.
fn hnsw_segment_builder(source: &Segment, temp_dir: &Path) -> SegmentBuilder {
    let vector_config = &source.segment_config.vector_data[DEFAULT_VECTOR_NAME];
    let segment_config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: vector_config.size,
                distance: vector_config.distance,
                storage_type: VectorStorageType::default(),
                index: Indexes::Hnsw(Default::default()),
                quantization_config: None,
                multivector_config: None,
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
        id_tracker_memory: None,
    };

    let mut builder = SegmentBuilder::new(
        temp_dir,
        &segment_config,
        &HnswGlobalConfig::default(),
        FeatureFlags::default(),
    )
    .unwrap();
    builder
        .update(
            &[source],
            &AtomicBool::new(false),
            &HardwareCounterCell::new(),
        )
        .unwrap();
    builder
}

fn build_segment(
    builder: SegmentBuilder,
    segments_path: &Path,
    stopped: &AtomicBool,
    progress: ProgressTracker,
) -> Result<Segment, OperationError> {
    let permit = ResourcePermit::dummy(build_threads() as u32);
    builder.build(
        segments_path,
        Uuid::new_v4(),
        None,
        true,
        permit,
        stopped,
        &mut rand::rng(),
        &HardwareCounterCell::new(),
        progress,
    )
}

fn build_threads() -> u64 {
    get_num_indexing_threads(0) as u64
}

fn assert_cancelled(result: Result<Segment, OperationError>) {
    match result {
        Err(OperationError::Cancelled { .. }) => {}
        Ok(_) => panic!("build completed although it was cancelled"),
        Err(err) => panic!("expected cancellation, got: {err}"),
    }
}

/// A cancelled build must not leave a segment or temporary files behind.
fn assert_nothing_left_behind(segments_path: &Path, temp_dir: &Path) {
    for dir in [segments_path, temp_dir] {
        let entries = fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert!(
            entries.is_empty(),
            "cancelled build left files behind in {}: {entries:?}",
            dir.display(),
        );
    }
}

fn find_progress<'a>(tree: &'a ProgressTree, name: &str) -> Option<&'a ProgressTree> {
    if tree.name == name {
        return Some(tree);
    }
    tree.children
        .iter()
        .find_map(|child| find_progress(child, name))
}

/// Points inserted into the main HNSW graph so far, if that phase has started.
fn main_graph_done(progress: &ProgressView) -> Option<u64> {
    find_progress(&progress.snapshot("segment"), "main_graph").and_then(|node| node.done)
}

/// A build that is already cancelled when it starts must bail out during setup,
/// before any vector index work.
#[test]
fn test_building_cancelled_before_start() {
    let source_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segments_dir = Builder::new().prefix("segments_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let source = cancellation_test_segment(source_dir.path());
    let builder = hnsw_segment_builder(&source, temp_dir.path());

    let stopped = AtomicBool::new(true);
    let (progress_view, progress) = new_progress_tracker();

    assert_cancelled(build_segment(
        builder,
        segments_dir.path(),
        &stopped,
        progress,
    ));

    let progress = progress_view.snapshot("segment");
    let vector_index = find_progress(&progress, "vector_index").unwrap();
    assert!(
        vector_index.children.is_empty(),
        "vector index build started despite cancellation: {vector_index:?}",
    );

    assert_nothing_left_behind(segments_dir.path(), temp_dir.path());
}

/// Cancelling in the middle of the main HNSW graph must stop the build right away,
/// not at the end of the phase. Measured in inserted points rather than wall time,
/// so it does not depend on how fast the machine is.
#[test]
fn test_building_cancelled_during_main_graph() {
    let source_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segments_dir = Builder::new().prefix("segments_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let source = cancellation_test_segment(source_dir.path());
    let builder = hnsw_segment_builder(&source, temp_dir.path());

    let stopped = Arc::new(AtomicBool::new(false));
    let build_finished = Arc::new(AtomicBool::new(false));
    let (progress_view, progress) = new_progress_tracker();

    // Set the stop flag once the main graph reaches the target, and report how many
    // points were inserted at that moment.
    let canceller = std::thread::spawn({
        let stopped = stopped.clone();
        let build_finished = build_finished.clone();
        let progress_view = progress_view.clone();
        move || {
            while !build_finished.load(Ordering::Acquire) {
                if main_graph_done(&progress_view)
                    .is_some_and(|done| done >= CANCEL_AT_MAIN_GRAPH_POINTS)
                {
                    stopped.store(true, Ordering::Release);
                    return main_graph_done(&progress_view);
                }
                std::thread::yield_now();
            }
            None
        }
    });

    let result = build_segment(builder, segments_dir.path(), &stopped, progress);
    build_finished.store(true, Ordering::Release);

    let done_at_cancel = canceller
        .join()
        .unwrap()
        .expect("main graph finished before the build could be cancelled");
    assert_cancelled(result);

    let done_final = main_graph_done(&progress_view).unwrap();
    assert!(
        done_final < CANCELLATION_TEST_POINTS,
        "main graph was completed despite cancellation",
    );
    // Every insertion checks the stop flag first, so only the insertions already in flight
    // may complete: one per build thread. Allow twice that to tolerate the flag becoming
    // visible to other threads slightly later.
    let max_points_after_cancel = 2 * build_threads();
    assert!(
        done_final - done_at_cancel <= max_points_after_cancel,
        "{} points were inserted after cancellation (at {done_at_cancel} of {CANCELLATION_TEST_POINTS}), \
         expected at most {max_points_after_cancel}",
        done_final - done_at_cancel,
    );

    assert_nothing_left_behind(segments_dir.path(), temp_dir.path());
}

/// `SegmentBuilder::update` must reject schema mismatches in both directions
/// to avoid silently producing a merged segment with the wrong schema.
///
/// Direction A — target has a vector the source lacks. The existing check
/// fires; documents the symmetric case for completeness.
///
/// Direction B — source has a vector the target lacks. This is the case the
/// optimizer-vs-`CreateVectorName(V)` race produces: an optimizer launched
/// before V was added captures a `target_config` without V, but a concurrent
/// `CreateVectorName(V)` mutates the source segments to include V. Without
/// the source-superset check, `update` would silently drop V's data and
/// emit a broken merged segment at version >= V_opnum, breaking the next
/// optimization round.
#[test]
fn test_segment_builder_rejects_target_with_extra_vector_name() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let stopped = AtomicBool::new(false);
    let hw_counter = HardwareCounterCell::new();

    let segment1 = build_segment_1(dir.path());

    let added_vector_name = "added_vec";
    let mut target_config = segment1.segment_config.clone();
    target_config.vector_data.insert(
        added_vector_name.to_owned(),
        VectorDataConfig {
            size: 4,
            distance: Distance::Dot,
            storage_type: VectorStorageType::default(),
            index: Indexes::Plain {},
            quantization_config: None,
            multivector_config: None,
            datatype: None,
        },
    );

    let mut builder = SegmentBuilder::new(
        temp_dir.path(),
        &target_config,
        &HnswGlobalConfig::default(),
        FeatureFlags::default(),
    )
    .unwrap();

    let err = builder
        .update(&[&segment1], &stopped, &hw_counter)
        .expect_err("merge must reject sources missing a target vector");
    let msg = err.to_string();
    assert!(
        msg.contains("missing vector name") && msg.contains(added_vector_name),
        "unexpected error message: {msg}",
    );
}

/// Build a source segment that carries the default vector plus `extra_vector_name`, together with a
/// target schema that lacks `extra_vector_name`. This is the shape both races produce: a source
/// vector name absent from the optimizer's target. The returned [`TempDir`]s must be kept alive for
/// the duration of the test.
fn build_source_with_extra_vector(
    extra_vector_name: &str,
    hw_counter: &HardwareCounterCell,
) -> (Segment, SegmentConfig, Vec<TempDir>) {
    use segment::segment_constructor::build_segment;

    let source_dir = Builder::new().prefix("segment_source").tempdir().unwrap();

    let template = build_segment_1(source_dir.path());
    let mut source_config = template.segment_config.clone();
    source_config.vector_data.insert(
        extra_vector_name.to_owned(),
        VectorDataConfig {
            size: 4,
            distance: Distance::Dot,
            storage_type: VectorStorageType::default(),
            index: Indexes::Plain {},
            quantization_config: None,
            multivector_config: None,
            datatype: None,
        },
    );
    drop(template);

    let source_dir2 = Builder::new().prefix("segment_source2").tempdir().unwrap();
    let (mut source, _) = build_segment(source_dir2.path(), &source_config, None, true).unwrap();
    for i in 0..3u64 {
        let vectors = NamedVectors::from_pairs([
            (DEFAULT_VECTOR_NAME.to_owned(), vec![0.5, 0.5, 0.5, 0.5]),
            (extra_vector_name.to_owned(), vec![1.0, 1.0, 1.0, 1.0]),
        ]);
        source
            .upsert_point(10 + i, (100 + i).into(), vectors, hw_counter)
            .unwrap();
    }

    // Target schema lacks the extra vector.
    let mut target_config = source_config;
    target_config.vector_data.remove(extra_vector_name);

    (source, target_config, vec![source_dir, source_dir2])
}

#[test]
fn test_segment_builder_rejects_source_with_extra_vector_name() {
    // Conservative default: without a live schema (`set_live_vector_names` not called), a source
    // vector name absent from the target cancels the merge. This covers the
    // CreateVectorName-vs-optimizer race, where dropping the vector would corrupt the next round.
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();
    let stopped = AtomicBool::new(false);
    let hw_counter = HardwareCounterCell::new();
    let extra_vector_name = "extra_vec";

    let (source, target_config, _dirs) =
        build_source_with_extra_vector(extra_vector_name, &hw_counter);

    let mut builder = SegmentBuilder::new(
        temp_dir.path(),
        &target_config,
        &HnswGlobalConfig::default(),
        FeatureFlags::default(),
    )
    .unwrap();

    let err = builder
        .update(&[&source], &stopped, &hw_counter)
        .expect_err("merge must reject a source carrying a vector not in target");
    let msg = err.to_string();
    assert!(
        msg.contains("extra vector name") && msg.contains(extra_vector_name),
        "unexpected error message: {msg}",
    );
}

#[test]
fn test_segment_builder_drops_deleted_source_vector_name() {
    // DeleteVectorName recovery: the extra vector is absent from the live collection schema, so the
    // merge prunes the stale data and succeeds rather than cancelling forever.
    let build_dir = Builder::new().prefix("segment_build").tempdir().unwrap();
    let out_dir = Builder::new().prefix("segment_out").tempdir().unwrap();
    let stopped = AtomicBool::new(false);
    let hw_counter = HardwareCounterCell::new();
    let extra_vector_name = "extra_vec";

    let (source, target_config, _dirs) =
        build_source_with_extra_vector(extra_vector_name, &hw_counter);

    let mut builder = SegmentBuilder::new(
        build_dir.path(),
        &target_config,
        &HnswGlobalConfig::default(),
        FeatureFlags::default(),
    )
    .unwrap();

    // Live schema has only the default vector — the extra one was deleted from the collection.
    builder.set_live_vector_names(HashSet::from([DEFAULT_VECTOR_NAME.to_owned()]));

    builder
        .update(&[&source], &stopped, &hw_counter)
        .expect("merge should succeed by dropping the deleted source vector");

    let built = builder.build_for_test(out_dir.path());
    assert!(
        !built.vector_data.contains_key(extra_vector_name),
        "built segment must not contain the dropped vector {extra_vector_name}",
    );
    assert!(
        built.vector_data.contains_key(DEFAULT_VECTOR_NAME),
        "built segment must retain the default vector",
    );
}

#[test]
fn test_segment_builder_rejects_source_when_extra_vector_still_live() {
    // CreateVectorName race: the extra vector is still present in the live collection schema (it was
    // just created), only this optimizer's frozen target lags behind. Dropping it would corrupt the
    // next round, so the merge must cancel even with a live schema set.
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();
    let stopped = AtomicBool::new(false);
    let hw_counter = HardwareCounterCell::new();
    let extra_vector_name = "extra_vec";

    let (source, target_config, _dirs) =
        build_source_with_extra_vector(extra_vector_name, &hw_counter);

    let mut builder = SegmentBuilder::new(
        temp_dir.path(),
        &target_config,
        &HnswGlobalConfig::default(),
        FeatureFlags::default(),
    )
    .unwrap();

    // Live schema still carries the extra vector.
    builder.set_live_vector_names(HashSet::from([
        DEFAULT_VECTOR_NAME.to_owned(),
        extra_vector_name.to_owned(),
    ]));

    let err = builder
        .update(&[&source], &stopped, &hw_counter)
        .expect_err("merge must reject a source whose extra vector is still in the live schema");
    let msg = err.to_string();
    assert!(
        msg.contains("extra vector name") && msg.contains(extra_vector_name),
        "unexpected error message: {msg}",
    );
}

#[test]
fn test_building_new_segment_with_mmap_payload() {
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let mut segment1 = build_simple_segment_with_payload_storage(
        segment_dir.path(),
        4,
        Distance::Dot,
        PayloadStorageType::Mmap,
    )
    .unwrap();

    assert_eq!(
        segment1.segment_config.payload_storage_type,
        PayloadStorageType::Mmap
    );

    let hw_counter = HardwareCounterCell::new();

    // add one point
    segment1
        .upsert_point(
            1,
            1.into(),
            only_default_vector(&[1.0, 0.0, 1.0, 1.0]),
            &hw_counter,
        )
        .unwrap();

    let builder = SegmentBuilder::new(
        temp_dir.path(),
        &segment1.segment_config,
        &HnswGlobalConfig::default(),
        FeatureFlags::default(),
    )
    .unwrap();

    let temp_segment_count = fs::read_dir(temp_dir.path()).unwrap().count();

    assert_eq!(temp_segment_count, 1);

    // Now we finalize building
    let new_segment = builder.build_for_test(segment_dir.path());
    assert_eq!(
        new_segment.segment_config.payload_storage_type,
        PayloadStorageType::Mmap
    );

    let new_segment_count = fs::read_dir(segment_dir.path()).unwrap().count();

    assert_eq!(new_segment_count, 2);
}
