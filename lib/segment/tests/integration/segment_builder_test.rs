use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use common::progress_tracker::ProgressTracker;
use fs_err as fs;
use itertools::Itertools;
use segment::common::operation_error::OperationError;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, VectorRef, only_default_vector};
use segment::entry::entry_point::{NonAppendableSegmentEntry, ReadSegmentEntry, SegmentEntry};
use segment::id_tracker::IdTrackerRead;
use segment::index::hnsw_index::get_num_indexing_threads;
use segment::json_path::JsonPath;
use segment::segment::Segment;
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

fn estimate_build_time(segment: &Segment, stop_delay_millis: Option<u64>) -> (u64, bool) {
    let mut rng = rand::rng();
    let stopped = Arc::new(AtomicBool::new(false));

    let dir = Builder::new().prefix("segment_dir1").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let segment_config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: segment.segment_config.vector_data[DEFAULT_VECTOR_NAME].size,
                distance: segment.segment_config.vector_data[DEFAULT_VECTOR_NAME].distance,
                storage_type: VectorStorageType::default(),
                index: Indexes::Hnsw(Default::default()),
                quantization_config: None,
                multivector_config: None,
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };

    let mut builder = SegmentBuilder::new(
        temp_dir.path(),
        &segment_config,
        &HnswGlobalConfig::default(),
    )
    .unwrap();

    let hw_counter = HardwareCounterCell::new();
    builder.update(&[segment], &stopped, &hw_counter).unwrap();

    let now = Instant::now();

    if let Some(stop_delay_millis) = stop_delay_millis {
        let stopped_t = stopped.clone();

        std::thread::Builder::new()
            .name("build_estimator_timeout".to_string())
            .spawn(move || {
                std::thread::sleep(Duration::from_millis(stop_delay_millis));
                stopped_t.store(true, Ordering::Release);
            })
            .unwrap();
    }

    let permit_cpu_count = get_num_indexing_threads(0);
    let permit = ResourcePermit::dummy(permit_cpu_count as u32);
    let hw_counter = HardwareCounterCell::new();
    let progress = ProgressTracker::new_for_test();

    let res = builder.build(
        dir.path(),
        Uuid::new_v4(),
        None,
        permit,
        &stopped,
        &mut rng,
        &hw_counter,
        progress,
    );

    let is_cancelled = match res {
        Ok(_) => false,
        Err(OperationError::Cancelled { .. }) => true,
        Err(err) => {
            eprintln!("Was expecting cancellation signal but got unexpected error: {err:?}");
            false
        }
    };

    (now.elapsed().as_millis() as u64, is_cancelled)
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

#[test]
fn test_building_cancellation() {
    let baseline_dir = Builder::new()
        .prefix("segment_dir_baseline")
        .tempdir()
        .unwrap();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let dir_2 = Builder::new().prefix("segment_dir_2").tempdir().unwrap();

    let mut baseline_segment = empty_segment(baseline_dir.path());
    let mut segment = empty_segment(dir.path());
    let mut segment_2 = empty_segment(dir_2.path());

    let hw_counter = HardwareCounterCell::new();

    for idx in 0..2000 {
        baseline_segment
            .upsert_point(
                1,
                idx.into(),
                only_default_vector(&[0., 0., 0., 0.]),
                &hw_counter,
            )
            .unwrap();
        segment
            .upsert_point(
                1,
                idx.into(),
                only_default_vector(&[0., 0., 0., 0.]),
                &hw_counter,
            )
            .unwrap();
        segment_2
            .upsert_point(
                1,
                idx.into(),
                only_default_vector(&[0., 0., 0., 0.]),
                &hw_counter,
            )
            .unwrap();
    }

    // Get normal build time
    let (time_baseline, was_cancelled_baseline) = estimate_build_time(&baseline_segment, None);
    assert!(!was_cancelled_baseline);
    eprintln!("baseline time: {time_baseline}");

    // Checks that optimization with longer cancellation delay will also finish fast
    let early_stop_delay = time_baseline / 20;
    let (time_fast, was_cancelled_early) = estimate_build_time(&segment, Some(early_stop_delay));
    let late_stop_delay = time_baseline / 5;
    let (time_long, was_cancelled_later) = estimate_build_time(&segment_2, Some(late_stop_delay));

    // Timing on CI (especially Windows) can be noisy due to scheduler delays.
    // Reaction to the stop flag is bounded by the largest non-interruptible
    // chunk of build work plus scheduler noise, neither of which shrinks with
    // the baseline, so the fixed floor must dominate. Reactions of ~860ms were
    // observed on windows-latest with a ~3.5s baseline (see #9183).
    let acceptable_stopping_delay = std::cmp::max(1500, time_baseline / 4); // millis

    assert!(was_cancelled_early);
    assert!(
        time_fast < early_stop_delay + acceptable_stopping_delay,
        "time_early: {time_fast}, early_stop_delay: {early_stop_delay}"
    );

    assert!(was_cancelled_later);
    assert!(
        time_long < late_stop_delay + acceptable_stopping_delay,
        "time_later: {time_long}, late_stop_delay: {late_stop_delay}"
    );

    // Comparative assertions between runs (early vs late vs baseline) are
    // deliberately avoided: the reaction time to the stop flag can vary by
    // more than the difference between the stop delays, which made such
    // assertions flaky on CI. Ignored cancellation is already caught by the
    // `was_cancelled_*` checks (the build would complete with `Ok`), and
    // prompt reaction is covered by the per-run bounds above.
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
