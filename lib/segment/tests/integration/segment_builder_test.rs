use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::cpu::CpuPermit;
use itertools::Itertools;
use segment::common::operation_error::OperationError;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::{only_default_vector, VectorRef, DEFAULT_VECTOR_NAME};
use segment::entry::entry_point::SegmentEntry;
use segment::index::hnsw_index::num_rayon_threads;
use segment::json_path::JsonPath;
use segment::segment::Segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment_with_payload_storage;
use segment::types::{
    Distance, Indexes, PayloadContainer, PayloadKeyType, PayloadStorageType, SegmentConfig,
    VectorDataConfig, VectorStorageType,
};
use serde_json::Value;
use sparse::common::sparse_vector::SparseVector;
use tempfile::Builder;

use crate::fixtures::segment::{
    build_segment_1, build_segment_2, build_segment_sparse_1, build_segment_sparse_2,
    empty_segment, PAYLOAD_KEY,
};

#[test]
fn test_building_new_segment() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let stopped = AtomicBool::new(false);

    let segment1 = build_segment_1(dir.path());
    let mut segment2 = build_segment_2(dir.path());

    let mut builder =
        SegmentBuilder::new(dir.path(), temp_dir.path(), &segment1.segment_config).unwrap();

    // Include overlapping with segment1 to check the
    segment2
        .upsert_point(100, 3.into(), only_default_vector(&[0., 0., 0., 0.]))
        .unwrap();

    builder
        .update(&[&segment1, &segment2, &segment2], &stopped)
        .unwrap();

    // Check what happens if segment building fails here

    let segment_count = dir.path().read_dir().unwrap().count();

    assert_eq!(segment_count, 2);

    let temp_segment_count = temp_dir.path().read_dir().unwrap().count();

    assert_eq!(temp_segment_count, 1);

    // Now we finalize building

    let permit_cpu_count = num_rayon_threads(0);
    let permit = CpuPermit::dummy(permit_cpu_count as u32);

    let merged_segment: Segment = builder.build(permit, &stopped).unwrap();

    let new_segment_count = dir.path().read_dir().unwrap().count();

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

    let mut segment1 = build_segment_1(dir.path());
    segment1
        .create_field_index(7, &defragment_key, None)
        .unwrap();

    let mut segment2 = build_segment_2(dir.path());
    segment2
        .create_field_index(17, &defragment_key, None)
        .unwrap();

    let mut builder =
        SegmentBuilder::new(dir.path(), temp_dir.path(), &segment1.segment_config).unwrap();

    // Include overlapping with segment1 to check the
    segment2
        .upsert_point(100, 3.into(), only_default_vector(&[0., 0., 0., 0.]))
        .unwrap();

    builder.set_defragment_keys(vec![defragment_key.clone()]);

    builder.update(&[&segment1, &segment2], &stopped).unwrap();

    // Check what happens if segment building fails here

    let segment_count = dir.path().read_dir().unwrap().count();

    assert_eq!(segment_count, 2);

    let temp_segment_count = temp_dir.path().read_dir().unwrap().count();

    assert_eq!(temp_segment_count, 1);

    // Now we finalize building

    let permit_cpu_count = num_rayon_threads(0);
    let permit = CpuPermit::dummy(permit_cpu_count as u32);

    let merged_segment: Segment = builder.build(permit, &stopped).unwrap();

    let new_segment_count = dir.path().read_dir().unwrap().count();

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

    for internal_id in id_tracker.iter_internal() {
        let external_id = id_tracker.external_id(internal_id).unwrap();
        let payload = segment.payload(external_id).unwrap();
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

    let segment1 = build_segment_sparse_1(dir.path());
    let mut segment2 = build_segment_sparse_2(dir.path());

    let mut builder =
        SegmentBuilder::new(dir.path(), temp_dir.path(), &segment1.segment_config).unwrap();

    // Include overlapping with segment1 to check the
    let vec = SparseVector::new(vec![0, 1, 2, 3], vec![0.0, 0.0, 0.0, 0.0]).unwrap();
    segment2
        .upsert_point(
            100,
            3.into(),
            NamedVectors::from_ref("sparse", VectorRef::Sparse(&vec)),
        )
        .unwrap();

    builder
        .update(&[&segment1, &segment2, &segment2], &stopped)
        .unwrap();

    // Check what happens if segment building fails here

    let segment_count = dir.path().read_dir().unwrap().count();

    assert_eq!(segment_count, 2);

    let temp_segment_count = temp_dir.path().read_dir().unwrap().count();

    assert_eq!(temp_segment_count, 1);

    // Now we finalize building

    let permit_cpu_count = num_rayon_threads(0);
    let permit = CpuPermit::dummy(permit_cpu_count as u32);

    let merged_segment: Segment = builder.build(permit, &stopped).unwrap();

    let new_segment_count = dir.path().read_dir().unwrap().count();

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
    let stopped = Arc::new(AtomicBool::new(false));

    let dir = Builder::new().prefix("segment_dir1").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let segment_config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: segment.segment_config.vector_data[DEFAULT_VECTOR_NAME].size,
                distance: segment.segment_config.vector_data[DEFAULT_VECTOR_NAME].distance,
                storage_type: VectorStorageType::Memory,
                index: Indexes::Hnsw(Default::default()),
                quantization_config: None,
                multivector_config: None,
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };

    let mut builder = SegmentBuilder::new(dir.path(), temp_dir.path(), &segment_config).unwrap();

    builder.update(&[segment], &stopped).unwrap();

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

    let permit_cpu_count = num_rayon_threads(0);
    let permit = CpuPermit::dummy(permit_cpu_count as u32);

    let res = builder.build(permit, &stopped);

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

    for idx in 0..2000 {
        baseline_segment
            .upsert_point(1, idx.into(), only_default_vector(&[0., 0., 0., 0.]))
            .unwrap();
        segment
            .upsert_point(1, idx.into(), only_default_vector(&[0., 0., 0., 0.]))
            .unwrap();
        segment_2
            .upsert_point(1, idx.into(), only_default_vector(&[0., 0., 0., 0.]))
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

    let acceptable_stopping_delay = 600; // millis

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

    assert!(
        time_fast < time_long,
        "time_early: {time_fast}, time_later: {time_long}, was_cancelled_later: {was_cancelled_later}",
    );
}

#[test]
fn test_building_new_segment_with_mmap_payload() {
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let stopped = AtomicBool::new(false);

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

    // add one point
    segment1
        .upsert_point(1, 1.into(), only_default_vector(&[1.0, 0.0, 1.0, 1.0]))
        .unwrap();

    let builder = SegmentBuilder::new(
        segment_dir.path(),
        temp_dir.path(),
        &segment1.segment_config,
    )
    .unwrap();

    let temp_segment_count = temp_dir.path().read_dir().unwrap().count();

    assert_eq!(temp_segment_count, 1);

    // Now we finalize building
    let permit_cpu_count = num_rayon_threads(0);
    let permit = CpuPermit::dummy(permit_cpu_count as u32);

    let new_segment: Segment = builder.build(permit, &stopped).unwrap();
    assert_eq!(
        new_segment.segment_config.payload_storage_type,
        PayloadStorageType::Mmap
    );

    let new_segment_count = segment_dir.path().read_dir().unwrap().count();

    assert_eq!(new_segment_count, 2);
}
