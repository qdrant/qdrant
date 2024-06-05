use std::collections::HashMap;
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
use segment::segment::Segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::types::{Indexes, SegmentConfig, VectorDataConfig, VectorStorageType};
use sparse::common::sparse_vector::SparseVector;
use tempfile::Builder;

use crate::fixtures::segment::{build_segment_1, build_segment_2, build_segment_sparse_1, build_segment_sparse_2, empty_segment};

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

    builder.update_from(&segment1, &stopped).unwrap();
    builder.update_from(&segment2, &stopped).unwrap();
    builder.update_from(&segment2, &stopped).unwrap();

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
        .upsert_point(100, 3.into(), NamedVectors::from_ref("sparse", VectorRef::Sparse(&vec)))
        .unwrap();

    builder.update_from(&segment1, &stopped).unwrap();
    builder.update_from(&segment2, &stopped).unwrap();
    builder.update_from(&segment2, &stopped).unwrap();

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

fn estimate_build_time(segment: &Segment, stop_delay_millis: u64) -> (u64, bool) {
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
                multivec_config: None,
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };

    let mut builder = SegmentBuilder::new(dir.path(), temp_dir.path(), &segment_config).unwrap();

    builder.update_from(segment, &stopped).unwrap();

    let now = Instant::now();

    let stopped_t = stopped.clone();

    std::thread::Builder::new()
        .name("build_estimator_timeout".to_string())
        .spawn(move || {
            std::thread::sleep(Duration::from_millis(stop_delay_millis));
            stopped_t.store(true, Ordering::Release);
        })
        .unwrap();

    let permit_cpu_count = num_rayon_threads(0);
    let permit = CpuPermit::dummy(permit_cpu_count as u32);

    let res = builder.build(permit, &stopped);

    let is_cancelled = match res {
        Ok(_) => false,
        Err(OperationError::Cancelled { .. }) => true,
        Err(err) => {
            eprintln!(
                "Was expecting cancellation signal but got unexpected error: {:?}",
                err
            );
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
    let (time_baseline, was_cancelled_baseline) = estimate_build_time(&baseline_segment, 20000);
    assert!(!was_cancelled_baseline);
    eprintln!("baseline time: {}", time_baseline);

    // Checks that optimization with longer cancellation delay will also finish fast
    let early_stop_delay = time_baseline / 20;
    let (time_fast, was_cancelled_early) = estimate_build_time(&segment, early_stop_delay);
    let late_stop_delay = time_baseline / 5;
    let (time_long, was_cancelled_later) = estimate_build_time(&segment_2, late_stop_delay);

    let acceptable_stopping_delay = 600; // millis

    assert!(was_cancelled_early);
    assert!(
        time_fast < early_stop_delay + acceptable_stopping_delay,
        "time_early: {}, early_stop_delay: {}",
        time_fast,
        early_stop_delay
    );

    assert!(was_cancelled_later);
    assert!(
        time_long < late_stop_delay + acceptable_stopping_delay,
        "time_later: {}, late_stop_delay: {}",
        time_long,
        late_stop_delay
    );

    assert!(
        time_fast < time_long,
        "time_early: {}, time_later: {}, was_cancelled_later: {}",
        time_fast,
        time_long,
        was_cancelled_later,
    );
}
