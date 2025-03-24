const NUM_VECTORS_1: u64 = 300;
const NUM_VECTORS_2: u64 = 500;

use std::sync::atomic::AtomicBool;

use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use segment::data_types::named_vectors::NamedVectors;
use segment::entry::entry_point::SegmentEntry;
use segment::index::hnsw_index::num_rayon_threads;
use segment::segment::Segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::segment_constructor::simple_segment_constructor::{
    VECTOR1_NAME, VECTOR2_NAME, build_multivec_segment,
};
use segment::types::Distance;
use segment::vector_storage::VectorStorage;
use tempfile::Builder;

#[test]
fn test_rebuild_with_removed_vectors() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let stopped = AtomicBool::new(false);

    let mut segment1 = build_multivec_segment(dir.path(), 4, 6, Distance::Dot).unwrap();
    let mut segment2 = build_multivec_segment(dir.path(), 4, 6, Distance::Dot).unwrap();

    let hw_counter = HardwareCounterCell::new();

    for i in 0..NUM_VECTORS_1 {
        segment1
            .upsert_point(
                1,
                i.into(),
                NamedVectors::from_pairs([
                    (VECTOR1_NAME.into(), vec![i as f32, 0., 0., 0.]),
                    (VECTOR2_NAME.into(), vec![0., i as f32, 0., 0., 0., 0.]),
                ]),
                &hw_counter,
            )
            .unwrap();
    }

    for i in 0..NUM_VECTORS_2 {
        let vectors = if i % 5 == 0 {
            NamedVectors::from_pairs([(VECTOR1_NAME.into(), vec![0., 0., i as f32, 0.])])
        } else {
            NamedVectors::from_pairs([
                (VECTOR1_NAME.into(), vec![0., 0., i as f32, 0.]),
                (VECTOR2_NAME.into(), vec![0., 0., 0., i as f32, 0., 0.]),
            ])
        };

        segment2
            .upsert_point(1, (NUM_VECTORS_1 + i).into(), vectors, &hw_counter)
            .unwrap();
    }

    for i in 0..NUM_VECTORS_2 {
        if i % 3 == 0 {
            segment2
                .delete_vector(2, (NUM_VECTORS_1 + i).into(), VECTOR1_NAME)
                .unwrap();
            segment2
                .delete_vector(2, (NUM_VECTORS_1 + i).into(), VECTOR2_NAME)
                .unwrap();
        }
        if i % 3 == 1 {
            segment2
                .delete_vector(2, (NUM_VECTORS_1 + i).into(), VECTOR2_NAME)
                .unwrap();
        }
        if i % 2 == 0 {
            segment2
                .delete_point(2, (NUM_VECTORS_1 + i).into(), &hw_counter)
                .unwrap();
        }
    }

    let mut reference = vec![];

    for i in 0..20 {
        if i % 2 == 0 {
            continue;
        }
        let idx = NUM_VECTORS_1 + i;
        let vec = segment2.all_vectors(idx.into()).unwrap();
        reference.push(vec);
    }

    let mut builder =
        SegmentBuilder::new(dir.path(), temp_dir.path(), &segment1.segment_config).unwrap();

    builder.update(&[&segment1, &segment2], &stopped).unwrap();

    let permit_cpu_count = num_rayon_threads(0);
    let permit = ResourcePermit::dummy(permit_cpu_count as u32);
    let hw_counter = HardwareCounterCell::new();

    let merged_segment: Segment = builder.build(permit, &stopped, &hw_counter).unwrap();

    let merged_points_count = merged_segment.available_point_count();

    assert_eq!(
        merged_points_count,
        (NUM_VECTORS_1 + NUM_VECTORS_2 / 2) as usize
    );

    let vec1_count = merged_segment
        .vector_data
        .get(VECTOR1_NAME)
        .unwrap()
        .vector_storage
        .borrow()
        .available_vector_count();
    let vec2_count = merged_segment
        .vector_data
        .get(VECTOR2_NAME)
        .unwrap()
        .vector_storage
        .borrow()
        .available_vector_count();

    assert_ne!(vec1_count, vec2_count);

    assert!(vec1_count > NUM_VECTORS_1 as usize);
    assert!(vec2_count > NUM_VECTORS_1 as usize);
    assert!(vec1_count < NUM_VECTORS_1 as usize + NUM_VECTORS_2 as usize);
    assert!(vec2_count < NUM_VECTORS_1 as usize + NUM_VECTORS_2 as usize);

    let mut merged_reference = vec![];

    for i in 0..20 {
        if i % 2 == 0 {
            continue;
        }
        let idx = NUM_VECTORS_1 + i;
        let vec = merged_segment.all_vectors(idx.into()).unwrap();
        merged_reference.push(vec);
    }

    for i in 0..merged_reference.len() {
        assert_eq!(merged_reference[i], reference[i]);
    }
}
