use std::collections::HashMap;
use std::path::Path;

use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::{only_default_vector, DenseVector, IntoDenseVector};
use segment::entry::entry_point::SegmentEntry;
use segment::segment::Segment;
use segment::segment_constructor::build_segment;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{Distance, Indexes, SegmentConfig, VectorDataConfig, VectorStorageType};
use serde_json::json;

pub fn empty_segment(path: &Path) -> Segment {
    build_simple_segment(path, 4, Distance::Dot).unwrap()
}

#[allow(dead_code)]
pub fn build_segment_1(path: &Path) -> Segment {
    let mut segment1 = empty_segment(path);

    let vec1 = [1.0, 0.0, 1.0, 1.0].into_dense_vector();
    let vec2 = [1.0, 0.0, 1.0, 0.0].into_dense_vector();
    let vec3 = [1.0, 1.0, 1.0, 1.0].into_dense_vector();
    let vec4 = [1.0, 1.0, 0.0, 1.0].into_dense_vector();
    let vec5 = [1.0, 0.0, 0.0, 0.0].into_dense_vector();

    segment1
        .upsert_point(1, 1.into(), only_default_vector(&vec1))
        .unwrap();
    segment1
        .upsert_point(2, 2.into(), only_default_vector(&vec2))
        .unwrap();
    segment1
        .upsert_point(3, 3.into(), only_default_vector(&vec3))
        .unwrap();
    segment1
        .upsert_point(4, 4.into(), only_default_vector(&vec4))
        .unwrap();
    segment1
        .upsert_point(5, 5.into(), only_default_vector(&vec5))
        .unwrap();

    let payload_key = "color";

    let payload_option1 = json!({ payload_key: vec!["red".to_owned()] }).into();
    let payload_option2 = json!({ payload_key: vec!["red".to_owned(), "blue".to_owned()] }).into();
    let payload_option3 = json!({ payload_key: vec!["blue".to_owned()] }).into();

    segment1
        .set_payload(6, 1.into(), &payload_option1, &None)
        .unwrap();
    segment1
        .set_payload(6, 2.into(), &payload_option1, &None)
        .unwrap();
    segment1
        .set_payload(6, 3.into(), &payload_option3, &None)
        .unwrap();
    segment1
        .set_payload(6, 4.into(), &payload_option2, &None)
        .unwrap();
    segment1
        .set_payload(6, 5.into(), &payload_option2, &None)
        .unwrap();

    segment1
}

#[allow(dead_code)]
pub fn build_segment_2(path: &Path) -> Segment {
    let mut segment2 = empty_segment(path);

    let vec1 = [-1.0, 0.0, 1.0, 1.0].into_dense_vector();
    let vec2 = [-1.0, 0.0, 1.0, 0.0].into_dense_vector();
    let vec3 = [-1.0, 1.0, 1.0, 1.0].into_dense_vector();
    let vec4 = [-1.0, 1.0, 0.0, 1.0].into_dense_vector();
    let vec5 = [-1.0, 0.0, 0.0, 0.0].into_dense_vector();

    segment2
        .upsert_point(11, 11.into(), only_default_vector(&vec1))
        .unwrap();
    segment2
        .upsert_point(12, 12.into(), only_default_vector(&vec2))
        .unwrap();
    segment2
        .upsert_point(13, 13.into(), only_default_vector(&vec3))
        .unwrap();
    segment2
        .upsert_point(14, 14.into(), only_default_vector(&vec4))
        .unwrap();
    segment2
        .upsert_point(15, 15.into(), only_default_vector(&vec5))
        .unwrap();

    let payload_key = "color";

    let payload_option1 = json!({ payload_key: vec!["red".to_owned()] }).into();
    let payload_option2 = json!({ payload_key: vec!["red".to_owned(), "blue".to_owned()] }).into();
    let payload_option3 = json!({ payload_key: vec!["blue".to_owned()] }).into();

    segment2
        .set_payload(16, 11.into(), &payload_option1, &None)
        .unwrap();
    segment2
        .set_payload(16, 12.into(), &payload_option1, &None)
        .unwrap();
    segment2
        .set_payload(16, 13.into(), &payload_option3, &None)
        .unwrap();
    segment2
        .set_payload(16, 14.into(), &payload_option2, &None)
        .unwrap();
    segment2
        .set_payload(16, 15.into(), &payload_option2, &None)
        .unwrap();

    segment2
}

#[allow(dead_code)]
pub fn build_segment_3(path: &Path) -> Segment {
    let mut segment3 = build_segment(
        path,
        &SegmentConfig {
            vector_data: HashMap::from([
                (
                    "vector1".to_owned(),
                    VectorDataConfig {
                        size: 4,
                        distance: Distance::Dot,
                        storage_type: VectorStorageType::Memory,
                        index: Indexes::Plain {},
                        quantization_config: None,
                        multi_vec_config: None,
                    },
                ),
                (
                    "vector2".to_owned(),
                    VectorDataConfig {
                        size: 1,
                        distance: Distance::Dot,
                        storage_type: VectorStorageType::Memory,
                        index: Indexes::Plain {},
                        quantization_config: None,
                        multi_vec_config: None,
                    },
                ),
                (
                    "vector3".to_owned(),
                    VectorDataConfig {
                        size: 4,
                        distance: Distance::Euclid,
                        storage_type: VectorStorageType::Memory,
                        index: Indexes::Plain {},
                        quantization_config: None,
                        multi_vec_config: None,
                    },
                ),
            ]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        },
        true,
    )
    .unwrap();

    let collect_points_data = |vectors: &[DenseVector]| {
        NamedVectors::from([
            ("vector1".to_owned(), vectors[0].clone()),
            ("vector2".to_owned(), vectors[1].clone()),
            ("vector3".to_owned(), vectors[2].clone()),
        ])
    };

    let vec1 = [
        [1.0, 0.0, 1.0, 1.0].into_dense_vector(),
        [0.0].into_dense_vector(),
        [-1.0, 0.0, 1.0, 1.0].into_dense_vector(),
    ];
    let vec2 = [
        [1.0, 0.0, 1.0, 0.0].into_dense_vector(),
        [1.0].into_dense_vector(),
        [-1.0, 0.0, 1.0, 1.0].into_dense_vector(),
    ];
    let vec3 = [
        [1.0, 1.0, 1.0, 1.0].into_dense_vector(),
        [2.0].into_dense_vector(),
        [-1.0, 0.0, 1.0, 1.0].into_dense_vector(),
    ];
    let vec4 = [
        [1.0, 1.0, 0.0, 1.0].into_dense_vector(),
        [3.0].into_dense_vector(),
        [-1.0, 0.0, 1.0, 1.0].into_dense_vector(),
    ];
    let vec5 = [
        [1.0, 0.0, 0.0, 0.0].into_dense_vector(),
        [4.0].into_dense_vector(),
        [-1.0, 0.0, 1.0, 1.0].into_dense_vector(),
    ];

    segment3
        .upsert_point(1, 1.into(), collect_points_data(&vec1))
        .unwrap();
    segment3
        .upsert_point(2, 2.into(), collect_points_data(&vec2))
        .unwrap();
    segment3
        .upsert_point(3, 3.into(), collect_points_data(&vec3))
        .unwrap();
    segment3
        .upsert_point(4, 4.into(), collect_points_data(&vec4))
        .unwrap();
    segment3
        .upsert_point(5, 5.into(), collect_points_data(&vec5))
        .unwrap();

    let payload_key = "color";

    let payload_option1 = json!({ payload_key: vec!["red".to_owned()] }).into();
    let payload_option2 = json!({ payload_key: vec!["red".to_owned(), "blue".to_owned()] }).into();
    let payload_option3 = json!({ payload_key: vec!["blue".to_owned()] }).into();

    segment3
        .set_payload(6, 1.into(), &payload_option1, &None)
        .unwrap();
    segment3
        .set_payload(6, 2.into(), &payload_option1, &None)
        .unwrap();
    segment3
        .set_payload(6, 3.into(), &payload_option3, &None)
        .unwrap();
    segment3
        .set_payload(6, 4.into(), &payload_option2, &None)
        .unwrap();
    segment3
        .set_payload(6, 5.into(), &payload_option2, &None)
        .unwrap();

    segment3
}
