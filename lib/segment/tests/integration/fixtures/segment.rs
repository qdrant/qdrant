use std::collections::HashMap;
use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::{only_default_vector, DenseVector, VectorRef};
use segment::entry::entry_point::SegmentEntry;
use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use segment::segment::Segment;
use segment::segment_constructor::build_segment;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    Distance, Indexes, SegmentConfig, SparseVectorDataConfig, SparseVectorStorageType,
    VectorDataConfig, VectorStorageType,
};
use serde_json::json;
use sparse::common::sparse_vector::SparseVector;

pub fn empty_segment(path: &Path) -> Segment {
    build_simple_segment(path, 4, Distance::Dot).unwrap()
}

pub const PAYLOAD_KEY: &str = "color";

pub fn build_segment_1(path: &Path) -> Segment {
    let mut segment1 = empty_segment(path);

    let vec1 = vec![1.0, 0.0, 1.0, 1.0];
    let vec2 = vec![1.0, 0.0, 1.0, 0.0];
    let vec3 = vec![1.0, 1.0, 1.0, 1.0];
    let vec4 = vec![1.0, 1.0, 0.0, 1.0];
    let vec5 = vec![1.0, 0.0, 0.0, 0.0];

    let hw_counter = HardwareCounterCell::new();

    segment1
        .upsert_point(1, 1.into(), only_default_vector(&vec1), &hw_counter)
        .unwrap();
    segment1
        .upsert_point(2, 2.into(), only_default_vector(&vec2), &hw_counter)
        .unwrap();
    segment1
        .upsert_point(3, 3.into(), only_default_vector(&vec3), &hw_counter)
        .unwrap();
    segment1
        .upsert_point(4, 4.into(), only_default_vector(&vec4), &hw_counter)
        .unwrap();
    segment1
        .upsert_point(5, 5.into(), only_default_vector(&vec5), &hw_counter)
        .unwrap();

    let payload_key = PAYLOAD_KEY;

    let payload_option1 = json!({ payload_key: vec!["red".to_owned()] }).into();
    let payload_option2 = json!({ payload_key: vec!["red".to_owned(), "blue".to_owned()] }).into();
    let payload_option3 = json!({ payload_key: vec!["blue".to_owned()] }).into();

    segment1
        .set_payload(6, 1.into(), &payload_option1, &None, &hw_counter)
        .unwrap();
    segment1
        .set_payload(6, 2.into(), &payload_option1, &None, &hw_counter)
        .unwrap();
    segment1
        .set_payload(6, 3.into(), &payload_option3, &None, &hw_counter)
        .unwrap();
    segment1
        .set_payload(6, 4.into(), &payload_option2, &None, &hw_counter)
        .unwrap();
    segment1
        .set_payload(6, 5.into(), &payload_option2, &None, &hw_counter)
        .unwrap();

    segment1
}

pub fn build_segment_2(path: &Path) -> Segment {
    let mut segment2 = empty_segment(path);

    let vec1 = vec![-1.0, 0.0, 1.0, 1.0];
    let vec2 = vec![-1.0, 0.0, 1.0, 0.0];
    let vec3 = vec![-1.0, 1.0, 1.0, 1.0];
    let vec4 = vec![-1.0, 1.0, 0.0, 1.0];
    let vec5 = vec![-1.0, 0.0, 0.0, 0.0];

    let hw_counter = HardwareCounterCell::new();

    segment2
        .upsert_point(11, 11.into(), only_default_vector(&vec1), &hw_counter)
        .unwrap();
    segment2
        .upsert_point(12, 12.into(), only_default_vector(&vec2), &hw_counter)
        .unwrap();
    segment2
        .upsert_point(13, 13.into(), only_default_vector(&vec3), &hw_counter)
        .unwrap();
    segment2
        .upsert_point(14, 14.into(), only_default_vector(&vec4), &hw_counter)
        .unwrap();
    segment2
        .upsert_point(15, 15.into(), only_default_vector(&vec5), &hw_counter)
        .unwrap();

    let payload_key = PAYLOAD_KEY;

    let payload_option1 = json!({ payload_key: vec!["red".to_owned()] }).into();
    let payload_option2 = json!({ payload_key: vec!["red".to_owned(), "blue".to_owned()] }).into();
    let payload_option3 = json!({ payload_key: vec!["blue".to_owned()] }).into();

    segment2
        .set_payload(16, 11.into(), &payload_option1, &None, &hw_counter)
        .unwrap();
    segment2
        .set_payload(16, 12.into(), &payload_option1, &None, &hw_counter)
        .unwrap();
    segment2
        .set_payload(16, 13.into(), &payload_option3, &None, &hw_counter)
        .unwrap();
    segment2
        .set_payload(16, 14.into(), &payload_option2, &None, &hw_counter)
        .unwrap();
    segment2
        .set_payload(16, 15.into(), &payload_option2, &None, &hw_counter)
        .unwrap();

    segment2
}

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
                        multivector_config: None,
                        datatype: None,
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
                        multivector_config: None,
                        datatype: None,
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
                        multivector_config: None,
                        datatype: None,
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
        NamedVectors::from_pairs([
            ("vector1".to_owned(), vectors[0].clone()),
            ("vector2".to_owned(), vectors[1].clone()),
            ("vector3".to_owned(), vectors[2].clone()),
        ])
    };

    let vec1 = [
        vec![1.0, 0.0, 1.0, 1.0],
        vec![0.0],
        vec![-1.0, 0.0, 1.0, 1.0],
    ];
    let vec2 = [
        vec![1.0, 0.0, 1.0, 0.0],
        vec![1.0],
        vec![-1.0, 0.0, 1.0, 1.0],
    ];
    let vec3 = [
        vec![1.0, 1.0, 1.0, 1.0],
        vec![2.0],
        vec![-1.0, 0.0, 1.0, 1.0],
    ];
    let vec4 = [
        vec![1.0, 1.0, 0.0, 1.0],
        vec![3.0],
        vec![-1.0, 0.0, 1.0, 1.0],
    ];
    let vec5 = [
        vec![1.0, 0.0, 0.0, 0.0],
        vec![4.0],
        vec![-1.0, 0.0, 1.0, 1.0],
    ];

    let hw_counter = HardwareCounterCell::new();

    segment3
        .upsert_point(1, 1.into(), collect_points_data(&vec1), &hw_counter)
        .unwrap();
    segment3
        .upsert_point(2, 2.into(), collect_points_data(&vec2), &hw_counter)
        .unwrap();
    segment3
        .upsert_point(3, 3.into(), collect_points_data(&vec3), &hw_counter)
        .unwrap();
    segment3
        .upsert_point(4, 4.into(), collect_points_data(&vec4), &hw_counter)
        .unwrap();
    segment3
        .upsert_point(5, 5.into(), collect_points_data(&vec5), &hw_counter)
        .unwrap();

    let payload_key = PAYLOAD_KEY;

    let payload_option1 = json!({ payload_key: vec!["red".to_owned()] }).into();
    let payload_option2 = json!({ payload_key: vec!["red".to_owned(), "blue".to_owned()] }).into();
    let payload_option3 = json!({ payload_key: vec!["blue".to_owned()] }).into();

    segment3
        .set_payload(6, 1.into(), &payload_option1, &None, &hw_counter)
        .unwrap();
    segment3
        .set_payload(6, 2.into(), &payload_option1, &None, &hw_counter)
        .unwrap();
    segment3
        .set_payload(6, 3.into(), &payload_option3, &None, &hw_counter)
        .unwrap();
    segment3
        .set_payload(6, 4.into(), &payload_option2, &None, &hw_counter)
        .unwrap();
    segment3
        .set_payload(6, 5.into(), &payload_option2, &None, &hw_counter)
        .unwrap();

    segment3
}

pub fn build_segment_sparse_1(path: &Path) -> Segment {
    let mut segment1 = build_segment(
        path,
        &SegmentConfig {
            vector_data: Default::default(),
            sparse_vector_data: HashMap::from([(
                "sparse".to_owned(),
                SparseVectorDataConfig {
                    index: SparseIndexConfig::new(None, SparseIndexType::MutableRam, None),
                    storage_type: SparseVectorStorageType::default(),
                },
            )]),
            payload_storage_type: Default::default(),
        },
        true,
    )
    .unwrap();

    let vec1 = SparseVector::new(vec![0, 1, 2, 3], vec![1.0, 0.0, 1.0, 1.0]).unwrap();
    let vec2 = SparseVector::new(vec![0, 1, 2, 3], vec![1.0, 0.0, 1.0, 0.0]).unwrap();
    let vec3 = SparseVector::new(vec![0, 1, 2, 3], vec![1.0, 1.0, 1.0, 1.0]).unwrap();
    let vec4 = SparseVector::new(vec![0, 1, 2, 3], vec![1.0, 1.0, 0.0, 1.0]).unwrap();
    let vec5 = SparseVector::new(vec![0, 1, 2, 3], vec![1.0, 0.0, 0.0, 0.0]).unwrap();

    let hw_counter = HardwareCounterCell::new();

    segment1
        .upsert_point(
            1,
            1.into(),
            NamedVectors::from_ref("sparse", VectorRef::Sparse(&vec1)),
            &hw_counter,
        )
        .unwrap();
    segment1
        .upsert_point(
            2,
            2.into(),
            NamedVectors::from_ref("sparse", VectorRef::Sparse(&vec2)),
            &hw_counter,
        )
        .unwrap();
    segment1
        .upsert_point(
            3,
            3.into(),
            NamedVectors::from_ref("sparse", VectorRef::Sparse(&vec3)),
            &hw_counter,
        )
        .unwrap();
    segment1
        .upsert_point(
            4,
            4.into(),
            NamedVectors::from_ref("sparse", VectorRef::Sparse(&vec4)),
            &hw_counter,
        )
        .unwrap();
    segment1
        .upsert_point(
            5,
            5.into(),
            NamedVectors::from_ref("sparse", VectorRef::Sparse(&vec5)),
            &hw_counter,
        )
        .unwrap();

    let payload_key = PAYLOAD_KEY;

    let payload_option1 = json!({ payload_key: vec!["red".to_owned()] }).into();
    let payload_option2 = json!({ payload_key: vec!["red".to_owned(), "blue".to_owned()] }).into();
    let payload_option3 = json!({ payload_key: vec!["blue".to_owned()] }).into();

    segment1
        .set_payload(6, 1.into(), &payload_option1, &None, &hw_counter)
        .unwrap();
    segment1
        .set_payload(6, 2.into(), &payload_option1, &None, &hw_counter)
        .unwrap();
    segment1
        .set_payload(6, 3.into(), &payload_option3, &None, &hw_counter)
        .unwrap();
    segment1
        .set_payload(6, 4.into(), &payload_option2, &None, &hw_counter)
        .unwrap();
    segment1
        .set_payload(6, 5.into(), &payload_option2, &None, &hw_counter)
        .unwrap();

    segment1
}

pub fn build_segment_sparse_2(path: &Path) -> Segment {
    let mut segment2 = build_segment(
        path,
        &SegmentConfig {
            vector_data: Default::default(),
            sparse_vector_data: HashMap::from([(
                "sparse".to_owned(),
                SparseVectorDataConfig {
                    index: SparseIndexConfig::new(None, SparseIndexType::MutableRam, None),
                    storage_type: SparseVectorStorageType::default(),
                },
            )]),
            payload_storage_type: Default::default(),
        },
        true,
    )
    .unwrap();

    let hw_counter = HardwareCounterCell::new();

    let vec1 = SparseVector::new(vec![0, 1, 2, 3], vec![-1.0, 0.0, 1.0, 1.0]).unwrap();
    let vec2 = SparseVector::new(vec![0, 1, 2, 3], vec![-1.0, 0.0, 1.0, 0.0]).unwrap();
    let vec3 = SparseVector::new(vec![0, 1, 2, 3], vec![-1.0, 1.0, 1.0, 1.0]).unwrap();
    let vec4 = SparseVector::new(vec![0, 1, 2, 3], vec![-1.0, 1.0, 0.0, 1.0]).unwrap();
    let vec5 = SparseVector::new(vec![0, 1, 2, 3], vec![-1.0, 0.0, 0.0, 0.0]).unwrap();

    segment2
        .upsert_point(
            11,
            11.into(),
            NamedVectors::from_ref("sparse", VectorRef::Sparse(&vec1)),
            &hw_counter,
        )
        .unwrap();
    segment2
        .upsert_point(
            12,
            12.into(),
            NamedVectors::from_ref("sparse", VectorRef::Sparse(&vec2)),
            &hw_counter,
        )
        .unwrap();
    segment2
        .upsert_point(
            13,
            13.into(),
            NamedVectors::from_ref("sparse", VectorRef::Sparse(&vec3)),
            &hw_counter,
        )
        .unwrap();
    segment2
        .upsert_point(
            14,
            14.into(),
            NamedVectors::from_ref("sparse", VectorRef::Sparse(&vec4)),
            &hw_counter,
        )
        .unwrap();
    segment2
        .upsert_point(
            15,
            15.into(),
            NamedVectors::from_ref("sparse", VectorRef::Sparse(&vec5)),
            &hw_counter,
        )
        .unwrap();

    let payload_key = PAYLOAD_KEY;

    let payload_option1 = json!({ payload_key: vec!["red".to_owned()] }).into();
    let payload_option2 = json!({ payload_key: vec!["red".to_owned(), "blue".to_owned()] }).into();
    let payload_option3 = json!({ payload_key: vec!["blue".to_owned()] }).into();

    segment2
        .set_payload(16, 11.into(), &payload_option1, &None, &hw_counter)
        .unwrap();
    segment2
        .set_payload(16, 12.into(), &payload_option1, &None, &hw_counter)
        .unwrap();
    segment2
        .set_payload(16, 13.into(), &payload_option3, &None, &hw_counter)
        .unwrap();
    segment2
        .set_payload(16, 14.into(), &payload_option2, &None, &hw_counter)
        .unwrap();
    segment2
        .set_payload(16, 15.into(), &payload_option2, &None, &hw_counter)
        .unwrap();

    segment2
}
