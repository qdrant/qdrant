use std::collections::HashMap;

use common::counter::hardware_counter::HardwareCounterCell;
use tempfile::Builder;

use super::*;
use crate::common::operation_error::OperationError;
use crate::entry::entry_point::SegmentEntry;
use crate::segment_constructor::build_segment;
use crate::types::{
    Distance, Indexes, SegmentConfig, VectorDataConfig, VectorStorageDatatype, VectorStorageType,
};

fn build_segment_with_datatype(
    dir: &std::path::Path,
    dim: usize,
    distance: Distance,
    datatype: VectorStorageDatatype,
) -> crate::segment::Segment {
    let config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: dim,
                distance,
                storage_type: VectorStorageType::ChunkedMmap,
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config: None,
                datatype: Some(datatype),
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
        id_tracker_memory: None,
    };
    let (segment, _) = build_segment(dir, &config, None, true).unwrap();
    segment
}

#[test]
fn test_float16_out_of_range_component_rejected() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment =
        build_segment_with_datatype(dir.path(), 4, Distance::Dot, VectorStorageDatatype::Float16);
    let hw_counter = HardwareCounterCell::new();

    let err = segment
        .upsert_point(
            100,
            1.into(),
            only_default_vector(&[70000.0, -70000.0, 0.5, 1.0]),
            &hw_counter,
        )
        .unwrap_err();
    assert_matches!(
        err,
        OperationError::WrongVectorValue {
            index: 0,
            datatype: "float16"
        }
    );

    segment
        .upsert_point(
            100,
            2.into(),
            only_default_vector(&[65504.0, -65504.0, 0.5, 1.0]),
            &hw_counter,
        )
        .unwrap();
    assert_eq!(
        segment
            .vector(DEFAULT_VECTOR_NAME, 2.into(), &hw_counter)
            .unwrap()
            .unwrap(),
        VectorInternal::Dense(vec![65504.0, -65504.0, 0.5, 1.0])
    );
}

#[test]
fn test_update_path_rejects_out_of_range_too() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment =
        build_segment_with_datatype(dir.path(), 2, Distance::Dot, VectorStorageDatatype::Float16);
    let hw_counter = HardwareCounterCell::new();
    segment
        .upsert_point(100, 1.into(), only_default_vector(&[0.5, 1.0]), &hw_counter)
        .unwrap();

    let err = SegmentEntry::update_vectors(
        &mut segment,
        200,
        1_u64.into(),
        NamedVectors::from_pairs([(DEFAULT_VECTOR_NAME.into(), vec![70000.0, 1.0])]),
        &hw_counter,
    )
    .unwrap_err();
    assert_matches!(
        err,
        OperationError::WrongVectorValue {
            index: 0,
            datatype: "float16"
        }
    );
}

#[test]
fn test_non_finite_component_rejected_for_every_datatype() {
    for datatype in [
        VectorStorageDatatype::Float32,
        VectorStorageDatatype::Float16,
    ] {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let mut segment = build_segment_with_datatype(dir.path(), 2, Distance::Dot, datatype);
        let hw_counter = HardwareCounterCell::new();

        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let err = segment
                .upsert_point(100, 1.into(), only_default_vector(&[bad, 1.0]), &hw_counter)
                .unwrap_err();
            assert_matches!(err, OperationError::WrongVectorValue { index: 0, .. });
        }

        segment
            .upsert_point(100, 1.into(), only_default_vector(&[0.5, 1.0]), &hw_counter)
            .unwrap();
    }
}

#[test]
fn test_float16_cosine_accepts_large_normalizable_components() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_segment_with_datatype(
        dir.path(),
        2,
        Distance::Cosine,
        VectorStorageDatatype::Float16,
    );
    let hw_counter = HardwareCounterCell::new();

    segment
        .upsert_point(
            100,
            1.into(),
            only_default_vector(&[1.0e6, 2.0e6]),
            &hw_counter,
        )
        .unwrap();
}
