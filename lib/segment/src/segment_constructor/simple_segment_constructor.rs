use std::collections::HashMap;
use std::path::Path;

use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::DEFAULT_VECTOR_NAME;
use crate::segment::Segment;
use crate::segment_constructor::build_segment;
use crate::types::{
    Distance, Indexes, PayloadStorageType, SegmentConfig, VectorDataConfig, VectorName,
    VectorStorageType,
};

pub const VECTOR1_NAME: &VectorName = "vector1";
pub const VECTOR2_NAME: &VectorName = "vector2";

/// Build new segment with plain index in given directory
///
/// # Arguments
///
/// * `path` - path to collection\`s segment directory
///
pub fn build_simple_segment(
    path: &Path,
    dim: usize,
    distance: Distance,
) -> OperationResult<Segment> {
    build_segment(
        path,
        &SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: dim,
                    distance,
                    storage_type: VectorStorageType::Memory,
                    index: Indexes::Plain {},
                    quantization_config: None,
                    multivector_config: None,
                    datatype: None,
                },
            )]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        },
        true,
    )
}

pub fn build_simple_segment_with_payload_storage(
    path: &Path,
    dim: usize,
    distance: Distance,
    payload_storage_type: PayloadStorageType,
) -> OperationResult<Segment> {
    build_segment(
        path,
        &SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: dim,
                    distance,
                    storage_type: VectorStorageType::Memory,
                    index: Indexes::Plain {},
                    quantization_config: None,
                    multivector_config: None,
                    datatype: None,
                },
            )]),
            sparse_vector_data: Default::default(),
            payload_storage_type,
        },
        true,
    )
}

pub fn build_multivec_segment(
    path: &Path,
    dim1: usize,
    dim2: usize,
    distance: Distance,
) -> OperationResult<Segment> {
    let mut vectors_config = HashMap::new();
    vectors_config.insert(
        VECTOR1_NAME.into(),
        VectorDataConfig {
            size: dim1,
            distance,
            storage_type: VectorStorageType::Memory,
            index: Indexes::Plain {},
            quantization_config: None,
            multivector_config: None,
            datatype: None,
        },
    );
    vectors_config.insert(
        VECTOR2_NAME.into(),
        VectorDataConfig {
            size: dim2,
            distance,
            storage_type: VectorStorageType::Memory,
            index: Indexes::Plain {},
            quantization_config: None,
            multivector_config: None,
            datatype: None,
        },
    );

    build_segment(
        path,
        &SegmentConfig {
            vector_data: vectors_config,
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        },
        true,
    )
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use tempfile::Builder;

    use super::*;
    use crate::common::operation_error::OperationError;
    use crate::data_types::vectors::only_default_vector;
    use crate::entry::entry_point::SegmentEntry;
    use crate::payload_json;

    #[test]
    fn test_create_simple_segment() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let segment = build_simple_segment(dir.path(), 100, Distance::Dot).unwrap();
        eprintln!(" = {:?}", segment.version());
    }

    #[test]
    fn test_add_and_search() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let mut segment = build_simple_segment(dir.path(), 4, Distance::Dot).unwrap();

        let wrong_vec = vec![1.0, 1.0, 1.0];

        let vec1 = vec![1.0, 0.0, 1.0, 1.0];
        let vec2 = vec![1.0, 0.0, 1.0, 0.0];
        let vec3 = vec![1.0, 1.0, 1.0, 1.0];
        let vec4 = vec![1.0, 1.0, 0.0, 1.0];
        let vec5 = vec![1.0, 0.0, 0.0, 0.0];

        let hw_counter = HardwareCounterCell::new();

        match segment.upsert_point(1, 120.into(), only_default_vector(&wrong_vec), &hw_counter) {
            Err(OperationError::WrongVectorDimension { .. }) => (),
            Err(_) => panic!("Wrong error"),
            Ok(_) => panic!("Operation with wrong vector should fail"),
        };

        segment
            .upsert_point(2, 1.into(), only_default_vector(&vec1), &hw_counter)
            .unwrap();
        segment
            .upsert_point(2, 2.into(), only_default_vector(&vec2), &hw_counter)
            .unwrap();
        segment
            .upsert_point(2, 3.into(), only_default_vector(&vec3), &hw_counter)
            .unwrap();
        segment
            .upsert_point(2, 4.into(), only_default_vector(&vec4), &hw_counter)
            .unwrap();
        segment
            .upsert_point(2, 5.into(), only_default_vector(&vec5), &hw_counter)
            .unwrap();

        segment
            .set_payload(
                3,
                1.into(),
                &payload_json! {"color": vec!["red".to_owned(), "green".to_owned()]},
                &None,
                &hw_counter,
            )
            .unwrap();

        segment
            .set_payload(
                3,
                2.into(),
                &payload_json! {"color": vec!["red".to_owned(), "blue".to_owned()]},
                &None,
                &hw_counter,
            )
            .unwrap();

        segment
            .set_payload(
                3,
                3.into(),
                &payload_json! {"color": vec!["red".to_owned(), "yellow".to_owned()]},
                &None,
                &hw_counter,
            )
            .unwrap();

        segment
            .set_payload(
                3,
                4.into(),
                &payload_json! {"color": vec!["red".to_owned(), "green".to_owned()]},
                &None,
                &hw_counter,
            )
            .unwrap();

        // Replace vectors
        segment
            .upsert_point(4, 1.into(), only_default_vector(&vec1), &hw_counter)
            .unwrap();
        segment
            .upsert_point(5, 2.into(), only_default_vector(&vec2), &hw_counter)
            .unwrap();
        segment
            .upsert_point(6, 3.into(), only_default_vector(&vec3), &hw_counter)
            .unwrap();
        segment
            .upsert_point(7, 4.into(), only_default_vector(&vec4), &hw_counter)
            .unwrap();
        segment
            .upsert_point(8, 5.into(), only_default_vector(&vec5), &hw_counter)
            .unwrap();

        assert_eq!(segment.version(), 8);

        let declined = segment
            .upsert_point(3, 5.into(), only_default_vector(&vec5), &hw_counter)
            .unwrap();

        // Should not be processed due to operation number
        assert!(!declined);
    }

    // ToDo: More tests
}
