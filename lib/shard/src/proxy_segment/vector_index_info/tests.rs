use common::counter::hardware_counter::HardwareCounterCell;
use segment::data_types::vector_name_config::VectorNameConfig;
use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
use segment::entry::{NonAppendableSegmentEntry, ReadSegmentEntry, VectorIndexInfoProvider};
use segment::index::VectorIndexType;

use crate::fixtures::build_segment_1;
use crate::locked_segment::LockedSegment;
use crate::proxy_segment::UnsyncedProxySegment;

fn dense_config(size: usize) -> VectorNameConfig {
    serde_json::from_value(serde_json::json!({
        "dense": { "size": size, "distance": "Dot" },
    }))
    .unwrap()
}

#[test]
fn proxy_index_metadata_drops_deleted_and_replaced_vector_names() {
    let dir = tempfile::tempdir().unwrap();
    let segment = build_segment_1(dir.path());
    let expected = segment.vector_index_info();
    let mut proxy = UnsyncedProxySegment::new(LockedSegment::from(segment))
        .unwrap()
        .finalize();
    assert_eq!(proxy.vector_index_info(), expected);
    assert_eq!(
        expected[DEFAULT_VECTOR_NAME].index_type,
        VectorIndexType::Plain
    );

    proxy.delete_vector_name(100, DEFAULT_VECTOR_NAME).unwrap();
    assert!(proxy.vector_index_info().is_empty());
    proxy
        .create_vector_name(101, DEFAULT_VECTOR_NAME, &dense_config(8))
        .unwrap();
    assert!(proxy.vector_index_info().is_empty());

    proxy
        .create_vector_name(102, "new", &dense_config(4))
        .unwrap();
    assert!(!proxy.vector_index_info().contains_key("new"));
}

#[test]
fn proxy_index_vector_count_describes_underlying_storage() {
    let dir = tempfile::tempdir().unwrap();
    let segment = build_segment_1(dir.path());
    let expected = segment.vector_index_info();
    let mut proxy = UnsyncedProxySegment::new(LockedSegment::from(segment))
        .unwrap()
        .finalize();
    let points_count = proxy.available_point_count();
    proxy
        .delete_point(100, 1.into(), &HardwareCounterCell::disposable())
        .unwrap();
    assert_eq!(proxy.available_point_count(), points_count - 1);
    assert_eq!(proxy.vector_index_info(), expected);
}
