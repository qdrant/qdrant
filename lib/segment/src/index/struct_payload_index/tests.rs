use std::str::FromStr;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use tempfile::Builder;
use uuid::Uuid;

use crate::data_types::vectors::only_default_vector;
use crate::entry::{NonAppendableSegmentEntry, SegmentEntry};
use crate::index::payload_config::{
    FullPayloadIndexType, IndexMutability, PayloadConfig, PayloadIndexType,
};
use crate::json_path::JsonPath;
use crate::segment_constructor::load_segment;
use crate::segment_constructor::simple_segment_constructor::build_simple_segment;
use crate::types::{Distance, Payload, PayloadFieldSchema, PayloadSchemaType};

#[test]
fn test_load_payload_index() {
    let data = r#"
               {
                   "name": "John Doe"
               }"#;

    let dir = Builder::new().prefix("payload_dir").tempdir().unwrap();
    let dim = 2;

    let hw_counter = HardwareCounterCell::new();

    let key = JsonPath::from_str("name").unwrap();

    let full_segment_path = {
        let mut segment = build_simple_segment(dir.path(), dim, Distance::Dot).unwrap();
        segment
            .upsert_point(0, 0.into(), only_default_vector(&[1.0, 1.0]), &hw_counter)
            .unwrap();

        let payload: Payload = serde_json::from_str(data).unwrap();

        segment
            .set_full_payload(0, 0.into(), &payload, &hw_counter)
            .unwrap();

        segment
            .create_field_index(
                0,
                &key,
                Some(&PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword)),
                &HardwareCounterCell::new(),
            )
            .unwrap();

        segment.segment_path.clone()
    };

    let check_index_types = |index_types: &[FullPayloadIndexType]| -> bool {
        index_types.len() == 2
            && index_types[0].index_type == PayloadIndexType::KeywordIndex
            && index_types[0].mutability == IndexMutability::Mutable
            && index_types[1].index_type == PayloadIndexType::NullIndex
            && index_types[1].mutability == IndexMutability::Mutable
    };

    let payload_config_path = full_segment_path.join("payload_index/config.json");
    let mut payload_config = PayloadConfig::load(&payload_config_path).unwrap();

    assert_eq!(payload_config.indices.len(), 1);

    let schema = payload_config.indices.get_mut(&key).unwrap();
    assert!(check_index_types(&schema.types));

    // Clear index types to check loading from an old segment.
    schema.types.clear();
    payload_config.save(&payload_config_path).unwrap();
    drop(payload_config);

    // Load once and drop.
    load_segment(
        &full_segment_path,
        Uuid::nil(),
        None,
        &AtomicBool::new(false),
    )
    .unwrap();

    // Check that index type has been written to disk again.
    // Proves we'll always persist the exact index type if it wasn't known yet at that time
    let payload_config = PayloadConfig::load(&payload_config_path).unwrap();
    assert_eq!(payload_config.indices.len(), 1);

    let schema = payload_config.indices.get(&key).unwrap();
    assert!(check_index_types(&schema.types));
}
