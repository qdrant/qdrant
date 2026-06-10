use std::str::FromStr;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use ordered_float::OrderedFloat;
use tempfile::Builder;
use uuid::Uuid;

use crate::data_types::index::IntegerIndexParams;
use crate::data_types::vectors::only_default_vector;
use crate::entry::{NonAppendableSegmentEntry, SegmentEntry};
use crate::fixtures::payload_context_fixture::{
    create_id_tracker_fixture, create_payload_storage_fixture,
};
use crate::fixtures::payload_fixtures::INT_KEY;
use crate::index::PayloadIndex;
use crate::index::payload_config::{
    FullPayloadIndexType, IndexMutability, PayloadConfig, PayloadIndexType,
};
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::json_path::JsonPath;
use crate::payload_json;
use crate::payload_storage::PayloadStorageEnum;
use crate::segment_constructor::load_segment;
use crate::segment_constructor::simple_segment_constructor::build_simple_segment;
use crate::types::{
    Condition, Distance, FieldCondition, Filter, Match, MatchValue, Payload, PayloadFieldSchema,
    PayloadSchemaParams, PayloadSchemaType, Range, RangeInterface, ValueVariants,
};

#[test]
fn integer_index_match_and_range_use_or_logic() {
    // See: <https://github.com/qdrant/qdrant/issues/9066>
    let dir = Builder::new().prefix("match_range_or").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    let mut payload_storage = create_payload_storage_fixture(1, 42);
    payload_storage
        .overwrite(0, &payload_json! { "int": 5 }, &hw_counter)
        .unwrap();

    let payload_storage = std::sync::Arc::new(atomic_refcell::AtomicRefCell::new(
        PayloadStorageEnum::from(payload_storage),
    ));
    let id_tracker = std::sync::Arc::new(atomic_refcell::AtomicRefCell::new(
        create_id_tracker_fixture(1),
    ));

    let mut index = StructPayloadIndex::open(
        payload_storage,
        id_tracker,
        std::collections::HashMap::new(),
        dir.path(),
        true,
        true,
    )
    .unwrap();

    let field = JsonPath::from_str(INT_KEY).unwrap();
    let schema = PayloadFieldSchema::FieldParams(PayloadSchemaParams::Integer(
        IntegerIndexParams {
            lookup: Some(true),
            range: Some(true),
            ..Default::default()
        },
    ));

    index.build_index(&field, &schema, &hw_counter).unwrap();

    let filter = Filter::new_must(Condition::Field(FieldCondition {
        key: field,
        r#match: Some(Match::Value(MatchValue {
            value: ValueVariants::Integer(999),
        })),
        range: Some(RangeInterface::Float(Range {
            lt: None,
            gt: None,
            gte: Some(OrderedFloat(0.0)),
            lte: Some(OrderedFloat(10.0)),
        })),
        geo_bounding_box: None,
        geo_radius: None,
        geo_polygon: None,
        values_count: None,
        is_empty: None,
        is_null: None,
    }));

    let results = index
        .with_view(|view| {
            view.query_points(&filter, &hw_counter, &AtomicBool::new(false))
        })
        .unwrap();

    assert!(
        results.contains(&0),
        "point with int=5 must match via range even though match=999 fails"
    );
}

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

#[test]
fn drop_index_if_incompatible_keeps_non_appendable_index_on_on_disk_only_change() {
    use std::collections::HashMap;
    use std::sync::Arc;

    use atomic_refcell::AtomicRefCell;

    use super::StructPayloadIndex;
    use crate::data_types::index::IntegerIndexParams;
    use crate::fixtures::payload_context_fixture::{
        create_id_tracker_fixture, create_payload_storage_fixture,
    };
    use crate::fixtures::payload_fixtures::INT_KEY;
    use crate::index::PayloadIndex;
    use crate::types::PayloadSchemaParams;

    let dir = Builder::new().prefix("payload_dir").tempdir().unwrap();
    let payload_storage = Arc::new(AtomicRefCell::new(
        create_payload_storage_fixture(100, 42).into(),
    ));
    let id_tracker = Arc::new(AtomicRefCell::new(create_id_tracker_fixture(100)));

    // Non-appendable: `on_disk` is meaningful, so an on_disk-only change is
    // reused (reloaded in `build_index`), not dropped.
    let mut index = StructPayloadIndex::open(
        payload_storage,
        id_tracker,
        HashMap::new(),
        dir.path(),
        false,
        true,
    )
    .unwrap();

    let field = JsonPath::from_str(INT_KEY).unwrap();
    index
        .set_indexed(
            &field,
            PayloadSchemaType::Integer,
            &HardwareCounterCell::new(),
        )
        .unwrap();

    // Same integer index, but with on_disk flipped to true.
    let on_disk_schema =
        PayloadFieldSchema::FieldParams(PayloadSchemaParams::Integer(IntegerIndexParams {
            on_disk: Some(true),
            ..Default::default()
        }));

    let dropped = index
        .drop_index_if_incompatible(&field, &on_disk_schema)
        .unwrap();

    assert!(
        !dropped,
        "a non-appendable on_disk-only change must be reused, not dropped",
    );
    assert!(
        index.field_indexes.contains_key(&field),
        "the index must remain present after an on_disk-only change",
    );
}

#[test]
fn drop_index_if_incompatible_drops_appendable_index_on_on_disk_only_change() {
    use crate::data_types::index::IntegerIndexParams;
    use crate::fixtures::payload_context_fixture::create_struct_payload_index;
    use crate::fixtures::payload_fixtures::INT_KEY;
    use crate::index::PayloadIndex;
    use crate::types::PayloadSchemaParams;

    let dir = Builder::new().prefix("payload_dir").tempdir().unwrap();
    // Appendable Gridstore: `on_disk` has no storage-layer effect, so an
    // on_disk-only change falls back to the drop-and-rebuild path (optimizing
    // the appendable case is handled separately).
    let mut index = create_struct_payload_index(dir.path(), 100, 42);
    let field = JsonPath::from_str(INT_KEY).unwrap();

    let on_disk_schema =
        PayloadFieldSchema::FieldParams(PayloadSchemaParams::Integer(IntegerIndexParams {
            on_disk: Some(true),
            ..Default::default()
        }));

    let dropped = index
        .drop_index_if_incompatible(&field, &on_disk_schema)
        .unwrap();

    assert!(
        dropped,
        "an appendable on_disk-only change drops (rebuild path) on this branch",
    );
    assert!(
        !index.field_indexes.contains_key(&field),
        "the appendable index is removed by the drop",
    );
}

#[test]
fn build_index_reloads_in_new_mode_on_on_disk_change() {
    use std::collections::HashMap;
    use std::sync::Arc;

    use atomic_refcell::AtomicRefCell;

    use super::StructPayloadIndex;
    use crate::data_types::index::IntegerIndexParams;
    use crate::fixtures::payload_context_fixture::{
        create_id_tracker_fixture, create_payload_storage_fixture,
    };
    use crate::fixtures::payload_fixtures::INT_KEY;
    use crate::index::field_index::PayloadFieldIndexRead;
    use crate::index::{BuildIndexResult, PayloadIndex};
    use crate::types::PayloadSchemaParams;

    let dir = Builder::new().prefix("payload_dir").tempdir().unwrap();
    let payload_storage = Arc::new(AtomicRefCell::new(
        create_payload_storage_fixture(100, 42).into(),
    ));
    let id_tracker = Arc::new(AtomicRefCell::new(create_id_tracker_fixture(100)));

    // Non-appendable: indexes are mmap-backed, so `on_disk` is meaningful.
    let mut index = StructPayloadIndex::open(
        payload_storage,
        id_tracker,
        HashMap::new(),
        dir.path(),
        false,
        true,
    )
    .unwrap();

    let field = JsonPath::from_str(INT_KEY).unwrap();
    let hw_counter = HardwareCounterCell::new();
    index
        .set_indexed(&field, PayloadSchemaType::Integer, &hw_counter)
        .unwrap();

    // Indexed-point count of the freshly built (on_disk = false) index.
    let before_count: usize = index
        .field_indexes
        .get(&field)
        .unwrap()
        .iter()
        .map(|i| i.count_indexed_points())
        .sum();
    assert!(before_count > 0, "fixture should index some integer points");

    // Same integer index, but with on_disk flipped to true.
    let on_disk_schema =
        PayloadFieldSchema::FieldParams(PayloadSchemaParams::Integer(IntegerIndexParams {
            on_disk: Some(true),
            ..Default::default()
        }));

    match index
        .build_index(&field, &on_disk_schema, &hw_counter)
        .unwrap()
    {
        BuildIndexResult::Built(indexes) => {
            assert!(
                indexes.iter().any(|i| i.is_on_disk()),
                "an on_disk-only change must reload the existing index in on-disk mode",
            );
            let after_count: usize = indexes.iter().map(|i| i.count_indexed_points()).sum();
            assert_eq!(
                after_count, before_count,
                "reloading in the new mode must preserve the indexed points",
            );
        }
        BuildIndexResult::AlreadyBuilt => {
            panic!("expected Built (reloaded from files), got AlreadyBuilt")
        }
        BuildIndexResult::IncompatibleSchema => {
            panic!("expected Built (reloaded from files), got IncompatibleSchema")
        }
    }
}
