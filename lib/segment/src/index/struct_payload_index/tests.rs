// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

use std::str::FromStr;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use tempfile::Builder;
use uuid::Uuid;

use crate::data_types::vectors::only_default_vector;
use crate::entry::{NonAppendableSegmentEntry, SegmentEntry, StorageSegmentEntry};
use crate::index::PayloadIndexRead;
use crate::index::payload_config::{
    FullPayloadIndexType, IndexMutability, PayloadConfig, PayloadIndexType,
};
use crate::json_path::JsonPath;
use crate::segment_constructor::load_segment;
use crate::segment_constructor::simple_segment_constructor::build_simple_segment;
use crate::types::{
    Condition, Distance, FieldCondition, Filter, Match, Payload, PayloadFieldSchema,
    PayloadSchemaType, ValueVariants,
};

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

fn name_filter(key: &JsonPath, value: &str) -> Filter {
    Filter::new_must(Condition::Field(FieldCondition::new_match(
        key.clone(),
        Match::new_value(ValueVariants::String(value.to_string())),
    )))
}

/// The durable config must never list an index whose data is still buffered in
/// memory. `Segment::build_field_index` flushes the built indexes before
/// `apply_index` saves the config, so a crash right after `create_field_index`
/// reloads a complete index.
///
/// Invariant guard rather than a failing-direction regression test: without the
/// flush, this scenario recovers only by accident — the mutable index files were
/// never created, so loading fails and triggers a rebuild. Index types whose build
/// writes files immediately (mmap/on-disk) load "successfully" incomplete instead;
/// the flush-before-config makes the guarantee structural for all of them.
#[test]
fn create_field_index_persists_index_data_with_config() {
    let dir = Builder::new().prefix("payload_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();
    let key = JsonPath::from_str("name").unwrap();
    let schema = PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword);
    let payload: Payload = serde_json::from_str(r#"{ "name": "John Doe" }"#).unwrap();

    let segment_path = {
        let mut segment = build_simple_segment(dir.path(), 2, Distance::Dot).unwrap();
        segment
            .upsert_point(1, 0.into(), only_default_vector(&[1.0, 1.0]), &hw_counter)
            .unwrap();
        segment
            .set_full_payload(2, 0.into(), &payload, &hw_counter)
            .unwrap();
        segment.flush(true).unwrap();

        // No flush after this op: its own persistence must be self-contained.
        segment
            .create_field_index(4, &key, Some(&schema), &hw_counter)
            .unwrap();

        segment.segment_path.clone()
        // Dropped without a further flush: simulates a crash right after the op.
    };

    // The op committed the index durably: the config lists the field.
    let payload_config =
        PayloadConfig::load(&segment_path.join("payload_index/config.json")).unwrap();
    assert!(
        payload_config.indices.contains_key(&key),
        "create_field_index must durably commit the config",
    );

    let segment = load_segment(&segment_path, Uuid::nil(), None, &AtomicBool::new(false))
        .expect("segment must load after simulated crash");

    // Queried before any replay on purpose: the index data itself must be complete,
    // not repaired by re-applied operations.
    let hits = segment
        .payload_index
        .borrow()
        .with_view(|view| {
            view.query_points(
                &name_filter(&key, "John Doe"),
                &hw_counter,
                &AtomicBool::new(false),
            )
        })
        .unwrap();
    assert_eq!(
        hits,
        vec![0],
        "the index listed by the durable config must cover the durable payload rows",
    );
}

/// Switching a field's index to an incompatible type (keyword → full-text) must
/// leave a complete, durable new index even across a crash. The ordering that makes
/// `wipe_field_dirs` safe here: the incompatible keyword index is dropped (config
/// entry, in-memory index, and files) by `delete_field_index_if_incompatible`
/// *before* the text build starts, so the wipe never touches a live index's files —
/// it only clears leftovers for a field the config no longer lists. The rebuilt
/// index is then flushed before the config commits it.
#[test]
fn switch_incompatible_index_type_survives_crash() {
    let dir = Builder::new().prefix("payload_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();
    let key = JsonPath::from_str("name").unwrap();
    let keyword_schema = PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword);
    let text_schema = PayloadFieldSchema::FieldType(PayloadSchemaType::Text);
    let payload: Payload = serde_json::from_str(r#"{ "name": "John Doe" }"#).unwrap();

    let segment_path = {
        let mut segment = build_simple_segment(dir.path(), 2, Distance::Dot).unwrap();
        segment
            .upsert_point(1, 0.into(), only_default_vector(&[1.0, 1.0]), &hw_counter)
            .unwrap();
        segment
            .set_full_payload(2, 0.into(), &payload, &hw_counter)
            .unwrap();
        segment
            .create_field_index(3, &key, Some(&keyword_schema), &hw_counter)
            .unwrap();
        segment.flush(true).unwrap();

        // Incompatible switch: drops the keyword index, then builds full-text.
        // No flush after this op: its own persistence must be self-contained.
        segment
            .create_field_index(4, &key, Some(&text_schema), &hw_counter)
            .unwrap();

        segment.segment_path.clone()
        // Dropped without a further flush: simulates a crash right after the op.
    };

    // The switch committed durably: the config lists the field with the new schema.
    let payload_config =
        PayloadConfig::load(&segment_path.join("payload_index/config.json")).unwrap();
    assert_eq!(
        payload_config.indices.get(&key).map(|entry| &entry.schema),
        Some(&text_schema),
        "the durable config must list the new index schema",
    );

    let segment = load_segment(&segment_path, Uuid::nil(), None, &AtomicBool::new(false))
        .expect("segment must load after simulated crash");

    // Queried before any replay on purpose: the new index data must be complete.
    let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        key,
        Match::Text("John".into()),
    )));
    let hits = segment
        .payload_index
        .borrow()
        .with_view(|view| view.query_points(&filter, &hw_counter, &AtomicBool::new(false)))
        .unwrap();
    assert_eq!(
        hits,
        vec![0],
        "the full-text index must be complete after the crash",
    );
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
        .map(|i| i.count_indexed_points().unwrap())
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
            let after_count: usize = indexes
                .iter()
                .map(|i| i.count_indexed_points().unwrap())
                .sum();
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
