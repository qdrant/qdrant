//! End-to-end check of the `append_only_storages` feature flag: a segment
//! built with it holds Logstore-mode storages, and the ordinary write paths —
//! upserts, payload updates, multi-step same-operation writes, deletes, index
//! builds — run against them.
//!
//! A standalone test binary, because feature flags are process-global: this
//! process runs with `serverless_compatible` on from the start, the way a
//! serverless deployment would.

use std::path::Path;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::{FeatureFlags, init_feature_flags};
use common::types::DeferredBehavior;
use segment::data_types::vectors::only_default_vector;
use segment::entry::entry_point::{
    NonAppendableSegmentEntry as _, ReadSegmentEntry as _, SegmentEntry as _,
    StorageSegmentEntry as _,
};
use segment::payload_json;
use segment::segment_constructor::load_segment;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{Distance, PayloadFieldSchema, PayloadSchemaType};
use tempfile::Builder;
use uuid::Uuid;

const DIM: usize = 4;

/// The persisted mode of the Blobstore at `dir`, from its own config file.
fn storage_mode(dir: &Path) -> String {
    let config: serde_json::Value =
        serde_json::from_str(&fs_err::read_to_string(dir.join("config.json")).unwrap()).unwrap();
    config["mode"].as_str().unwrap_or("mutable").to_string()
}

#[test]
fn append_only_storages_serve_the_ordinary_write_paths() {
    // The field is private, set through the same route a config file takes.
    let flags: FeatureFlags = serde_json::from_str(r#"{ "serverless_compatible": true }"#).unwrap();
    init_feature_flags(flags);
    assert!(common::flags::feature_flags().append_only_storages);

    let dir = Builder::new()
        .prefix("append_only_storages")
        .tempdir()
        .unwrap();
    let hw_counter = HardwareCounterCell::new();

    // The segment lives in a uuid subdirectory of `dir`.
    let mut segment = build_simple_segment(dir.path(), DIM, Distance::Dot).unwrap();
    let segment_path = segment.data_path();
    assert!(segment.append_only_mutations, "forced by the storages flag");

    // The payload storage was created in the append-only mode.
    assert_eq!(
        storage_mode(&segment_path.join("payload_storage")),
        "append_only"
    );

    // Inserts, and an update of an existing point.
    for id in 1..=5u64 {
        let vector: Vec<f32> = vec![id as f32; DIM];
        segment
            .upsert_point(1, id.into(), only_default_vector(&vector), &hw_counter)
            .unwrap();
    }
    segment
        .set_full_payload(
            2,
            3.into(),
            &payload_json! { "kind": "updated" },
            &hw_counter,
        )
        .unwrap();

    // A multi-step write within one operation, which may not reuse the slot.
    segment
        .upsert_point(3, 6.into(), only_default_vector(&[6.0; DIM]), &hw_counter)
        .unwrap();
    segment
        .set_full_payload(
            3,
            6.into(),
            &payload_json! { "kind": "multi-step" },
            &hw_counter,
        )
        .unwrap();

    // A delete is a tombstone.
    segment.delete_point(4, 5.into(), &hw_counter).unwrap();

    // The index storage comes out append-only too.
    let key = "kind".parse().unwrap();
    segment
        .create_field_index(
            5,
            &key,
            Some(&PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword)),
            &hw_counter,
        )
        .unwrap();
    // The index directory name carries a hash prefix, locate it by suffix.
    let index_dir = fs_err::read_dir(segment_path.join("payload_index"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().ends_with("-kind-map"))
        })
        .expect("keyword index directory exists");
    assert_eq!(storage_mode(&index_dir), "append_only");

    segment.flush(true).unwrap();
    drop(segment);

    // Everything survives a reload through the ordinary loader.
    let segment = load_segment(&segment_path, Uuid::nil(), None, &AtomicBool::new(false)).unwrap();
    assert!(segment.append_only_mutations);

    assert_eq!(segment.available_point_count(), 5);
    let payload = segment.payload(3.into(), &hw_counter).unwrap();
    assert_eq!(payload, payload_json! { "kind": "updated" });
    let payload = segment.payload(6.into(), &hw_counter).unwrap();
    assert_eq!(payload, payload_json! { "kind": "multi-step" });
    assert!(!segment.has_point(5.into(), DeferredBehavior::WithDeferred));
}
