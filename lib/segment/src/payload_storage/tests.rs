use serde_json::json;

use super::mmap_payload_storage::MmapPayloadStorage;
use super::on_disk_payload_storage::OnDiskPayloadStorage;
use super::simple_payload_storage::SimplePayloadStorage;
use super::PayloadStorage;
use crate::common::rocksdb_wrapper::open_db;
use crate::types::Payload;

fn test_trait_impl<S: PayloadStorage>(mut storage: S) {
    let payload: Payload = json!({
        "a": "some text",
    })
    .into();

    // set
    storage.set(0, &payload).unwrap();
    assert_eq!(storage.get(0).unwrap(), payload);

    // set on existing
    let payload_to_merge = json!({
        "zzz": "some other text",
    })
    .into();

    storage.set(0, &payload_to_merge).unwrap();

    let stored = storage.get(0).unwrap();
    assert_eq!(
        stored,
        json!({
            "a": "some text",
            "zzz": "some other text",
        })
        .into()
    );

    // set_by_key
    let nested_payload = json!({
        "layer2": true,
    })
    .into();
    storage
        .set_by_key(0, &nested_payload, &"layer1".try_into().unwrap())
        .unwrap();
    let stored = storage.get(0).unwrap();
    assert_eq!(
        stored,
        json!({
            "a": "some text",
            "zzz": "some other text",
            "layer1": {
                "layer2": true,
            }
        })
        .into()
    );

    // delete key
    storage.delete(0, &"layer1".try_into().unwrap()).unwrap();
    let stored = storage.get(0).unwrap();
    assert_eq!(
        stored,
        json!({
            "a": "some text",
            "zzz": "some other text",
        })
        .into()
    );

    // overwrite
    let new_payload = json!({
        "new": "new text",
        "other_new": "other new text",
    })
    .into();
    storage.overwrite(0, &new_payload).unwrap();
    let stored = storage.get(0).unwrap();
    assert_eq!(stored, new_payload);

    storage.clear(0).unwrap();
    assert_eq!(storage.get(0).unwrap(), json!({}).into());
}

#[test]
fn test_in_memory_storage() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path(), &[""]).unwrap();
    let storage = SimplePayloadStorage::open(db).unwrap();
    test_trait_impl(storage);
}

#[test]
fn test_mmap_storage() {
    let dir = tempfile::tempdir().unwrap();
    test_trait_impl(MmapPayloadStorage::open_or_create(dir.path()).unwrap());
}

#[test]
fn test_on_disk_storage() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path(), &[""]).unwrap();
    let storage = OnDiskPayloadStorage::open(db).unwrap();

    test_trait_impl(storage);
}
