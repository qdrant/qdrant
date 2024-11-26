use std::path::Path;

use serde_json::json;

use super::mmap_payload_storage::MmapPayloadStorage;
use super::on_disk_payload_storage::OnDiskPayloadStorage;
use super::simple_payload_storage::SimplePayloadStorage;
use super::PayloadStorage;
use crate::common::rocksdb_wrapper::open_db;
use crate::types::Payload;

fn test_trait_impl<S: PayloadStorage>(open: impl Fn(&Path) -> S) {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = open(dir.path());

    assert_eq!(storage.get_storage_size_bytes().unwrap(), 0);

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

    for i in 1..10 {
        storage.set(i, &payload).unwrap();
    }

    let assert_payloads = |storage: &S| {
        storage
            .iter(|key, value| {
                if key == 0 {
                    assert_eq!(value, &json!({}).into());
                    return Ok(true);
                }
                assert_eq!(value, &payload);
                Ok(true)
            })
            .unwrap();
    };

    assert_payloads(&storage);
    eprintln!("storage is correct before drop");

    // flush, drop, and reopen
    storage.flusher()().unwrap();
    drop(storage);
    let storage = open(dir.path());

    // check if the data is still there
    assert_payloads(&storage);
    eprintln!("storage is correct after drop");

    assert!(storage.get_storage_size_bytes().unwrap() > 0);
}

#[test]
fn test_in_memory_storage() {
    test_trait_impl(|path| {
        let db = open_db(path, &[""]).unwrap();
        SimplePayloadStorage::open(db).unwrap()
    });
}

#[test]
fn test_mmap_storage() {
    test_trait_impl(|path| MmapPayloadStorage::open_or_create(path).unwrap());
}

#[test]
fn test_on_disk_storage() {
    test_trait_impl(|path| {
        let db = open_db(path, &[""]).unwrap();
        OnDiskPayloadStorage::open(db).unwrap()
    });
}
