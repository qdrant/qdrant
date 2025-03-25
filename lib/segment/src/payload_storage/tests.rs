use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;

use super::PayloadStorage;
use super::mmap_payload_storage::MmapPayloadStorage;
use super::on_disk_payload_storage::OnDiskPayloadStorage;
use super::simple_payload_storage::SimplePayloadStorage;
use crate::common::rocksdb_wrapper::open_db;
use crate::payload_json;

fn test_trait_impl<S: PayloadStorage>(open: impl Fn(&Path) -> S) {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = open(dir.path());

    assert_eq!(storage.get_storage_size_bytes().unwrap(), 0);

    let payload = payload_json! {
        "a": "some text",
    };

    let hw_counter = HardwareCounterCell::new();

    // set
    storage.set(0, &payload, &hw_counter).unwrap();
    assert_eq!(storage.get(0, &hw_counter).unwrap(), payload);

    // set on existing
    let payload_to_merge = payload_json! {
        "zzz": "some other text",
    };

    storage.set(0, &payload_to_merge, &hw_counter).unwrap();

    let stored = storage.get(0, &hw_counter).unwrap();
    assert_eq!(
        stored,
        payload_json! {
            "a": "some text",
            "zzz": "some other text",
        },
    );

    // set_by_key
    let nested_payload = payload_json! {
        "layer2": true,
    };
    storage
        .set_by_key(
            0,
            &nested_payload,
            &"layer1".try_into().unwrap(),
            &hw_counter,
        )
        .unwrap();
    let stored = storage.get(0, &hw_counter).unwrap();

    assert_eq!(
        stored,
        payload_json! {
            "a": "some text",
            "zzz": "some other text",
            "layer1": {
                "layer2": true,
            }
        },
    );

    // delete key
    storage
        .delete(0, &"layer1".try_into().unwrap(), &hw_counter)
        .unwrap();
    let stored = storage.get(0, &hw_counter).unwrap();
    assert_eq!(
        stored,
        payload_json! {
            "a": "some text",
            "zzz": "some other text",
        },
    );

    // overwrite
    let new_payload = payload_json! {
        "new": "new text",
        "other_new": "other new text",
    };
    storage.overwrite(0, &new_payload, &hw_counter).unwrap();
    let stored = storage.get(0, &hw_counter).unwrap();
    assert_eq!(stored, new_payload);

    storage.clear(0, &hw_counter).unwrap();
    assert_eq!(storage.get(0, &hw_counter).unwrap(), payload_json! {});

    for i in 1..10 {
        storage.set(i, &payload, &hw_counter).unwrap();
    }

    let assert_payloads = |storage: &S| {
        storage
            .iter(
                |key, value| {
                    if key == 0 {
                        assert_eq!(value, &payload_json! {});
                        return Ok(true);
                    }
                    assert_eq!(value, &payload);
                    Ok(true)
                },
                &hw_counter,
            )
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
