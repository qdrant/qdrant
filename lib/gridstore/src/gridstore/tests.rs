use std::io::BufReader;
use std::thread;
use std::time::Duration;

use common::counter::hardware_counter::HardwareCounterCell;
use fs_err::File;
use itertools::Itertools;
use rand::distr::Uniform;
use rand::prelude::Distribution;
use rand::seq::SliceRandom;
use rand::{Rng, RngExt};
use rstest::rstest;
use tempfile::Builder;

use super::view::{compress_lz4, decompress_lz4};
use super::*;
use crate::blob::Blob;
use crate::config::{
    Compression, DEFAULT_BLOCK_SIZE_BYTES, DEFAULT_PAGE_SIZE_BYTES, DEFAULT_REGION_SIZE_BLOCKS,
};
use crate::fixtures::{HM_FIELDS, Payload, empty_storage, empty_storage_sized, random_payload};

#[test]
fn test_empty_payload_storage() {
    let hw_counter = HardwareCounterCell::new();
    let (_dir, storage) = empty_storage();
    let payload = storage.get_value::<false>(0, &hw_counter).unwrap();
    assert!(payload.is_none());
    assert_eq!(storage.get_storage_size_bytes(), 0);
}

#[test]
fn test_put_single_empty_value() {
    let (_dir, mut storage) = empty_storage();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter = hw_counter.ref_payload_io_write_counter();

    // TODO: should we actually use the pages for empty values?
    let payload = Payload::default();
    storage.put_value(0, &payload, hw_counter).unwrap();
    assert_eq!(storage.pages.read().len(), 1);
    assert_eq!(storage.tracker.read().mapping_len(), 1);

    let hw_counter = HardwareCounterCell::new();
    let stored_payload = storage.get_value::<false>(0, &hw_counter).unwrap();
    assert!(stored_payload.is_some());
    assert_eq!(stored_payload.unwrap(), Payload::default());
    assert_eq!(storage.get_storage_size_bytes(), DEFAULT_BLOCK_SIZE_BYTES);
}

#[test]
fn test_put_single_payload() {
    let (_dir, mut storage) = empty_storage();

    let mut payload = Payload::default();
    payload.0.insert(
        "key".to_string(),
        serde_json::Value::String("value".to_string()),
    );

    let hw_counter = HardwareCounterCell::new();
    let hw_counter = hw_counter.ref_payload_io_write_counter();

    storage.put_value(0, &payload, hw_counter).unwrap();
    assert_eq!(storage.pages.read().len(), 1);
    assert_eq!(storage.tracker.read().mapping_len(), 1);

    let page_mapping = storage.get_pointer(0).unwrap();
    assert_eq!(page_mapping.page_id, 0); // first page
    assert_eq!(page_mapping.block_offset, 0); // first cell

    let hw_counter = HardwareCounterCell::new();
    let stored_payload = storage.get_value::<false>(0, &hw_counter).unwrap();
    assert!(stored_payload.is_some());
    assert_eq!(stored_payload.unwrap(), payload);
    assert_eq!(storage.get_storage_size_bytes(), DEFAULT_BLOCK_SIZE_BYTES);
}

#[test]
fn test_storage_files() {
    let (dir, mut storage) = empty_storage();

    let mut payload = Payload::default();
    payload.0.insert(
        "key".to_string(),
        serde_json::Value::String("value".to_string()),
    );

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    storage.put_value(0, &payload, hw_counter_ref).unwrap();
    assert_eq!(storage.pages.read().len(), 1);
    assert_eq!(storage.tracker.read().mapping_len(), 1);
    let files = storage.files();
    let actual_files: Vec<_> = fs::read_dir(dir.path()).unwrap().try_collect().unwrap();
    assert_eq!(
        files.len(),
        actual_files.len(),
        "The directory has {} files, but we are reporting {}\nreported: {files:?}\n actual: {actual_files:?}",
        actual_files.len(),
        files.len()
    );
    assert_eq!(files.len(), 5, "Expected 5 files, got {files:?}");
    assert_eq!(files[0].file_name().unwrap(), "tracker.dat");
    assert_eq!(files[1].file_name().unwrap(), "page_0.dat");
    assert_eq!(files[2].file_name().unwrap(), "config.json");
    assert_eq!(files[3].file_name().unwrap(), "bitmask.dat");
    assert_eq!(files[4].file_name().unwrap(), "gaps.dat");
}

#[rstest]
#[case(100000, 2)]
#[case(100, 2000)]
fn test_put_payload(#[case] num_payloads: u32, #[case] payload_size_factor: usize) {
    let (_dir, mut storage) = empty_storage();

    let rng = &mut rand::make_rng::<rand::rngs::SmallRng>();

    let mut payloads = (0..num_payloads)
        .map(|point_offset| (point_offset, random_payload(rng, payload_size_factor)))
        .collect::<Vec<_>>();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    for (point_offset, payload) in payloads.iter() {
        storage
            .put_value(*point_offset, payload, hw_counter_ref)
            .unwrap();

        let stored_payload = storage
            .get_value::<false>(*point_offset, &hw_counter)
            .unwrap();
        assert!(stored_payload.is_some());
        assert_eq!(&stored_payload.unwrap(), payload);
    }

    // read randomly
    payloads.shuffle(rng);
    for (point_offset, payload) in payloads.iter() {
        let stored_payload = storage
            .get_value::<false>(*point_offset, &hw_counter)
            .unwrap();
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload.clone());
    }
}

#[test]
fn test_delete_single_payload() {
    let (_dir, mut storage) = empty_storage();

    let mut payload = Payload::default();
    payload.0.insert(
        "key".to_string(),
        serde_json::Value::String("value".to_string()),
    );

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    storage.put_value(0, &payload, hw_counter_ref).unwrap();
    assert_eq!(storage.pages.read().len(), 1);

    let page_mapping = storage.get_pointer(0).unwrap();
    assert_eq!(page_mapping.page_id, 0); // first page
    assert_eq!(page_mapping.block_offset, 0); // first cell

    let stored_payload = storage.get_value::<false>(0, &hw_counter).unwrap();
    assert_eq!(stored_payload, Some(payload));
    assert_eq!(storage.get_storage_size_bytes(), DEFAULT_BLOCK_SIZE_BYTES);

    // delete payload
    let deleted = storage.delete_value(0).unwrap();
    assert_eq!(deleted, stored_payload);
    assert_eq!(storage.pages.read().len(), 1);

    // get payload again
    let stored_payload = storage.get_value::<false>(0, &hw_counter).unwrap();
    assert!(stored_payload.is_none());
    storage.flusher()().unwrap();
    assert_eq!(storage.get_storage_size_bytes(), 0);
}

#[test]
fn test_update_single_payload() {
    let (_dir, mut storage) = empty_storage();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    let put_payload =
        |storage: &mut Gridstore<Payload>, payload_value: &str, expected_block_offset: u32| {
            let mut payload = Payload::default();
            payload.0.insert(
                "key".to_string(),
                serde_json::Value::String(payload_value.to_string()),
            );

            storage.put_value(0, &payload, hw_counter_ref).unwrap();
            assert_eq!(storage.pages.read().len(), 1);
            assert_eq!(storage.tracker.read().mapping_len(), 1);

            let page_mapping = storage.get_pointer(0).unwrap();
            assert_eq!(page_mapping.page_id, 0); // first page
            assert_eq!(page_mapping.block_offset, expected_block_offset);

            let hw_counter = HardwareCounterCell::new();
            let stored_payload = storage.get_value::<false>(0, &hw_counter).unwrap();
            assert!(stored_payload.is_some());
            assert_eq!(stored_payload.unwrap(), payload);
        };

    put_payload(&mut storage, "value", 0);

    put_payload(&mut storage, "updated", 1);

    put_payload(&mut storage, "updated again", 2);

    storage.flusher()().unwrap();

    // First block offset should be available again, so we can reuse it
    put_payload(&mut storage, "updated after flush", 0);
}

#[test]
fn test_write_across_pages() {
    let page_size = DEFAULT_BLOCK_SIZE_BYTES * DEFAULT_REGION_SIZE_BLOCKS;
    let (_dir, mut storage) = empty_storage_sized(page_size, Compression::None);

    storage.create_new_page().unwrap();

    let value_len = 1000;

    // Value should span 8 blocks
    let value = (0..)
        .map(|i| (i % 24) as u8)
        .take(value_len)
        .collect::<Vec<_>>();

    // Let's write it near the end
    let block_offset = DEFAULT_REGION_SIZE_BLOCKS - 10;
    storage
        .write_into_pages(&value, 0, block_offset as u32)
        .unwrap();

    let read_value = storage
        .with_view(|view| view.read_from_pages::<false>(0, block_offset as u32, value_len as u32))
        .unwrap();
    assert_eq!(value, read_value);
}

enum Operation {
    // Insert point with payload
    Put(PointOffset, Payload),
    // Delete point by offset
    Delete(PointOffset),
    // Get point by offset
    Get(PointOffset),
    // Flush after delay
    FlushDelay(Duration),
    // Clear storage
    Clear,
    // Iter up to limit
    Iter(PointOffset),
}

impl Operation {
    fn random(rng: &mut impl Rng, max_point_offset: u32) -> Self {
        let workload = rand::distr::weighted::WeightedIndex::new([
            max_point_offset,         // put
            max_point_offset / 100,   // delete
            max_point_offset,         // get
            max_point_offset / 500,   // flush
            max_point_offset / 5_000, // clear
            max_point_offset / 5_000, // iter
        ])
        .unwrap();

        let operation = workload.sample(rng);
        match operation {
            0 => {
                let size_factor = rng.random_range(1..10);
                let payload = random_payload(rng, size_factor);
                let point_offset = rng.random_range(0..=max_point_offset);
                Operation::Put(point_offset, payload)
            }
            1 => {
                let point_offset = rng.random_range(0..=max_point_offset);
                Operation::Delete(point_offset)
            }
            2 => {
                let point_offset = rng.random_range(0..=max_point_offset);
                Operation::Get(point_offset)
            }
            3 => {
                let delay_ms = rng.random_range(0..=500);
                let delay = Duration::from_millis(delay_ms);
                Operation::FlushDelay(delay)
            }
            4 => Operation::Clear,
            5 => {
                let limit = rng.random_range(0..=10);
                Operation::Iter(limit)
            }
            op => panic!("{op} out of range"),
        }
    }
}

#[rstest]
fn test_behave_like_hashmap(
    #[values(1_048_576, 2_097_152, DEFAULT_PAGE_SIZE_BYTES)] page_size: usize,
    #[values(Compression::None, Compression::LZ4)] compression: Compression,
) {
    use ahash::AHashMap;

    let operation_count = 100_000;
    let max_point_offset = 10_000u32;

    let _ = env_logger::builder().is_test(true).try_init();

    let (dir, mut storage) = empty_storage_sized(page_size, compression);

    let rng = &mut rand::make_rng::<rand::rngs::SmallRng>();

    let mut model_hashmap = AHashMap::with_capacity(max_point_offset as usize);

    let operations = (0..operation_count).map(|_| Operation::random(rng, max_point_offset));

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

    // ensure no concurrent flushing & flusher instances
    let has_flusher_lock = Arc::new(parking_lot::Mutex::new(false));

    let mut flush_thread_handles = Vec::new();

    // apply operations to storage and model_hashmap
    for (i, operation) in operations.enumerate() {
        match operation {
            Operation::Clear => {
                log::debug!("op:{i} CLEAR");
                storage.clear().unwrap();
                assert_eq!(storage.max_point_offset(), 0, "storage should be empty");
                model_hashmap.clear();
            }
            Operation::Iter(limit) => {
                log::debug!("op:{i} ITER limit:{limit}");
                storage
                    .iter::<_, GridstoreError>(
                        |point_offset, payload| {
                            if point_offset >= limit {
                                return Ok(false); // shortcut iteration
                            }
                            assert_eq!(
                                model_hashmap.get(&point_offset), Some(&payload),
                                "storage and model are different when using `iter` for offset:{point_offset}"
                            );
                            Ok(true) // no shortcutting
                        },
                        hw_counter_ref,
                    )
                    .unwrap();
            }
            Operation::Put(point_offset, payload) => {
                log::debug!("op:{i} PUT offset:{point_offset}");
                let old1 = storage
                    .put_value(point_offset, &payload, hw_counter_ref)
                    .unwrap();
                let old2 = model_hashmap.insert(point_offset, payload);
                assert_eq!(
                    old1,
                    old2.is_some(),
                    "put failed for point_offset: {point_offset} with {old1:?} vs {old2:?}",
                );
            }
            Operation::Delete(point_offset) => {
                log::debug!("op:{i} DELETE offset:{point_offset}");
                let old1 = storage.delete_value(point_offset).unwrap();
                let old2 = model_hashmap.remove(&point_offset);
                assert_eq!(
                    old1, old2,
                    "same deletion failed for point_offset: {point_offset} with {old1:?} vs {old2:?}",
                );
            }
            Operation::Get(point_offset) => {
                log::debug!("op:{i} GET offset:{point_offset}");
                let v1_seq = storage
                    .get_value::<true>(point_offset, &hw_counter)
                    .unwrap();
                let v1_rand = storage
                    .get_value::<false>(point_offset, &hw_counter)
                    .unwrap();
                let v2 = model_hashmap.get(&point_offset).cloned();
                assert_eq!(
                    v1_seq, v2,
                    "get sequential failed for point_offset: {point_offset} with {v1_seq:?} vs {v2:?}",
                );
                assert_eq!(
                    v1_rand, v2,
                    "get_rand sequential failed for point_offset: {point_offset} with {v1_rand:?} vs {v2:?}",
                );
            }
            Operation::FlushDelay(delay) => {
                let mut flush_lock_guard = has_flusher_lock.lock();
                if *flush_lock_guard {
                    log::debug!("op:{i} Skip flushing because a Flusher has already been created");
                } else {
                    log::debug!("op:{i} Scheduling flush in {delay:?}");
                    let flusher = storage.flusher();
                    *flush_lock_guard = true;
                    drop(flush_lock_guard);
                    let flush_lock = has_flusher_lock.clone();
                    let handle = std::thread::Builder::new()
                        .name("background_flush".to_string())
                        .spawn(move || {
                            thread::sleep(delay); // keep flusher alive while other operation are applied
                            let mut flush_lock_guard = flush_lock.lock();
                            assert!(*flush_lock_guard, "there must be a flusher marked as alive");
                            log::debug!("op:{i} STARTING FLUSH after waiting {delay:?}");
                            match flusher() {
                                Ok(_) => log::debug!("op:{i} FLUSH DONE"),
                                Err(err) => log::error!("op:{i} FLUSH failed {err:?}"),
                            }
                            *flush_lock_guard = false; // no flusher alive
                            drop(flush_lock_guard);
                        })
                        .unwrap();
                    flush_thread_handles.push(handle);
                }
            }
        }
    }

    log::debug!("All operations successfully applied - now checking consistency...");

    // wait for all background flushes to complete
    for handle in flush_thread_handles {
        handle.join().expect("flush thread should not panic");
    }

    // asset same length
    assert_eq!(
        storage.tracker.read().mapping_len(),
        model_hashmap.len(),
        "different number of points"
    );

    // validate storage and model_hashmap are the same
    for point_offset in 0..=max_point_offset {
        let stored_payload = storage
            .get_value::<false>(point_offset, &hw_counter)
            .unwrap();
        let model_payload = model_hashmap.get(&point_offset);
        assert_eq!(
            stored_payload.as_ref(),
            model_payload,
            "storage and model differ for offset:{point_offset} {stored_payload:?} vs {model_payload:?}"
        );
    }

    // flush data
    storage.flusher()().unwrap();

    let before_size = storage.get_storage_size_bytes();
    // drop storage
    drop(storage);

    // reopen storage
    let storage = Gridstore::<Payload>::open(dir.path().to_path_buf()).unwrap();
    // assert same size
    assert_eq!(storage.get_storage_size_bytes(), before_size);
    // assert same length
    assert_eq!(storage.tracker.read().mapping_len(), model_hashmap.len());

    // validate storage and model_hashmap are the same
    for point_offset in 0..=max_point_offset {
        let stored_payload = storage
            .get_value::<false>(point_offset, &hw_counter)
            .unwrap();
        let model_payload = model_hashmap.get(&point_offset);
        assert_eq!(
            stored_payload.as_ref(),
            model_payload,
            "failed for point_offset: {point_offset}",
        );
    }

    // wipe storage
    storage.wipe().unwrap();

    // assert base folder is gone
    assert!(
        !dir.path().exists(),
        "base folder should be deleted by wipe"
    );
}

#[test]
fn test_handle_huge_payload() {
    let (_dir, mut storage) = empty_storage();
    assert_eq!(storage.pages.read().len(), 1);

    let mut payload = Payload::default();

    let huge_payload_size = 1024 * 1024 * 50; // 50MB

    let distr = Uniform::new('a', 'z').unwrap();
    let rng = rand::make_rng::<rand::rngs::SmallRng>();

    let huge_value =
        serde_json::Value::String(distr.sample_iter(rng).take(huge_payload_size).collect());
    payload.0.insert("huge".to_string(), huge_value);

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    storage.put_value(0, &payload, hw_counter_ref).unwrap();
    assert_eq!(storage.pages.read().len(), 2);

    let page_mapping = storage.get_pointer(0).unwrap();
    assert_eq!(page_mapping.page_id, 0); // first page
    assert_eq!(page_mapping.block_offset, 0); // first cell

    let stored_payload = storage.get_value::<false>(0, &hw_counter).unwrap();
    assert!(stored_payload.is_some());
    assert_eq!(stored_payload.unwrap(), payload);

    {
        // the fitting page should be 64MB, so we should still have about 14MB of free space
        let free_blocks = storage.bitmask.read().free_blocks_for_page(1);
        let min_expected = 1024 * 1024 * 13 / DEFAULT_BLOCK_SIZE_BYTES;
        let max_expected = 1024 * 1024 * 15 / DEFAULT_BLOCK_SIZE_BYTES;
        assert!((min_expected..max_expected).contains(&free_blocks));
    }

    {
        // delete payload
        let deleted = storage.delete_value(0).unwrap();
        assert!(deleted.is_some());
        assert_eq!(storage.pages.read().len(), 2);

        assert!(
            storage
                .get_value::<false>(0, &hw_counter)
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn test_storage_persistence_basic() {
    let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
    let path = dir.path().to_path_buf();

    let mut payload = Payload::default();
    payload.0.insert(
        "key".to_string(),
        serde_json::Value::String("value".to_string()),
    );

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    {
        let mut storage = Gridstore::new(path.clone(), Default::default()).unwrap();
        storage.put_value(0, &payload, hw_counter_ref).unwrap();
        assert_eq!(storage.pages.read().len(), 1);

        let page_mapping = storage.get_pointer(0).unwrap();
        assert_eq!(page_mapping.page_id, 0); // first page
        assert_eq!(page_mapping.block_offset, 0); // first cell

        let stored_payload = storage.get_value::<false>(0, &hw_counter).unwrap();
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);

        // flush storage before dropping
        storage.flusher()().unwrap();
    }

    // reopen storage
    let storage = Gridstore::<Payload>::open(path).unwrap();
    assert_eq!(storage.pages.read().len(), 1);

    let stored_payload = storage.get_value::<false>(0, &hw_counter).unwrap();
    assert!(stored_payload.is_some());
    assert_eq!(stored_payload.unwrap(), payload);
}

#[test]
#[ignore = "this test is too slow for ci, and has similar coverage to the hashmap tests"]
fn test_with_real_hm_data() {
    const EXPECTED_LEN: usize = 105_542;

    fn write_data(storage: &mut Gridstore<Payload>, init_offset: u32) -> u32 {
        let csv_path = dataset::Dataset::HMArticles
            .download()
            .expect("download should succeed");

        let csv_file = BufReader::new(File::open(csv_path).expect("file should open"));

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

        let mut rdr = csv::Reader::from_reader(csv_file);
        let mut point_offset = init_offset;
        for result in rdr.records() {
            let record = result.unwrap();
            let mut payload = Payload::default();
            for (i, &field) in HM_FIELDS.iter().enumerate() {
                payload.0.insert(
                    field.to_string(),
                    serde_json::Value::String(record.get(i).unwrap().to_string()),
                );
            }
            storage
                .put_value(point_offset, &payload, hw_counter_ref)
                .unwrap();
            point_offset += 1;
        }
        storage.flusher()().unwrap();
        point_offset
    }

    fn storage_double_pass_is_consistent(storage: &Gridstore<Payload>, right_shift_offset: u32) {
        let csv_path = dataset::Dataset::HMArticles
            .download()
            .expect("download should succeed");

        let csv_file = BufReader::new(File::open(csv_path).expect("file should open"));
        let hw_counter = HardwareCounterCell::new();

        let mut rdr = csv::Reader::from_reader(csv_file);
        for (row_index, result) in rdr.records().enumerate() {
            let record = result.unwrap();
            let storage_index = row_index as u32 + right_shift_offset;
            let first = storage
                .get_value::<false>(storage_index, &hw_counter)
                .unwrap()
                .unwrap();
            let second = storage
                .get_value::<false>(storage_index + EXPECTED_LEN as u32, &hw_counter)
                .unwrap()
                .unwrap();
            assert_eq!(first, second);
            for (i, field) in HM_FIELDS.iter().enumerate() {
                assert_eq!(
                    first.0.get(*field).unwrap().as_str().unwrap(),
                    record.get(i).unwrap(),
                    "failed for id {row_index} with shift {right_shift_offset} for field: {field}",
                );
            }
        }
    }

    let (dir, mut storage) = empty_storage();
    let point_offset = write_data(&mut storage, 0);
    assert_eq!(point_offset, EXPECTED_LEN as u32);
    assert_eq!(storage.tracker.read().mapping_len(), EXPECTED_LEN);
    assert_eq!(storage.pages.read().len(), 2);

    let point_offset = write_data(&mut storage, point_offset);
    assert_eq!(point_offset, EXPECTED_LEN as u32 * 2);
    assert_eq!(storage.tracker.read().mapping_len(), EXPECTED_LEN * 2);
    assert_eq!(storage.pages.read().len(), 4);

    storage_double_pass_is_consistent(&storage, 0);

    storage.flusher()().unwrap();
    drop(storage);

    let mut storage = Gridstore::open(dir.path().to_path_buf()).unwrap();
    assert_eq!(point_offset, EXPECTED_LEN as u32 * 2);
    assert_eq!(storage.pages.read().len(), 4);
    assert_eq!(storage.tracker.read().mapping_len(), EXPECTED_LEN * 2);

    storage_double_pass_is_consistent(&storage, 0);

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

    let offset: u32 = 1;
    for i in (0..EXPECTED_LEN).rev() {
        let payload = storage
            .get_value::<false>(i as u32, &hw_counter)
            .unwrap()
            .unwrap();
        storage
            .put_value(i as u32 + offset, &payload, hw_counter_ref)
            .unwrap();
        storage
            .put_value(
                i as u32 + offset + EXPECTED_LEN as u32,
                &payload,
                hw_counter_ref,
            )
            .unwrap();
    }
    storage_double_pass_is_consistent(&storage, offset);

    assert!(dir.path().exists());
    storage.wipe().unwrap();
    assert!(!dir.path().exists());
}

#[test]
fn test_payload_compression() {
    let payload = random_payload(&mut rand::make_rng::<rand::rngs::SmallRng>(), 2);
    let payload_bytes = payload.to_bytes();
    let compressed = compress_lz4(&payload_bytes);
    let decompressed = decompress_lz4(&compressed);
    let decompressed_payload = <Payload as Blob>::from_bytes(&decompressed);
    assert_eq!(payload, decompressed_payload);
}

#[rstest]
#[case(64)]
#[case(128)]
#[case(256)]
#[case(512)]
fn test_different_block_sizes(#[case] block_size_bytes: usize) {
    use crate::fixtures::minimal_payload;

    let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
    let blocks_per_page = DEFAULT_PAGE_SIZE_BYTES / block_size_bytes;
    let options = StorageOptions {
        block_size_bytes: Some(block_size_bytes),
        ..Default::default()
    };
    let mut storage = Gridstore::new(dir.path().to_path_buf(), options).unwrap();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    let payload = minimal_payload();
    let last_point_id = 3 * blocks_per_page as u32;
    for point_offset in 0..=last_point_id {
        storage
            .put_value(point_offset, &payload, hw_counter_ref)
            .unwrap();
    }

    storage.flusher()().unwrap();
    println!("{last_point_id}");

    assert_eq!(storage.pages.read().len(), 4);
    let last_pointer = storage.get_pointer(last_point_id).unwrap();
    assert_eq!(last_pointer.block_offset, 0);
    assert_eq!(last_pointer.page_id, 3);
}

/// Test that data is only actually flushed when we invoke the flush closure
///
/// Specifically:
/// - version of data we write to disk is that of when the flusher was created
/// - data is only written to disk when closure is invoked
#[test]
fn test_deferred_flush() {
    let (dir, mut storage) = empty_storage();
    let path = dir.path().to_path_buf();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    let get_payload = |storage: &Gridstore<Payload>| {
        storage
            .get_value::<false>(0, &hw_counter)
            .expect("no io error")
            .expect("offset exists")
            .0
            .get("key")
            .expect("key exists")
            .as_str()
            .expect("value is a string")
            .to_owned()
    };
    let put_payload =
        |storage: &mut Gridstore<Payload>, payload_value: &str, expected_block_offset: u32| {
            let mut payload = Payload::default();
            payload.0.insert(
                "key".to_string(),
                serde_json::Value::String(payload_value.to_string()),
            );

            storage.put_value(0, &payload, hw_counter_ref).unwrap();
            assert_eq!(storage.pages.read().len(), 1);
            assert_eq!(storage.tracker.read().mapping_len(), 1);

            let page_mapping = storage.get_pointer(0).unwrap();
            assert_eq!(page_mapping.page_id, 0); // first page
            assert_eq!(page_mapping.block_offset, expected_block_offset);

            let hw_counter = HardwareCounterCell::new();
            let stored_payload = storage.get_value::<false>(0, &hw_counter).unwrap();
            assert!(stored_payload.is_some());
            assert_eq!(stored_payload.unwrap(), payload);
        };

    put_payload(&mut storage, "value 1", 0);
    assert_eq!(get_payload(&storage), "value 1");

    put_payload(&mut storage, "value 2", 1);
    assert_eq!(get_payload(&storage), "value 2");

    let flusher = storage.flusher();

    put_payload(&mut storage, "value 3", 2);
    assert_eq!(get_payload(&storage), "value 3");

    // We drop the flusher so nothing happens
    drop(flusher);
    assert_eq!(get_payload(&storage), "value 3");

    let flusher = storage.flusher();

    put_payload(&mut storage, "value 4", 3);
    assert_eq!(get_payload(&storage), "value 4");

    // We flush and still expect to read latest data
    flusher().unwrap();
    assert_eq!(get_payload(&storage), "value 4");

    // We flushed and freed blocks 0 and 1, we expect to reuse block 0
    put_payload(&mut storage, "value 5", 0);
    assert_eq!(get_payload(&mut storage), "value 5");

    // Reopen gridstore
    drop(storage);
    let mut storage = Gridstore::<Payload>::open(path).unwrap();
    assert_eq!(storage.pages.read().len(), 1);

    // On reopen, we expect to read the data at the time the flusher was created
    assert_eq!(get_payload(&storage), "value 3");

    // Bitslice is not buffered and can be flushed by the kernel, expect to reuse block 1
    // It means that we might lose unoccupied storage, but it can only happen on crash and the
    // optimizer will eventually take care of this when building a fresh segment
    put_payload(&mut storage, "value 6", 1);
    assert_eq!(get_payload(&storage), "value 6");

    put_payload(&mut storage, "value 7", 4);
    assert_eq!(get_payload(&storage), "value 7");

    put_payload(&mut storage, "value 8", 5);
    assert_eq!(get_payload(&storage), "value 8");
}

/// Similar to [`test_deferred_flush`] but more complex, including multiple flushers and deletes
#[test]
fn test_deferred_flush_with_delete() {
    let (dir, mut storage) = empty_storage();
    let path = dir.path().to_path_buf();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    let get_payload = |storage: &Gridstore<Payload>| {
        Some(
            storage
                .get_value::<false>(0, &hw_counter)
                .unwrap()?
                .0
                .get("key")
                .expect("key exists")
                .as_str()
                .expect("value is a string")
                .to_owned(),
        )
    };
    let put_payload =
        |storage: &mut Gridstore<Payload>, payload_value: &str, expected_block_offset: u32| {
            let mut payload = Payload::default();
            payload.0.insert(
                "key".to_string(),
                serde_json::Value::String(payload_value.to_string()),
            );

            storage.put_value(0, &payload, hw_counter_ref).unwrap();
            assert_eq!(storage.pages.read().len(), 1);
            assert_eq!(storage.tracker.read().mapping_len(), 1);

            let page_mapping = storage.get_pointer(0).unwrap();
            assert_eq!(page_mapping.page_id, 0); // first page
            assert_eq!(page_mapping.block_offset, expected_block_offset);

            let hw_counter = HardwareCounterCell::new();
            let stored_payload = storage.get_value::<false>(0, &hw_counter).unwrap();
            assert!(stored_payload.is_some());
            assert_eq!(stored_payload.unwrap(), payload);
        };

    put_payload(&mut storage, "value 1", 0);
    assert_eq!(get_payload(&storage).unwrap(), "value 1");

    put_payload(&mut storage, "value 2", 1);
    assert_eq!(get_payload(&storage).unwrap(), "value 2");

    let flusher = storage.flusher();

    put_payload(&mut storage, "value 3", 2);
    assert_eq!(get_payload(&storage).unwrap(), "value 3");

    storage.delete_value(0).unwrap();
    assert!(get_payload(&storage).is_none());

    // Flush, storage is still open so we expect the point not to exist
    flusher().unwrap();
    assert!(get_payload(&storage).is_none());

    // Reopen gridstore
    drop(storage);
    let mut storage = Gridstore::<Payload>::open(path.clone()).unwrap();
    assert_eq!(storage.pages.read().len(), 1);

    let flusher = storage.flusher();

    // On reopen, later updates and the delete were not flushed, expect to read value 2
    assert_eq!(get_payload(&storage).unwrap(), "value 2");

    flusher().unwrap();

    storage.delete_value(0).unwrap();
    assert!(get_payload(&storage).is_none());

    storage.flusher()().unwrap();
    storage.flusher()().unwrap();
    assert!(get_payload(&storage).is_none());

    // Reopen gridstore
    drop(storage);
    let mut storage = Gridstore::<Payload>::open(path.clone()).unwrap();
    assert_eq!(storage.pages.read().len(), 1);

    // On reopen, delete was flushed this time, expect point to be missing
    assert!(get_payload(&storage).is_none());

    put_payload(&mut storage, "value 4", 0);
    assert_eq!(get_payload(&storage).unwrap(), "value 4");

    assert_eq!(
        storage.tracker.read().pending_updates.len(),
        1,
        "expect 1 pending update",
    );

    storage.flusher()().unwrap();

    assert_eq!(
        storage.tracker.read().pending_updates.len(),
        0,
        "expect 0 pending updates",
    );

    // Reopen gridstore
    drop(storage);
    let mut storage = Gridstore::<Payload>::open(path.clone()).unwrap();
    assert_eq!(storage.pages.read().len(), 1);

    // On reopen, value 4 was flushed, expect to read it
    assert_eq!(get_payload(&storage).unwrap(), "value 4");

    put_payload(&mut storage, "value 5", 1);
    assert_eq!(get_payload(&storage).unwrap(), "value 5");

    let flusher_1_value_5 = storage.flusher();

    storage.delete_value(0).unwrap();
    assert!(get_payload(&storage).is_none());

    let flusher_2_delete = storage.flusher();

    put_payload(&mut storage, "value 6", 3);
    assert_eq!(get_payload(&storage).unwrap(), "value 6");

    let flusher_3_value_6 = storage.flusher();

    put_payload(&mut storage, "value 7", 4);
    assert_eq!(get_payload(&storage).unwrap(), "value 7");

    // Not flushed, still expect to read value 4
    {
        let tmp_storage = Gridstore::<Payload>::open(path.clone()).unwrap();
        assert_eq!(get_payload(&tmp_storage).unwrap(), "value 4");
    }

    // First flusher flushed, expect to read value 5 if we load from disk
    flusher_1_value_5().unwrap();
    {
        let tmp_storage = Gridstore::<Payload>::open(path.clone()).unwrap();
        assert_eq!(get_payload(&tmp_storage).unwrap(), "value 5");
    }

    // Second flusher flushed, expect point to be missing if we load from disk
    flusher_2_delete().unwrap();
    {
        let tmp_storage = Gridstore::<Payload>::open(path.clone()).unwrap();
        assert!(get_payload(&tmp_storage).is_none());
    }

    // Third flusher flushed, expect to read value 6 if we load from disk
    flusher_3_value_6().unwrap();
    {
        let tmp_storage = Gridstore::<Payload>::open(path).unwrap();
        assert_eq!(get_payload(&tmp_storage).unwrap(), "value 6");
    }

    // Main storage still isn't flushed, but has value 7
    assert_eq!(get_payload(&storage).unwrap(), "value 7");
    assert_eq!(
        storage.tracker.read().pending_updates.len(),
        1,
        "expect 1 pending update",
    );
}

/// Test that data is only actually flushed when the Gridstore instance is still valid
///
/// Specifically:
/// - ensure that 'late' flushers don't write any data if already invalidated by a clear or
///   something else
#[test]
fn test_skip_deferred_flush_after_clear() {
    let (dir, mut storage) = empty_storage();
    let path = dir.path().to_path_buf();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    let put_payload = |storage: &mut Gridstore<Payload>, point_offset: u32, payload_value: &str| {
        let mut payload = Payload::default();
        payload.0.insert(
            "key".to_string(),
            serde_json::Value::String(payload_value.to_string()),
        );

        storage
            .put_value(point_offset, &payload, hw_counter_ref)
            .unwrap();
        assert_eq!(storage.pages.read().len(), 1);

        let hw_counter = HardwareCounterCell::new();
        let stored_payload = storage
            .get_value::<false>(point_offset, &hw_counter)
            .unwrap();
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);
    };

    // Write enough new pointers so that they don't fit in the default tracker file size
    // On flush, the tracker file will be resized and reopened, significant for this test
    let file_size = storage.tracker.read().mmap_file_size();
    const POINTER_SIZE: usize = size_of::<Option<ValuePointer>>();
    let last_point_offset = (file_size / POINTER_SIZE) as u32;
    for i in 0..=last_point_offset {
        put_payload(&mut storage, i, "value x");
    }

    let flusher = storage.flusher();

    // Clone arcs to keep storage alive after clear
    // Necessary for this test to allow the flusher task to still upgrade its weak arcs when we
    // call the flusher. This allows us to trigger broken flush behavior in old versions.
    // The same is possible without cloning these arcs, but it would require specific timing
    // conditions. Cloning arcs here is much more reliable for this test case.
    let storage_arcs = (
        Arc::clone(&storage.pages),
        Arc::clone(&storage.tracker),
        Arc::clone(&storage.bitmask),
    );

    // We clear the storage, pending flusher must not write anything anymore
    storage.clear().unwrap();

    // Flusher is invalidated and does nothing
    // This was broken before <https://github.com/qdrant/qdrant/pull/7702>
    assert!(flusher().is_err_and(|err| matches!(err, GridstoreError::FlushCancelled)));

    drop(storage_arcs);

    // We expect the storage to be empty
    assert!(storage.get_pointer(0).is_none(), "point must not exist");

    // If we reopen the storage it must still be empty
    drop(storage);
    let storage = Gridstore::<Payload>::open(path.clone()).unwrap();
    assert_eq!(storage.pages.read().len(), 1);
    assert!(storage.get_pointer(0).is_none(), "point must not exist");
    assert_eq!(storage.max_point_offset(), 0, "must have zero points");
}
