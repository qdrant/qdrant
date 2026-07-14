use std::io::BufReader;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::{Random, Sequential};
use common::universal_io::{MmapFile, MmapFs};
use fs_err as fs;
use fs_err::File;
use itertools::Itertools;
use rand::distr::Uniform;
use rand::prelude::Distribution;
use rand::seq::SliceRandom;
use rand::{Rng, RngExt};
use rstest::rstest;
use tempfile::Builder;

use super::*;
use crate::blob::Blob;
use crate::config::{
    Compression, DEFAULT_BLOCK_SIZE_BYTES, DEFAULT_PAGE_SIZE_BYTES, DEFAULT_REGION_SIZE_BLOCKS,
    Mode, StorageConfig, compress_lz4, decompress_lz4,
};
use crate::fixtures::{
    HM_FIELDS, Payload, empty_storage, empty_storage_mode, empty_storage_sized, minimal_payload,
    random_payload,
};

#[rstest]
fn test_empty_payload_storage(#[values(Mode::Dynamic, Mode::AppendOnly)] mode: Mode) {
    let hw_counter = HardwareCounterCell::new();
    let (_dir, storage) = empty_storage_mode(mode);
    let payload = storage.get_value::<Random>(0, &hw_counter).unwrap();
    assert!(payload.is_none());
    assert_eq!(storage.get_storage_size_bytes().unwrap(), 0);
}

#[rstest]
fn test_put_single_empty_value(#[values(Mode::Dynamic, Mode::AppendOnly)] mode: Mode) {
    let (_dir, mut storage) = empty_storage_mode(mode);

    let hw_counter = HardwareCounterCell::new();
    let hw_counter = hw_counter.ref_payload_io_write_counter();

    // TODO: should we actually use the pages for empty values?
    let payload = Payload::default();
    storage.put_value(0, &payload, hw_counter).unwrap();
    assert_eq!(storage.max_point_offset(), 1);
    if mode == Mode::Dynamic {
        assert_eq!(storage.as_gridstore().pages.read().num_pages(), 1);
        assert_eq!(
            storage.as_gridstore().tracker.read().mapping_len().unwrap(),
            1
        );
    }

    let hw_counter = HardwareCounterCell::new();
    let stored_payload = storage.get_value::<Random>(0, &hw_counter).unwrap();
    assert!(stored_payload.is_some());
    assert_eq!(stored_payload.unwrap(), Payload::default());

    let size = storage.get_storage_size_bytes().unwrap();
    match mode {
        // Values occupy whole blocks
        Mode::Dynamic => assert_eq!(size, DEFAULT_BLOCK_SIZE_BYTES),
        // Values are packed exactly, this is the compressed payload size
        Mode::AppendOnly => assert!(size > 0 && size < DEFAULT_BLOCK_SIZE_BYTES),
    }
}

#[rstest]
fn test_put_single_payload(#[values(Mode::Dynamic, Mode::AppendOnly)] mode: Mode) {
    let (_dir, mut storage) = empty_storage_mode(mode);

    let mut payload = Payload::default();
    payload.0.insert(
        "key".to_string(),
        serde_json::Value::String("value".to_string()),
    );

    let hw_counter = HardwareCounterCell::new();
    let hw_counter = hw_counter.ref_payload_io_write_counter();

    storage.put_value(0, &payload, hw_counter).unwrap();
    assert_eq!(storage.max_point_offset(), 1);
    if mode == Mode::Dynamic {
        assert_eq!(storage.as_gridstore().pages.read().num_pages(), 1);
        assert_eq!(
            storage.as_gridstore().tracker.read().mapping_len().unwrap(),
            1
        );
    }

    let page_mapping = storage.get_pointer(0).unwrap();
    assert_eq!(page_mapping.page_id, 0); // first page
    assert_eq!(page_mapping.block_offset, 0); // first cell

    let hw_counter = HardwareCounterCell::new();
    let stored_payload = storage.get_value::<Random>(0, &hw_counter).unwrap();
    assert!(stored_payload.is_some());
    assert_eq!(stored_payload.unwrap(), payload);

    let size = storage.get_storage_size_bytes().unwrap();
    match mode {
        // Values occupy whole blocks
        Mode::Dynamic => assert_eq!(size, DEFAULT_BLOCK_SIZE_BYTES),
        // Values are packed exactly, this is the compressed payload size
        Mode::AppendOnly => assert!(size > 0 && size < DEFAULT_BLOCK_SIZE_BYTES),
    }
}

#[rstest]
fn test_storage_files(#[values(Mode::Dynamic, Mode::AppendOnly)] mode: Mode) {
    let (dir, mut storage) = empty_storage_mode(mode);

    let mut payload = Payload::default();
    payload.0.insert(
        "key".to_string(),
        serde_json::Value::String("value".to_string()),
    );

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    storage.put_value(0, &payload, hw_counter_ref).unwrap();

    let files = storage.files();
    let actual_files: Vec<_> = fs::read_dir(dir.path()).unwrap().try_collect().unwrap();
    assert_eq!(
        files.len(),
        actual_files.len(),
        "The directory has {} files, but we are reporting {}\nreported: {files:?}\n actual: {actual_files:?}",
        actual_files.len(),
        files.len()
    );

    let expected_files: &[&str] = match mode {
        Mode::Dynamic => &[
            "tracker.dat",
            "page_0.dat",
            "config.json",
            "bitmask.dat",
            "gaps.dat",
        ],
        Mode::AppendOnly => &[
            "append_only_tracker.dat",
            "append_only_page_0.dat",
            "config.json",
        ],
    };
    assert_eq!(
        files.len(),
        expected_files.len(),
        "Expected {} files, got {files:?}",
        expected_files.len(),
    );
    for (file, expected_name) in files.iter().zip(expected_files) {
        assert_eq!(file.file_name().unwrap(), *expected_name);
    }
}

#[rstest]
#[case(50000, 2)]
#[case(100, 2000)]
fn test_put_payload(
    #[case] num_payloads: u32,
    #[case] payload_size_factor: usize,
    #[values(Mode::Dynamic, Mode::AppendOnly)] mode: Mode,
) {
    let (_dir, mut storage) = empty_storage_mode(mode);

    let rng = &mut rand::make_rng::<rand::rngs::SmallRng>();

    let mut payloads = (0..num_payloads)
        .map(|point_offset| (point_offset, random_payload(rng, payload_size_factor)))
        .collect::<Vec<_>>();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    for (point_offset, payload) in &payloads {
        storage
            .put_value(*point_offset, payload, hw_counter_ref)
            .unwrap();

        let stored_payload = storage
            .get_value::<Random>(*point_offset, &hw_counter)
            .unwrap();
        assert!(stored_payload.is_some());
        assert_eq!(&stored_payload.unwrap(), payload);
    }

    // read randomly
    payloads.shuffle(rng);
    for (point_offset, payload) in &payloads {
        let stored_payload = storage
            .get_value::<Random>(*point_offset, &hw_counter)
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
    assert_eq!(storage.as_gridstore().pages.read().num_pages(), 1);

    let page_mapping = storage.get_pointer(0).unwrap();
    assert_eq!(page_mapping.page_id, 0); // first page
    assert_eq!(page_mapping.block_offset, 0); // first cell

    let stored_payload = storage.get_value::<Random>(0, &hw_counter).unwrap();
    assert_eq!(stored_payload, Some(payload));
    assert_eq!(
        storage.get_storage_size_bytes().unwrap(),
        DEFAULT_BLOCK_SIZE_BYTES
    );

    // delete payload
    let deleted = storage.delete_value(0).unwrap();
    assert_eq!(deleted, stored_payload);
    assert_eq!(storage.as_gridstore().pages.read().num_pages(), 1);

    // get payload again
    let stored_payload = storage.get_value::<Random>(0, &hw_counter).unwrap();
    assert!(stored_payload.is_none());
    storage.flusher()().unwrap();
    assert_eq!(storage.get_storage_size_bytes().unwrap(), 0);
}

#[test]
fn test_update_single_payload() {
    let (_dir, mut storage) = empty_storage();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    let put_payload =
        |storage: &mut Blobstore<Payload>, payload_value: &str, expected_block_offset: u32| {
            let mut payload = Payload::default();
            payload.0.insert(
                "key".to_string(),
                serde_json::Value::String(payload_value.to_string()),
            );

            storage.put_value(0, &payload, hw_counter_ref).unwrap();
            assert_eq!(storage.as_gridstore().pages.read().num_pages(), 1);
            assert_eq!(
                storage.as_gridstore().tracker.read().mapping_len().unwrap(),
                1
            );

            let page_mapping = storage.get_pointer(0).unwrap();
            assert_eq!(page_mapping.page_id, 0); // first page
            assert_eq!(page_mapping.block_offset, expected_block_offset);

            let hw_counter = HardwareCounterCell::new();
            let stored_payload = storage.get_value::<Random>(0, &hw_counter).unwrap();
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
    let config = StorageConfig {
        page_size_bytes: page_size,
        block_size_bytes: DEFAULT_BLOCK_SIZE_BYTES,
        region_size_blocks: DEFAULT_REGION_SIZE_BLOCKS,
        compression: Compression::None,
        mode: Mode::Dynamic,
    };

    storage.as_gridstore_mut().create_new_page().unwrap();

    let value_len = 1000;

    // Value should span 8 blocks
    let value = (0..)
        .map(|i| (i % 24) as u8)
        .take(value_len)
        .collect::<Vec<_>>();

    // Let's write it near the end
    let block_offset = (DEFAULT_REGION_SIZE_BLOCKS - 10) as u32;
    let pointer = ValuePointer::new(0, block_offset, value_len as u32);

    storage
        .as_gridstore()
        .pages
        .write()
        .write_to_pages(pointer, &value, &config)
        .unwrap();

    let read_value = storage
        .as_gridstore()
        .with_view(|view| {
            view.read_from_pages::<Random>(pointer)
                .map(|val| val.into_owned())
        })
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
    // Batched get of several offsets via `read_values`
    GetBatch(Vec<PointOffset>),
    // Flush after delay
    FlushDelay(Duration),
    // Clear storage
    Clear,
    // Iter up to limit
    Iter(PointOffset),
    // Warm the mmap cache (pages, tracker, bitmask)
    Populate,
    // Drop the disk cache
    ClearCache,
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
            max_point_offset / 10,    // get_batch
            max_point_offset / 1_000, // populate
            max_point_offset / 1_000, // clear_cache
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
            6 => {
                let batch_size = rng.random_range(1..=16);
                let offsets = (0..batch_size)
                    .map(|_| rng.random_range(0..=max_point_offset))
                    .collect();
                Operation::GetBatch(offsets)
            }
            7 => Operation::Populate,
            8 => Operation::ClearCache,
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

    #[cfg(target_os = "windows")]
    let operation_count = 10_000;
    #[cfg(not(target_os = "windows"))]
    let operation_count = 50_000;
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
                    .get_value::<Sequential>(point_offset, &hw_counter)
                    .unwrap();
                let v1_rand = storage
                    .get_value::<Random>(point_offset, &hw_counter)
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
            Operation::GetBatch(point_offsets) => {
                log::debug!("op:{i} GET_BATCH size:{}", point_offsets.len());
                let mut batch_results: Vec<Option<Payload>> = vec![None; point_offsets.len()];
                storage
                    .read_values::<Random, _, GridstoreError>(
                        point_offsets.iter().copied().enumerate(),
                        |idx, _, value| {
                            batch_results[idx] = value;
                            Ok(())
                        },
                        hw_counter.payload_io_read_counter(),
                    )
                    .unwrap();
                for (idx, &point_offset) in point_offsets.iter().enumerate() {
                    let v_batch = &batch_results[idx];
                    let v_model = model_hashmap.get(&point_offset).cloned();
                    assert_eq!(
                        v_batch, &v_model,
                        "get_batch failed for point_offset: {point_offset} with {v_batch:?} vs {v_model:?}",
                    );
                }
            }
            Operation::Populate => {
                log::debug!("op:{i} POPULATE");
                storage.populate().unwrap();
            }
            Operation::ClearCache => {
                log::debug!("op:{i} CLEAR_CACHE");
                storage.clear_cache().unwrap();
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
        storage.as_gridstore().tracker.read().mapping_len().unwrap(),
        model_hashmap.len(),
        "different number of points"
    );

    // validate storage and model_hashmap are the same
    for point_offset in 0..=max_point_offset {
        let stored_payload = storage
            .get_value::<Random>(point_offset, &hw_counter)
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

    let before_size = storage.get_storage_size_bytes().unwrap();
    // drop storage
    drop(storage);

    // reopen storage
    let storage =
        Blobstore::<Payload>::open(MmapFs, dir.path().to_path_buf(), Populate::No).unwrap();
    // assert same size
    assert_eq!(storage.get_storage_size_bytes().unwrap(), before_size);
    // assert same length
    assert_eq!(
        storage.as_gridstore().tracker.read().mapping_len().unwrap(),
        model_hashmap.len()
    );

    // validate storage and model_hashmap are the same
    for point_offset in 0..=max_point_offset {
        let stored_payload = storage
            .get_value::<Random>(point_offset, &hw_counter)
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
    assert_eq!(storage.as_gridstore().pages.read().num_pages(), 1);

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
    assert_eq!(storage.as_gridstore().pages.read().num_pages(), 2);

    let page_mapping = storage.get_pointer(0).unwrap();
    assert_eq!(page_mapping.page_id, 0); // first page
    assert_eq!(page_mapping.block_offset, 0); // first cell

    let stored_payload = storage.get_value::<Random>(0, &hw_counter).unwrap();
    assert!(stored_payload.is_some());
    assert_eq!(stored_payload.unwrap(), payload);

    {
        // the fitting page should be 64MB, so we should still have about 14MB of free space
        let free_blocks = storage
            .as_gridstore()
            .bitmask
            .read()
            .free_blocks_for_page(1)
            .unwrap();
        let min_expected = 1024 * 1024 * 13 / DEFAULT_BLOCK_SIZE_BYTES;
        let max_expected = 1024 * 1024 * 15 / DEFAULT_BLOCK_SIZE_BYTES;
        assert!((min_expected..max_expected).contains(&free_blocks));
    }

    {
        // delete payload
        let deleted = storage.delete_value(0).unwrap();
        assert!(deleted.is_some());
        assert_eq!(storage.as_gridstore().pages.read().num_pages(), 2);

        assert!(
            storage
                .get_value::<Random>(0, &hw_counter)
                .unwrap()
                .is_none()
        );
    }
}

#[rstest]
fn test_storage_persistence_basic(#[values(Mode::Dynamic, Mode::AppendOnly)] mode: Mode) {
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
        let options = StorageOptions {
            mode: Some(mode),
            ..Default::default()
        };
        let mut storage = Blobstore::<_>::new(MmapFs, path.clone(), options).unwrap();
        storage.put_value(0, &payload, hw_counter_ref).unwrap();

        let page_mapping = storage.get_pointer(0).unwrap();
        assert_eq!(page_mapping.page_id, 0); // first page
        assert_eq!(page_mapping.block_offset, 0); // first cell

        let stored_payload = storage.get_value::<Random>(0, &hw_counter).unwrap();
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);

        // flush storage before dropping
        storage.flusher()().unwrap();
    }

    // reopen storage, the mode is selected automatically
    let storage = Blobstore::<Payload>::open(MmapFs, path, Populate::No).unwrap();
    assert_eq!(storage.max_point_offset(), 1);

    let stored_payload = storage.get_value::<Random>(0, &hw_counter).unwrap();
    assert!(stored_payload.is_some());
    assert_eq!(stored_payload.unwrap(), payload);
}

/// Configs written before the `mode` field existed must open in dynamic mode.
#[test]
fn test_open_config_without_mode_as_dynamic() {
    let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
    let path = dir.path().to_path_buf();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    {
        let mut storage = Blobstore::<_>::new(MmapFs, path.clone(), Default::default()).unwrap();
        storage
            .put_value(0, &minimal_payload(), hw_counter_ref)
            .unwrap();
        storage.flusher()().unwrap();
    }

    // Strip the mode field from the config, like configs of older versions
    let config_path = path.join("config.json");
    let config_json = fs::read_to_string(&config_path).unwrap();
    let mut config: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(&config_json).unwrap();
    assert_eq!(
        config.remove("mode"),
        Some(serde_json::Value::String("dynamic".into())),
    );
    fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();

    // Both the storage and the reader open it in dynamic mode
    let storage = Blobstore::<Payload>::open(MmapFs, path.clone(), Populate::No).unwrap();
    storage.as_gridstore();
    assert_eq!(
        storage.get_value::<Random>(0, &hw_counter).unwrap(),
        Some(minimal_payload()),
    );

    let reader = BlobstoreReader::<Payload, MmapFile>::open(&MmapFs, path, Populate::No).unwrap();
    assert_eq!(
        reader.get_value::<Random>(0, &hw_counter).unwrap(),
        Some(minimal_payload()),
    );
}

/// A corrupt config with zero sized blocks must be rejected when opening, it would otherwise
/// break pointer arithmetic.
#[rstest]
fn test_open_rejects_corrupt_config(#[values(Mode::Dynamic, Mode::AppendOnly)] mode: Mode) {
    let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
    let path = dir.path().to_path_buf();

    {
        let options = StorageOptions {
            mode: Some(mode),
            ..Default::default()
        };
        let storage = Blobstore::<Payload>::new(MmapFs, path.clone(), options).unwrap();
        storage.flusher()().unwrap();
    }

    // Corrupt the config by zeroing the block size
    let config_path = path.join("config.json");
    let config_json = fs::read_to_string(&config_path).unwrap();
    let mut config: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(&config_json).unwrap();
    config.insert("block_size_bytes".to_string(), 0.into());
    fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();

    assert!(Blobstore::<Payload>::open(MmapFs, path.clone(), Populate::No).is_err());
    assert!(BlobstoreReader::<Payload, MmapFile>::open(&MmapFs, path, Populate::No).is_err());
}

#[test]
#[ignore = "this test is too slow for ci, and has similar coverage to the hashmap tests"]
fn test_with_real_hm_data() {
    const EXPECTED_LEN: usize = 105_542;

    fn write_data(storage: &mut Blobstore<Payload>, init_offset: u32) -> u32 {
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

    fn storage_double_pass_is_consistent(storage: &Blobstore<Payload>, right_shift_offset: u32) {
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
                .get_value::<Random>(storage_index, &hw_counter)
                .unwrap()
                .unwrap();
            let second = storage
                .get_value::<Random>(storage_index + EXPECTED_LEN as u32, &hw_counter)
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
    assert_eq!(
        storage.as_gridstore().tracker.read().mapping_len().unwrap(),
        EXPECTED_LEN
    );
    assert_eq!(storage.as_gridstore().pages.read().num_pages(), 2);

    let point_offset = write_data(&mut storage, point_offset);
    assert_eq!(point_offset, EXPECTED_LEN as u32 * 2);
    assert_eq!(
        storage.as_gridstore().tracker.read().mapping_len().unwrap(),
        EXPECTED_LEN * 2
    );
    assert_eq!(storage.as_gridstore().pages.read().num_pages(), 4);

    storage_double_pass_is_consistent(&storage, 0);

    storage.flusher()().unwrap();
    drop(storage);

    let mut storage = Blobstore::open(MmapFs, dir.path().to_path_buf(), Populate::No).unwrap();
    assert_eq!(point_offset, EXPECTED_LEN as u32 * 2);
    assert_eq!(storage.as_gridstore().pages.read().num_pages(), 4);
    assert_eq!(
        storage.as_gridstore().tracker.read().mapping_len().unwrap(),
        EXPECTED_LEN * 2
    );

    storage_double_pass_is_consistent(&storage, 0);

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

    let offset: u32 = 1;
    for i in (0..EXPECTED_LEN).rev() {
        let payload = storage
            .get_value::<Random>(i as u32, &hw_counter)
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
#[case(128)]
#[case(256)]
#[case(512)]
fn test_different_block_sizes(
    #[case] block_size_bytes: usize,
    #[values(Mode::Dynamic, Mode::AppendOnly)] mode: Mode,
) {
    use crate::fixtures::minimal_payload;

    let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
    let blocks_per_page = DEFAULT_PAGE_SIZE_BYTES / block_size_bytes;
    let options = StorageOptions {
        block_size_bytes: Some(block_size_bytes),
        mode: Some(mode),
        ..Default::default()
    };
    let mut storage = Blobstore::<_>::new(MmapFs, dir.path().to_path_buf(), options).unwrap();

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

    let last_pointer = storage.get_pointer(last_point_id).unwrap();
    match mode {
        // Each value occupies one block, spilling over into a new page every blocks_per_page
        Mode::Dynamic => {
            assert_eq!(storage.as_gridstore().pages.read().num_pages(), 4);
            assert_eq!(last_pointer.block_offset, 0);
            assert_eq!(last_pointer.page_id, 3);
        }
        // Values are packed back to back in bytes, blocks don't apply; pages roll over at
        // the configured page size
        Mode::AppendOnly => {
            let value_len = u64::from(last_pointer.length);
            let values_per_page = DEFAULT_PAGE_SIZE_BYTES as u64 / value_len;
            assert_eq!(
                u64::from(last_pointer.page_id),
                u64::from(last_point_id) / values_per_page,
            );
            assert_eq!(
                u64::from(last_pointer.block_offset),
                u64::from(last_point_id) % values_per_page * value_len,
            );
        }
    }

    let stored_payload = storage
        .get_value::<Random>(last_point_id, &hw_counter)
        .unwrap();
    assert_eq!(stored_payload, Some(payload));
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
    let get_payload = |storage: &Blobstore<Payload>| {
        storage
            .get_value::<Random>(0, &hw_counter)
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
        |storage: &mut Blobstore<Payload>, payload_value: &str, expected_block_offset: u32| {
            let mut payload = Payload::default();
            payload.0.insert(
                "key".to_string(),
                serde_json::Value::String(payload_value.to_string()),
            );

            storage.put_value(0, &payload, hw_counter_ref).unwrap();
            assert_eq!(storage.as_gridstore().pages.read().num_pages(), 1);
            assert_eq!(
                storage.as_gridstore().tracker.read().mapping_len().unwrap(),
                1
            );

            let page_mapping = storage.get_pointer(0).unwrap();
            assert_eq!(page_mapping.page_id, 0); // first page
            assert_eq!(page_mapping.block_offset, expected_block_offset);

            let hw_counter = HardwareCounterCell::new();
            let stored_payload = storage.get_value::<Random>(0, &hw_counter).unwrap();
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
    let mut storage = Blobstore::<Payload>::open(MmapFs, path, Populate::No).unwrap();
    assert_eq!(storage.as_gridstore().pages.read().num_pages(), 1);

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
    let get_payload = |storage: &Blobstore<Payload>| {
        Some(
            storage
                .get_value::<Random>(0, &hw_counter)
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
        |storage: &mut Blobstore<Payload>, payload_value: &str, expected_block_offset: u32| {
            let mut payload = Payload::default();
            payload.0.insert(
                "key".to_string(),
                serde_json::Value::String(payload_value.to_string()),
            );

            storage.put_value(0, &payload, hw_counter_ref).unwrap();
            assert_eq!(storage.as_gridstore().pages.read().num_pages(), 1);
            assert_eq!(
                storage.as_gridstore().tracker.read().mapping_len().unwrap(),
                1
            );

            let page_mapping = storage.get_pointer(0).unwrap();
            assert_eq!(page_mapping.page_id, 0); // first page
            assert_eq!(page_mapping.block_offset, expected_block_offset);

            let hw_counter = HardwareCounterCell::new();
            let stored_payload = storage.get_value::<Random>(0, &hw_counter).unwrap();
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
    let mut storage = Blobstore::<Payload>::open(MmapFs, path.clone(), Populate::No).unwrap();
    assert_eq!(storage.as_gridstore().pages.read().num_pages(), 1);

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
    let mut storage = Blobstore::<Payload>::open(MmapFs, path.clone(), Populate::No).unwrap();
    assert_eq!(storage.as_gridstore().pages.read().num_pages(), 1);

    // On reopen, delete was flushed this time, expect point to be missing
    assert!(get_payload(&storage).is_none());

    put_payload(&mut storage, "value 4", 0);
    assert_eq!(get_payload(&storage).unwrap(), "value 4");

    assert_eq!(
        storage.as_gridstore().tracker.read().pending_updates.len(),
        1,
        "expect 1 pending update",
    );

    storage.flusher()().unwrap();

    assert_eq!(
        storage.as_gridstore().tracker.read().pending_updates.len(),
        0,
        "expect 0 pending updates",
    );

    // Reopen gridstore
    drop(storage);
    let mut storage = Blobstore::<Payload>::open(MmapFs, path.clone(), Populate::No).unwrap();
    assert_eq!(storage.as_gridstore().pages.read().num_pages(), 1);

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
        let tmp_storage = Blobstore::<Payload>::open(MmapFs, path.clone(), Populate::No).unwrap();
        assert_eq!(get_payload(&tmp_storage).unwrap(), "value 4");
    }

    // First flusher flushed, expect to read value 5 if we load from disk
    flusher_1_value_5().unwrap();
    {
        let tmp_storage = Blobstore::<Payload>::open(MmapFs, path.clone(), Populate::No).unwrap();
        assert_eq!(get_payload(&tmp_storage).unwrap(), "value 5");
    }

    // Second flusher flushed, expect point to be missing if we load from disk
    flusher_2_delete().unwrap();
    {
        let tmp_storage = Blobstore::<Payload>::open(MmapFs, path.clone(), Populate::No).unwrap();
        assert!(get_payload(&tmp_storage).is_none());
    }

    // Third flusher flushed, expect to read value 6 if we load from disk
    flusher_3_value_6().unwrap();
    {
        let tmp_storage = Blobstore::<Payload>::open(MmapFs, path, Populate::No).unwrap();
        assert_eq!(get_payload(&tmp_storage).unwrap(), "value 6");
    }

    // Main storage still isn't flushed, but has value 7
    assert_eq!(get_payload(&storage).unwrap(), "value 7");
    assert_eq!(
        storage.as_gridstore().tracker.read().pending_updates.len(),
        1,
        "expect 1 pending update",
    );
}

/// Test that `BlobstoreReader::live_reload` picks up new data written by a `Blobstore`.
///
/// Scenario:
/// 1. Create a writable Blobstore, write some data, flush.
/// 2. Open a BlobstoreReader on the same path.
/// 3. Verify reader sees initial data.
/// 4. Write more data via Blobstore, flush.
/// 5. Call `live_reload` on reader, verify it sees new data.
/// 6. Also test that live_reload is a no-op when nothing changed.
#[rstest]
fn test_live_reload(#[values(Mode::Dynamic, Mode::AppendOnly)] mode: Mode) {
    let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
    let path = dir.path().to_path_buf();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

    let make_payload = |key: &str, value: &str| -> Payload {
        let mut payload = Payload::default();
        payload.0.insert(
            key.to_string(),
            serde_json::Value::String(value.to_string()),
        );
        payload
    };

    // Step 1: Write initial data and flush
    let options = StorageOptions {
        mode: Some(mode),
        ..Default::default()
    };
    let mut storage = Blobstore::<_>::new(MmapFs, path.clone(), options).unwrap();

    let payload_0 = make_payload("key", "value_0");
    let payload_1 = make_payload("key", "value_1");
    storage.put_value(0, &payload_0, hw_counter_ref).unwrap();
    storage.put_value(1, &payload_1, hw_counter_ref).unwrap();
    storage.flusher()().unwrap();

    // Step 2: Open a reader
    let mut reader =
        BlobstoreReader::<Payload, MmapFile>::open(&MmapFs, path.clone(), Populate::No).unwrap();
    assert_eq!(reader.max_point_offset().unwrap(), 2);

    // Step 3: Verify reader sees initial data
    let v0 = reader.get_value::<Random>(0, &hw_counter).unwrap();
    assert_eq!(v0.as_ref(), Some(&payload_0));
    let v1 = reader.get_value::<Random>(1, &hw_counter).unwrap();
    assert_eq!(v1.as_ref(), Some(&payload_1));

    // Step 4: live_reload when nothing changed must keep serving the same data
    reader.live_reload(&MmapFs).unwrap();
    let v0 = reader.get_value::<Random>(0, &hw_counter).unwrap();
    assert_eq!(v0.as_ref(), Some(&payload_0));

    // Step 5: Write more data via writable storage and flush
    let payload_2 = make_payload("key", "value_2");
    let payload_3 = make_payload("key", "value_3");
    storage.put_value(2, &payload_2, hw_counter_ref).unwrap();
    storage.put_value(3, &payload_3, hw_counter_ref).unwrap();
    storage.flusher()().unwrap();

    // Step 6: live_reload should make new data accessible
    reader.live_reload(&MmapFs).unwrap();
    assert_eq!(reader.max_point_offset().unwrap(), 4);
    let v2 = reader.get_value::<Random>(2, &hw_counter).unwrap();
    assert_eq!(v2.as_ref(), Some(&payload_2));
    let v3 = reader.get_value::<Random>(3, &hw_counter).unwrap();
    assert_eq!(v3.as_ref(), Some(&payload_3));

    // Original data should still be readable
    let v0 = reader.get_value::<Random>(0, &hw_counter).unwrap();
    assert_eq!(v0.as_ref(), Some(&payload_0));
    let v1 = reader.get_value::<Random>(1, &hw_counter).unwrap();
    assert_eq!(v1.as_ref(), Some(&payload_1));
}

/// Case-3 regression of the live-reload staleness audit: the reader goes
/// through a caching backend ([`DiskCacheFs`]) whose `reopen` assumes
/// append-only files. The tracker file is preallocated and mutated in place
/// (header rewrites, slot updates), so a held handle would serve the stale
/// pre-write state forever: the reader would keep reporting values as
/// missing after a live-reload. [`ReadOnlyTracker::live_reload`] opens a
/// fresh handle instead.
///
/// [`DiskCacheFs`]: common::universal_io::DiskCacheFs
/// [`ReadOnlyTracker::live_reload`]: crate::tracker::ReadOnlyTracker::live_reload
#[test]
fn test_live_reload_disk_cache() {
    use std::sync::Arc;

    use common::universal_io::{
        DiskCache, DiskCacheConfig, DiskCacheFs, DiskCacheFsContext, MmapFile, UniversalReadFileOps,
    };

    let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
    let remote_root = dir.path().join("remote");
    let local_root = dir.path().join("local");
    let path = remote_root.join("storage");
    fs::create_dir_all(&path).unwrap();
    fs::create_dir_all(&local_root).unwrap();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

    let make_payload = |key: &str, value: &str| -> Payload {
        let mut payload = Payload::default();
        payload.0.insert(
            key.to_string(),
            serde_json::Value::String(value.to_string()),
        );
        payload
    };

    // The writer works on the "remote" directly; the reader mirrors it into
    // `local_root` through the disk cache.
    let mut storage = Blobstore::<_>::new(MmapFs, path.clone(), Default::default()).unwrap();

    let payload_0 = make_payload("key", "value_0");
    storage.put_value(0, &payload_0, hw_counter_ref).unwrap();
    storage.flusher()().unwrap();

    let cache_fs = DiskCacheFs::<MmapFile>::from_context(DiskCacheFsContext {
        config: Arc::new(DiskCacheConfig::new(remote_root, local_root).unwrap()),
        remote: Default::default(),
    })
    .unwrap();
    let mut reader = BlobstoreReader::<Payload, DiskCache<MmapFile>>::open(
        &cache_fs,
        path.clone(),
        Populate::No,
    )
    .unwrap();

    // Read through the cache so the tracker's header/slot blocks are mirrored
    // locally — the staleness this test guards against only occurs once the
    // pre-write state is cached.
    assert_eq!(reader.max_point_offset().unwrap(), 1);
    let v0 = reader.get_value::<Random>(0, &hw_counter).unwrap();
    assert_eq!(v0.as_ref(), Some(&payload_0));

    // Write another value; the tracker header (and slot block 0) are
    // rewritten in place, the pages grow.
    let payload_1 = make_payload("key", "value_1");
    storage.put_value(1, &payload_1, hw_counter_ref).unwrap();
    storage.flusher()().unwrap();

    reader.live_reload(&cache_fs).unwrap();

    assert_eq!(reader.max_point_offset().unwrap(), 2);
    let v1 = reader.get_value::<Random>(1, &hw_counter).unwrap();
    assert_eq!(v1.as_ref(), Some(&payload_1));
    let v0 = reader.get_value::<Random>(0, &hw_counter).unwrap();
    assert_eq!(v0.as_ref(), Some(&payload_0));
}

/// Test that `live_reload` works across page boundaries.
///
/// Writes enough data to fill pages, then writes more data that creates new pages,
/// and verifies that live_reload picks up the new pages too.
#[test]
fn test_live_reload_across_pages() {
    use crate::config::DEFAULT_BLOCK_SIZE_BYTES;
    use crate::fixtures::minimal_payload;

    let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
    let path = dir.path().to_path_buf();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

    // Use small page size to force multiple pages
    let page_size = DEFAULT_BLOCK_SIZE_BYTES * DEFAULT_REGION_SIZE_BLOCKS;
    let options = StorageOptions {
        page_size_bytes: Some(page_size),
        ..Default::default()
    };
    let mut storage = Blobstore::<_>::new(MmapFs, path.clone(), options).unwrap();

    let payload = minimal_payload();

    // Fill one page
    let blocks_per_page = page_size / DEFAULT_BLOCK_SIZE_BYTES;
    let first_batch = blocks_per_page as u32;
    for i in 0..first_batch {
        storage.put_value(i, &payload, hw_counter_ref).unwrap();
    }
    storage.flusher()().unwrap();

    let initial_pages = storage.as_gridstore().pages.read().num_pages();

    // Open reader
    let mut reader =
        BlobstoreReader::<Payload, MmapFile>::open(&MmapFs, path.clone(), Populate::No).unwrap();
    assert_eq!(reader.max_point_offset().unwrap(), first_batch);

    // Verify reader can read all initial data
    for i in 0..first_batch {
        let v = reader.get_value::<Random>(i, &hw_counter).unwrap();
        assert_eq!(v.as_ref(), Some(&payload), "missing point {i}");
    }

    // Write more data that should create new pages
    let second_batch = blocks_per_page as u32;
    for i in first_batch..(first_batch + second_batch) {
        storage.put_value(i, &payload, hw_counter_ref).unwrap();
    }
    storage.flusher()().unwrap();

    let new_pages = storage.as_gridstore().pages.read().num_pages();
    assert!(
        new_pages > initial_pages,
        "expected more pages after second batch: {new_pages} vs {initial_pages}"
    );

    // NOTE: no read of the second batch before the reload — over a
    // read-through backend (mmap) the tracker already sees the new slots,
    // whose pointers reference pages the reader has not attached yet.
    // Reading them would panic on the page index rather than return None.

    // Live reload should pick up new pages and data
    reader.live_reload(&MmapFs).unwrap();
    assert_eq!(
        reader.max_point_offset().unwrap(),
        first_batch + second_batch
    );

    // Verify all data is readable
    for i in 0..(first_batch + second_batch) {
        let v = reader.get_value::<Random>(i, &hw_counter).unwrap();
        assert_eq!(v.as_ref(), Some(&payload), "missing point {i} after reload");
    }
}

/// Test that data is only actually flushed when the Blobstore instance is still valid
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
    let put_payload = |storage: &mut Blobstore<Payload>, point_offset: u32, payload_value: &str| {
        let mut payload = Payload::default();
        payload.0.insert(
            "key".to_string(),
            serde_json::Value::String(payload_value.to_string()),
        );

        storage
            .put_value(point_offset, &payload, hw_counter_ref)
            .unwrap();
        assert_eq!(storage.as_gridstore().pages.read().num_pages(), 1);

        let hw_counter = HardwareCounterCell::new();
        let stored_payload = storage
            .get_value::<Random>(point_offset, &hw_counter)
            .unwrap();
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);
    };

    // Write enough new pointers so that they don't fit in the default tracker file size
    // On flush, the tracker file will be resized and reopened, significant for this test
    let file_size = storage
        .as_gridstore()
        .tracker
        .read()
        .mmap_file_size()
        .unwrap();
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
        Arc::clone(&storage.as_gridstore().pages),
        Arc::clone(&storage.as_gridstore().tracker),
        Arc::clone(&storage.as_gridstore().bitmask),
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
    let storage = Blobstore::<Payload>::open(MmapFs, path.clone(), Populate::No).unwrap();
    assert_eq!(storage.as_gridstore().pages.read().num_pages(), 1);
    assert!(storage.get_pointer(0).is_none(), "point must not exist");
    assert_eq!(storage.max_point_offset(), 0, "must have zero points");
}

/// `Pages::read_batch_from_pages` must yield the same bytes as calling
/// `read_from_pages` for each pointer individually. Covers the small/multi-page mix
/// and synthetic zero-length pointers.
#[test]
fn test_read_batch_from_pages_congruent_with_read_from_pages() {
    // Use small pages so larger payloads span multiple pages.
    let page_size = DEFAULT_BLOCK_SIZE_BYTES * DEFAULT_REGION_SIZE_BLOCKS;
    let (_dir, mut storage) = empty_storage_sized(page_size, Compression::None);

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

    let rng = &mut rand::make_rng::<rand::rngs::SmallRng>();

    // Mix of payload sizes so we exercise both single-page and multi-page reads.
    let num_payloads = 50u32;
    for point_offset in 0..num_payloads {
        let size_factor = (point_offset % 8) as usize + 1;
        let payload = random_payload(rng, size_factor);
        storage
            .put_value(point_offset, &payload, hw_counter_ref)
            .unwrap();
    }

    let mut pointers: Vec<ValuePointer> = (0..num_payloads)
        .map(|i| storage.get_pointer(i).unwrap())
        .collect();

    // Synthetic zero-length pointer to exercise that branch.
    pointers.push(ValuePointer::new(0, 0, 0));

    pointers.shuffle(rng);

    let pages = storage.as_gridstore().pages.read();

    let single_results: Vec<Vec<u8>> = pointers
        .iter()
        .map(|ptr| {
            pages
                .read_from_pages::<Random>(*ptr, &storage.as_gridstore().config)
                .unwrap()
                .into_owned()
        })
        .collect();

    let mut batch_results: Vec<Option<Vec<u8>>> = vec![None; pointers.len()];

    pages
        .read_batch_from_pages::<Random, _, GridstoreError>(
            &storage.as_gridstore().config,
            pointers.into_iter().enumerate(),
            |idx, bytes| {
                batch_results[idx] = Some(bytes.into_owned());
                Ok(true)
            },
        )
        .unwrap();

    for (i, single) in single_results.iter().enumerate() {
        assert_eq!(
            batch_results[i].as_ref(),
            Some(single),
            "batch read mismatch at idx {i}, len {}",
            single.len(),
        );
    }
}

/// `BlobstoreView::for_each_in_batch` must yield the same values as calling
/// `get_value` per offset, including missing/out-of-range offsets that should be
/// silently skipped (matching `get_value`'s `Ok(None)`).
#[rstest]
fn test_for_each_in_batch_congruent_with_get_value(
    #[values(Mode::Dynamic, Mode::AppendOnly)] mode: Mode,
) {
    let (_dir, mut storage) = empty_storage_mode(mode);

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

    let rng = &mut rand::make_rng::<rand::rngs::SmallRng>();

    // A few missing point offsets to create gaps
    let missing = [3u32, 17, 42, 88];

    let num_payloads = 100u32;
    for point_offset in 0..num_payloads {
        // In append-only mode deletes are unsupported, create the gaps by skipping puts instead
        if mode == Mode::AppendOnly && missing.contains(&point_offset) {
            continue;
        }

        let size_factor = (point_offset % 5) as usize + 1;
        let payload = random_payload(rng, size_factor);
        storage
            .put_value(point_offset, &payload, hw_counter_ref)
            .unwrap();
    }

    // Delete a few values to create gaps.
    if mode == Mode::Dynamic {
        for &id in &missing {
            storage.delete_value(id).unwrap();
        }
    }

    // Build offsets including deleted points and out-of-range ones.
    let mut offsets: Vec<u32> = (0..num_payloads).collect();
    offsets.push(num_payloads + 5);
    offsets.push(num_payloads + 100);
    offsets.shuffle(rng);

    let single_results: Vec<Option<Payload>> = offsets
        .iter()
        .map(|&o| storage.get_value::<Random>(o, &hw_counter).unwrap())
        .collect();

    let mut batch_results: Vec<Option<Payload>> = vec![None; offsets.len()];
    storage
        .read_values::<Random, _, GridstoreError>(
            offsets.iter().copied().enumerate(),
            |idx, _, value| {
                batch_results[idx] = value;
                Ok(())
            },
            hw_counter.payload_io_read_counter(),
        )
        .unwrap();

    for (i, (single, batch)) in single_results.iter().zip(&batch_results).enumerate() {
        assert_eq!(single, batch, "mismatch at idx {i} (offset {})", offsets[i]);
    }
}

/// Regression: a value that is persisted on disk and then deleted in memory (a pending
/// pointer-unset that hasn't been flushed) must read back as absent through *both* the single
/// `get_value` path and the batched `read_values` path. The batched tracker lookup used to skip
/// the `pending.current == None` case and fall through to the stale persisted pointer, so
/// `read_values` resurrected the old value while `get_value` correctly reported it gone.
#[test]
fn test_batch_read_honors_pending_unset_of_flushed_value() {
    let (_dir, mut storage) = empty_storage();

    let hw_counter = HardwareCounterCell::new();
    let hw_write = hw_counter.ref_payload_io_write_counter();

    let rng = &mut rand::make_rng::<rand::rngs::SmallRng>();

    // Write and FLUSH so the pointer is persisted on disk.
    let payload = random_payload(rng, 3);
    storage.put_value(0, &payload, hw_write).unwrap();
    storage.flusher()().unwrap();

    // Delete WITHOUT flushing: leaves a pending pointer-unset (`current == None`) over a
    // persisted pointer — the exact precondition the batched read must respect.
    storage.delete_value(0).unwrap();

    let single = storage.get_value::<Random>(0, &hw_counter).unwrap();
    assert_eq!(single, None, "single get must see the pending delete");

    let mut batch = [Some(Payload::default())];
    storage
        .read_values::<Random, _, GridstoreError>(
            std::iter::once((0usize, 0u32)),
            |idx, _, value| {
                batch[idx] = value;
                Ok(())
            },
            hw_counter.payload_io_read_counter(),
        )
        .unwrap();
    assert_eq!(
        batch[0], None,
        "batched read must also see the pending delete, not the stale persisted value",
    );
}

/// Preopen must schedule every file `open` reads through the backend, in both operating modes.
///
/// Merely opening after a preopen proves nothing: `CachedFs` falls back to a regular open for
/// paths that were never scheduled. Instead, delete the backend-read files after the preopen —
/// the open can then only succeed if it is served from the prefetch pool.
#[rstest]
fn test_preopen_schedules_files_for_open(#[values(Mode::Dynamic, Mode::AppendOnly)] mode: Mode) {
    use common::universal_io::{
        CachedFs, CachedReadFs, Populate, ReadOnly, UniversalRead, UniversalReadFileOps,
    };

    let hw_counter = HardwareCounterCell::new();

    // Build a storage, write one value, flush, then drop it.
    let (dir, mut storage) = empty_storage_mode(mode);
    let payload = minimal_payload();
    storage
        .put_value(0, &payload, hw_counter.ref_payload_io_write_counter())
        .unwrap();
    storage.flusher()().unwrap();
    drop(storage);

    // Same order as the segment open path: snapshot the file listing, preopen, then open.
    type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
    let fs = RoFs::from_context(Default::default()).unwrap();
    let mut cached_fs = CachedFs::new(fs, dir.path()).unwrap();
    cached_fs.cache_file_info().unwrap();
    BlobstoreReader::<Payload, ReadOnly<MmapFile>>::preopen(
        &cached_fs,
        dir.path().to_path_buf(),
        Populate::No,
    )
    .unwrap();

    // Delete everything `open` reads through the backend. The append-only tracker is read
    // directly from disk, not through the backend, so it must stay.
    let backend_read_files: &[&str] = match mode {
        Mode::Dynamic => &["config.json", "tracker.dat", "page_0.dat"],
        Mode::AppendOnly => &["config.json", "append_only_page_0.dat"],
    };
    for file in backend_read_files {
        fs::remove_file(dir.path().join(file)).unwrap();
    }

    let reader = BlobstoreReader::<Payload, ReadOnly<MmapFile>>::open(
        &cached_fs,
        dir.path().to_path_buf(),
        Populate::No,
    )
    .unwrap();
    assert_eq!(
        reader.get_value::<Random>(0, &hw_counter).unwrap(),
        Some(payload),
    );
}

/// Opening a [`BlobstoreReader`] must not require a writable backend: a reader
/// only reads. Backing it with the write-enforced `ReadOnly<MmapFile>` (which
/// `debug_assert!`s every open is non-writable) only succeeds if the reader
/// opens its pages and tracker read-only.
#[test]
fn read_only_reader_over_write_enforced_backend() {
    use common::universal_io::{MmapFile, ReadOnly, UniversalRead, UniversalReadFileOps};

    let hw_counter = HardwareCounterCell::new();

    // Build a writable gridstore, write one value, flush, then drop it.
    let (dir, mut storage) = empty_storage();
    let mut payload = Payload::default();
    payload
        .0
        .insert("k".to_string(), serde_json::Value::String("v".to_string()));
    storage
        .put_value(0, &payload, hw_counter.ref_payload_io_write_counter())
        .unwrap();
    storage.flusher()().unwrap();
    drop(storage);

    // Reopen read-only over the write-enforced backend.
    type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
    let fs = RoFs::from_context(Default::default()).unwrap();
    let reader = BlobstoreReader::<Payload, ReadOnly<MmapFile>>::open(
        &fs,
        dir.path().to_path_buf(),
        Populate::No,
    )
    .unwrap();

    let stored = reader.get_value::<Random>(0, &hw_counter).unwrap();
    assert_eq!(stored, Some(payload));
}
