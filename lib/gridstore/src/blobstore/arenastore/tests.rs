use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::universal_io::{MmapFile, MmapFs, Populate};
use fs_err as fs;
use tempfile::TempDir;

use crate::blobstore::reader::CONFIG_FILENAME;
use crate::config::{Compression, DEFAULT_BLOCK_SIZE_BYTES, Mode, StorageOptions};
use crate::error::GridstoreError;
use crate::fixtures::{Payload, empty_storage_append_only, random_payload};
use crate::tracker::ValuePointer;
use crate::{Blobstore, BlobstoreReader, direct_io};

/// Size in bytes of a single mapping entry in the tracker file
const TRACKER_ENTRY_SIZE: u64 = 16;

/// Create an empty append-only storage of raw byte values, for precise size assertions.
fn empty_byte_storage(compression: Compression) -> (TempDir, Blobstore<Vec<u8>>) {
    let dir = TempDir::new().unwrap();
    let options = StorageOptions {
        compression: Some(compression),
        mode: Some(Mode::AppendOnly),
        ..Default::default()
    };
    let storage = Blobstore::new(MmapFs, dir.path().to_path_buf(), options).unwrap();
    (dir, storage)
}

fn tracker_file_len(dir: &TempDir) -> u64 {
    fs::metadata(dir.path().join("append_only_tracker.dat"))
        .unwrap()
        .len()
}

#[test]
fn test_empty_storage() {
    let (dir, storage) = empty_storage_append_only();

    let hw_counter = HardwareCounterCell::new();
    assert_eq!(storage.get_value::<Random>(0, &hw_counter).unwrap(), None);
    assert_eq!(storage.max_point_offset(), 0);
    assert_eq!(storage.get_storage_size_bytes().unwrap(), 0);

    // Only three files: tracker, page and config
    let files = storage.files();
    assert_eq!(files.len(), 3, "Expected 3 files, got {files:?}");
    assert_eq!(files[0].file_name().unwrap(), "append_only_tracker.dat");
    assert_eq!(files[1].file_name().unwrap(), "append_only_page_0.dat");
    assert_eq!(files[2].file_name().unwrap(), "config.json");
    let actual_files = fs::read_dir(dir.path()).unwrap().count();
    assert_eq!(files.len(), actual_files);

    let immutable_files = storage.immutable_files();
    assert_eq!(immutable_files.len(), 1);
    assert_eq!(immutable_files[0].file_name().unwrap(), "config.json");

    // Both files start empty, they are not preallocated
    assert_eq!(tracker_file_len(&dir), 0);
    assert_eq!(
        fs::metadata(dir.path().join("append_only_page_0.dat"))
            .unwrap()
            .len(),
        0,
    );
}

#[rstest::rstest]
#[case(Compression::None)]
#[case(Compression::LZ4)]
fn test_put_get_roundtrip(#[case] compression: Compression) {
    let dir = TempDir::new().unwrap();
    let options = StorageOptions {
        compression: Some(compression),
        mode: Some(Mode::AppendOnly),
        ..Default::default()
    };
    let mut storage = Blobstore::<Payload>::new(MmapFs, dir.path().to_path_buf(), options).unwrap();

    let rng = &mut rand::make_rng::<rand::rngs::SmallRng>();
    let payloads = (0..100)
        .map(|point_offset| (point_offset, random_payload(rng, 2)))
        .collect::<Vec<_>>();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    for (point_offset, payload) in &payloads {
        let is_update = storage
            .put_value(*point_offset, payload, hw_counter_ref)
            .unwrap();
        assert!(!is_update);
    }
    assert!(hw_counter.payload_io_write_counter().get() > 0);
    assert_eq!(storage.max_point_offset(), 100);

    for (point_offset, payload) in &payloads {
        let stored = storage
            .get_value::<Random>(*point_offset, &hw_counter)
            .unwrap();
        assert_eq!(stored.as_ref(), Some(payload));
    }

    storage.flusher()().unwrap();

    // The tracker file length matches the exact number of mappings
    assert_eq!(tracker_file_len(&dir), 100 * TRACKER_ENTRY_SIZE);

    // Everything is still there after reopening, the mode is selected automatically
    drop(storage);
    let storage =
        Blobstore::<Payload>::open(MmapFs, dir.path().to_path_buf(), Populate::No).unwrap();
    storage.as_arenastore();
    assert_eq!(storage.max_point_offset(), 100);
    for (point_offset, payload) in &payloads {
        let stored = storage
            .get_value::<Random>(*point_offset, &hw_counter)
            .unwrap();
        assert_eq!(stored.as_ref(), Some(payload));
    }
}

/// Puts buffer both the value data and the mapping in memory: nothing lands on disk until a
/// flush, which batches everything into a single write per file.
#[test]
fn test_put_buffers_value_and_mapping_until_flush() {
    let dir = TempDir::new().unwrap();
    let options = StorageOptions {
        compression: Some(Compression::None),
        mode: Some(Mode::AppendOnly),
        ..Default::default()
    };
    let mut storage = Blobstore::<Vec<u8>>::new(MmapFs, dir.path().to_path_buf(), options).unwrap();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

    let page_path = dir.path().join("append_only_page_0.dat");
    let tracker_path = dir.path().join("append_only_tracker.dat");

    for point_offset in 0..3 {
        storage
            .put_value(point_offset, &vec![point_offset as u8; 100], hw_counter_ref)
            .unwrap();
    }

    // Nothing is written to disk until a flush
    assert_eq!(fs::metadata(&page_path).unwrap().len(), 0);
    assert_eq!(fs::metadata(&tracker_path).unwrap().len(), 0);

    // The buffered values are still fully readable, served from memory
    assert_eq!(storage.max_point_offset(), 3);
    assert_eq!(storage.get_storage_size_bytes().unwrap(), 2 * 128 + 100);
    for point_offset in 0..3 {
        assert_eq!(
            storage
                .get_value::<Random>(point_offset, &hw_counter)
                .unwrap(),
            Some(vec![point_offset as u8; 100]),
        );
    }

    // The flush lands all buffered values and mappings at once, in exactly their extents
    storage.flusher()().unwrap();
    assert_eq!(
        fs::metadata(&page_path).unwrap().len(),
        2 * 128 + 100,
        "the page file must hold exactly the appended values",
    );
    assert_eq!(
        fs::metadata(&tracker_path).unwrap().len(),
        3 * TRACKER_ENTRY_SIZE,
        "the tracker file must hold exactly the appended mappings",
    );

    // The values remain readable, now served from the files, and survive a reopen
    for point_offset in 0..3 {
        assert_eq!(
            storage
                .get_value::<Random>(point_offset, &hw_counter)
                .unwrap(),
            Some(vec![point_offset as u8; 100]),
        );
    }
    drop(storage);
    let storage =
        Blobstore::<Vec<u8>>::open(MmapFs, dir.path().to_path_buf(), Populate::No).unwrap();
    assert_eq!(storage.max_point_offset(), 3);
    for point_offset in 0..3 {
        assert_eq!(
            storage
                .get_value::<Random>(point_offset, &hw_counter)
                .unwrap(),
            Some(vec![point_offset as u8; 100]),
        );
    }
}

#[test]
fn test_put_rejects_out_of_order_point_offsets() {
    let (_dir, mut storage) = empty_byte_storage(Compression::None);

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

    storage.put_value(0, &vec![1; 100], hw_counter_ref).unwrap();
    assert_eq!(storage.get_storage_size_bytes().unwrap(), 100);

    // Putting the same point offset twice is rejected
    let err = storage
        .put_value(0, &vec![2; 100], hw_counter_ref)
        .unwrap_err();
    assert!(matches!(err, GridstoreError::UnsupportedOperation { .. }));

    storage.put_value(2, &vec![3; 100], hw_counter_ref).unwrap();

    // Putting a lower point offset is rejected, even if it was never set
    let err = storage
        .put_value(1, &vec![4; 100], hw_counter_ref)
        .unwrap_err();
    assert!(matches!(err, GridstoreError::UnsupportedOperation { .. }));

    // Rejected puts must not append any value data
    assert_eq!(storage.get_storage_size_bytes().unwrap(), 128 + 100);

    // The stored values are unaffected
    assert_eq!(
        storage.get_value::<Random>(0, &hw_counter).unwrap(),
        Some(vec![1; 100]),
    );
    assert_eq!(
        storage.get_value::<Random>(2, &hw_counter).unwrap(),
        Some(vec![3; 100]),
    );
}

#[test]
fn test_delete_is_rejected() {
    let (_dir, mut storage) = empty_storage_append_only();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    storage
        .put_value(0, &Payload::default(), hw_counter_ref)
        .unwrap();

    let err = storage.delete_value(0).unwrap_err();
    assert!(matches!(err, GridstoreError::UnsupportedOperation { .. }));

    // The value is still there
    assert_eq!(
        storage.get_value::<Random>(0, &hw_counter).unwrap(),
        Some(Payload::default()),
    );
}

#[test]
fn test_skipped_point_offsets_read_as_none() {
    let (_dir, mut storage) = empty_byte_storage(Compression::None);

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

    for point_offset in [0, 3, 4, 10] {
        storage
            .put_value(point_offset, &vec![point_offset as u8; 10], hw_counter_ref)
            .unwrap();
    }
    assert_eq!(storage.max_point_offset(), 11);

    for point_offset in [1, 2, 5, 6, 7, 8, 9, 11] {
        assert_eq!(
            storage
                .get_value::<Random>(point_offset, &hw_counter)
                .unwrap(),
            None,
        );
    }

    // Iteration skips the gaps
    let mut collected = Vec::new();
    storage
        .iter(
            |point_offset, value: Vec<u8>| {
                collected.push((point_offset, value));
                Ok::<_, GridstoreError>(true)
            },
            hw_counter.ref_payload_io_read_counter(),
        )
        .unwrap();
    assert_eq!(
        collected,
        vec![
            (0, vec![0; 10]),
            (3, vec![3; 10]),
            (4, vec![4; 10]),
            (10, vec![10; 10]),
        ]
    );

    // Iteration can stop early
    let mut count = 0;
    storage
        .iter(
            |_, _: Vec<u8>| {
                count += 1;
                Ok::<_, GridstoreError>(false)
            },
            hw_counter.ref_payload_io_read_counter(),
        )
        .unwrap();
    assert_eq!(count, 1);

    // Batched reads yield None for gaps and out of range point offsets
    let mut collected = Vec::new();
    storage
        .read_values::<Random, _, GridstoreError>(
            [0, 1, 5, 10, 3, 20].iter().map(|&offset| ((), offset)),
            |_, point_offset, value| {
                collected.push((point_offset, value));
                Ok(())
            },
            hw_counter.payload_io_read_counter(),
        )
        .unwrap();
    assert_eq!(
        collected,
        vec![
            (0, Some(vec![0; 10])),
            (1, None),
            (5, None),
            (10, Some(vec![10; 10])),
            (3, Some(vec![3; 10])),
            (20, None),
        ]
    );
}

#[test]
fn test_values_are_block_aligned() {
    let (_dir, mut storage) = empty_byte_storage(Compression::None);

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

    // Each value starts at a block boundary (128 byte default), without trailing padding
    storage.put_value(0, &vec![1; 100], hw_counter_ref).unwrap();
    assert_eq!(storage.get_storage_size_bytes().unwrap(), 100);
    storage.put_value(1, &vec![2; 50], hw_counter_ref).unwrap();
    assert_eq!(storage.get_storage_size_bytes().unwrap(), 128 + 50);
    storage.put_value(2, &vec![3; 300], hw_counter_ref).unwrap();
    assert_eq!(storage.get_storage_size_bytes().unwrap(), 256 + 300);

    let pointer = storage.get_pointer(1).unwrap();
    assert_eq!(pointer.page_id, 0);
    assert_eq!(pointer.block_offset, 1);
    assert_eq!(pointer.length, 50);
    let pointer = storage.get_pointer(2).unwrap();
    assert_eq!(pointer.block_offset, 2);

    for (point_offset, value) in [(0, vec![1; 100]), (1, vec![2; 50]), (2, vec![3; 300])] {
        assert_eq!(
            storage
                .get_value::<Random>(point_offset, &hw_counter)
                .unwrap(),
            Some(value),
        );
    }
}

#[test]
fn test_empty_and_huge_values() {
    let (_dir, mut storage) = empty_byte_storage(Compression::None);

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

    // An empty value takes no space at all
    storage.put_value(0, &vec![], hw_counter_ref).unwrap();
    assert_eq!(storage.get_storage_size_bytes().unwrap(), 0);
    assert_eq!(
        storage.get_value::<Random>(0, &hw_counter).unwrap(),
        Some(vec![]),
    );

    // A huge value simply grows the single page file, values never span pages
    let huge = (0..2_000_000).map(|i| i as u8).collect::<Vec<u8>>();
    storage.put_value(1, &huge, hw_counter_ref).unwrap();
    assert_eq!(storage.get_storage_size_bytes().unwrap(), huge.len());
    assert_eq!(
        storage.get_value::<Random>(1, &hw_counter).unwrap(),
        Some(huge),
    );
}

#[test]
fn test_unflushed_puts_are_lost_after_reopen() {
    let dir = TempDir::new().unwrap();
    let options = StorageOptions {
        compression: Some(Compression::None),
        mode: Some(Mode::AppendOnly),
        ..Default::default()
    };
    let mut storage = Blobstore::<Vec<u8>>::new(MmapFs, dir.path().to_path_buf(), options).unwrap();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

    // Both the value data and the mappings are buffered in memory until the next flush
    for point_offset in 0..3 {
        storage
            .put_value(point_offset, &vec![7; 100], hw_counter_ref)
            .unwrap();
    }
    drop(storage);

    // Without a flush, the puts are gone after reopening, and left nothing behind on disk
    let mut storage =
        Blobstore::<Vec<u8>>::open(MmapFs, dir.path().to_path_buf(), Populate::No).unwrap();
    assert_eq!(storage.max_point_offset(), 0);
    assert_eq!(storage.get_value::<Random>(0, &hw_counter).unwrap(), None);
    assert_eq!(storage.get_storage_size_bytes().unwrap(), 0);

    // New puts start from a clean page file
    storage.put_value(0, &vec![9; 10], hw_counter_ref).unwrap();
    storage.flusher()().unwrap();
    assert_eq!(storage.get_storage_size_bytes().unwrap(), 10);
    assert_eq!(
        storage.get_value::<Random>(0, &hw_counter).unwrap(),
        Some(vec![9; 10]),
    );
}

#[test]
fn test_stale_flusher_is_noop() {
    let dir = TempDir::new().unwrap();
    let options = StorageOptions {
        mode: Some(Mode::AppendOnly),
        ..Default::default()
    };
    let mut storage = Blobstore::<Payload>::new(MmapFs, dir.path().to_path_buf(), options).unwrap();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

    for point_offset in 0..3 {
        storage
            .put_value(point_offset, &Payload::default(), hw_counter_ref)
            .unwrap();
    }
    let stale_flusher = storage.flusher();

    storage
        .put_value(3, &Payload::default(), hw_counter_ref)
        .unwrap();
    storage.flusher()().unwrap();
    assert_eq!(tracker_file_len(&dir), 4 * TRACKER_ENTRY_SIZE);
    let page_len = fs::metadata(dir.path().join("append_only_page_0.dat"))
        .unwrap()
        .len();

    // The stale flusher must not write anything again, its values and mappings are already
    // persisted
    stale_flusher().unwrap();
    assert_eq!(tracker_file_len(&dir), 4 * TRACKER_ENTRY_SIZE);
    assert_eq!(
        fs::metadata(dir.path().join("append_only_page_0.dat"))
            .unwrap()
            .len(),
        page_len,
    );

    drop(storage);
    let storage =
        Blobstore::<Payload>::open(MmapFs, dir.path().to_path_buf(), Populate::No).unwrap();
    assert_eq!(storage.max_point_offset(), 4);
    for point_offset in 0..4 {
        assert_eq!(
            storage
                .get_value::<Random>(point_offset, &hw_counter)
                .unwrap(),
            Some(Payload::default()),
        );
    }
}

#[test]
fn test_flusher_after_clear_is_cancelled() {
    let (_dir, mut storage) = empty_storage_append_only();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    storage
        .put_value(0, &Payload::default(), hw_counter_ref)
        .unwrap();

    let flusher = storage.flusher();
    storage.clear().unwrap();

    let err = flusher().unwrap_err();
    assert!(matches!(err, GridstoreError::FlushCancelled));
}

#[test]
fn test_clear_preserves_append_only_mode() {
    let (dir, mut storage) = empty_storage_append_only();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    for point_offset in 0..3 {
        storage
            .put_value(point_offset, &Payload::default(), hw_counter_ref)
            .unwrap();
    }

    storage.clear().unwrap();
    assert_eq!(storage.max_point_offset(), 0);
    assert_eq!(storage.get_storage_size_bytes().unwrap(), 0);

    // The storage is usable again, and putting restarts at point offset zero
    storage
        .put_value(0, &Payload::default(), hw_counter_ref)
        .unwrap();
    storage.flusher()().unwrap();

    // The recreated storage is still in append-only mode after reopening
    drop(storage);
    let storage =
        Blobstore::<Payload>::open(MmapFs, dir.path().to_path_buf(), Populate::No).unwrap();
    storage.as_arenastore();
    assert_eq!(storage.max_point_offset(), 1);
}

#[test]
fn test_wipe_removes_all_files() {
    let (dir, mut storage) = empty_storage_append_only();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    storage
        .put_value(0, &Payload::default(), hw_counter_ref)
        .unwrap();

    storage.wipe().unwrap();
    assert!(!dir.path().exists());
}

#[test]
fn test_open_or_create_keeps_mode_on_disk() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("storage");

    let options = StorageOptions {
        mode: Some(Mode::AppendOnly),
        ..Default::default()
    };
    let storage =
        Blobstore::<Payload>::open_or_create(MmapFs, path.clone(), options, Populate::No).unwrap();
    storage.as_arenastore();
    drop(storage);

    // Opening again ignores the create options, the mode comes from the persisted config
    let storage =
        Blobstore::<Payload>::open_or_create(MmapFs, path, StorageOptions::default(), Populate::No)
            .unwrap();
    storage.as_arenastore();
}

#[test]
fn test_reader_on_append_only_storage() {
    let dir = TempDir::new().unwrap();
    let options = StorageOptions {
        compression: Some(Compression::None),
        mode: Some(Mode::AppendOnly),
        ..Default::default()
    };
    let mut storage = Blobstore::<Vec<u8>>::new(MmapFs, dir.path().to_path_buf(), options).unwrap();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    for point_offset in [0, 1, 4] {
        storage
            .put_value(point_offset, &vec![point_offset as u8; 10], hw_counter_ref)
            .unwrap();
    }
    storage.flusher()().unwrap();

    // The reader selects the append-only mode automatically
    let reader =
        BlobstoreReader::<Vec<u8>, MmapFile>::open(&MmapFs, dir.path().to_path_buf(), Populate::No)
            .unwrap();
    assert_eq!(reader.max_point_offset().unwrap(), 5);
    assert!(reader.is_on_disk());
    // Three values, packed at consecutive blocks: point offset gaps take no page space
    assert_eq!(reader.get_storage_size_bytes(), 2 * 128 + 10);
    reader.clear_cache().unwrap();

    assert_eq!(
        reader.get_value::<Random>(0, &hw_counter).unwrap(),
        Some(vec![0; 10]),
    );
    assert_eq!(reader.get_value::<Random>(2, &hw_counter).unwrap(), None);

    // Reader files match the writer files
    let mut reader_files = reader.files();
    let mut writer_files = storage.files();
    reader_files.sort();
    writer_files.sort();
    assert_eq!(reader_files, writer_files);

    // Iteration is bounded by the given maximum id and skips gaps
    let mut collected = Vec::new();
    reader
        .iter(
            4,
            |point_offset, value: Vec<u8>| {
                collected.push((point_offset, value));
                Ok::<_, GridstoreError>(true)
            },
            hw_counter.ref_payload_io_read_counter(),
        )
        .unwrap();
    assert_eq!(collected, vec![(0, vec![0; 10]), (1, vec![1; 10])]);

    // Batched reads through the reader and its view
    let mut collected = Vec::new();
    reader
        .read_values::<Random, _, GridstoreError>(
            [(0, 0), (1, 3), (2, 4)].iter().copied(),
            |user_data, point_offset, value| {
                collected.push((user_data, point_offset, value.is_some()));
                Ok(())
            },
            hw_counter.payload_io_read_counter(),
        )
        .unwrap();
    assert_eq!(collected, vec![(0, 0, true), (1, 3, false), (2, 4, true)]);

    let view = reader.view();
    assert_eq!(view.max_point_offset().unwrap(), 5);
    assert_eq!(view.get_storage_size_bytes(), 2 * 128 + 10);
    assert_eq!(
        view.get_value::<Random>(4, &hw_counter).unwrap(),
        Some(vec![4; 10]),
    );
}

#[test]
fn test_reader_live_reload() {
    let dir = TempDir::new().unwrap();
    let options = StorageOptions {
        compression: Some(Compression::None),
        mode: Some(Mode::AppendOnly),
        ..Default::default()
    };
    let mut storage = Blobstore::<Vec<u8>>::new(MmapFs, dir.path().to_path_buf(), options).unwrap();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    for point_offset in 0..3 {
        storage
            .put_value(point_offset, &vec![point_offset as u8; 10], hw_counter_ref)
            .unwrap();
    }
    storage.flusher()().unwrap();

    let mut reader =
        BlobstoreReader::<Vec<u8>, MmapFile>::open(&MmapFs, dir.path().to_path_buf(), Populate::No)
            .unwrap();
    assert_eq!(reader.max_point_offset().unwrap(), 3);

    // Without new data, a live reload changes nothing
    reader.live_reload(&MmapFs).unwrap();
    assert_eq!(reader.max_point_offset().unwrap(), 3);

    // The writer appends more values, the reader picks them up after a live reload
    for point_offset in 3..6 {
        storage
            .put_value(point_offset, &vec![point_offset as u8; 10], hw_counter_ref)
            .unwrap();
    }
    storage.flusher()().unwrap();

    reader.live_reload(&MmapFs).unwrap();
    assert_eq!(reader.max_point_offset().unwrap(), 6);
    assert_eq!(reader.get_storage_size_bytes(), 5 * 128 + 10);
    for point_offset in 0..6 {
        assert_eq!(
            reader
                .get_value::<Random>(point_offset, &hw_counter)
                .unwrap(),
            Some(vec![point_offset as u8; 10]),
        );
    }

    // An unflushed put is buffered in the writer and puts nothing on disk, so a live reload
    // sees no change at all
    storage.put_value(6, &vec![6; 10], hw_counter_ref).unwrap();
    reader.live_reload(&MmapFs).unwrap();
    assert_eq!(reader.max_point_offset().unwrap(), 6);
    assert_eq!(reader.get_storage_size_bytes(), 5 * 128 + 10);
}

#[test]
fn test_reader_iter_many_values() {
    let dir = TempDir::new().unwrap();
    let options = StorageOptions {
        compression: Some(Compression::None),
        mode: Some(Mode::AppendOnly),
        ..Default::default()
    };
    let mut storage = Blobstore::<Vec<u8>>::new(MmapFs, dir.path().to_path_buf(), options).unwrap();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

    // More values than a single iteration batch (256)
    const COUNT: u32 = 1000;
    for point_offset in 0..COUNT {
        storage
            .put_value(
                point_offset,
                &point_offset.to_le_bytes().to_vec(),
                hw_counter_ref,
            )
            .unwrap();
    }
    storage.flusher()().unwrap();

    let reader =
        BlobstoreReader::<Vec<u8>, MmapFile>::open(&MmapFs, dir.path().to_path_buf(), Populate::No)
            .unwrap();

    let mut expected = 0;
    reader
        .iter(
            COUNT,
            |point_offset, value: Vec<u8>| {
                assert_eq!(point_offset, expected);
                assert_eq!(value, point_offset.to_le_bytes().to_vec());
                expected += 1;
                Ok::<_, GridstoreError>(true)
            },
            hw_counter.ref_payload_io_read_counter(),
        )
        .unwrap();
    assert_eq!(expected, COUNT);

    // Early stop works across batches
    let mut count = 0;
    reader
        .iter(
            COUNT,
            |_, _: Vec<u8>| {
                count += 1;
                Ok::<_, GridstoreError>(count < 300)
            },
            hw_counter.ref_payload_io_read_counter(),
        )
        .unwrap();
    assert_eq!(count, 300);
}

#[test]
fn test_open_rejects_truncated_page_file() {
    let dir = TempDir::new().unwrap();
    let options = StorageOptions {
        compression: Some(Compression::None),
        mode: Some(Mode::AppendOnly),
        ..Default::default()
    };
    let mut storage = Blobstore::<Vec<u8>>::new(MmapFs, dir.path().to_path_buf(), options).unwrap();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    for point_offset in 0..3 {
        storage
            .put_value(point_offset, &vec![7; 100], hw_counter_ref)
            .unwrap();
    }
    storage.flusher()().unwrap();
    drop(storage);

    // Truncate the page file below what the tracker references, like a partial copy would
    let page_path = dir.path().join("append_only_page_0.dat");
    let file = fs::OpenOptions::new().write(true).open(&page_path).unwrap();
    file.set_len(100).unwrap();
    drop(file);

    assert!(Blobstore::<Vec<u8>>::open(MmapFs, dir.path().to_path_buf(), Populate::No).is_err());
    assert!(
        BlobstoreReader::<Vec<u8>, MmapFile>::open(
            &MmapFs,
            dir.path().to_path_buf(),
            Populate::No,
        )
        .is_err()
    );
}

#[test]
fn test_read_from_pages_rejects_unknown_page() {
    let (dir, mut storage) = empty_storage_append_only();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    storage
        .put_value(0, &Payload::default(), hw_counter_ref)
        .unwrap();
    storage.flusher()().unwrap();

    let reader =
        BlobstoreReader::<Payload, MmapFile>::open(&MmapFs, dir.path().to_path_buf(), Populate::No)
            .unwrap();

    // All values live in page 0, a pointer to any other page must not read anything
    let view = reader.view();
    let pointer = ValuePointer::new(1, 0, 8);
    let err = view.read_from_pages::<Random>(pointer).unwrap_err();
    assert!(matches!(err, GridstoreError::PageNotFound { page_id: 1 },));
}

/// All writes must be pure appends: the tracker and page files only ever grow, and
/// previously written bytes are never touched.
#[test]
fn test_writes_only_append() {
    let (dir, mut storage) = empty_storage_append_only();

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    let put = |storage: &mut Blobstore<Payload>, point_offset: u32, value: &str| {
        let mut payload = Payload::default();
        payload.0.insert(
            "key".to_string(),
            serde_json::Value::String(value.to_string()),
        );
        storage
            .put_value(point_offset, &payload, hw_counter_ref)
            .unwrap();
    };

    let page_path = dir.path().join("append_only_page_0.dat");
    let tracker_path = dir.path().join("append_only_tracker.dat");

    // Write, flush, and snapshot the raw file bytes
    put(&mut storage, 0, "first value");
    storage.flusher()().unwrap();
    let page_snapshot = fs::read(&page_path).unwrap();
    let tracker_snapshot = fs::read(&tracker_path).unwrap();
    assert!(!page_snapshot.is_empty(), "page must hold the first value");
    assert!(
        !tracker_snapshot.is_empty(),
        "tracker must hold the first mapping",
    );

    // Puts alone buffer in memory and leave both files byte-for-byte untouched
    put(&mut storage, 1, "second value");
    put(&mut storage, 2, "third value");
    assert_eq!(fs::read(&page_path).unwrap(), page_snapshot);
    assert_eq!(fs::read(&tracker_path).unwrap(), tracker_snapshot);

    // Flushing the puts must only extend both files
    storage.flusher()().unwrap();

    let page_grown = fs::read(&page_path).unwrap();
    assert!(
        page_grown.len() > page_snapshot.len(),
        "append-only writes must grow the page ({} -> {})",
        page_snapshot.len(),
        page_grown.len(),
    );
    assert_eq!(
        &page_grown[..page_snapshot.len()],
        &page_snapshot[..],
        "previously written page bytes must be untouched (append-only)",
    );

    let tracker_grown = fs::read(&tracker_path).unwrap();
    assert!(
        tracker_grown.len() > tracker_snapshot.len(),
        "append-only writes must grow the tracker ({} -> {})",
        tracker_snapshot.len(),
        tracker_grown.len(),
    );
    assert_eq!(
        &tracker_grown[..tracker_snapshot.len()],
        &tracker_snapshot[..],
        "previously written tracker bytes must be untouched (append-only)",
    );
}

/// New mappings are appended right at the end of the tracker file, which always covers the
/// exact number of mappings.
#[test]
fn test_tracker_appends_new_mappings_at_end() {
    const ENTRY_SIZE: usize = TRACKER_ENTRY_SIZE as usize;

    let (dir, mut storage) = empty_storage_append_only();
    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    let rng = &mut rand::make_rng::<rand::rngs::SmallRng>();
    let tracker_path = dir.path().join("append_only_tracker.dat");

    let first_batch = 10u32;
    let second_batch = 25u32;

    for point_offset in 0..first_batch {
        storage
            .put_value(point_offset, &random_payload(rng, 1), hw_counter_ref)
            .unwrap();
    }
    storage.flusher()().unwrap();

    let before = fs::read(&tracker_path).unwrap();
    assert_eq!(
        before.len(),
        first_batch as usize * ENTRY_SIZE,
        "file must grow to exactly the appended mappings",
    );

    for point_offset in first_batch..second_batch {
        storage
            .put_value(point_offset, &random_payload(rng, 1), hw_counter_ref)
            .unwrap();
    }
    storage.flusher()().unwrap();

    let after = fs::read(&tracker_path).unwrap();
    assert_eq!(
        after.len(),
        second_batch as usize * ENTRY_SIZE,
        "file must grow to exactly the appended mappings",
    );

    // The full prefix must be byte-for-byte untouched
    assert_eq!(
        after[..before.len()],
        before[..],
        "existing bytes must stay untouched",
    );
    assert!(
        after[before.len()..]
            .chunks(ENTRY_SIZE)
            .all(|entry| entry.iter().any(|&byte| byte != 0)),
        "new mappings must fill the entries right after the existing ones",
    );
}

/// A mapping append past the end of the file pads the write with zeroed (unmapped) entries
/// so the mapping lands at its correct place.
#[test]
fn test_tracker_pads_mapping_gap_with_zeroes() {
    const ENTRY_SIZE: usize = TRACKER_ENTRY_SIZE as usize;

    let (dir, mut storage) = empty_storage_append_only();
    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    let rng = &mut rand::make_rng::<rand::rngs::SmallRng>();

    // Write points 0 and 3, skipping 1 and 2
    let payload_0 = random_payload(rng, 1);
    let payload_3 = random_payload(rng, 1);
    storage.put_value(0, &payload_0, hw_counter_ref).unwrap();
    storage.put_value(3, &payload_3, hw_counter_ref).unwrap();
    storage.flusher()().unwrap();

    // The file covers entries 0..=3 exactly; the skipped entries are zero
    let bytes = fs::read(dir.path().join("append_only_tracker.dat")).unwrap();
    assert_eq!(bytes.len(), 4 * ENTRY_SIZE);
    assert!(
        bytes[ENTRY_SIZE..3 * ENTRY_SIZE]
            .iter()
            .all(|&byte| byte == 0),
        "skipped entries must be zero-padded",
    );

    // The skipped points read as missing; the mapped ones round-trip
    assert!(
        storage
            .get_value::<Random>(1, &hw_counter)
            .unwrap()
            .is_none()
    );
    assert!(
        storage
            .get_value::<Random>(2, &hw_counter)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        storage
            .get_value::<Random>(0, &hw_counter)
            .unwrap()
            .as_ref(),
        Some(&payload_0),
    );
    assert_eq!(
        storage
            .get_value::<Random>(3, &hw_counter)
            .unwrap()
            .as_ref(),
        Some(&payload_3),
    );

    // Same after reopen; the count is derived from the exact file length
    let path = dir.path().to_path_buf();
    drop(storage);
    let storage = Blobstore::<Payload>::open(MmapFs, path, Populate::No).unwrap();
    assert_eq!(storage.max_point_offset(), 4);
    assert!(
        storage
            .get_value::<Random>(1, &hw_counter)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        storage
            .get_value::<Random>(3, &hw_counter)
            .unwrap()
            .as_ref(),
        Some(&payload_3),
    );
}

/// Values are packed back to back at block aligned offsets: each value starts at the block
/// boundary right after the previous value, and the page file ends exactly at the last value.
#[test]
fn test_values_are_packed_block_aligned() {
    let (dir, mut storage) = empty_byte_storage(Compression::None);

    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

    let num_values = 64u32;
    let value = |i: u32| vec![i as u8; 1 + (i as usize * 37) % 300];
    for point_offset in 0..num_values {
        storage
            .put_value(point_offset, &value(point_offset), hw_counter_ref)
            .unwrap();
    }
    storage.flusher()().unwrap();

    // Each value starts at the block boundary right after the previous value
    let block_size = DEFAULT_BLOCK_SIZE_BYTES as u64;
    let mut expected_start = 0;
    for point_offset in 0..num_values {
        let pointer = storage.get_pointer(point_offset).unwrap();
        assert_eq!(pointer.page_id, 0);
        assert_eq!(
            u64::from(pointer.block_offset) * block_size,
            expected_start,
            "value {point_offset} must start right after the previous one",
        );
        expected_start = (expected_start + u64::from(pointer.length)).next_multiple_of(block_size);

        assert_eq!(
            storage
                .get_value::<Random>(point_offset, &hw_counter)
                .unwrap(),
            Some(value(point_offset)),
        );
    }

    // The page file ends exactly at the last value, without trailing padding
    let last_pointer = storage.get_pointer(num_values - 1).unwrap();
    let last_end =
        u64::from(last_pointer.block_offset) * block_size + u64::from(last_pointer.length);
    let page_len = fs::metadata(dir.path().join("append_only_page_0.dat"))
        .unwrap()
        .len();
    assert_eq!(page_len, last_end);
    assert_eq!(storage.get_storage_size_bytes().unwrap() as u64, last_end);
}

/// Append-only mode never creates or reports the block flag files of the dynamic mode.
#[test]
fn test_has_no_block_flag_files() {
    let (dir, mut storage) = empty_storage_append_only();
    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    let rng = &mut rand::make_rng::<rand::rngs::SmallRng>();

    for point_offset in 0..32u32 {
        storage
            .put_value(point_offset, &random_payload(rng, 2), hw_counter_ref)
            .unwrap();
    }
    storage.flusher()().unwrap();

    // Not on disk...
    let on_disk: Vec<String> = fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        !on_disk
            .iter()
            .any(|name| name == "bitmask.dat" || name == "gaps.dat"),
        "append-only mode must not create block flag files, found {on_disk:?}",
    );

    // ...and not reported by files()
    let reported: Vec<String> = storage
        .files()
        .into_iter()
        .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    assert!(
        !reported
            .iter()
            .any(|name| name == "bitmask.dat" || name == "gaps.dat"),
        "files() must not report block flag files, got {reported:?}",
    );
}

/// A flusher persists exactly the mappings that existed when it was created: values put
/// afterwards stay pending and are lost when reopening without another flush.
#[test]
fn test_flusher_persists_mappings_up_to_creation() {
    let (dir, mut storage) = empty_storage_append_only();
    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    let rng = &mut rand::make_rng::<rand::rngs::SmallRng>();

    let payloads: Vec<_> = (0..5).map(|_| random_payload(rng, 1)).collect();
    for (point_offset, payload) in payloads.iter().enumerate().take(3) {
        storage
            .put_value(point_offset as u32, payload, hw_counter_ref)
            .unwrap();
    }

    let flusher = storage.flusher();

    for (point_offset, payload) in payloads.iter().enumerate().skip(3) {
        storage
            .put_value(point_offset as u32, payload, hw_counter_ref)
            .unwrap();
    }

    // The flusher only persists the values and mappings that existed when it was created
    flusher().unwrap();
    assert_eq!(tracker_file_len(&dir), 3 * TRACKER_ENTRY_SIZE);

    // The page file holds exactly the extent of the values covered by the flusher
    let covered_pointer = storage.get_pointer(2).unwrap();
    let covered_extent =
        u64::from(covered_pointer.block_offset) * 128 + u64::from(covered_pointer.length);
    assert_eq!(
        fs::metadata(dir.path().join("append_only_page_0.dat"))
            .unwrap()
            .len(),
        covered_extent,
    );

    // In this session all values are readable, the later ones from the in-memory buffers
    for (point_offset, payload) in payloads.iter().enumerate() {
        assert_eq!(
            storage
                .get_value::<Random>(point_offset as u32, &hw_counter)
                .unwrap()
                .as_ref(),
            Some(payload),
        );
    }

    // On reopen, only the flushed mappings are left
    let path = dir.path().to_path_buf();
    drop(storage);
    let storage = Blobstore::<Payload>::open(MmapFs, path, Populate::No).unwrap();
    assert_eq!(storage.max_point_offset(), 3);
    for (point_offset, payload) in payloads.iter().enumerate() {
        let expected = (point_offset < 3).then_some(payload);
        assert_eq!(
            storage
                .get_value::<Random>(point_offset as u32, &hw_counter)
                .unwrap()
                .as_ref(),
            expected,
        );
    }
}

/// The two modes have deliberately distinct file names, so a config that claims the wrong
/// mode fails loudly instead of loading the incompatible file format of the other mode.
#[rstest::rstest]
#[case(Mode::Dynamic, Mode::AppendOnly)]
#[case(Mode::AppendOnly, Mode::Dynamic)]
fn test_open_wrong_mode_fails(#[case] created: Mode, #[case] tampered: Mode) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    let options = StorageOptions {
        mode: Some(created),
        ..Default::default()
    };
    let mut storage = Blobstore::<Payload>::new(MmapFs, path.clone(), options).unwrap();
    let hw_counter = HardwareCounterCell::new();
    storage
        .put_value(
            0,
            &Payload::default(),
            hw_counter.ref_payload_io_write_counter(),
        )
        .unwrap();
    storage.flusher()().unwrap();
    drop(storage);

    // Tamper with the persisted mode, pointing it at the other mode
    let config_path = path.join(CONFIG_FILENAME);
    let config_json = fs::read_to_string(&config_path).unwrap();
    let mut config: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(&config_json).unwrap();
    let mode_name = match tampered {
        Mode::Dynamic => "dynamic",
        Mode::AppendOnly => "append_only",
    };
    config.insert("mode".to_string(), mode_name.into());
    fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();

    // The other mode never finds its own files, so opening fails instead of misreading
    assert!(Blobstore::<Payload>::open(MmapFs, path.clone(), Populate::No).is_err());
    assert!(BlobstoreReader::<Payload, MmapFile>::open(&MmapFs, path, Populate::No).is_err());
}

/// Replaying puts of already persisted point offsets (e.g. a WAL redo after a crash where
/// the flush completed but was never acknowledged) is rejected without writing anything;
/// [`Blobstore::max_point_offset`] is the exact offset a replay must resume at.
#[test]
fn test_replayed_puts_leave_storage_intact() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    let rng = &mut rand::make_rng::<rand::rngs::SmallRng>();

    let payloads: Vec<_> = (0..3).map(|_| random_payload(rng, 1)).collect();
    {
        let options = StorageOptions {
            mode: Some(Mode::AppendOnly),
            ..Default::default()
        };
        let mut storage = Blobstore::<Payload>::new(MmapFs, path.clone(), options).unwrap();
        for (point_offset, payload) in payloads.iter().enumerate() {
            storage
                .put_value(point_offset as u32, payload, hw_counter_ref)
                .unwrap();
        }
        storage.flusher()().unwrap();
    }

    // "Crash" and replay all operations against the reopened storage
    let mut storage = Blobstore::<Payload>::open(MmapFs, path, Populate::No).unwrap();
    let tracker_snapshot = fs::read(storage.files()[0].clone()).unwrap();
    let page_len = storage.get_storage_size_bytes().unwrap();

    let resume_at = storage.max_point_offset();
    assert_eq!(resume_at, 3);
    for (point_offset, payload) in payloads.iter().enumerate() {
        let err = storage
            .put_value(point_offset as u32, payload, hw_counter_ref)
            .unwrap_err();
        assert!(matches!(err, GridstoreError::UnsupportedOperation { .. }));
    }

    // The rejected replays must not have appended any value data or mappings
    assert_eq!(storage.get_storage_size_bytes().unwrap(), page_len);
    storage.flusher()().unwrap();
    assert_eq!(
        fs::read(storage.files()[0].clone()).unwrap(),
        tracker_snapshot,
    );

    // The stored values are unaffected, and the replay resumes at the reported offset
    for (point_offset, payload) in payloads.iter().enumerate() {
        assert_eq!(
            storage
                .get_value::<Random>(point_offset as u32, &hw_counter)
                .unwrap()
                .as_ref(),
            Some(payload),
        );
    }
    storage
        .put_value(resume_at, &random_payload(rng, 1), hw_counter_ref)
        .unwrap();
    assert_eq!(storage.max_point_offset(), 4);
}

/// The accepted crash case: a tracker file extended with zeroed bytes (e.g. the file length
/// was persisted before the entry bytes) counts as mappings that permanently read as `None`.
/// The storage must stay consistent and writable past them.
#[test]
fn test_zero_extended_tracker_reads_none() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    let rng = &mut rand::make_rng::<rand::rngs::SmallRng>();

    let payloads: Vec<_> = (0..3).map(|_| random_payload(rng, 1)).collect();
    {
        let options = StorageOptions {
            mode: Some(Mode::AppendOnly),
            ..Default::default()
        };
        let mut storage = Blobstore::<Payload>::new(MmapFs, path.clone(), options).unwrap();
        for (point_offset, payload) in payloads.iter().enumerate() {
            storage
                .put_value(point_offset as u32, payload, hw_counter_ref)
                .unwrap();
        }
        storage.flusher()().unwrap();
    }

    // Simulate a crash mid flush that persisted the grown file length as zeroes
    let tracker_path = path.join("append_only_tracker.dat");
    let file = fs::OpenOptions::new()
        .write(true)
        .open(&tracker_path)
        .unwrap();
    file.set_len(5 * TRACKER_ENTRY_SIZE).unwrap();
    drop(file);

    // The zeroed entries count as mappings without a value
    let mut storage = Blobstore::<Payload>::open(MmapFs, path.clone(), Populate::No).unwrap();
    assert_eq!(storage.max_point_offset(), 5);
    assert!(
        storage
            .get_value::<Random>(3, &hw_counter)
            .unwrap()
            .is_none()
    );
    assert!(
        storage
            .get_value::<Random>(4, &hw_counter)
            .unwrap()
            .is_none()
    );

    // They can never be put again, the storage continues past them
    assert!(
        storage
            .put_value(3, &random_payload(rng, 1), hw_counter_ref)
            .is_err()
    );
    storage
        .put_value(5, &random_payload(rng, 1), hw_counter_ref)
        .unwrap();
    assert_eq!(storage.max_point_offset(), 6);
    storage.flusher()().unwrap();

    // Data before the zeroed entries is unaffected
    for (point_offset, payload) in payloads.iter().enumerate() {
        assert_eq!(
            storage
                .get_value::<Random>(point_offset as u32, &hw_counter)
                .unwrap()
                .as_ref(),
            Some(payload),
        );
    }

    // The reader agrees on the same view
    drop(storage);
    let reader = BlobstoreReader::<Payload, MmapFile>::open(&MmapFs, path, Populate::No).unwrap();
    assert_eq!(reader.max_point_offset().unwrap(), 6);
    assert!(
        reader
            .get_value::<Random>(4, &hw_counter)
            .unwrap()
            .is_none()
    );
}

/// The read-only reader must never modify the files: not on open (it must not truncate a
/// torn tracker tail), not on reads, and not on live reloads.
#[test]
fn test_reader_never_writes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    let rng = &mut rand::make_rng::<rand::rngs::SmallRng>();

    let payloads: Vec<_> = (0..5).map(|_| random_payload(rng, 1)).collect();
    {
        let options = StorageOptions {
            mode: Some(Mode::AppendOnly),
            ..Default::default()
        };
        let mut storage = Blobstore::<Payload>::new(MmapFs, path.clone(), options).unwrap();
        for (point_offset, payload) in payloads.iter().enumerate() {
            storage
                .put_value(point_offset as u32, payload, hw_counter_ref)
                .unwrap();
        }
        storage.flusher()().unwrap();
    }

    // Leave a torn trailing entry, as if a writer append was cut short
    let tracker_path = path.join("append_only_tracker.dat");
    let page_path = path.join("append_only_page_0.dat");
    {
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&tracker_path)
            .unwrap();
        direct_io::write_all_at(&file, &[0xAA; 7], 5 * TRACKER_ENTRY_SIZE).unwrap();
    }

    let tracker_snapshot = fs::read(&tracker_path).unwrap();
    let page_snapshot = fs::read(&page_path).unwrap();

    // Open a reader and exercise every read path
    let mut reader =
        BlobstoreReader::<Payload, MmapFile>::open(&MmapFs, path, Populate::No).unwrap();
    assert_eq!(
        reader.max_point_offset().unwrap(),
        5,
        "torn tail must be ignored"
    );
    for (point_offset, payload) in payloads.iter().enumerate() {
        assert_eq!(
            reader
                .get_value::<Random>(point_offset as u32, &hw_counter)
                .unwrap()
                .as_ref(),
            Some(payload),
        );
    }
    let mut count = 0;
    reader
        .iter(
            u32::MAX,
            |_, _: Payload| {
                count += 1;
                Ok::<_, GridstoreError>(true)
            },
            hw_counter.ref_payload_io_read_counter(),
        )
        .unwrap();
    assert_eq!(count, 5);
    reader.live_reload(&MmapFs).unwrap();
    reader.get_storage_size_bytes();
    reader.clear_cache().unwrap();
    drop(reader);

    // The reader must have left both files byte-for-byte untouched
    assert_eq!(fs::read(&tracker_path).unwrap(), tracker_snapshot);
    assert_eq!(fs::read(&page_path).unwrap(), page_snapshot);
}

/// Every reopen must expose exactly the flushed prefix: values flushed before are readable,
/// unflushed ones are gone, and the mapping count always matches the exact file length.
#[test]
fn test_reopen_always_exposes_flushed_prefix() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let hw_counter = HardwareCounterCell::new();
    let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
    let rng = &mut rand::make_rng::<rand::rngs::SmallRng>();

    let options = StorageOptions {
        mode: Some(Mode::AppendOnly),
        ..Default::default()
    };
    let mut storage = Blobstore::<Payload>::new(MmapFs, path.clone(), options).unwrap();

    // The values that must be visible after a reopen
    let mut flushed: Vec<Payload> = Vec::new();

    for round in 0..6 {
        // Put a batch at the next offsets; after a reopen unflushed offsets are reused
        let batch: Vec<_> = (0..3).map(|_| random_payload(rng, 1)).collect();
        for (index, payload) in batch.iter().enumerate() {
            let point_offset = flushed.len() as u32 + index as u32;
            storage
                .put_value(point_offset, payload, hw_counter_ref)
                .unwrap();
        }

        // Only flush every other round
        if round % 2 == 0 {
            storage.flusher()().unwrap();
            flushed.extend(batch);
        }

        // Every reopen must expose exactly the flushed values
        drop(storage);
        storage = Blobstore::<Payload>::open(MmapFs, path.clone(), Populate::No).unwrap();

        assert_eq!(storage.max_point_offset(), flushed.len() as u32);
        assert_eq!(
            fs::metadata(path.join("append_only_tracker.dat"))
                .unwrap()
                .len(),
            flushed.len() as u64 * TRACKER_ENTRY_SIZE,
        );

        // The page file holds exactly the flushed values, unflushed puts leave no bytes behind
        let flushed_extent = flushed
            .len()
            .checked_sub(1)
            .and_then(|last| storage.get_pointer(last as u32))
            .map_or(0, |pointer| {
                u64::from(pointer.block_offset) * 128 + u64::from(pointer.length)
            });
        assert_eq!(
            fs::metadata(path.join("append_only_page_0.dat"))
                .unwrap()
                .len(),
            flushed_extent,
            "the page file must hold exactly the flushed values",
        );

        for (point_offset, payload) in flushed.iter().enumerate() {
            assert_eq!(
                storage
                    .get_value::<Random>(point_offset as u32, &hw_counter)
                    .unwrap()
                    .as_ref(),
                Some(payload),
                "flushed value {point_offset} must be readable after reopen",
            );
        }
        assert!(
            storage
                .get_value::<Random>(flushed.len() as u32, &hw_counter)
                .unwrap()
                .is_none(),
            "unflushed values must be gone after reopen",
        );
    }
}
