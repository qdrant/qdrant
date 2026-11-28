//! Tests for overlapping flushers: a flusher captured before an earlier one ran, and flushers
//! running in any order.

use std::path::Path;

use ahash::AHashMap;
use common::ambient;
use common::ambient::hw::HwMetric;
use common::generic_consts::Random;
use common::universal_io::{MmapFile, MmapFs, Populate};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use super::Gridstore;
use crate::blobstore::{Blobstore, BlobstoreReader, Flusher};
use crate::config::{Compression, DEFAULT_PAGE_SIZE_BYTES};
use crate::fixtures::{Payload, empty_storage_sized};
use crate::tracker::{PointOffset, ValuePointer};

const READ: HwMetric = HwMetric::PayloadIoRead;
const WRITE: HwMetric = HwMetric::PayloadIoWrite;

type Values = AHashMap<PointOffset, Payload>;

fn empty_storage() -> (tempfile::TempDir, Blobstore<Payload>) {
    // No compression, so equally long values take equally many blocks and reuse freed blocks
    empty_storage_sized(DEFAULT_PAGE_SIZE_BYTES, Compression::None)
}

fn payload(value: &str) -> Payload {
    let mut payload = Payload::default();
    payload.0.insert(
        "key".to_string(),
        serde_json::Value::String(value.to_string()),
    );
    payload
}

fn get(storage: &Blobstore<Payload>, point_offset: PointOffset) -> Option<Payload> {
    storage.get_value::<Random>(point_offset, READ).unwrap()
}

/// Read the values from disk, as a reopen would see them, without replaying the journal
fn get_on_disk(path: &Path, point_offsets: impl Iterator<Item = PointOffset>) -> Values {
    let reader =
        BlobstoreReader::<Payload, MmapFile>::open(&MmapFs, path.to_path_buf(), Populate::No)
            .unwrap();
    point_offsets
        .filter_map(|point_offset| {
            let value = reader.get_value::<Random>(point_offset, READ).unwrap()?;
            Some((point_offset, value))
        })
        .collect()
}

fn position(pointer: Option<ValuePointer>) -> (u32, u32) {
    let pointer = pointer.unwrap();
    (pointer.page_id, pointer.block_offset)
}

fn blocks_of(storage: &Blobstore<Payload>, pointer: ValuePointer) -> u32 {
    let block_size = storage.as_gridstore().config.block_size_bytes;
    Gridstore::<Payload, MmapFile>::blocks_for_value(pointer.length as usize, block_size)
}

/// Assert that the blocks of all values are marked as used, so they can't be reused
fn assert_values_blocks_used(storage: &Blobstore<Payload>) {
    let bitmask = storage.as_gridstore().bitmask.read();
    for point_offset in 0..storage.max_point_offset() {
        let Some(pointer) = storage.get_pointer(point_offset) else {
            continue;
        };
        for block in 0..blocks_of(storage, pointer) {
            assert!(
                bitmask
                    .is_block_used(pointer.page_id, pointer.block_offset + block)
                    .unwrap(),
                "block {block} of point {point_offset} is free: {pointer:?}",
            );
        }
    }
}

/// Two flushers capturing the same pending updates must not free the pointer they both write.
#[test]
fn test_overlapping_flushers_keep_written_pointer() {
    let (dir, mut storage) = empty_storage();
    let _scope = ambient::test_guard();

    storage.put_value(0, &payload("value 0"), WRITE).unwrap();

    let flusher_1 = storage.flusher();
    let flusher_2 = storage.flusher();
    flusher_1().unwrap();
    flusher_2().unwrap();
    assert_values_blocks_used(&storage);

    // Must not reuse the blocks of point 0
    storage.put_value(1, &payload("value 1"), WRITE).unwrap();
    assert_ne!(
        position(storage.get_pointer(0)),
        position(storage.get_pointer(1)),
    );
    assert_eq!(get(&storage, 0), Some(payload("value 0")));
    assert_eq!(get(&storage, 1), Some(payload("value 1")));

    storage.flusher()().unwrap();
    drop(storage);
    let storage =
        Blobstore::<Payload>::open(MmapFs, dir.path().to_path_buf(), Populate::No).unwrap();
    assert_eq!(get(&storage, 0), Some(payload("value 0")));
    assert_eq!(get(&storage, 1), Some(payload("value 1")));
}

/// Overlapping flushers must free an old pointer once, its blocks may be reused in between.
#[test]
fn test_overlapping_flushers_free_old_pointer_once() {
    let (dir, mut storage) = empty_storage();
    let _scope = ambient::test_guard();

    storage.put_value(0, &payload("value 0"), WRITE).unwrap();
    storage.flusher()().unwrap();
    let freed = storage.get_pointer(0);

    storage.delete_value(0).unwrap();
    let flusher_1 = storage.flusher();
    storage.put_value(1, &payload("value 1"), WRITE).unwrap();
    let flusher_2 = storage.flusher();
    // Keeps updates of point 0 pending after the first flusher
    storage
        .put_value(0, &payload("value 0 again"), WRITE)
        .unwrap();

    // Frees the old blocks of point 0, point 2 reuses them
    flusher_1().unwrap();
    storage.put_value(2, &payload("value 2"), WRITE).unwrap();
    assert_eq!(position(storage.get_pointer(2)), position(freed));

    // Must not free the old blocks of point 0 again, they belong to point 2 now
    flusher_2().unwrap();
    assert_values_blocks_used(&storage);

    storage.put_value(3, &payload("value 3"), WRITE).unwrap();
    assert_ne!(
        position(storage.get_pointer(2)),
        position(storage.get_pointer(3)),
    );
    assert_eq!(get(&storage, 0), Some(payload("value 0 again")));
    assert_eq!(get(&storage, 1), Some(payload("value 1")));
    assert_eq!(get(&storage, 2), Some(payload("value 2")));
    assert_eq!(get(&storage, 3), Some(payload("value 3")));

    storage.flusher()().unwrap();
    drop(storage);
    let storage =
        Blobstore::<Payload>::open(MmapFs, dir.path().to_path_buf(), Populate::No).unwrap();
    assert_eq!(get(&storage, 0), Some(payload("value 0 again")));
    assert_eq!(get(&storage, 1), Some(payload("value 1")));
    assert_eq!(get(&storage, 2), Some(payload("value 2")));
    assert_eq!(get(&storage, 3), Some(payload("value 3")));
}

/// An old pointer freed by an earlier flusher may be reused by the same point, and become an old
/// pointer again. Overlapping flushers must not take it for the one they hold.
#[test]
fn test_overlapping_flushers_tell_reused_pointers_apart() {
    let (dir, mut storage) = empty_storage();
    let path = dir.path().to_path_buf();
    let _scope = ambient::test_guard();

    storage.put_value(0, &payload("value a"), WRITE).unwrap();
    let reused = storage.get_pointer(0);
    storage.put_value(0, &payload("value b"), WRITE).unwrap();
    let flusher_1 = storage.flusher();
    let flusher_2 = storage.flusher();

    // Frees value a, value c reuses its pointer
    flusher_1().unwrap();
    storage.put_value(0, &payload("value c"), WRITE).unwrap();
    assert_eq!(storage.get_pointer(0), reused);
    let flusher_3 = storage.flusher();
    storage.put_value(0, &payload("value d"), WRITE).unwrap();

    // Must not free value c, the third flusher persists it
    flusher_2().unwrap();
    flusher_3().unwrap();
    assert_eq!(
        get_on_disk(&path, 0..1),
        Values::from_iter([(0, payload("value c"))]),
    );

    // Must not reuse the blocks of value c
    storage.put_value(1, &payload("value e"), WRITE).unwrap();
    assert_values_blocks_used(&storage);
    assert_eq!(
        get_on_disk(&path, 0..1),
        Values::from_iter([(0, payload("value c"))]),
    );
    assert_eq!(get(&storage, 0), Some(payload("value d")));
    assert_eq!(get(&storage, 1), Some(payload("value e")));
}

/// A flusher running after a later captured one must not revert what that one persisted.
#[test]
fn test_outdated_flusher_is_skipped() {
    let (dir, mut storage) = empty_storage();
    let path = dir.path().to_path_buf();
    let _scope = ambient::test_guard();

    storage.put_value(0, &payload("value A"), WRITE).unwrap();
    let outdated = storage.flusher();
    storage.put_value(0, &payload("value B"), WRITE).unwrap();
    let latest = storage.flusher();

    latest().unwrap();
    outdated().unwrap();
    assert_values_blocks_used(&storage);
    assert_eq!(get(&storage, 0), Some(payload("value B")));
    assert_eq!(
        get_on_disk(&path, 0..1),
        Values::from_iter([(0, payload("value B"))]),
    );

    // Must not reuse the blocks of point 0
    storage.put_value(1, &payload("value C"), WRITE).unwrap();
    assert_eq!(get(&storage, 0), Some(payload("value B")));
    assert_eq!(get(&storage, 1), Some(payload("value C")));

    // Reopening replays the journal, it must not revert point 0 either
    drop(storage);
    let storage = Blobstore::<Payload>::open(MmapFs, path, Populate::No).unwrap();
    assert_eq!(get(&storage, 0), Some(payload("value B")));
}

/// Deleting a value that was replaced since the last flush must free the persisted value too.
#[test]
fn test_delete_frees_replaced_persisted_value() {
    let (_dir, mut storage) = empty_storage();
    let _scope = ambient::test_guard();

    storage.put_value(0, &payload("value A"), WRITE).unwrap();
    storage.flusher()().unwrap();
    storage.put_value(0, &payload("value B"), WRITE).unwrap();
    storage.delete_value(0).unwrap();
    storage.flusher()().unwrap();

    let used_blocks = storage.as_gridstore().bitmask.read().used_blocks().unwrap();
    assert_eq!(used_blocks, 0, "all blocks must be free");
}

/// Randomly interleave updates with capturing flushers, and running or dropping them in any
/// order. Compare against a model in memory and on disk after every step.
#[test]
fn test_overlapping_flushers_behave_like_hashmap() {
    // Without reopening, so no blocks leak and we can check that all are freed
    for seed in 0..16 {
        overlapping_flushers_behave_like_hashmap(seed, 1_500, false);
    }
    for seed in 16..32 {
        overlapping_flushers_behave_like_hashmap(seed, 1_500, true);
    }
}

fn overlapping_flushers_behave_like_hashmap(seed: u64, operations: usize, reopen: bool) {
    // Few points and value lengths, so freed blocks are reused often, also by the same point with
    // an equal pointer
    const POINTS: PointOffset = 6;
    const VALUE_LENGTHS: [usize; 4] = [8, 100, 200, 400];

    let rng = &mut StdRng::seed_from_u64(seed);
    let (dir, mut storage) = empty_storage();
    let path = dir.path().to_path_buf();
    let _scope = ambient::test_guard();

    // Latest values
    let mut model = Values::new();
    // Values on disk, as captured by the last flusher that ran
    let mut disk_model = Values::new();
    // Flushers that didn't run yet, with their capture number and the values they persist
    let mut flushers: Vec<(usize, Flusher, Values)> = Vec::new();
    let mut captures = 0;
    let mut last_run_capture = 0;
    // Reopening with unflushed changes leaks blocks, the bitmask is persisted eagerly
    let mut may_leak = false;
    let mut next_value = 0;

    for operation in 0..operations {
        let context = format!("seed {seed}, operation {operation}");

        match rng.random_range(0..100) {
            // Put
            0..35 => {
                let point_offset = rng.random_range(0..POINTS);
                let length = VALUE_LENGTHS[rng.random_range(0..VALUE_LENGTHS.len())];
                let value = format!("{next_value:0length$}");
                next_value += 1;
                storage
                    .put_value(point_offset, &payload(&value), WRITE)
                    .unwrap();
                model.insert(point_offset, payload(&value));
            }
            // Delete
            35..50 => {
                let point_offset = rng.random_range(0..POINTS);
                let deleted = storage.delete_value(point_offset).unwrap();
                assert_eq!(deleted, model.remove(&point_offset), "{context}");
            }
            // Capture a flusher
            50..72 => {
                captures += 1;
                flushers.push((captures, storage.flusher(), model.clone()));
            }
            // Run a flusher, in any order
            72..95 => {
                if flushers.is_empty() {
                    continue;
                }
                let (capture, flusher, values) =
                    flushers.swap_remove(rng.random_range(0..flushers.len()));
                flusher().unwrap();

                // An outdated flusher is skipped
                if capture > last_run_capture {
                    last_run_capture = capture;
                    disk_model = values;
                }
                assert_eq!(
                    get_on_disk(&path, 0..POINTS),
                    disk_model,
                    "{context}: on disk",
                );
            }
            // Drop a flusher
            95..98 => {
                if !flushers.is_empty() {
                    let (_, flusher, _) = flushers.swap_remove(rng.random_range(0..flushers.len()));
                    drop(flusher);
                }
            }
            // Reopen, losing unflushed changes
            98..100 if reopen => {
                flushers.clear();
                may_leak |= !storage
                    .as_gridstore()
                    .tracker
                    .read()
                    .pending_updates
                    .is_empty();
                drop(storage);
                storage = Blobstore::open(MmapFs, path.clone(), Populate::No).unwrap();
                model = disk_model.clone();
            }
            98..100 => {}
            _ => unreachable!(),
        }

        for point_offset in 0..POINTS {
            assert_eq!(
                get(&storage, point_offset).as_ref(),
                model.get(&point_offset),
                "{context}: point {point_offset}",
            );
        }
        assert_values_blocks_used(&storage);
    }

    // Run the remaining flushers in any order, then flush everything
    while !flushers.is_empty() {
        let (_, flusher, _) = flushers.swap_remove(rng.random_range(0..flushers.len()));
        flusher().unwrap();
    }
    storage.flusher()().unwrap();
    assert_eq!(get_on_disk(&path, 0..POINTS), model, "seed {seed}: on disk");
    assert_values_blocks_used(&storage);

    // All blocks of replaced and deleted values must be freed
    if !may_leak {
        let used_blocks = storage.as_gridstore().bitmask.read().used_blocks().unwrap();
        let value_blocks: u32 = (0..POINTS)
            .filter_map(|point_offset| storage.get_pointer(point_offset))
            .map(|pointer| blocks_of(&storage, pointer))
            .sum();
        assert_eq!(
            used_blocks, value_blocks as usize,
            "seed {seed}: leaked blocks"
        );
    }

    drop(storage);
    let storage = Blobstore::<Payload>::open(MmapFs, path, Populate::No).unwrap();
    for point_offset in 0..POINTS {
        assert_eq!(
            get(&storage, point_offset).as_ref(),
            model.get(&point_offset),
            "seed {seed}: point {point_offset} after reopen",
        );
    }
}
