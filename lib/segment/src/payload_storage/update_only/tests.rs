use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{
    MmapFile, MmapFs, Populate, ReadOnly, UniversalRead, UniversalReadFileOps as _,
};
use tempfile::TempDir;

use super::UpdateOnlyPayloadStorage;
use crate::payload_json;
use crate::payload_storage::PayloadStorageRead as _;
use crate::payload_storage::payload_storage_impl::PayloadStorageImpl;
use crate::payload_storage::read_only::ReadOnlyPayloadStorage;
use crate::types::Payload;

type Writer = UpdateOnlyPayloadStorage<MmapFile>;

fn payload(n: u64) -> Payload {
    payload_json! { "n": n, "text": format!("payload {n}") }
}

/// Read the segment's payload storage back through the read-only side, over the
/// write-enforced backend, so what the writer produced is checked against a
/// reader that cannot have written any of it.
fn read_back(
    segment_path: &Path,
    slots: impl IntoIterator<Item = PointOffsetType>,
) -> Vec<Payload> {
    type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
    let fs = RoFs::from_context(Default::default()).unwrap();
    let storage: ReadOnlyPayloadStorage<ReadOnly<MmapFile>> =
        ReadOnlyPayloadStorage::open(&fs, segment_path.to_path_buf(), Populate::No).unwrap();

    let hw_counter = HardwareCounterCell::new();
    slots
        .into_iter()
        .map(|slot| storage.get(slot, &hw_counter).unwrap())
        .collect()
}

/// A batch is durable once `append_many` returns, and a second writer resumes
/// where the first one left off — the state lives in the files, not the writer.
#[test]
fn batches_are_durable_and_resume() {
    let dir = TempDir::with_prefix("update_only_payload").unwrap();
    let hw_counter = HardwareCounterCell::new();

    let first: Vec<Payload> = (0..3).map(payload).collect();
    let mut writer = Writer::open(MmapFs, dir.path()).unwrap();
    writer
        .append_many(
            first.iter().enumerate().map(|(i, p)| (i as u32, p)),
            &hw_counter,
        )
        .unwrap();
    drop(writer);

    assert_eq!(read_back(dir.path(), 0..3), first);

    let second: Vec<Payload> = (10..13).map(payload).collect();
    let mut writer = Writer::open(MmapFs, dir.path()).unwrap();
    writer
        .append_many(
            second.iter().enumerate().map(|(i, p)| (i as u32 + 3, p)),
            &hw_counter,
        )
        .unwrap();
    drop(writer);

    assert_eq!(
        read_back(dir.path(), 0..6),
        first.iter().chain(&second).cloned().collect::<Vec<_>>(),
    );
}

/// Slots with an empty payload are not written at all, and read back empty —
/// including a gap the batch skips entirely.
#[test]
fn empty_payloads_and_gaps_read_back_empty() {
    let dir = TempDir::with_prefix("update_only_payload").unwrap();
    let hw_counter = HardwareCounterCell::new();

    let empty = Payload::default();
    let mut writer = Writer::open(MmapFs, dir.path()).unwrap();
    writer
        .append_many(
            [(0, &payload(0)), (1, &empty), (4, &payload(4))],
            &hw_counter,
        )
        .unwrap();
    drop(writer);

    assert_eq!(
        read_back(dir.path(), 0..6),
        vec![
            payload(0),
            empty.clone(),
            empty.clone(),
            empty.clone(),
            payload(4),
            empty,
        ],
    );
}

/// A slot far above everything the storage holds is written where it belongs,
/// including on a storage that holds nothing at all — the case of a segment
/// whose points had no payloads until now.
#[test]
fn first_payload_may_land_on_a_high_slot() {
    let dir = TempDir::with_prefix("update_only_payload").unwrap();
    let hw_counter = HardwareCounterCell::new();

    let mut writer = Writer::open(MmapFs, dir.path()).unwrap();
    writer
        .append_many([(1_000, &payload(1_000))], &hw_counter)
        .unwrap();
    drop(writer);

    assert_eq!(
        read_back(dir.path(), [0, 999, 1_000]),
        vec![Payload::default(), Payload::default(), payload(1_000),]
    );
}

/// The storage is append-only: a slot it already holds a payload for cannot be
/// written a second time, whether within one batch or by a later writer.
#[test]
fn rewriting_a_slot_is_rejected() {
    let dir = TempDir::with_prefix("update_only_payload").unwrap();
    let hw_counter = HardwareCounterCell::new();

    let mut writer = Writer::open(MmapFs, dir.path()).unwrap();
    writer
        .append_many([(0, &payload(0)), (1, &payload(1))], &hw_counter)
        .unwrap();

    // Out of order within a batch
    assert!(
        writer
            .append_many([(3, &payload(3)), (2, &payload(2))], &hw_counter)
            .is_err(),
    );
    drop(writer);

    // And a slot a previous writer already used
    let mut writer = Writer::open(MmapFs, dir.path()).unwrap();
    assert!(writer.append_many([(0, &payload(0))], &hw_counter).is_err());
}

/// A payload storage created in mutable mode is not something this writer can
/// append to, and is refused rather than opened.
#[test]
fn mutable_storage_is_refused() {
    let dir = TempDir::with_prefix("update_only_payload").unwrap();

    let storage: PayloadStorageImpl =
        PayloadStorageImpl::open_or_create(dir.path().to_path_buf(), false).unwrap();
    drop(storage);

    assert!(Writer::open(MmapFs, dir.path()).is_err());
}
