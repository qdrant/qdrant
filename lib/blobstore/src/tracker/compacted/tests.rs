use common::generic_consts::Random;
use common::universal_io::{MmapFs, UniversalWriteFs};
use tempfile::TempDir;

use super::format::{Header, encode};
use super::{CompactedTracker, FILE_NAME};
use crate::tracker::{PointOffset, PointerItem, TrackerRead, ValuePointer};

/// Pointers packed back to back across pages of the given capacity, with `None` at the
/// given point offsets, as the append-only pages lay values out.
fn packed_pointers(
    lengths: &[u32],
    gaps: &[usize],
    page_capacity: u32,
) -> Vec<Option<ValuePointer>> {
    let (mut page_id, mut offset) = (0, 0);
    lengths
        .iter()
        .enumerate()
        .map(|(index, &length)| {
            if gaps.contains(&index) {
                return None;
            }
            if offset + length > page_capacity {
                page_id += 1;
                offset = 0;
            }
            let pointer = ValuePointer::new(page_id, offset, length);
            offset += length;
            Some(pointer)
        })
        .collect()
}

fn tracker_with(pointers: &[Option<ValuePointer>]) -> (TempDir, CompactedTracker) {
    let dir = TempDir::new().unwrap();
    let mut tracker = CompactedTracker::new(&MmapFs, dir.path()).unwrap();
    for (point_offset, pointer) in pointers.iter().enumerate() {
        if let Some(pointer) = pointer {
            tracker.set(point_offset as PointOffset, *pointer);
        }
    }
    (dir, tracker)
}

fn assert_reads(tracker: &CompactedTracker, expected: &[Option<ValuePointer>]) {
    let count = expected.len() as PointOffset;
    assert_eq!(tracker.pointer_count(), count);
    assert_eq!(tracker.max_point_offset().unwrap(), count);

    for (point_offset, pointer) in expected.iter().enumerate() {
        assert_eq!(
            tracker.get::<Random>(point_offset as PointOffset).unwrap(),
            *pointer,
        );
    }
    assert_eq!(tracker.get::<Random>(count).unwrap(), None);
    assert_eq!(tracker.get::<Random>(count + 100).unwrap(), None);

    // Range crossing the end is padded, range past the end is all None
    let mut padded = expected.to_vec();
    padded.resize(expected.len() + 2, None);
    assert_eq!(tracker.get_range::<Random>(0..count + 2).unwrap(), padded);
    assert_eq!(
        tracker.get_range::<Random>(count + 1..count + 3).unwrap(),
        [None, None]
    );

    let items = tracker
        .iter((0..count + 1).map(|point_offset| (point_offset, point_offset)))
        .unwrap()
        .map(|item| item.unwrap())
        .collect::<Vec<_>>();
    let expected_items = expected
        .iter()
        .map(|pointer| PointerItem::from(*pointer))
        .chain([PointerItem::OutOfRange])
        .enumerate()
        .map(|(point_offset, item)| (point_offset as PointOffset, item))
        .collect::<Vec<_>>();
    assert_eq!(items, expected_items);
}

/// Flush, reopen, and check that the reopened tracker serves `expected`.
fn assert_roundtrip(tracker: &CompactedTracker, dir: &TempDir, expected: &[Option<ValuePointer>]) {
    tracker.flusher(MmapFs)().unwrap();
    let reopened = CompactedTracker::open(&MmapFs, dir.path()).unwrap();
    assert_reads(&reopened, expected);
    assert_eq!(reopened.files(), vec![dir.path().join(FILE_NAME)]);
}

#[test]
fn test_new_tracker_is_empty_and_reopens() {
    let (dir, tracker) = tracker_with(&[]);
    assert!(dir.path().join(FILE_NAME).exists());
    assert_reads(&tracker, &[]);

    let reopened = CompactedTracker::open(&MmapFs, dir.path()).unwrap();
    assert_reads(&reopened, &[]);
}

#[test]
fn test_open_missing_tracker_fails() {
    let dir = TempDir::new().unwrap();
    let err = CompactedTracker::open(&MmapFs, dir.path()).unwrap_err();
    assert!(err.to_string().contains("does not exist"), "{err}");
}

#[test]
fn test_roundtrip_packed_with_gaps_and_page_rollover() {
    let lengths = [10, 0, 300, 25, 25, 1000, 7, 7, 7, 4000, 1];
    let expected = packed_pointers(&lengths, &[1, 4, 8], 1024);
    let (dir, tracker) = tracker_with(&expected);
    assert_reads(&tracker, &expected);
    assert_roundtrip(&tracker, &dir, &expected);
}

#[test]
fn test_roundtrip_arbitrary_pointers() {
    // Not packed: page ids go down, offsets jump around, so every delta sign shows up
    let expected = vec![
        Some(ValuePointer::new(5, 100, 3)),
        Some(ValuePointer::new(2, 0, u32::MAX)),
        None,
        Some(ValuePointer::new(2, u32::MAX, 0)),
        Some(ValuePointer::new(u32::MAX, 1, 1)),
        Some(ValuePointer::new(0, 0, 0)),
    ];
    let (dir, tracker) = tracker_with(&expected);
    assert_roundtrip(&tracker, &dir, &expected);
}

#[test]
fn test_set_any_order_and_overwrite() {
    let (dir, mut tracker) = tracker_with(&[]);
    tracker.set(5, ValuePointer::new(0, 50, 5));
    tracker.set(2, ValuePointer::new(0, 20, 2));
    tracker.set(5, ValuePointer::new(1, 0, 7));

    let expected = [
        None,
        None,
        Some(ValuePointer::new(0, 20, 2)),
        None,
        None,
        Some(ValuePointer::new(1, 0, 7)),
    ];
    assert_reads(&tracker, &expected);
    assert_roundtrip(&tracker, &dir, &expected);
}

#[test]
fn test_unflushed_mappings_are_not_persisted() {
    let expected = packed_pointers(&[1, 2, 3], &[], 1024);
    let (dir, mut tracker) = tracker_with(&expected);
    tracker.flusher(MmapFs)().unwrap();

    tracker.set(3, ValuePointer::new(0, 6, 4));
    let reopened = CompactedTracker::open(&MmapFs, dir.path()).unwrap();
    assert_reads(&reopened, &expected);
}

#[test]
fn test_flusher_snapshots_and_rewrites_whole_file() {
    let first = packed_pointers(&[1, 2, 3], &[], 1024);
    let (dir, mut tracker) = tracker_with(&first);
    let flusher = tracker.flusher(MmapFs);

    // Set after the flusher was created, not part of its snapshot
    tracker.set(3, ValuePointer::new(0, 6, 4));
    flusher().unwrap();
    let reopened = CompactedTracker::open(&MmapFs, dir.path()).unwrap();
    assert_reads(&reopened, &first);

    // The next flush rewrites the file with everything
    tracker.flusher(MmapFs)().unwrap();
    let reopened = CompactedTracker::open(&MmapFs, dir.path()).unwrap();
    let mut second = first;
    second.push(Some(ValuePointer::new(0, 6, 4)));
    assert_reads(&reopened, &second);
}

#[test]
fn test_clean_tracker_flush_is_noop() {
    let (dir, mut tracker) = tracker_with(&[]);
    let path = dir.path().join(FILE_NAME);
    assert!(!tracker.is_dirty());

    // A clean flush does not touch the disk: a removed file stays removed
    MmapFs.remove(&path).unwrap();
    tracker.flusher(MmapFs)().unwrap();
    assert!(!path.exists());

    tracker.set(0, ValuePointer::new(0, 0, 1));
    assert!(tracker.is_dirty());
    tracker.flusher(MmapFs)().unwrap();
    assert!(path.exists());
    assert!(!tracker.is_dirty());

    // Clean again after the flush
    MmapFs.remove(&path).unwrap();
    tracker.flusher(MmapFs)().unwrap();
    assert!(!path.exists());

    // Opening a file makes a clean tracker as well
    tracker.set(1, ValuePointer::new(0, 1, 1));
    tracker.flusher(MmapFs)().unwrap();
    let reopened = CompactedTracker::open(&MmapFs, dir.path()).unwrap();
    assert!(!reopened.is_dirty());
}

#[test]
fn test_failed_flush_leaves_tracker_dirty() {
    let (dir, mut tracker) = tracker_with(&[]);
    tracker.set(0, ValuePointer::new(0, 0, 1));
    let flusher = tracker.flusher(MmapFs);
    assert!(
        !tracker.is_dirty(),
        "taking the copy marks the tracker clean"
    );

    // Without the directory the file cannot be written
    MmapFs.remove(&dir.path().join(FILE_NAME)).unwrap();
    MmapFs.remove_dir(dir.path()).unwrap();
    assert!(flusher().is_err());
    assert!(
        tracker.is_dirty(),
        "a failed flush must be retried by the next one"
    );

    MmapFs.create_dir(dir.path()).unwrap();
    tracker.flusher(MmapFs)().unwrap();
    assert!(!tracker.is_dirty());
    let reopened = CompactedTracker::open(&MmapFs, dir.path()).unwrap();
    assert_reads(&reopened, &[Some(ValuePointer::new(0, 0, 1))]);
}

#[test]
fn test_invalid_files_are_rejected() {
    let expected = packed_pointers(&[1, 2, 3], &[], 1024);
    let (dir, tracker) = tracker_with(&expected);
    let valid = encode(&tracker.pointers).unwrap();
    let header_size = size_of::<Header>();

    let mut bad_magic = valid.clone();
    bad_magic[0] ^= 0xff;

    let mut bad_version = valid.clone();
    bad_version[8..12].copy_from_slice(&2u32.to_le_bytes());

    // The count is inside the checksummed frame, so recompress with another one
    let with_count = |count: u32| {
        let mut raw = zstd::decode_all(&valid[header_size..]).unwrap();
        raw[..4].copy_from_slice(&count.to_le_bytes());
        let mut bytes = valid[..header_size].to_vec();
        bytes.extend(zstd::encode_all(&raw[..], 0).unwrap());
        bytes
    };

    // A count off by a few can hide in the padding bits of the bitmap, so the count checks are
    // only as strong as the byte layout: a whole bitmap byte more than the payload holds
    let count_too_large = with_count(3 + 8);
    let count_too_small = with_count(2);

    let mut corrupt_payload = valid.clone();
    *corrupt_payload.last_mut().unwrap() ^= 0xff;

    let cases = [
        ("empty", Vec::new()),
        ("truncated header", valid[..header_size - 1].to_vec()),
        ("truncated payload", valid[..valid.len() - 1].to_vec()),
        ("bad magic", bad_magic),
        ("bad version", bad_version),
        ("count too large", count_too_large),
        ("count too small", count_too_small),
        ("corrupt payload", corrupt_payload),
    ];
    for (name, bytes) in cases {
        MmapFs
            .atomic_save(&dir.path().join(FILE_NAME), &bytes)
            .unwrap();
        let err = CompactedTracker::open(&MmapFs, dir.path()).unwrap_err();
        assert!(
            err.to_string().contains("Invalid compacted tracker"),
            "{name}: {err}"
        );
    }

    // And the untouched encoding still opens
    MmapFs
        .atomic_save(&dir.path().join(FILE_NAME), &valid)
        .unwrap();
    let reopened = CompactedTracker::open(&MmapFs, dir.path()).unwrap();
    assert_reads(&reopened, &expected);
}

#[test]
fn test_packed_mappings_encode_small() {
    let lengths = (0..10_000).map(|i| 100 + (i % 37)).collect::<Vec<u32>>();
    let expected = packed_pointers(&lengths, &[], 1 << 20);
    let (_dir, tracker) = tracker_with(&expected);

    let bytes = encode(&tracker.pointers).unwrap();
    // 16 bytes per entry in the flat tracker file; the deltas are zeros here, only the
    // lengths carry information, so the file should be a small fraction of that
    assert!(
        bytes.len() < expected.len(),
        "{} bytes for {} mappings",
        bytes.len(),
        expected.len()
    );
}
