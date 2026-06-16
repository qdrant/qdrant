use std::path::PathBuf;

use common::universal_io::{MmapFile, MmapFs};
use rstest::rstest;
use tempfile::Builder;

use super::*;

type TestTracker = Tracker<MmapFile>;

impl TestTracker {
    /// Get the length of the mapping
    /// Excludes None values
    /// Warning: performs a full scan of the tracker.
    #[allow(clippy::unnecessary_wraps)]
    pub fn mapping_len(&self) -> Result<usize> {
        let mut count = 0;
        for i in 0..self.next_pointer_offset {
            if self.get(i).ok().flatten().is_some() {
                count += 1;
            }
        }
        Ok(count)
    }

    pub fn is_empty(&self) -> bool {
        self.mapping_len().unwrap_or(0) == 0
    }

    /// Return the size of the underlying file
    pub fn mmap_file_size(&self) -> Result<usize> {
        self.storage
            .len::<u8>()
            .map(|u| u as usize)
            .map_err(Into::into)
    }

    pub fn write_pending_and_flush_internal(&mut self) -> Result<Vec<ValuePointer>> {
        let pending_updates = std::mem::take(&mut self.pending_updates);
        let res = self.write_pending(pending_updates)?;
        self.storage.flusher()()?;
        Ok(res)
    }
}

#[test]
fn test_file_name() {
    let path: PathBuf = "/tmp/test".into();
    let file_name = TestTracker::tracker_file_name(&path);
    assert_eq!(file_name, path.join(TestTracker::FILE_NAME));
}

#[test]
fn test_page_tracker_files() {
    let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
    let path = file.path();
    let tracker = TestTracker::new(&MmapFs, path, None).unwrap();
    let files = tracker.files();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0], path.join(TestTracker::FILE_NAME));
}

#[test]
fn test_new_tracker() {
    let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
    let path = file.path();
    let tracker = TestTracker::new(&MmapFs, path, None).unwrap();
    assert!(tracker.is_empty());
    assert_eq!(tracker.mapping_len().unwrap(), 0);
    assert_eq!(tracker.pointer_count(), 0);
}

#[rstest]
#[case(10)]
#[case(100)]
#[case(1000)]
fn test_mapping_len_tracker(#[case] initial_tracker_size: usize) {
    let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
    let path = file.path();
    let mut tracker = TestTracker::new(&MmapFs, path, Some(initial_tracker_size)).unwrap();
    assert!(tracker.is_empty());
    tracker.set(0, ValuePointer::new(1, 1, 1));

    tracker.write_pending_and_flush_internal().unwrap();

    assert!(!tracker.is_empty());
    assert_eq!(tracker.mapping_len().unwrap(), 1);

    tracker.set(100, ValuePointer::new(2, 2, 2));

    tracker.write_pending_and_flush_internal().unwrap();

    assert_eq!(tracker.pointer_count(), 101);
    assert_eq!(tracker.mapping_len().unwrap(), 2);
}

#[rstest]
#[case(10)]
#[case(100)]
#[case(1000)]
fn test_set_get_clear_tracker(#[case] initial_tracker_size: usize) {
    let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
    let path = file.path();
    let mut tracker = TestTracker::new(&MmapFs, path, Some(initial_tracker_size)).unwrap();
    tracker.set(0, ValuePointer::new(1, 1, 1));
    tracker.set(1, ValuePointer::new(2, 2, 2));
    tracker.set(2, ValuePointer::new(3, 3, 3));
    tracker.set(10, ValuePointer::new(10, 10, 10));

    tracker.write_pending_and_flush_internal().unwrap();
    assert!(!tracker.is_empty());
    assert_eq!(tracker.mapping_len().unwrap(), 4);
    assert_eq!(tracker.pointer_count(), 11); // accounts for empty slots

    assert_eq!(
        tracker.get_raw(0).unwrap(),
        Some(ValuePointer::new(1, 1, 1))
    );
    assert_eq!(
        tracker.get_raw(1).unwrap(),
        Some(ValuePointer::new(2, 2, 2))
    );
    assert_eq!(
        tracker.get_raw(2).unwrap(),
        Some(ValuePointer::new(3, 3, 3))
    );
    assert_eq!(tracker.get_raw(3).unwrap(), None); // intermediate empty slot
    assert_eq!(
        tracker.get_raw(10).unwrap(),
        Some(ValuePointer::new(10, 10, 10))
    );
    assert_eq!(tracker.get_raw(100_000).unwrap(), None); // out of bounds

    tracker.unset(1).unwrap();

    tracker.write_pending_and_flush_internal().unwrap();

    // the value has been cleared but the entry is still there
    assert_eq!(tracker.get_raw(1).unwrap(), None);
    assert_eq!(tracker.get(1).unwrap(), None);

    assert_eq!(tracker.mapping_len().unwrap(), 3);
    assert_eq!(tracker.pointer_count(), 11);

    // overwrite some values
    tracker.set(0, ValuePointer::new(10, 10, 10));
    tracker.set(2, ValuePointer::new(30, 30, 30));

    tracker.write_pending_and_flush_internal().unwrap();

    assert_eq!(tracker.get(0).unwrap(), Some(ValuePointer::new(10, 10, 10)));
    assert_eq!(tracker.get(2).unwrap(), Some(ValuePointer::new(30, 30, 30)));
}

#[rstest]
#[case(10)]
#[case(100)]
#[case(1000)]
fn test_persist_and_open_tracker(#[case] initial_tracker_size: usize) {
    let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
    let path = file.path();

    let value_count: usize = 1000;

    let mut tracker = TestTracker::new(&MmapFs, path, Some(initial_tracker_size)).unwrap();

    for i in 0..value_count {
        // save only half of the values
        if i % 2 == 0 {
            tracker.set(i as u32, ValuePointer::new(i as u32, i as u32, i as u32));
        }
    }
    tracker.write_pending_and_flush_internal().unwrap();

    assert_eq!(tracker.mapping_len().unwrap(), value_count / 2);
    assert_eq!(tracker.pointer_count(), value_count as u32 - 1);

    // drop the tracker
    drop(tracker);

    // reopen the tracker
    let tracker = TestTracker::open(&MmapFs, path, true).unwrap();
    assert_eq!(tracker.mapping_len().unwrap(), value_count / 2);
    assert_eq!(tracker.pointer_count(), value_count as u32 - 1);

    // check the values
    for i in 0..value_count {
        if i % 2 == 0 {
            assert_eq!(
                tracker.get(i as u32).unwrap(),
                Some(ValuePointer::new(i as u32, i as u32, i as u32))
            );
        } else {
            assert_eq!(tracker.get(i as u32).unwrap(), None);
        }
    }
}

#[rstest]
#[case(10, 16)]
#[case(100, 128)]
#[case(1000, 1024)]
#[case(1024, 1024)]
fn test_page_tracker_resize(
    #[case] desired_tracker_size: usize,
    #[case] actual_tracker_size: usize,
) {
    let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
    let path = file.path();

    let mut tracker = TestTracker::new(&MmapFs, path, Some(desired_tracker_size)).unwrap();
    assert_eq!(tracker.mapping_len().unwrap(), 0);
    assert_eq!(tracker.mmap_file_size().unwrap(), actual_tracker_size);

    for i in 0..100_000 {
        tracker.set(i, ValuePointer::new(i, i, i));
    }

    tracker.write_pending_and_flush_internal().unwrap();

    assert_eq!(tracker.mapping_len().unwrap(), 100_000);
    assert!(tracker.mmap_file_size().unwrap() > actual_tracker_size);
}

#[test]
fn test_track_non_sequential_large_offset() {
    let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
    let path = file.path();

    let mut tracker = TestTracker::new(&MmapFs, path, None).unwrap();
    assert_eq!(tracker.mapping_len().unwrap(), 0);

    let page_pointer = ValuePointer::new(1, 1, 1);
    let key = 1_000_000;

    tracker.set(key, page_pointer);
    assert_eq!(tracker.get(key).unwrap(), Some(page_pointer));
}

#[test]
fn test_value_pointer_drain() {
    let mut updates = PointerUpdates::default();
    updates.set(ValuePointer::new(1, 1, 1));

    // When all updates are persisted, drop the entry
    assert!(updates.clone().drain_persisted(&updates), "must drop entry");

    updates.set(ValuePointer::new(1, 2, 1));

    // When all updates are persisted, drop the entry
    assert!(updates.clone().drain_persisted(&updates), "must drop entry");

    let persisted = updates.clone();
    updates.set(ValuePointer::new(1, 3, 1));

    // Last pointer was not persisted, only keep it for the next flush
    {
        let mut updates = updates.clone();
        assert!(!updates.drain_persisted(&persisted));
        assert_eq!(updates.current, Some(ValuePointer::new(1, 3, 1))); // set block offset 3
        assert_eq!(
            updates.to_free.as_slice(),
            &[
                ValuePointer::new(1, 2, 1), // unset block offset 2 (last persisted was set)
            ]
        );
    }

    updates.set(ValuePointer::new(1, 4, 1));

    // Last two pointers were not persisted, only keep them for the next flush
    {
        let mut updates = updates.clone();
        assert!(!updates.drain_persisted(&persisted));
        assert_eq!(updates.current, Some(ValuePointer::new(1, 4, 1))); // set block offset 4
        assert_eq!(
            updates.to_free.as_slice(),
            &[
                ValuePointer::new(1, 2, 1), // unset block offset 2 (last persisted was set)
                ValuePointer::new(1, 3, 1), // unset block offset 3
            ]
        );
    }

    let persisted = updates.clone();
    updates.unset(ValuePointer::new(1, 4, 1));

    // Last pointer write is persisted, but the delete of the last pointer is not
    // Then we keep the last pointer with set=false to flush the delete next time
    {
        let mut updates = updates.clone();
        assert!(!updates.drain_persisted(&persisted));
        assert_eq!(updates.current, None);
        assert_eq!(updates.to_free.as_slice(), &[ValuePointer::new(1, 4, 1)]);
    }

    // Even if the history would somehow be shuffled we'd still properly drain
    {
        let mut updates = updates.clone();
        let mut persisted = updates.clone();
        persisted.to_free.swap(0, 1);
        persisted.to_free.swap(1, 3);
        assert!(updates.drain_persisted(&persisted));
    }
}

/// Test pointer drain edge case that was previously broken.
///
/// See: <https://github.com/qdrant/qdrant/pull/7741>
#[test]
fn test_value_pointer_drain_bug_7741() {
    // current:
    // - latest: true
    // - history: [block_offset:1, block_offset:2]
    //
    // persisted:
    // - latest: false
    // - history: [block_offset:1]
    //
    // expected current after drain:
    // - latest: true
    // - history: [block_offset:2]

    let mut updates = PointerUpdates::default();

    // Put and delete block offset 1
    updates.set(ValuePointer::new(1, 1, 1));
    updates.unset(ValuePointer::new(1, 1, 1));

    // Clone this set of updates to flush later
    let persisted = updates.clone();

    // Put block offset 2
    updates.set(ValuePointer::new(1, 2, 1));

    // Drain persisted updates and don't drop, still need to persist block offset 2 later
    let do_drop = updates.drain_persisted(&persisted);
    assert!(!do_drop, "must not drop entry");

    // Pending updates must only have set for block offset 2
    let expected = {
        let mut expected = PointerUpdates::default();
        expected.set(ValuePointer::new(1, 2, 1));
        expected
    };
    assert_eq!(
        updates, expected,
        "must have one pending update to set block offset 2",
    );
}

#[test]
fn test_option_value_pointer_layout() {
    #[repr(align(4))]
    struct AlignedData([u8; std::mem::size_of::<OptionalPointer>()]);

    assert_eq!(
        std::mem::size_of::<OptionalPointer>(),
        std::mem::size_of::<Option<ValuePointer>>()
    );

    let none_data = AlignedData([0; _]);
    let none_val: &OptionalPointer = bytemuck::cast_ref(&none_data.0);
    assert!(none_val.to_option().is_none());

    let some_data = AlignedData([
        1, 0, 0, 0, // discriminant with padding
        0x44, 0x33, 0x22, 0x11, // page_id
        0x88, 0x77, 0x66, 0x55, // block_offset
        0xDD, 0xCC, 0xBB, 0xAA, // length
    ]);
    let some_val: &OptionalPointer = bytemuck::cast_ref(&some_data.0);
    // N.B. fails on a big-endian machine.
    assert_eq!(
        some_val.to_option(),
        Some(ValuePointer {
            page_id: 0x11223344,
            block_offset: 0x55667788,
            length: 0xAABBCCDD,
        })
    );
}

#[test]
#[ignore = "contains undefined behavior"]
fn test_layout_compatibility() {
    assert_eq!(
        std::mem::size_of::<OptionalPointer>(),
        std::mem::size_of::<Option<ValuePointer>>()
    );

    let old_none = Option::<ValuePointer>::None;
    let new_none = OptionalPointer::none();

    // KLUDGE: actually this is UNDEFINED BEHAVIOR because old_one type contains padding bytes.
    // But we don't have any better option.
    unsafe { compare_layout(&old_none, &new_none) };

    const PAGE_ID: PageId = 89;
    const BLOCK_OFFSET: BlockOffset = 1000;
    const LENGTH: u32 = 1234567;

    let old_value = Some(ValuePointer {
        page_id: PAGE_ID,
        block_offset: BLOCK_OFFSET,
        length: LENGTH,
    });
    let new_value = OptionalPointer::some(ValuePointer::new(PAGE_ID, BLOCK_OFFSET, LENGTH));

    // KLUDGE: actually this is UNDEFINED BEHAVIOR because old_one type contains padding bytes.
    // But we don't have any better option.
    unsafe { compare_layout(&old_value, &new_value) };

    unsafe fn compare_layout<A, B>(a: &A, b: &B) {
        use std::slice::from_raw_parts;

        let a_data =
            unsafe { from_raw_parts((a as *const A).cast::<u8>(), std::mem::size_of::<A>()) };
        let b_data =
            unsafe { from_raw_parts((b as *const B).cast::<u8>(), std::mem::size_of::<B>()) };

        assert_eq!(a_data, b_data);
    }
}
