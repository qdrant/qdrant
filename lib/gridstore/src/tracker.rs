use std::path::{Path, PathBuf};

use ahash::{AHashMap, AHashSet};
use common::mmap::{Advice, AdviceSetting, MmapFlusher, MmapSlice};
#[expect(deprecated, reason = "legacy code")]
use common::mmap::{create_and_ensure_length, open_write_mmap, transmute_from_u8, transmute_to_u8};
use smallvec::SmallVec;
use zerocopy::FromZeros;

use crate::Result;
use crate::error::GridstoreError;

pub type PointOffset = u32;
pub type BlockOffset = u32;
pub type PageId = u32;

const TRACKER_MEM_ADVICE: Advice = Advice::Random;

/// A type similar to [`std::option::Option`], but with stable layout. It is intended to be compatible with older
/// gridstore files, but it is well-defined, unlike [`std::option::Option`].
///
/// Please note that it uses 32-bit tag and is intended to be used for `ValuePointer` without padding bytes.
///
/// If `T` is a POD type, then `Optional<T>` is a POD type.
#[derive(Copy, Clone, zerocopy::FromBytes)]
#[repr(C)]
struct Optional<T> {
    discriminant: u32,
    value: T,
}

impl<T: FromZeros> From<Option<T>> for Optional<T> {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => Self::some(value),
            None => Self::none(),
        }
    }
}

impl<T: FromZeros> Optional<T> {
    const OPTIONAL_NONE: u32 = 0;
    const OPTIONAL_SOME: u32 = 1;

    /// None value is all zeroes.
    pub fn none() -> Self {
        Self {
            discriminant: Self::OPTIONAL_NONE,
            value: FromZeros::new_zeroed(),
        }
    }

    /// Some is 1 for the discriminant, and value is stored as is.
    pub const fn some(value: T) -> Self {
        Self {
            discriminant: Self::OPTIONAL_SOME,
            value,
        }
    }

    pub fn is_some(&self) -> Option<&T> {
        if self.discriminant == Self::OPTIONAL_NONE {
            None
        } else {
            Some(&self.value)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, zerocopy::FromBytes)]
#[repr(C)]
pub struct ValuePointer {
    /// Which page the value is stored in
    pub page_id: PageId,

    /// Start offset (in blocks) of the value
    pub block_offset: BlockOffset,

    /// Length in bytes of the value
    pub length: u32,
}

impl ValuePointer {
    pub fn new(page_id: PageId, block_offset: BlockOffset, length: u32) -> Self {
        Self {
            page_id,
            block_offset,
            length,
        }
    }
}

/// Pointer updates for a given point offset
///
/// Keeps track of the places where the value for a point offset have been written, until we persist them.
///
/// In context of Gridstore, for each point offset this means:
///
/// - `current` is the value the tracker should report and become persisted when flushing.
///   If exists, `Some`; otherwise, `None`.
///
/// - `to_free` is the list of pointers that should be freed in the bitmask during flush, so that
///   the space in the pages can be reused.
///
/// When flushing, we persist all changes we have currently collected. It is possible that new changes
/// come in between preparing the flusher and executing it. After we've written to disk, we remove (drain),
/// the now persisted changes from these pointer updates. With this mechanism we write each update to
/// disk exactly once.
#[derive(Debug, Default, Clone, PartialEq)]
pub(super) struct PointerUpdates {
    /// Pointer to write in tracker when persisting
    current: Option<ValuePointer>,
    /// List of pointers to free in bitmask when persisting
    to_free: SmallVec<[ValuePointer; 1]>,
}

impl PointerUpdates {
    /// Mark this pointer as set
    ///
    /// It will mark the pointer as used on disk on flush, and will free all previous pending
    /// pointers
    fn set(&mut self, pointer: ValuePointer) {
        if self.current == Some(pointer) {
            debug_assert!(false, "we should not set the same point twice");
            return;
        }

        // Move the current pointer to the pointers to free, if it exists
        if let Some(old_pointer) = self.current.replace(pointer) {
            self.to_free.push(old_pointer);
            debug_assert_eq!(
                self.to_free.iter().copied().collect::<AHashSet<_>>().len(),
                self.to_free.len(),
                "should not have duplicate pointers to free",
            );
        }

        debug_assert!(
            !self.to_free.contains(&pointer),
            "old list cannot contain pointer we just set",
        );
    }

    /// Mark this pointer as unset
    ///
    /// It will completely free the pointer on disk on flush including all it's previous pending
    /// pointers
    fn unset(&mut self, pointer: ValuePointer) {
        let old_pointer = self.current.take();

        // Fallback: if the pointer to unset is not the current one, free both pointers, though this shouldn't happen
        debug_assert!(
            old_pointer.is_none_or(|p| p == pointer),
            "new unset pointer should match with current one, if any",
        );
        if let Some(old_pointer) = old_pointer
            && old_pointer != pointer
        {
            self.to_free.push(old_pointer);
        }

        self.to_free.push(pointer);

        debug_assert_eq!(
            self.to_free.iter().copied().collect::<AHashSet<_>>().len(),
            self.to_free.len(),
            "should not have duplicate pointers to free",
        );
    }

    /// Pointer is empty if there is no set nor unsets
    fn is_empty(&self) -> bool {
        self.current.is_none() && self.to_free.is_empty()
    }

    /// Remove all pointers from self that have been persisted
    ///
    /// After calling this self may end up being empty. The caller is responsible for dropping
    /// empty structures if desired.
    ///
    /// Unknown pointers in `persisted` are ignored.
    ///
    /// Returns if the structure is empty after this operation
    fn drain_persisted(&mut self, persisted: &Self) -> bool {
        debug_assert!(!self.is_empty(), "must have at least one pointer");
        debug_assert!(
            !persisted.is_empty(),
            "persisted must have at least one pointer",
        );

        // Shortcut: we persisted everything if both are equal, we can empty this structure
        if self == persisted {
            *self = Self::default();
            return true;
        }

        let Self {
            current: previous_current,
            to_free: freed,
        } = persisted;

        // Remove self set if persisted
        if let (Some(current), Some(previous_current)) = (self.current, *previous_current)
            && current == previous_current
        {
            self.current.take();
        }

        // Only keep unsets that are not persisted
        self.to_free.retain(|pointer| !freed.contains(pointer));

        self.is_empty()
    }
}

#[derive(Debug, Default, Clone)]
#[repr(C)]
struct TrackerHeader {
    next_pointer_offset: u32,
}

#[derive(Debug)]
pub struct Tracker {
    /// Path to the file
    path: PathBuf,
    /// Header of the file
    header: TrackerHeader,
    /// Mmap of the file
    mmap: MmapSlice<u8>,
    /// Updates that haven't been flushed
    ///
    /// When flushing, these updates get written into the mmap and flushed at once.
    pub(super) pending_updates: AHashMap<PointOffset, PointerUpdates>,

    /// The maximum pointer offset in the tracker (updated in memory).
    next_pointer_offset: PointOffset,
}

impl Tracker {
    const FILE_NAME: &'static str = "tracker.dat";
    const DEFAULT_SIZE: usize = 1024 * 1024; // 1MB

    pub fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    fn tracker_file_name(path: &Path) -> PathBuf {
        path.join(Self::FILE_NAME)
    }

    /// Create a new PageTracker at the given dir path
    /// The file is created with the default size if no size hint is given
    pub fn new(path: &Path, size_hint: Option<usize>) -> Self {
        let path = Self::tracker_file_name(path);
        let size = size_hint.unwrap_or(Self::DEFAULT_SIZE).next_power_of_two();
        assert!(size > size_of::<TrackerHeader>(), "Size hint is too small");
        create_and_ensure_length(&path, size).expect("Failed to create page tracker file");
        let mmap = open_write_mmap(&path, AdviceSetting::from(TRACKER_MEM_ADVICE), false)
            .expect("Failed to open page tracker mmap");
        let mmap = unsafe { MmapSlice::<u8>::try_from(mmap) }
            .expect("Failed to create MmapSlice for tracker");
        let header = TrackerHeader::default();
        let pending_updates = AHashMap::new();
        let mut page_tracker = Self {
            path,
            header,
            mmap,
            pending_updates,
            next_pointer_offset: 0,
        };
        page_tracker.write_header();
        page_tracker
    }

    /// Open an existing PageTracker at the given path
    /// If the file does not exist, return None
    pub fn open(path: &Path) -> Result<Self> {
        let path = Self::tracker_file_name(path);
        if !path.exists() {
            return Err(GridstoreError::service_error(format!(
                "Tracker file does not exist: {}",
                path.display()
            )));
        }
        let mmap = open_write_mmap(&path, AdviceSetting::from(TRACKER_MEM_ADVICE), false)?;
        #[expect(deprecated, reason = "legacy code")]
        let header: TrackerHeader =
            // TODO SAFETY
            unsafe { transmute_from_u8::<TrackerHeader>(&mmap[0..size_of::<TrackerHeader>()]) }
                .clone();
        let mmap = unsafe { MmapSlice::<u8>::try_from(mmap)? };
        let pending_updates = AHashMap::new();
        Ok(Self {
            next_pointer_offset: header.next_pointer_offset,
            path,
            header,
            mmap,
            pending_updates,
        })
    }

    /// Writes the accumulated pending updates to mmap and flushes it
    ///
    /// Changes should be captured from [`self.pending_updates`]. This method may therefore flush
    /// an earlier version of changes.
    ///
    /// This updates the list of pending updates inside this tracker for each given update that is
    /// processed.
    ///
    /// Returns the old pointers that were overwritten, so that they can be freed in the bitmask.
    #[must_use = "The old pointers need to be freed in the bitmask"]
    pub fn write_pending(
        &mut self,
        pending_updates: AHashMap<PointOffset, PointerUpdates>,
    ) -> Vec<ValuePointer> {
        let mut old_pointers = Vec::new();

        for (point_offset, updates) in pending_updates {
            match updates.current {
                // Write to store a new pointer
                Some(new_pointer) => {
                    // Mark any existing pointer for removal to free its blocks
                    if let Some(Some(old_pointer)) = self.get_raw(point_offset) {
                        old_pointers.push(*old_pointer);
                    }

                    self.persist_pointer(point_offset, Some(new_pointer));
                }
                // Write to empty the pointer
                None => self.persist_pointer(point_offset, None),
            }

            // Mark all old pointers for removal to free its blocks
            old_pointers.extend(&updates.to_free);

            // Remove all persisted updates from the latest updates, drop if no changes are left
            if let Some(latest_updates) = self.pending_updates.get_mut(&point_offset) {
                let is_empty = latest_updates.drain_persisted(&updates);
                if is_empty {
                    let prev = self.pending_updates.remove(&point_offset);
                    if let Some(prev) = prev {
                        debug_assert!(
                            prev.is_empty(),
                            "remove pending element should be empty but got {prev:?}"
                        );
                    }
                }
            }
        }

        // Increment header count if necessary
        self.write_pointer_count();

        old_pointers
    }

    pub fn flusher(&self) -> MmapFlusher {
        self.mmap.flusher()
    }

    #[cfg(test)]
    pub fn write_pending_and_flush_internal(&mut self) -> Result<Vec<ValuePointer>> {
        let pending_updates = std::mem::take(&mut self.pending_updates);
        let res = self.write_pending(pending_updates);
        self.mmap.flusher()()?;
        Ok(res)
    }

    /// Return the size of the underlying mmapped file
    #[cfg(test)]
    pub fn mmap_file_size(&self) -> usize {
        self.mmap.len()
    }

    pub fn pointer_count(&self) -> u32 {
        self.next_pointer_offset
    }

    /// Write the current page header to the memory map
    fn write_header(&mut self) {
        // Safety: TrackerHeader is a POD type.
        #[expect(deprecated, reason = "legacy code")]
        let header_bytes = unsafe { transmute_to_u8(&self.header) };
        self.mmap[0..header_bytes.len()].copy_from_slice(header_bytes);
    }

    /// Save the mapping at the given offset
    /// The file is resized if necessary
    fn persist_pointer(&mut self, point_offset: PointOffset, pointer: Option<ValuePointer>) {
        if pointer.is_none() && point_offset as usize >= self.mmap.len() {
            return;
        }

        let point_offset = point_offset as usize;
        let start_offset =
            size_of::<TrackerHeader>() + point_offset * size_of::<Optional<ValuePointer>>();
        let end_offset = start_offset + size_of::<Optional<ValuePointer>>();

        // Grow tracker file if it isn't big enough
        if self.mmap.len() < end_offset {
            self.mmap.flusher()().unwrap();
            let new_size = end_offset.next_power_of_two();
            create_and_ensure_length(&self.path, new_size).unwrap();
            let new_mmap =
                open_write_mmap(&self.path, AdviceSetting::from(TRACKER_MEM_ADVICE), false)
                    .unwrap();
            self.mmap = unsafe { MmapSlice::<u8>::try_from(new_mmap) }.unwrap();
        }

        let pointer: Optional<_> = pointer.into();
        // Safety: Optional<ValuePointer> is a POD type.
        #[expect(deprecated, reason = "legacy code")]
        self.mmap[start_offset..end_offset].copy_from_slice(unsafe { transmute_to_u8(&pointer) });
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.mapping_len() == 0
    }

    /// Get the length of the mapping
    /// Excludes None values
    /// Warning: performs a full scan of the tracker.
    #[cfg(test)]
    pub fn mapping_len(&self) -> usize {
        (0..self.next_pointer_offset)
            .filter(|i| self.get(*i).is_some())
            .count()
    }

    /// Iterate over the pointers in the tracker
    /// Starts from the given point offset
    pub fn iter_pointers(
        &self,
        from: PointOffset,
    ) -> impl Iterator<Item = (PointOffset, Option<ValuePointer>)> + '_ {
        (from..self.next_pointer_offset).map(move |i| (i, self.get(i as PointOffset)))
    }

    /// Get the raw value at the given point offset
    fn get_raw(&self, point_offset: PointOffset) -> Option<Option<&ValuePointer>> {
        let start_offset = size_of::<TrackerHeader>()
            + point_offset as usize * size_of::<Optional<ValuePointer>>();
        let end_offset = start_offset + size_of::<Optional<ValuePointer>>();
        if end_offset > self.mmap.len() {
            return None;
        }
        // Safety: Optional<ValuePointer> is a POD type.
        #[expect(deprecated, reason = "legacy code")]
        let page_pointer: &Optional<_> =
            unsafe { transmute_from_u8(&self.mmap[start_offset..end_offset]) };
        Some(page_pointer.is_some())
    }

    /// Get the page pointer at the given point offset
    pub fn get(&self, point_offset: PointOffset) -> Option<ValuePointer> {
        match self.pending_updates.get(&point_offset) {
            // Pending update exists but is empty, should not happen, fall back to real data
            Some(pending) if pending.is_empty() => {
                debug_assert!(false, "pending updates must not be empty");
                self.get_raw(point_offset).flatten().cloned()
            }
            // Use set from pending updates
            Some(pending) => pending.current,
            // No pending update, use real data
            None => self.get_raw(point_offset).flatten().cloned(),
        }
    }

    /// Increment the header count if the given point offset is larger than the current count
    fn write_pointer_count(&mut self) {
        self.header.next_pointer_offset = self.next_pointer_offset;
        self.write_header();
    }

    pub fn has_pointer(&self, point_offset: PointOffset) -> bool {
        self.get(point_offset).is_some()
    }

    pub fn set(&mut self, point_offset: PointOffset, value_pointer: ValuePointer) {
        self.pending_updates
            .entry(point_offset)
            .or_default()
            .set(value_pointer);
        self.next_pointer_offset = self.next_pointer_offset.max(point_offset + 1);
    }

    /// Unset the value at the given point offset and return its previous value
    pub fn unset(&mut self, point_offset: PointOffset) -> Option<ValuePointer> {
        let pointer_opt = self.get(point_offset);

        if let Some(pointer) = pointer_opt {
            self.pending_updates
                .entry(point_offset)
                .or_default()
                .unset(pointer);
        }

        pointer_opt
    }

    pub fn populate(&self) -> std::io::Result<()> {
        self.mmap.populate()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    #[expect(deprecated, reason = "legacy code")]
    use common::mmap::transmute_from_u8;
    use rstest::rstest;
    use tempfile::Builder;

    use super::{PointerUpdates, Tracker, ValuePointer};
    use crate::tracker::{BlockOffset, Optional, PageId};

    #[test]
    fn test_file_name() {
        let path: PathBuf = "/tmp/test".into();
        let file_name = Tracker::tracker_file_name(&path);
        assert_eq!(file_name, path.join(Tracker::FILE_NAME));
    }

    #[test]
    fn test_page_tracker_files() {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();
        let tracker = Tracker::new(path, None);
        let files = tracker.files();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], path.join(Tracker::FILE_NAME));
    }

    #[test]
    fn test_new_tracker() {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();
        let tracker = Tracker::new(path, None);
        assert!(tracker.is_empty());
        assert_eq!(tracker.mapping_len(), 0);
        assert_eq!(tracker.pointer_count(), 0);
    }

    #[rstest]
    #[case(10)]
    #[case(100)]
    #[case(1000)]
    fn test_mapping_len_tracker(#[case] initial_tracker_size: usize) {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();
        let mut tracker = Tracker::new(path, Some(initial_tracker_size));
        assert!(tracker.is_empty());
        tracker.set(0, ValuePointer::new(1, 1, 1));

        tracker.write_pending_and_flush_internal().unwrap();

        assert!(!tracker.is_empty());
        assert_eq!(tracker.mapping_len(), 1);

        tracker.set(100, ValuePointer::new(2, 2, 2));

        tracker.write_pending_and_flush_internal().unwrap();

        assert_eq!(tracker.pointer_count(), 101);
        assert_eq!(tracker.mapping_len(), 2);
    }

    #[rstest]
    #[case(10)]
    #[case(100)]
    #[case(1000)]
    fn test_set_get_clear_tracker(#[case] initial_tracker_size: usize) {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();
        let mut tracker = Tracker::new(path, Some(initial_tracker_size));
        tracker.set(0, ValuePointer::new(1, 1, 1));
        tracker.set(1, ValuePointer::new(2, 2, 2));
        tracker.set(2, ValuePointer::new(3, 3, 3));
        tracker.set(10, ValuePointer::new(10, 10, 10));

        tracker.write_pending_and_flush_internal().unwrap();
        assert!(!tracker.is_empty());
        assert_eq!(tracker.mapping_len(), 4);
        assert_eq!(tracker.pointer_count(), 11); // accounts for empty slots

        assert_eq!(tracker.get_raw(0), Some(Some(&ValuePointer::new(1, 1, 1))));
        assert_eq!(tracker.get_raw(1), Some(Some(&ValuePointer::new(2, 2, 2))));
        assert_eq!(tracker.get_raw(2), Some(Some(&ValuePointer::new(3, 3, 3))));
        assert_eq!(tracker.get_raw(3), Some(None)); // intermediate empty slot
        assert_eq!(
            tracker.get_raw(10),
            Some(Some(&ValuePointer::new(10, 10, 10)))
        );
        assert_eq!(tracker.get_raw(100_000), None); // out of bounds

        tracker.unset(1);

        tracker.write_pending_and_flush_internal().unwrap();

        // the value has been cleared but the entry is still there
        assert_eq!(tracker.get_raw(1), Some(None));
        assert_eq!(tracker.get(1), None);

        assert_eq!(tracker.mapping_len(), 3);
        assert_eq!(tracker.pointer_count(), 11);

        // overwrite some values
        tracker.set(0, ValuePointer::new(10, 10, 10));
        tracker.set(2, ValuePointer::new(30, 30, 30));

        tracker.write_pending_and_flush_internal().unwrap();

        assert_eq!(tracker.get(0), Some(ValuePointer::new(10, 10, 10)));
        assert_eq!(tracker.get(2), Some(ValuePointer::new(30, 30, 30)));
    }

    #[rstest]
    #[case(10)]
    #[case(100)]
    #[case(1000)]
    fn test_persist_and_open_tracker(#[case] initial_tracker_size: usize) {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();

        let value_count: usize = 1000;

        let mut tracker = Tracker::new(path, Some(initial_tracker_size));

        for i in 0..value_count {
            // save only half of the values
            if i % 2 == 0 {
                tracker.set(i as u32, ValuePointer::new(i as u32, i as u32, i as u32));
            }
        }
        tracker.write_pending_and_flush_internal().unwrap();

        assert_eq!(tracker.mapping_len(), value_count / 2);
        assert_eq!(tracker.pointer_count(), value_count as u32 - 1);

        // drop the tracker
        drop(tracker);

        // reopen the tracker
        let tracker = Tracker::open(path).unwrap();
        assert_eq!(tracker.mapping_len(), value_count / 2);
        assert_eq!(tracker.pointer_count(), value_count as u32 - 1);

        // check the values
        for i in 0..value_count {
            if i % 2 == 0 {
                assert_eq!(
                    tracker.get(i as u32),
                    Some(ValuePointer::new(i as u32, i as u32, i as u32))
                );
            } else {
                assert_eq!(tracker.get(i as u32), None);
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

        let mut tracker = Tracker::new(path, Some(desired_tracker_size));
        assert_eq!(tracker.mapping_len(), 0);
        assert_eq!(tracker.mmap_file_size(), actual_tracker_size);

        for i in 0..100_000 {
            tracker.set(i, ValuePointer::new(i, i, i));
        }

        tracker.write_pending_and_flush_internal().unwrap();

        assert_eq!(tracker.mapping_len(), 100_000);
        assert!(tracker.mmap_file_size() > actual_tracker_size);
    }

    #[test]
    fn test_track_non_sequential_large_offset() {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();

        let mut tracker = Tracker::new(path, None);
        assert_eq!(tracker.mapping_len(), 0);

        let page_pointer = ValuePointer::new(1, 1, 1);
        let key = 1_000_000;

        tracker.set(key, page_pointer);
        assert_eq!(tracker.get(key), Some(page_pointer));
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
        struct AlignedData([u8; size_of::<Optional<ValuePointer>>()]);

        assert_eq!(
            size_of::<Optional<ValuePointer>>(),
            size_of::<Option<ValuePointer>>()
        );

        let none_data = AlignedData([0; _]);
        #[expect(deprecated, reason = "legacy code")]
        let none_val: &Optional<ValuePointer> = unsafe { transmute_from_u8(&none_data.0) };
        assert!(none_val.is_some().is_none());

        let some_data = AlignedData([
            1, 0, 0, 0, // discriminant with padding
            0x44, 0x33, 0x22, 0x11, // page_id
            0x88, 0x77, 0x66, 0x55, // block_offset
            0xDD, 0xCC, 0xBB, 0xAA, // length
        ]);
        #[expect(deprecated, reason = "legacy code")]
        let some_val: &Optional<ValuePointer> = unsafe { transmute_from_u8(&some_data.0) };
        // N.B. fails on a big-endian machine.
        assert_eq!(
            some_val.is_some(),
            Some(&ValuePointer {
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
            size_of::<Optional<ValuePointer>>(),
            size_of::<Option<ValuePointer>>()
        );

        let old_none = Option::<ValuePointer>::None;
        let new_none = Optional::<ValuePointer>::none();

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
        let new_value = Optional::some(ValuePointer::new(PAGE_ID, BLOCK_OFFSET, LENGTH));

        // KLUDGE: actually this is UNDEFINED BEHAVIOR because old_one type contains padding bytes.
        // But we don't have any better option.
        unsafe { compare_layout(&old_value, &new_value) };

        unsafe fn compare_layout<A, B>(a: &A, b: &B) {
            use std::slice::from_raw_parts;

            let a_data = unsafe { from_raw_parts((a as *const A).cast::<u8>(), size_of::<A>()) };
            let b_data = unsafe { from_raw_parts((b as *const B).cast::<u8>(), size_of::<B>()) };

            assert_eq!(a_data, b_data);
        }
    }
}
