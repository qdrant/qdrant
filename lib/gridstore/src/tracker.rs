use std::path::{Path, PathBuf};

use ahash::{AHashMap, AHashSet};
use memmap2::MmapMut;
use memory::madvise::{Advice, AdviceSetting, Madviseable};
use memory::mmap_ops::{
    create_and_ensure_length, open_write_mmap, transmute_from_u8, transmute_to_u8,
};
use smallvec::SmallVec;

pub type PointOffset = u32;
pub type BlockOffset = u32;
pub type PageId = u32;

const TRACKER_MEM_ADVICE: Advice = Advice::Random;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, PartialEq)]
pub(super) struct PointerUpdates {
    /// Whether the latest pointer is set (`true`) or unset (`false`).
    /// If this is `true`, then history must have at least one element.
    latest_is_set: bool,
    /// List of pointers where the value has been written
    history: SmallVec<[ValuePointer; 1]>,
    /// Whether this pointer updates set contains the last update for this pointer
    ///
    /// Using [`into_split_update_deletes`] may create two separate update structures of which one
    /// does not have the latest update.
    ///
    /// When persisting the pointer, we only unset the mapping if the latest update is not a set.
    has_latest_update: bool,
}

impl PointerUpdates {
    /// Construct new empty updates structure, assume this is the latest
    pub fn new() -> Self {
        Self {
            latest_is_set: false,
            history: SmallVec::new(),
            has_latest_update: true,
        }
    }

    /// Construct a updates structure from separate set and unsets
    fn from_set_unset(
        set: Option<ValuePointer>,
        unsets: SmallVec<[ValuePointer; 1]>,
        has_latest_update: bool,
    ) -> Self {
        let mut updates = Self {
            latest_is_set: false,
            history: unsets,
            has_latest_update,
        };
        if let Some(set) = set {
            updates.set(set);
        }
        updates
    }

    /// Set the current latest pointer
    fn set(&mut self, pointer: ValuePointer) {
        self.history.push(pointer);
        self.latest_is_set = true;
    }

    /// Mark this pointer as pending for freeing
    fn unset(&mut self, pointer: ValuePointer) {
        // Prevent duplicating pointers to free
        if self.history.last() != Some(&pointer) {
            self.history.push(pointer);
        }
        self.latest_is_set = false;
    }

    /// Set is Some, Unset is None
    fn latest(&self) -> Option<ValuePointer> {
        if self.latest_is_set {
            self.history.last().copied()
        } else {
            None
        }
    }

    /// Split these updates in pointers we set and unset
    fn split_set_unset(&self) -> (Option<ValuePointer>, &[ValuePointer]) {
        if self.latest_is_set {
            (
                self.history.last().copied(),
                &self.history.as_slice()[..self.history.len().saturating_sub(1)],
            )
        } else {
            (None, self.history.as_slice())
        }
    }

    /// Returns pointers that need to be freed, i.e. They have been written, and are no longer needed
    fn to_outdated_pointers(&self) -> impl Iterator<Item = ValuePointer> {
        self.split_set_unset().1.iter().copied()
    }

    /// Split this pointer update structure into two
    ///
    /// A structure that only sets the pointer, and a structure that only unsets pointers.
    pub fn into_split_update_deletes(self) -> (Option<Self>, Option<Self>) {
        let (set, unsets) = self.split_set_unset();

        // We only have a set structure if a pointer is set
        let set = set.map(|pointer| PointerUpdates {
            latest_is_set: true,
            history: SmallVec::from([pointer]),
            // A set is always considered the last update
            has_latest_update: true,
        });

        // We only have a unset structure if any pointer is unset
        let unsets = (!unsets.is_empty()).then(|| PointerUpdates {
            latest_is_set: false,
            history: SmallVec::from_slice(unsets),
            // The last unset is only considered the last update if we don't have a set
            has_latest_update: set.is_none(),
        });

        (set, unsets)
    }

    /// Bump this pointer updates structure to drain all details that have been persisted
    ///
    /// The pointer updates structure that we have persisted must be given. All persisted details
    /// that are inside the current pointer updates structure are removed in-place. The pointer
    /// updates structure we're left with only contains details that have not yet been persisted.
    ///
    /// Returns true if the structure is fully persisted. In that case, the caller must drop it
    /// from the list of pending updates.
    #[must_use = "if true is returned, entry must be dropped from pending changes"]
    fn drain_persisted_and_drop(&mut self, persisted: &Self) -> bool {
        // We don't expect duplicate pointers in history
        debug_assert!(
            !self.history.is_empty(),
            "self must not have empty pointer history",
        );
        debug_assert!(
            !persisted.history.is_empty(),
            "persisted must not have empty pointer history",
        );
        debug_assert_eq!(
            self.history.iter().copied().collect::<AHashSet<_>>().len(),
            self.history.len(),
            "self must not have duplicate pointers in history",
        );
        debug_assert_eq!(
            persisted
                .history
                .iter()
                .copied()
                .collect::<AHashSet<_>>()
                .len(),
            persisted.history.len(),
            "persisted must not have duplicate pointers in history",
        );

        // If both are the same, we have persisted the entry and can drop the pending change
        if self == persisted {
            self.latest_is_set = false;
            self.history.clear();
            return true;
        }

        let (mut our_set, our_unsets) = self.split_set_unset();
        let (persisted_set, persisted_unsets) = persisted.split_set_unset();

        // If we have persisted this exact set, we don't have to persist it again
        if our_set == persisted_set {
            our_set = None;
        }

        // Remove all persisted unsets
        let mut our_unsets = SmallVec::from_slice(our_unsets);
        our_unsets.retain(|pointer| !persisted_unsets.contains(pointer));

        // Rebuild our updates structure
        let new_pointer =
            PointerUpdates::from_set_unset(our_set, our_unsets, self.has_latest_update);
        *self = new_pointer;

        // Drop entry if history is exhausted
        self.history.is_empty()
    }
}

#[derive(Debug, Default, Clone)]
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
    mmap: MmapMut,
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
    pub fn open(path: &Path) -> Result<Self, String> {
        let path = Self::tracker_file_name(path);
        if !path.exists() {
            return Err(format!("Tracker file does not exist: {}", path.display()));
        }
        let mmap = open_write_mmap(&path, AdviceSetting::from(TRACKER_MEM_ADVICE), false)
            .map_err(|err| err.to_string())?;
        let header: &TrackerHeader = transmute_from_u8(&mmap[0..size_of::<TrackerHeader>()]);
        let pending_updates = AHashMap::new();
        Ok(Self {
            next_pointer_offset: header.next_pointer_offset,
            path,
            header: header.clone(),
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
    pub fn write_pending_and_flush(
        &mut self,
        pending_updates: AHashMap<PointOffset, PointerUpdates>,
    ) -> std::io::Result<Vec<ValuePointer>> {
        // Write pending updates from memory
        let mut old_pointers = Vec::new();
        for (point_offset, updates) in pending_updates {
            match updates.latest() {
                Some(new_pointer) => {
                    if let Some(old_pointer) =
                        self.get_raw(point_offset).and_then(|pointer| *pointer)
                    {
                        old_pointers.push(old_pointer);
                    }

                    // write the new pointer
                    self.persist_pointer(point_offset, Some(new_pointer));
                }
                // If this is the last update and have no set, we can nullify the pointer on disk
                None if updates.has_latest_update => {
                    self.persist_pointer(point_offset, None);
                }
                None => {}
            }
            old_pointers.extend(updates.to_outdated_pointers());

            // Bump the pending updates for this point offset, drop entry if fully persisted
            if let Some(pending_updates) = self.pending_updates.get_mut(&point_offset)
                && pending_updates.drain_persisted_and_drop(&updates)
            {
                self.pending_updates.remove(&point_offset);
            }
        }
        // increment header count if necessary
        self.persist_pointer_count();

        // Flush the mmap
        self.mmap.flush()?;

        Ok(old_pointers)
    }

    #[cfg(test)]
    pub fn write_pending_and_flush_internal(&mut self) -> std::io::Result<Vec<ValuePointer>> {
        let pending_updates = std::mem::take(&mut self.pending_updates);
        self.write_pending_and_flush(pending_updates)
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
        self.mmap[0..size_of::<TrackerHeader>()].copy_from_slice(transmute_to_u8(&self.header));
    }

    /// Save the mapping at the given offset
    /// The file is resized if necessary
    fn persist_pointer(&mut self, point_offset: PointOffset, pointer: Option<ValuePointer>) {
        if pointer.is_none() && point_offset as usize >= self.mmap.len() {
            return;
        }

        let point_offset = point_offset as usize;
        let start_offset =
            size_of::<TrackerHeader>() + point_offset * size_of::<Option<ValuePointer>>();
        let end_offset = start_offset + size_of::<Option<ValuePointer>>();

        // Grow tracker file if it isn't big enough
        if self.mmap.len() < end_offset {
            self.mmap.flush().unwrap();
            let new_size = end_offset.next_power_of_two();
            create_and_ensure_length(&self.path, new_size).unwrap();
            self.mmap = open_write_mmap(&self.path, AdviceSetting::from(TRACKER_MEM_ADVICE), false)
                .unwrap();
        }

        self.mmap[start_offset..end_offset].copy_from_slice(transmute_to_u8(&pointer));
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.mapping_len() == 0
    }

    /// Get the length of the mapping
    /// Excludes None values
    #[cfg(test)]
    pub fn mapping_len(&self) -> usize {
        (0..self.next_pointer_offset)
            .filter(|i| self.get(*i).is_some())
            .count()
    }

    /// Iterate over the pointers in the tracker
    pub fn iter_pointers(&self) -> impl Iterator<Item = (PointOffset, Option<ValuePointer>)> + '_ {
        (0..self.next_pointer_offset).map(move |i| (i, self.get(i as PointOffset)))
    }

    /// Get the raw value at the given point offset
    fn get_raw(&self, point_offset: PointOffset) -> Option<&Option<ValuePointer>> {
        let start_offset =
            size_of::<TrackerHeader>() + point_offset as usize * size_of::<Option<ValuePointer>>();
        let end_offset = start_offset + size_of::<Option<ValuePointer>>();
        if end_offset > self.mmap.len() {
            return None;
        }
        let page_pointer = transmute_from_u8(&self.mmap[start_offset..end_offset]);
        Some(page_pointer)
    }

    /// Get the page pointer at the given point offset
    pub fn get(&self, point_offset: PointOffset) -> Option<ValuePointer> {
        self.pending_updates
            .get(&point_offset)
            .map(PointerUpdates::latest)
            // if the value is not in the pending updates, check the mmap
            .or_else(|| self.get_raw(point_offset).copied())
            .flatten()
    }

    /// Increment the header count if the given point offset is larger than the current count
    fn persist_pointer_count(&mut self) {
        self.header.next_pointer_offset = self.next_pointer_offset;
        self.write_header();
    }

    pub fn has_pointer(&self, point_offset: PointOffset) -> bool {
        self.pending_updates.contains_key(&point_offset) || self.get(point_offset).is_some()
    }

    pub fn set(&mut self, point_offset: PointOffset, value_pointer: ValuePointer) {
        self.pending_updates
            .entry(point_offset)
            .or_insert_with(PointerUpdates::new)
            .set(value_pointer);
        self.next_pointer_offset = self.next_pointer_offset.max(point_offset + 1);
    }

    /// Unset the value at the given point offset and return its previous value
    pub fn unset(&mut self, point_offset: PointOffset) -> Option<ValuePointer> {
        let pointer_opt = self.get(point_offset);

        if let Some(pointer) = pointer_opt {
            self.pending_updates
                .entry(point_offset)
                .or_insert_with(PointerUpdates::new)
                .unset(pointer);
        }

        pointer_opt
    }

    pub fn populate(&self) {
        self.mmap.populate();
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rstest::rstest;
    use tempfile::Builder;

    use super::{PointerUpdates, Tracker, ValuePointer};

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

        assert_eq!(tracker.get_raw(0), Some(&Some(ValuePointer::new(1, 1, 1))));
        assert_eq!(tracker.get_raw(1), Some(&Some(ValuePointer::new(2, 2, 2))));
        assert_eq!(tracker.get_raw(2), Some(&Some(ValuePointer::new(3, 3, 3))));
        assert_eq!(tracker.get_raw(3), Some(&None)); // intermediate empty slot
        assert_eq!(
            tracker.get_raw(10),
            Some(&Some(ValuePointer::new(10, 10, 10)))
        );
        assert_eq!(tracker.get_raw(100_000), None); // out of bounds

        tracker.unset(1);

        tracker.write_pending_and_flush_internal().unwrap();

        // the value has been cleared but the entry is still there
        assert_eq!(tracker.get_raw(1), Some(&None));
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
        let mut updates = PointerUpdates::new();
        updates.set(ValuePointer::new(1, 1, 1));

        // When all updates are persisted, drop the entry
        assert!(
            updates.clone().drain_persisted_and_drop(&updates),
            "must drop entry"
        );

        updates.set(ValuePointer::new(1, 2, 1));

        // When all updates are persisted, drop the entry
        assert!(
            updates.clone().drain_persisted_and_drop(&updates),
            "must drop entry"
        );

        let persisted = updates.clone();
        updates.set(ValuePointer::new(1, 3, 1));

        // Last pointer was not persisted, only keep it for the next flush
        {
            let mut updates = updates.clone();
            assert!(!updates.drain_persisted_and_drop(&persisted));
            assert!(updates.latest_is_set);
            assert_eq!(
                updates.history.as_slice(),
                &[
                    ValuePointer::new(1, 2, 1), // unset
                    ValuePointer::new(1, 3, 1), // set
                ]
            );
        }

        updates.set(ValuePointer::new(1, 4, 1));

        // Last two pointers were not persisted, only keep them for the next flush
        {
            let mut updates = updates.clone();
            assert!(!updates.drain_persisted_and_drop(&persisted));
            assert!(updates.latest_is_set);
            assert_eq!(
                updates.history.as_slice(),
                &[
                    ValuePointer::new(1, 2, 1), // unset
                    ValuePointer::new(1, 3, 1), // unset
                    ValuePointer::new(1, 4, 1), // set
                ]
            );
        }

        let persisted = updates.clone();
        updates.unset(ValuePointer::new(1, 4, 1));

        // Last pointer write is persisted, but the delete of the last pointer is not
        // Then we keep the last pointer with set=false to flush the delete next time
        {
            let mut updates = updates.clone();
            assert!(!updates.drain_persisted_and_drop(&persisted));
            assert!(!updates.latest_is_set);
            assert_eq!(updates.history.as_slice(), &[ValuePointer::new(1, 4, 1)]);
        }

        // Even if the history would somehow be shuffled we'd still properly
        {
            let mut updates = updates.clone();
            let mut persisted = updates.clone();
            persisted.history.swap(0, 1);
            persisted.history.swap(1, 3);
            assert!(updates.drain_persisted_and_drop(&persisted));
        }
    }
}
