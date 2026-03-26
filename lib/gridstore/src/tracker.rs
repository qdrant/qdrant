use std::path::{Path, PathBuf};

use ahash::{AHashMap, AHashSet};
use common::generic_consts::Random;
use common::mmap::{Advice, AdviceSetting, create_and_ensure_length};
#[expect(deprecated, reason = "legacy code")]
use common::mmap::{transmute_from_u8, transmute_to_u8};
use common::universal_io::{
    OpenOptions, ReadRange, UniversalIoError, UniversalRead, UniversalWrite,
};
use smallvec::SmallVec;
use zerocopy::FromZeros;

use crate::Result;
use crate::error::GridstoreError;

pub type PointOffset = u32;
pub type BlockOffset = u32;
pub type PageId = u32;

/// OpenOptions for the tracker file (random access, no populate).
fn tracker_open_options() -> OpenOptions {
    OpenOptions {
        writeable: true,
        need_sequential: false,
        disk_parallel: None,
        populate: Some(false),
        advice: Some(AdviceSetting::Advice(Advice::Random)),
        prevent_caching: None,
    }
}

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
pub(crate) struct PointerUpdates {
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

#[derive(Debug, Default, Clone, Copy)]
#[repr(C)]
struct TrackerHeader {
    next_pointer_offset: u32,
}

#[derive(Debug)]
pub struct Tracker<S> {
    /// Path to the file
    path: PathBuf,
    /// Header of the file
    header: TrackerHeader,
    /// Storage for the file (universal io backend)
    storage: S,
    /// Updates that haven't been flushed
    ///
    /// When flushing, these updates get written into the storage and flushed at once.
    pub(super) pending_updates: AHashMap<PointOffset, PointerUpdates>,

    /// The maximum pointer offset in the tracker (updated in memory).
    next_pointer_offset: PointOffset,
}

// Methods that do not use storage (no trait bound).
impl<S> Tracker<S> {
    const FILE_NAME: &'static str = "tracker.dat";

    fn tracker_file_name(path: &Path) -> PathBuf {
        path.join(Self::FILE_NAME)
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    pub fn pointer_count(&self) -> u32 {
        self.next_pointer_offset
    }
}

// Read operations -- only require UniversalRead
impl<S: UniversalRead<u8>> Tracker<S> {
    /// Open an existing PageTracker at the given path
    /// If the file does not exist, return an error
    pub fn open(path: &Path) -> Result<Self> {
        let path = Self::tracker_file_name(path);
        let storage = Self::open_storage(&path)?;
        let header: TrackerHeader = Self::read_header(&storage)?;
        let pending_updates = AHashMap::new();
        Ok(Self {
            next_pointer_offset: header.next_pointer_offset,
            path,
            header,
            storage,
            pending_updates,
        })
    }

    fn read_header(storage: &S) -> Result<TrackerHeader> {
        let header_bytes = storage.read::<Random>(ReadRange {
            byte_offset: 0,
            length: std::mem::size_of::<TrackerHeader>() as u64,
        })?;
        #[expect(deprecated, reason = "legacy code")]
        Ok(*unsafe { transmute_from_u8::<TrackerHeader>(header_bytes.as_ref()) })
    }

    fn open_storage(path: &Path) -> Result<S> {
        let storage = match S::open(path, tracker_open_options()) {
            Err(UniversalIoError::NotFound { .. }) => {
                return Err(GridstoreError::service_error(format!(
                    "Tracker file does not exist: {}",
                    path.display()
                )));
            }
            other => other?,
        };
        Ok(storage)
    }

    /// This method reloads the tracker storage from "disk", so that
    /// it should make newly written data visible to the tracker.
    ///
    /// Important assumptions:
    ///
    /// - Should only be called on read-only instances of the tracker.
    /// - Only appending new pointers is supported, not modifications of existing pointers.
    /// - Partial writes are possible, but ignored. Header is a source of truth.
    ///
    /// Returns `true` if there are new changes, `false` if the tracker is already up to date.
    pub fn live_reload(&mut self) -> Result<bool> {
        let new_header = Self::read_header(&self.storage)?;

        if new_header.next_pointer_offset < self.next_pointer_offset {
            Err(GridstoreError::service_error(format!(
                "live reload cannot decrease pointer count, possible data loss: old count {:#?}, new count {:#?}",
                self.next_pointer_offset, new_header.next_pointer_offset
            )))
        } else if new_header.next_pointer_offset == self.next_pointer_offset {
            // No new pointers, no need to reload
            Ok(false)
        } else {
            // reopen storage to make new data visible
            // For some storages it should be a no-op.
            self.storage = Self::open_storage(&self.path)?;

            // Update in-memory state to reflect new pointers
            self.header = new_header;
            self.next_pointer_offset = new_header.next_pointer_offset;
            Ok(true)
        }
    }

    /// Get the raw value at the given point offset
    fn get_raw(&self, point_offset: PointOffset) -> Result<Option<Option<ValuePointer>>> {
        let start_offset = std::mem::size_of::<TrackerHeader>()
            + point_offset as usize * std::mem::size_of::<Optional<ValuePointer>>();
        let end_offset = start_offset + std::mem::size_of::<Optional<ValuePointer>>();
        let storage_len = self.storage.len()?;
        if end_offset as u64 > storage_len {
            return Ok(None);
        }
        let bytes = self.storage.read::<Random>(ReadRange {
            byte_offset: start_offset as u64,
            length: std::mem::size_of::<Optional<ValuePointer>>() as u64,
        })?;
        #[expect(deprecated, reason = "legacy code")]
        let opt: &Optional<ValuePointer> = unsafe { transmute_from_u8(bytes.as_ref()) };
        Ok(Some(opt.is_some().copied()))
    }

    /// Get the page pointer at the given point offset
    pub fn get(&self, point_offset: PointOffset) -> Result<Option<ValuePointer>> {
        match self.pending_updates.get(&point_offset) {
            // Pending update exists but is empty, should not happen, fall back to real data
            Some(pending) if pending.is_empty() => {
                debug_assert!(false, "pending updates must not be empty");
                Ok(self.get_raw(point_offset)?.and_then(|o| o))
            }
            // Use set from pending updates
            Some(pending) => Ok(pending.current),
            // No pending update, use real data
            None => Ok(self.get_raw(point_offset)?.and_then(|o| o)),
        }
    }

    /// Iterate over the pointers in the tracker
    /// Starts from the given point offset
    pub fn iter_pointers(
        &self,
        from: PointOffset,
        max: PointOffset,
    ) -> impl Iterator<Item = (PointOffset, Result<Option<ValuePointer>>)> + '_ {
        let to = self.next_pointer_offset.min(max.saturating_add(1));
        (from..to).map(move |i| (i, self.get(i)))
    }

    pub fn has_pointer(&self, point_offset: PointOffset) -> Result<bool> {
        Ok(self.get(point_offset)?.is_some())
    }

    pub fn populate(&self) -> Result<()> {
        self.storage.populate().map_err(Into::into)
    }

    /// Get the length of the mapping
    /// Excludes None values
    /// Warning: performs a full scan of the tracker.
    #[cfg(test)]
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

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.mapping_len().unwrap_or(0) == 0
    }

    /// Return the size of the underlying file
    #[cfg(test)]
    pub fn mmap_file_size(&self) -> Result<usize> {
        self.storage.len().map(|u| u as usize).map_err(Into::into)
    }
}

// Write operations and constructors -- require UniversalWrite
impl<S> Tracker<S>
where
    S: UniversalRead<u8> + UniversalWrite<u8>,
{
    const DEFAULT_SIZE: usize = 1024 * 1024; // 1MB

    /// Create a new PageTracker at the given dir path
    /// The file is created with the default size if no size hint is given
    pub fn new(path: &Path, size_hint: Option<usize>) -> Result<Self> {
        let path = Self::tracker_file_name(path);
        let size = size_hint.unwrap_or(Self::DEFAULT_SIZE).next_power_of_two();
        assert!(
            size > std::mem::size_of::<TrackerHeader>(),
            "Size hint is too small"
        );
        create_and_ensure_length(&path, size)?;
        let storage = S::open(&path, tracker_open_options())?;
        let header = TrackerHeader::default();
        let pending_updates = AHashMap::new();
        let mut page_tracker = Self {
            path,
            header,
            storage,
            pending_updates,
            next_pointer_offset: 0,
        };
        page_tracker.write_header()?;
        Ok(page_tracker)
    }

    /// Writes the accumulated pending updates to storage and flushes it
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
    ) -> Result<Vec<ValuePointer>> {
        let mut old_pointers = Vec::new();

        for (point_offset, updates) in pending_updates {
            match updates.current {
                // Write to store a new pointer
                Some(new_pointer) => {
                    // Mark any existing pointer for removal to free its blocks
                    if let Some(Some(old_pointer)) = self.get_raw(point_offset)? {
                        old_pointers.push(old_pointer);
                    }

                    self.persist_pointer(point_offset, Some(new_pointer))?;
                }
                // Write to empty the pointer
                None => self.persist_pointer(point_offset, None)?,
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
        self.write_pointer_count()?;

        Ok(old_pointers)
    }

    pub fn flusher(&self) -> crate::gridstore::Flusher {
        let inner = self.storage.flusher();
        Box::new(move || inner().map_err(Into::into))
    }

    #[cfg(test)]
    pub fn write_pending_and_flush_internal(&mut self) -> Result<Vec<ValuePointer>> {
        let pending_updates = std::mem::take(&mut self.pending_updates);
        let res = self.write_pending(pending_updates)?;
        self.storage.flusher()()?;
        Ok(res)
    }

    /// Write the current page header to the storage
    fn write_header(&mut self) -> Result<()> {
        #[expect(deprecated, reason = "legacy code")]
        let header_bytes = unsafe { transmute_to_u8(&self.header) };
        self.storage.write(0, header_bytes)?;
        Ok(())
    }

    /// Save the mapping at the given offset
    /// The file is resized if necessary
    fn persist_pointer(
        &mut self,
        point_offset: PointOffset,
        pointer: Option<ValuePointer>,
    ) -> Result<()> {
        let storage_len = self.storage.len()? as usize;
        if pointer.is_none() && point_offset as usize >= storage_len {
            return Ok(());
        }

        let point_offset = point_offset as usize;
        let start_offset = std::mem::size_of::<TrackerHeader>()
            + point_offset * std::mem::size_of::<Optional<ValuePointer>>();
        let end_offset = start_offset + std::mem::size_of::<Optional<ValuePointer>>();

        // Grow tracker file if it isn't big enough
        if storage_len < end_offset {
            self.storage.flusher()()?;
            let new_size = end_offset.next_power_of_two();
            create_and_ensure_length(&self.path, new_size)?;
            self.storage = S::open(&self.path, tracker_open_options())?;
        }

        let pointer: Optional<_> = pointer.into();
        #[expect(deprecated, reason = "legacy code")]
        let pointer_bytes = unsafe { transmute_to_u8(&pointer) };
        self.storage.write(start_offset as u64, pointer_bytes)?;
        Ok(())
    }

    /// Increment the header count if the given point offset is larger than the current count
    fn write_pointer_count(&mut self) -> Result<()> {
        self.header.next_pointer_offset = self.next_pointer_offset;
        self.write_header()
    }

    pub fn set(&mut self, point_offset: PointOffset, value_pointer: ValuePointer) {
        self.pending_updates
            .entry(point_offset)
            .or_default()
            .set(value_pointer);
        self.next_pointer_offset = self.next_pointer_offset.max(point_offset + 1);
    }

    /// Unset the value at the given point offset and return its previous value
    pub fn unset(&mut self, point_offset: PointOffset) -> Result<Option<ValuePointer>> {
        let pointer_opt = self.get(point_offset)?;

        if let Some(pointer) = pointer_opt {
            self.pending_updates
                .entry(point_offset)
                .or_default()
                .unset(pointer);
        }

        Ok(pointer_opt)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    #[expect(deprecated, reason = "legacy code")]
    use common::mmap::transmute_from_u8;
    use common::universal_io::MmapFile;
    use rstest::rstest;
    use tempfile::Builder;

    use super::{PointerUpdates, Tracker, ValuePointer};
    use crate::tracker::{BlockOffset, Optional, PageId};

    type TestTracker = Tracker<MmapFile>;

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
        let tracker = TestTracker::new(path, None).unwrap();
        let files = tracker.files();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], path.join(TestTracker::FILE_NAME));
    }

    #[test]
    fn test_new_tracker() {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();
        let tracker = TestTracker::new(path, None).unwrap();
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
        let mut tracker = TestTracker::new(path, Some(initial_tracker_size)).unwrap();
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
        let mut tracker = TestTracker::new(path, Some(initial_tracker_size)).unwrap();
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
            Some(Some(ValuePointer::new(1, 1, 1)))
        );
        assert_eq!(
            tracker.get_raw(1).unwrap(),
            Some(Some(ValuePointer::new(2, 2, 2)))
        );
        assert_eq!(
            tracker.get_raw(2).unwrap(),
            Some(Some(ValuePointer::new(3, 3, 3)))
        );
        assert_eq!(tracker.get_raw(3).unwrap(), Some(None)); // intermediate empty slot
        assert_eq!(
            tracker.get_raw(10).unwrap(),
            Some(Some(ValuePointer::new(10, 10, 10)))
        );
        assert_eq!(tracker.get_raw(100_000).unwrap(), None); // out of bounds

        tracker.unset(1).unwrap();

        tracker.write_pending_and_flush_internal().unwrap();

        // the value has been cleared but the entry is still there
        assert_eq!(tracker.get_raw(1).unwrap(), Some(None));
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

        let mut tracker = TestTracker::new(path, Some(initial_tracker_size)).unwrap();

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
        let tracker = TestTracker::open(path).unwrap();
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

        let mut tracker = TestTracker::new(path, Some(desired_tracker_size)).unwrap();
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

        let mut tracker = TestTracker::new(path, None).unwrap();
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
        struct AlignedData([u8; std::mem::size_of::<Optional<ValuePointer>>()]);

        assert_eq!(
            std::mem::size_of::<Optional<ValuePointer>>(),
            std::mem::size_of::<Option<ValuePointer>>()
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
            std::mem::size_of::<Optional<ValuePointer>>(),
            std::mem::size_of::<Option<ValuePointer>>()
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

            let a_data =
                unsafe { from_raw_parts((a as *const A).cast::<u8>(), std::mem::size_of::<A>()) };
            let b_data =
                unsafe { from_raw_parts((b as *const B).cast::<u8>(), std::mem::size_of::<B>()) };

            assert_eq!(a_data, b_data);
        }
    }
}
