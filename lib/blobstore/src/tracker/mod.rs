pub(crate) mod append_only;
pub mod iter;
pub mod read_only;

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};

use ahash::{AHashMap, AHashSet};
use common::generic_consts::Random;
use common::mmap::{Advice, AdviceSetting, create_and_ensure_length};
use common::universal_io::{
    CachedReadFs, OpenOptions, Populate, ReadRange, UniversalIoError, UniversalRead,
    UniversalReadFs, UniversalWrite, UserData,
};
use smallvec::SmallVec;

pub use self::iter::{Iter, PointerItem};
pub use self::read_only::ReadOnlyTracker;
use crate::Result;
use crate::error::GridstoreError;

pub type PointOffset = u32;
pub type BlockOffset = u32;
pub type PageId = u32;

/// OpenOptions for the tracker file (random access, no populate).
///
/// `writeable` is `false` for read-only readers (so the backend may be
/// write-enforced, e.g. `ReadOnly<MmapFile>`) and `true` for the writable
/// tracker that appends pointers.
fn tracker_open_options(populate: Populate, writeable: bool) -> OpenOptions {
    OpenOptions {
        writeable,
        need_sequential: false,
        populate,
        advice: AdviceSetting::Advice(Advice::Random),
    }
}

/// A type similar to [`std::option::Option<ValuePointer>`], but with stable layout. It is intended to be compatible with older
/// gridstore files, but it is well-defined, unlike [`std::option::Option`].
///
/// Please note that it uses 32-bit tag so that there's no padding before `ValuePointer`.
#[derive(Debug, Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C)]
pub(crate) struct OptionalPointer {
    discriminant: u32,
    value: ValuePointer,
}

impl From<Option<ValuePointer>> for OptionalPointer {
    fn from(value: Option<ValuePointer>) -> Self {
        match value {
            Some(value) => Self::some(value),
            None => Self::none(),
        }
    }
}

impl OptionalPointer {
    const OPTIONAL_NONE: u32 = 0;
    const OPTIONAL_SOME: u32 = 1;

    /// None value is all zeroes.
    pub fn none() -> Self {
        Self {
            discriminant: Self::OPTIONAL_NONE,
            value: ValuePointer::new(0, 0, 0),
        }
    }

    /// Some is 1 for the discriminant, and value is stored as is.
    pub const fn some(value: ValuePointer) -> Self {
        Self {
            discriminant: Self::OPTIONAL_SOME,
            value,
        }
    }

    pub fn to_option(self) -> Option<ValuePointer> {
        if self.discriminant == Self::OPTIONAL_NONE {
            None
        } else {
            Some(self.value)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C)]
pub struct ValuePointer {
    /// Which page the value is stored in
    pub page_id: PageId,

    /// Start offset of the value within the page
    ///
    /// Counted in blocks in mutable mode, in bytes in append-only mode (which packs values
    /// without blocks or alignment).
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

/// Read-side interface over the pointer tracker.
///
/// Implemented by the writable [`Tracker`] — whose reads see pending
/// in-memory updates — and by [`ReadOnlyTracker`], which serves plain
/// on-disk state. [`crate::GridstoreView`] is generic over this trait, so
/// the same read logic works for both.
pub trait TrackerRead<S: UniversalRead> {
    /// Exclusive upper bound of point offsets that may have a pointer, as
    /// maintained by the writer (in memory for [`Tracker`], in the stored
    /// header for [`ReadOnlyTracker`]).
    fn max_point_offset(&self) -> Result<PointOffset>;

    /// Get the page pointer at the given point offset.
    fn get(&self, point_offset: PointOffset) -> Result<Option<ValuePointer>>;

    /// Iterate page pointers for the given point offsets.
    ///
    /// Issues batched reads against the underlying storage, so async backends
    /// can fetch entries in parallel.
    fn iter<U, I>(&self, point_offsets: I) -> Result<Iter<'_, U, I, S>>
    where
        U: UserData,
        I: Iterator<Item = (U, PointOffset)>;
}

/// Read the slot for `point_offset` directly from `storage`.
///
/// Offsets beyond the file read as `None`; so do allocated-but-never-written
/// slots — the file is zero-initialized and all-zeroes is the `None` slot.
fn read_slot<S: UniversalRead>(
    storage: &S,
    point_offset: PointOffset,
) -> Result<Option<ValuePointer>> {
    let start_offset =
        size_of::<TrackerHeader>() + point_offset as usize * size_of::<OptionalPointer>();
    let end_offset = start_offset + size_of::<OptionalPointer>();
    let storage_len = storage.len::<u8>()?;
    if end_offset as u64 > storage_len {
        return Ok(None);
    }
    let opt = storage.read::<_, OptionalPointer>(ReadRange::one(start_offset as u64), Random)?[0];
    Ok(opt.to_option())
}

/// Pointer updates for a given point offset
///
/// Keeps track of the places where the value for a point offset have been written, until we persist them.
///
/// In context of Blobstore, for each point offset this means:
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

#[derive(Debug, Default, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
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
impl<S: UniversalRead> Tracker<S> {
    pub fn preopen<Fs: CachedReadFs<File = S>>(
        fs: &Fs,
        path: &Path,
        populate: Populate,
    ) -> Result<()> {
        let path = Self::tracker_file_name(path);
        // Default a lazy open to partially populating the header.
        let populate = populate.or_partial(0..size_of::<TrackerHeader>() as u64);
        fs.schedule_prefetch(&path, Some(tracker_open_options(populate, false)), None)?;
        Ok(())
    }

    /// Open an existing PageTracker at the given path
    /// If the file does not exist, return an error
    pub fn open<Fs: UniversalReadFs<File = S>>(
        fs: &Fs,
        path: &Path,
        populate: Populate,
        writeable: bool,
    ) -> Result<Self> {
        let path = Self::tracker_file_name(path);

        let storage = Self::open_storage(fs, &path, populate, writeable)?;

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
        let header = storage.read(ReadRange::one(0), Random)?[0];
        Ok(header)
    }

    fn open_storage<Fs: UniversalReadFs<File = S>>(
        fs: &Fs,
        path: &Path,
        populate: Populate,
        writeable: bool,
    ) -> Result<S> {
        let storage = match fs.open(
            path,
            tracker_open_options(populate, writeable),
            Default::default(),
        ) {
            Err(UniversalIoError::NotFound { .. }) => {
                // If config exists and storage doesn't,
                // it should be treated as inconsistent storage rather than a missing one
                return Err(GridstoreError::service_error(format!(
                    "Tracker file does not exist: {}",
                    path.display()
                )));
            }
            other => other?,
        };
        Ok(storage)
    }

    /// Get the raw value at the given point offset
    fn get_raw(&self, point_offset: PointOffset) -> Result<Option<ValuePointer>> {
        read_slot(&self.storage, point_offset)
    }

    /// Get the page pointer at the given point offset
    pub fn get(&self, point_offset: PointOffset) -> Result<Option<ValuePointer>> {
        match self.pending_updates.get(&point_offset) {
            // Pending update exists but is empty, should not happen, fall back to real data
            Some(pending) if pending.is_empty() => {
                debug_assert!(false, "pending updates must not be empty");
                self.get_raw(point_offset)
            }
            // Use set from pending updates
            Some(pending) => Ok(pending.current),
            // No pending update, use real data
            None => self.get_raw(point_offset),
        }
    }

    /// Iterate page pointers for the given point offsets.
    ///
    /// Issues batched reads against the underlying storage, so async backends
    /// can fetch entries in parallel.
    pub fn iter<U, I>(&self, point_offsets: I) -> Result<Iter<'_, U, I, S>>
    where
        U: UserData,
        I: Iterator<Item = (U, PointOffset)>,
    {
        Iter::new(point_offsets, &self.storage, &self.pending_updates)
    }

    pub fn has_pointer(&self, point_offset: PointOffset) -> Result<bool> {
        Ok(self.get(point_offset)?.is_some())
    }

    pub fn populate(&self) -> Result<()> {
        self.storage.populate().map_err(Into::into)
    }
}

impl<S: UniversalRead> TrackerRead<S> for Tracker<S> {
    /// Exact for the writable tracker: maintained in memory alongside the
    /// header (see [`Tracker::pointer_count`]).
    fn max_point_offset(&self) -> Result<PointOffset> {
        Ok(self.pointer_count())
    }

    fn get(&self, point_offset: PointOffset) -> Result<Option<ValuePointer>> {
        Tracker::get(self, point_offset)
    }

    fn iter<U, I>(&self, point_offsets: I) -> Result<Iter<'_, U, I, S>>
    where
        U: UserData,
        I: Iterator<Item = (U, PointOffset)>,
    {
        Tracker::iter(self, point_offsets)
    }
}

// Write operations and constructors -- require UniversalWrite
impl<S> Tracker<S>
where
    S: UniversalWrite,
{
    const DEFAULT_SIZE: usize = 1024 * 1024; // 1MB

    /// Create a new PageTracker at the given dir path
    /// The file is created with the default size if no size hint is given
    pub fn new(fs: &S::Fs, path: &Path, size_hint: Option<usize>) -> Result<Self> {
        let path = Self::tracker_file_name(path);
        let size = size_hint.unwrap_or(Self::DEFAULT_SIZE).next_power_of_two();
        assert!(
            size > std::mem::size_of::<TrackerHeader>(),
            "Size hint is too small"
        );
        create_and_ensure_length(&path, size)?;
        let storage = fs.open(
            &path,
            tracker_open_options(Populate::No, true),
            Default::default(),
        )?;
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
                    if let Some(old_pointer) = self.get_raw(point_offset)? {
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

    pub fn flusher(&self) -> crate::blobstore::Flusher {
        let inner = self.storage.flusher();
        Box::new(move || inner().map_err(Into::into))
    }

    /// Write the current page header to the storage
    fn write_header(&mut self) -> Result<()> {
        self.storage.write(0, &[self.header])?;
        Ok(())
    }

    /// Save the mapping at the given offset
    /// The file is resized if necessary
    fn persist_pointer(
        &mut self,
        point_offset: PointOffset,
        pointer: Option<ValuePointer>,
    ) -> Result<()> {
        let storage_len = self.storage.len::<u8>()? as usize;
        if pointer.is_none() && point_offset as usize >= storage_len {
            return Ok(());
        }

        let point_offset = point_offset as usize;
        let start_offset = size_of::<TrackerHeader>() + point_offset * size_of::<OptionalPointer>();
        let end_offset = start_offset + size_of::<OptionalPointer>();

        // Grow tracker file if it isn't big enough
        if storage_len < end_offset {
            self.storage.flusher()()?;
            let new_size = end_offset.next_power_of_two();
            create_and_ensure_length(&self.path, new_size)?;
            self.storage.reopen()?;
        }

        let pointer = OptionalPointer::from(pointer);
        self.storage.write(start_offset as u64, &[pointer])?;
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
