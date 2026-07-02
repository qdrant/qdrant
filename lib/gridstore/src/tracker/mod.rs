pub mod iter;

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};

use ahash::{AHashMap, AHashSet};
use common::generic_consts::{Random, Sequential};
use common::mmap::{Advice, AdviceSetting, create_and_ensure_length};
use common::universal_io::{
    OpenOptions, Populate, ReadRange, UniversalIoError, UniversalRead, UniversalReadFs,
    UniversalWrite, UserData,
};
use smallvec::SmallVec;

pub use self::iter::{Iter, PointerItem};
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
fn tracker_open_options(writeable: bool) -> OpenOptions {
    OpenOptions {
        writeable,
        need_sequential: false,
        populate: Populate::No,
        advice: AdviceSetting::Advice(Advice::Random),
    }
}

/// A type similar to [`std::option::Option<ValuePointer>`], but with stable layout. It is intended to be compatible with older
/// gridstore files, but it is well-defined, unlike [`std::option::Option`].
///
/// Please note that it uses 32-bit tag so that there's no padding before `ValuePointer`.
#[derive(Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C)]
struct OptionalPointer {
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
    /// Open an existing PageTracker at the given path
    /// If the file does not exist, return an error
    pub fn open(fs: &S::Fs, path: &Path, writeable: bool) -> Result<Self> {
        let path = Self::tracker_file_name(path);
        let storage = Self::open_storage(fs, &path, writeable)?;
        let header: TrackerHeader = Self::read_header(&storage)?;
        let next_pointer_offset = Self::scan_pointer_count(&storage, header.next_pointer_offset)?;
        let pending_updates = AHashMap::new();
        Ok(Self {
            path,
            header,
            storage,
            pending_updates,
            next_pointer_offset,
        })
    }

    /// Derive the pointer count: the last mapped slot + 1, or the header count if
    /// that is higher (trailing deleted points in dynamic mode).
    ///
    /// Append-only (serverless) flushes never rewrite the header, so mappings may
    /// exist beyond its count; scan for them from there.
    fn scan_pointer_count(storage: &S, header_count: PointOffset) -> Result<PointOffset> {
        const HEADER_SIZE: u64 = size_of::<TrackerHeader>() as u64;
        const SLOT_SIZE: u64 = size_of::<OptionalPointer>() as u64;

        let slots_in_file = storage.len::<u8>()?.saturating_sub(HEADER_SIZE) / SLOT_SIZE;
        let scan_range = ReadRange::new(
            HEADER_SIZE + u64::from(header_count) * SLOT_SIZE,
            slots_in_file.saturating_sub(u64::from(header_count)),
        );

        let mut count = header_count;
        for chunk in scan_range.iter_autochunks::<OptionalPointer>() {
            let first_slot = (chunk.byte_offset - HEADER_SIZE) / SLOT_SIZE;
            let slots = storage.read::<Sequential, OptionalPointer>(chunk)?;
            for (index, slot) in slots.iter().enumerate() {
                if slot.to_option().is_some() {
                    count = (first_slot + index as u64 + 1) as PointOffset;
                }
            }
        }
        Ok(count)
    }

    fn read_header(storage: &S) -> Result<TrackerHeader> {
        let header = storage.read::<Random, TrackerHeader>(ReadRange::one(0))?[0];
        Ok(header)
    }

    fn open_storage(fs: &S::Fs, path: &Path, writeable: bool) -> Result<S> {
        let storage = match fs.open(path, tracker_open_options(writeable), Default::default()) {
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

    /// This method reloads the tracker storage from "disk", so that
    /// it should make newly written data visible to the tracker.
    ///
    /// Important assumptions:
    ///
    /// - Should only be called on read-only instances of the tracker.
    /// - Only appending new pointers is supported, not modifications of existing pointers.
    /// - Partial writes are possible, but ignored. Header is a source of truth,
    ///   except in append-only (serverless) mode where the slots themselves are.
    ///
    /// Returns `true` if there are new changes, `false` if the tracker is already up to date.
    pub fn live_reload(&mut self, append_only: bool) -> Result<bool> {
        // Append-only flushes never rewrite the header; reopen (the file may have
        // grown) and rescan for appended mappings instead.
        if append_only {
            self.storage.reopen()?;
            let new_count = Self::scan_pointer_count(&self.storage, self.next_pointer_offset)?;
            if new_count == self.next_pointer_offset {
                return Ok(false);
            }
            self.next_pointer_offset = new_count;
            return Ok(true);
        }

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
            self.storage.reopen()?;

            // Update in-memory state to reflect new pointers
            self.header = new_header;
            self.next_pointer_offset = new_header.next_pointer_offset;
            Ok(true)
        }
    }

    /// Get the raw value at the given point offset
    fn get_raw(&self, point_offset: PointOffset) -> Result<Option<ValuePointer>> {
        let start_offset =
            size_of::<TrackerHeader>() + point_offset as usize * size_of::<OptionalPointer>();
        let end_offset = start_offset + size_of::<OptionalPointer>();
        let storage_len = self.storage.len::<u8>()?;
        if end_offset as u64 > storage_len {
            return Ok(None);
        }
        let opt = self
            .storage
            .read::<Random, OptionalPointer>(ReadRange::one(start_offset as u64))?[0];
        Ok(opt.to_option())
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

// Write operations and constructors -- require UniversalWrite
impl<S> Tracker<S>
where
    S: UniversalWrite,
{
    const DEFAULT_SIZE: usize = 1024 * 1024; // 1MB

    /// Create a new PageTracker at the given dir path
    /// The file is created with the default size if no size hint is given
    ///
    /// `append_only` (serverless): the file holds just the header and grows per
    /// appended mapping, rather than being pre-allocated.
    pub fn new(
        fs: &S::Fs,
        path: &Path,
        size_hint: Option<usize>,
        append_only: bool,
    ) -> Result<Self> {
        let path = Self::tracker_file_name(path);
        let size = if append_only {
            std::mem::size_of::<TrackerHeader>()
        } else {
            let size = size_hint.unwrap_or(Self::DEFAULT_SIZE).next_power_of_two();
            assert!(
                size > std::mem::size_of::<TrackerHeader>(),
                "Size hint is too small"
            );
            size
        };
        create_and_ensure_length(&path, size)?;
        let storage = fs.open(&path, tracker_open_options(true), Default::default())?;
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
    ///
    /// `append_only` (serverless): only appends new mappings — unmapped points
    /// stay in place on disk (the unset stays pending) and the header is not
    /// rewritten; the count is rescanned on open.
    #[must_use = "The old pointers need to be freed in the bitmask"]
    pub fn write_pending(
        &mut self,
        pending_updates: AHashMap<PointOffset, PointerUpdates>,
        append_only: bool,
    ) -> Result<Vec<ValuePointer>> {
        let mut old_pointers = Vec::new();

        // Append-only: persist in ascending order so the file only grows at the end
        let mut pending_updates: Vec<_> = pending_updates.into_iter().collect();
        if append_only {
            pending_updates.sort_unstable_by_key(|(point_offset, _)| *point_offset);
        }

        for (point_offset, updates) in pending_updates {
            match updates.current {
                // Write to store a new pointer
                Some(new_pointer) => {
                    // Mark any existing pointer for removal to free its blocks
                    let existing = self.get_raw(point_offset)?;
                    debug_assert_ne!(
                        existing,
                        Some(new_pointer),
                        "pointer is always expected to change"
                    );
                    debug_assert!(
                        !append_only || existing.is_none(),
                        "append-only tracker flush must not change persisted mapping for point {point_offset}",
                    );
                    if let Some(old_pointer) = existing {
                        old_pointers.push(old_pointer);
                    }

                    if append_only {
                        self.persist_pointer_append(point_offset, new_pointer)?;
                    } else {
                        self.persist_pointer(point_offset, Some(new_pointer))?;
                    }
                }
                // Append-only leaves unmapped points in place. Keep the unset
                // pending so reads keep seeing the delete; it resurrects on reopen.
                None if append_only => continue,
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
                            "remove pending element should be empty but got {prev:?}",
                        );
                    }
                }
            }
        }

        // Increment header count if necessary. Append-only never rewrites the
        // header; the count is rescanned on open.
        if !append_only {
            self.write_pointer_count()?;
        }

        Ok(old_pointers)
    }

    pub fn flusher(&self) -> crate::gridstore::Flusher {
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

    /// Truncate zero padding after the last mapping, e.g. from a dynamic-mode
    /// pre-allocation, so append-only writes land at the very end of the file.
    ///
    /// Call when opening in serverless mode. Only ever removes zero bytes: every
    /// slot past the scanned pointer count is unmapped. Consumes and reopens the
    /// tracker, since a mapped file cannot shrink in place.
    pub fn truncate_padding(self, fs: &S::Fs) -> Result<Self> {
        let Self {
            path,
            header,
            storage,
            pending_updates,
            next_pointer_offset,
        } = self;

        let expected_len = (size_of::<TrackerHeader>()
            + next_pointer_offset as usize * size_of::<OptionalPointer>())
            as u64;
        if storage.len::<u8>()? <= expected_len {
            return Ok(Self {
                path,
                header,
                storage,
                pending_updates,
                next_pointer_offset,
            });
        }

        storage.flusher()()?;
        drop(storage);
        create_and_ensure_length(&path, expected_len as usize)?;
        let storage = Self::open_storage(fs, &path, true)?;

        Ok(Self {
            path,
            header,
            storage,
            pending_updates,
            next_pointer_offset,
        })
    }

    /// Append a mapping at the given offset, growing the file by exactly the
    /// written slot (no pre-allocation).
    ///
    /// If the slot lands past the end of the file, the write is padded with
    /// zeroes (unmapped slots) so it is one contiguous append.
    fn persist_pointer_append(
        &mut self,
        point_offset: PointOffset,
        pointer: ValuePointer,
    ) -> Result<()> {
        let start_offset =
            size_of::<TrackerHeader>() + point_offset as usize * size_of::<OptionalPointer>();
        let file_len = self.storage.len::<u8>()? as usize;

        // Strictly append: the slot must be at the very end of the file, never
        // replacing existing bytes - not even zero padding.
        debug_assert!(
            start_offset >= file_len,
            "append-only tracker flush must write at the very end of the file, point {point_offset} lands within it",
        );

        let slot = OptionalPointer::some(pointer);
        let gap_bytes = start_offset.saturating_sub(file_len);
        if gap_bytes == 0 {
            self.storage.write_grow(start_offset as u64, &[slot])?;
        } else {
            // Pad the write with zeroes so the mapping lands in the correct
            // place, appended at the end of the file as one write.
            let mut buffer = vec![0u8; gap_bytes + size_of::<OptionalPointer>()];
            buffer[gap_bytes..].copy_from_slice(bytemuck::bytes_of(&slot));
            self.storage.write_grow(file_len as u64, &buffer)?;
        }
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
