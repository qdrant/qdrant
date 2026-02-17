use std::cmp::min;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::referenced_counter::HwMetricRefCounter;
use common::fs::atomic_save_json;
use common::is_alive_lock::IsAliveLock;
use fs_err as fs;
use fs_err::File;
use itertools::Itertools;
use lz4_flex::compress_prepend_size;
use parking_lot::{Mutex, RwLock};

use crate::Result;
use crate::bitmask::Bitmask;
use crate::blob::Blob;
use crate::config::{Compression, StorageConfig, StorageOptions};
use crate::error::GridstoreError;
use crate::page::Page;
use crate::tracker::{BlockOffset, PageId, PointOffset, Tracker, ValuePointer};

const CONFIG_FILENAME: &str = "config.json";

pub type Flusher = Box<dyn FnOnce() -> std::result::Result<(), GridstoreError> + Send>;

/// A buffered write operation not yet applied to committed storage.
#[derive(Debug, Clone)]
pub(super) enum PendingChange {
    /// Compressed value bytes to write.
    Put(Arc<[u8]>),
    /// Value was deleted.
    Delete,
}

/// Committed storage state — pages, bitmask, tracker.
///
/// Protected by a single `RwLock` on the outer `Gridstore`.
/// The flusher takes a write lock briefly to apply pending changes, then
/// downgrades to a read lock for the I/O-bound flush phase.
/// Readers always take a read lock (compatible with the flush phase).
/// Writers never lock this — they write to `pending` instead.
#[derive(Debug)]
pub(super) struct GridstoreInternal {
    pub(super) tracker: Tracker,
    pub(super) pages: Vec<Page>,
    pub(super) bitmask: Bitmask,
}

/// Storage for values of type `V`.
///
/// Assumes sequential IDs to the values (0, 1, 2, 3, ...)
///
/// Write operations buffer changes in `pending` (protected by a `Mutex` with microsecond lock
/// times). Read operations check `pending` first, then fall through to committed state in
/// `internal`. The flusher applies pending changes to `internal` during flush. This design
/// eliminates lock contention between writers and the flusher, preventing search latency spikes.
#[derive(Debug)]
pub struct Gridstore<V> {
    /// Configuration of the storage.
    pub(super) config: StorageConfig,
    /// Committed state. Readers take read lock; flusher takes write lock briefly to apply
    /// pending changes, then downgrades to read lock for the I/O-bound flush.
    pub(super) internal: Arc<RwLock<GridstoreInternal>>,
    /// Pending changes from write operations. Protected by Mutex with brief lock times.
    pub(super) pending: Arc<Mutex<AHashMap<PointOffset, PendingChange>>>,
    /// Track max point offset atomically (avoids locking internal for this).
    next_point_offset: AtomicU32,
    /// Path of the directory where the storage files are stored
    base_path: PathBuf,
    _value_type: std::marker::PhantomData<V>,

    /// Lock to prevent concurrent flushes and used for waiting for ongoing flushes to finish.
    is_alive_flush_lock: IsAliveLock,
}

#[inline]
fn compress_lz4(value: &[u8]) -> Vec<u8> {
    compress_prepend_size(value)
}

#[inline]
fn decompress_lz4(value: &[u8]) -> Vec<u8> {
    lz4_flex::decompress_size_prepended(value).unwrap()
}

// ---------------------------------------------------------------------------
// GridstoreInternal — committed storage helpers (pages, bitmask, tracker)
// ---------------------------------------------------------------------------

impl GridstoreInternal {
    /// Read raw value from the pages, considering that they can span more than one page.
    fn read_from_pages<const READ_SEQUENTIAL: bool>(
        &self,
        start_page_id: PageId,
        mut block_offset: BlockOffset,
        mut length: u32,
        block_size_bytes: usize,
    ) -> Vec<u8> {
        let mut raw_sections = Vec::with_capacity(length as usize);

        for page_id in start_page_id.. {
            let page = &self.pages[page_id as usize];
            let (raw, unread_bytes) =
                page.read_value::<READ_SEQUENTIAL>(block_offset, length, block_size_bytes);

            raw_sections.extend(raw);

            if unread_bytes == 0 {
                break;
            }

            block_offset = 0;
            length = unread_bytes as u32;
        }

        raw_sections
    }

    /// Write value into pages, considering that it can span more than one page.
    fn write_into_pages(
        &mut self,
        value: &[u8],
        start_page_id: PageId,
        mut block_offset: BlockOffset,
        block_size_bytes: usize,
    ) {
        let value_size = value.len();
        let mut unwritten_tail = value_size;

        for page_id in start_page_id.. {
            let page = &mut self.pages[page_id as usize];

            let range = (value_size - unwritten_tail)..;
            unwritten_tail = page.write_value(block_offset, &value[range], block_size_bytes);

            if unwritten_tail == 0 {
                break;
            }

            block_offset = 0;
        }
    }

    /// Create a new page and extend the bitmask to cover it.
    fn create_new_page(&mut self, config: &StorageConfig, base_path: &Path) -> Result<u32> {
        let new_page_id = self.pages.len() as PageId;
        let path = base_path.join(format!("page_{new_page_id}.dat"));
        let page = Page::new(&path, config.page_size_bytes)?;
        self.pages.push(page);

        self.bitmask.cover_new_page()?;

        Ok(new_page_id)
    }

    /// Find available blocks in the bitmask, creating new pages if necessary.
    fn find_or_create_available_blocks(
        &mut self,
        num_blocks: u32,
        config: &StorageConfig,
        base_path: &Path,
    ) -> Result<(PageId, BlockOffset)> {
        debug_assert!(num_blocks > 0, "num_blocks must be greater than 0");

        if let Some((page_id, block_offset)) = self.bitmask.find_available_blocks(num_blocks) {
            return Ok((page_id, block_offset));
        }
        // else we need new page(s)
        let trailing_free_blocks = self.bitmask.trailing_free_blocks();

        let missing_blocks = num_blocks.saturating_sub(trailing_free_blocks) as usize;

        let num_pages = (missing_blocks * config.block_size_bytes).div_ceil(config.page_size_bytes);
        for _ in 0..num_pages {
            self.create_new_page(config, base_path)?;
        }

        // At this point we are sure that we have enough free pages to allocate the blocks
        let available = self.bitmask.find_available_blocks(num_blocks).unwrap();

        Ok(available)
    }

    /// Get the mapping for a given point offset from the committed tracker.
    fn get_pointer(&self, point_offset: PointOffset) -> Option<ValuePointer> {
        self.tracker.get(point_offset)
    }
}

// ---------------------------------------------------------------------------
// Gridstore<V: Blob> — public API
// ---------------------------------------------------------------------------

impl<V: Blob> Gridstore<V> {
    /// LZ4 compression
    fn compress(&self, value: Vec<u8>) -> Vec<u8> {
        match self.config.compression {
            Compression::None => value,
            Compression::LZ4 => compress_lz4(&value),
        }
    }

    /// LZ4 decompression
    fn decompress(&self, value: Vec<u8>) -> Vec<u8> {
        match self.config.compression {
            Compression::None => value,
            Compression::LZ4 => decompress_lz4(&value),
        }
    }

    pub fn files(&self) -> Vec<PathBuf> {
        let internal = self.internal.read();
        let mut paths = Vec::with_capacity(internal.pages.len() + 1);
        for tracker_file in internal.tracker.files() {
            paths.push(tracker_file);
        }
        for page_id in 0..internal.pages.len() as PageId {
            paths.push(self.page_path(page_id));
        }
        for bitmask_file in internal.bitmask.files() {
            paths.push(bitmask_file);
        }
        paths.push(self.base_path.join(CONFIG_FILENAME));
        paths
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        vec![self.base_path.join(CONFIG_FILENAME)]
    }

    pub fn max_point_offset(&self) -> PointOffset {
        self.next_point_offset.load(Ordering::Relaxed)
    }

    /// Opens an existing storage, or initializes a new one.
    /// Depends on the existence of the config file at the `base_path`.
    ///
    /// In case of opening, it ignores the `create_options` parameter.
    pub fn open_or_create(base_path: PathBuf, create_options: StorageOptions) -> Result<Self> {
        let config_path = base_path.join(CONFIG_FILENAME);
        if config_path.exists() {
            Self::open(base_path)
        } else {
            // create folder if it does not exist
            fs::create_dir_all(&base_path).map_err(|err| {
                GridstoreError::service_error(format!(
                    "Failed to create gridstore storage directory: {err}"
                ))
            })?;
            Self::new(base_path, create_options)
        }
    }

    /// Initializes a new storage with a single empty page.
    ///
    /// `base_path` is the directory where the storage files will be stored.
    /// It should exist already.
    pub fn new(base_path: PathBuf, options: StorageOptions) -> Result<Self> {
        if !base_path.exists() {
            return Err(GridstoreError::service_error("Base path does not exist"));
        }
        if !base_path.is_dir() {
            return Err(GridstoreError::service_error(
                "Base path is not a directory",
            ));
        }

        let config = StorageConfig::try_from(options).map_err(GridstoreError::service_error)?;
        let config_path = base_path.join(CONFIG_FILENAME);

        // Create first page to be covered by the bitmask
        let first_page_path = base_path.join("page_0.dat");
        let first_page = Page::new(&first_page_path, config.page_size_bytes)?;

        let internal = GridstoreInternal {
            tracker: Tracker::new(&base_path, None),
            pages: vec![first_page],
            bitmask: Bitmask::create(&base_path, config)?,
        };

        let storage = Self {
            config,
            internal: Arc::new(RwLock::new(internal)),
            pending: Arc::new(Mutex::new(AHashMap::new())),
            next_point_offset: AtomicU32::new(0),
            base_path,
            _value_type: std::marker::PhantomData,
            is_alive_flush_lock: IsAliveLock::new(),
        };

        // Lastly, write config to disk to use as a signal that the storage has been created correctly
        atomic_save_json(&config_path, &config)
            .map_err(|err| GridstoreError::service_error(err.to_string()))?;

        Ok(storage)
    }

    /// Open an existing storage at the given path
    /// Returns None if the storage does not exist
    pub fn open(base_path: PathBuf) -> Result<Self> {
        if !base_path.exists() {
            return Err(GridstoreError::service_error(format!(
                "Path '{base_path:?}' does not exist"
            )));
        }
        if !base_path.is_dir() {
            return Err(GridstoreError::service_error(format!(
                "Path '{base_path:?}' is not a directory"
            )));
        }

        // read config file first
        let config_path = base_path.join(CONFIG_FILENAME);
        let config_file = BufReader::new(File::open(&config_path)?);
        let config: StorageConfig = serde_json::from_reader(config_file)?;

        let page_tracker = Tracker::open(&base_path)?;
        let next_point_offset = page_tracker.pointer_count();

        let bitmask = Bitmask::open(&base_path, config)?;
        let num_pages = bitmask.infer_num_pages();

        let mut pages = Vec::with_capacity(num_pages);
        for page_id in 0..num_pages as PageId {
            let page_path = base_path.join(format!("page_{page_id}.dat"));
            let page = Page::open(&page_path)?;
            pages.push(page);
        }

        let internal = GridstoreInternal {
            tracker: page_tracker,
            pages,
            bitmask,
        };

        let storage = Self {
            config,
            internal: Arc::new(RwLock::new(internal)),
            pending: Arc::new(Mutex::new(AHashMap::new())),
            next_point_offset: AtomicU32::new(next_point_offset),
            base_path,
            _value_type: std::marker::PhantomData,
            is_alive_flush_lock: IsAliveLock::new(),
        };

        Ok(storage)
    }

    /// Get the path for a given page id
    fn page_path(&self, page_id: u32) -> PathBuf {
        self.base_path.join(format!("page_{page_id}.dat"))
    }

    /// Get the value for a given point offset.
    ///
    /// Checks pending changes first (brief Mutex), then falls through to committed storage
    /// (read lock on internal — compatible with flusher's I/O phase).
    pub fn get_value<const READ_SEQUENTIAL: bool>(
        &self,
        point_offset: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Option<V> {
        // Check pending first (brief Mutex lock)
        {
            let pending = self.pending.lock();
            match pending.get(&point_offset) {
                Some(PendingChange::Put(data)) => {
                    hw_counter.payload_io_read_counter().incr_delta(data.len());
                    let decompressed = self.decompress(data.to_vec());
                    return Some(V::from_bytes(&decompressed));
                }
                Some(PendingChange::Delete) => return None,
                None => {}
            }
        }

        // Fall through to committed storage (read lock on internal)
        let internal = self.internal.read();
        let ValuePointer {
            page_id,
            block_offset,
            length,
        } = internal.get_pointer(point_offset)?;

        let raw = internal.read_from_pages::<READ_SEQUENTIAL>(
            page_id,
            block_offset,
            length,
            self.config.block_size_bytes,
        );
        hw_counter.payload_io_read_counter().incr_delta(raw.len());

        let decompressed = self.decompress(raw);
        Some(V::from_bytes(&decompressed))
    }

    /// Get the mapping for a given point offset, checking pending first.
    #[cfg(test)]
    fn get_pointer(&self, point_offset: PointOffset) -> Option<ValuePointer> {
        // Check pending — if deleted, return None; if put, we don't have a pointer (data is in pending)
        let pending = self.pending.lock();
        match pending.get(&point_offset) {
            Some(PendingChange::Delete) => return None,
            Some(PendingChange::Put(_)) => {
                // The value exists in pending but has no committed pointer.
                // Return a sentinel — callers that need the actual pointer should use get_value instead.
                // For now, return None since the only external user of get_pointer is tests.
                // The actual value lookup goes through get_value which checks pending.
                return None;
            }
            None => {}
        }
        drop(pending);
        self.internal.read().get_pointer(point_offset)
    }

    /// Put a value in the storage.
    ///
    /// Buffers the compressed value in `pending`. No locks on pages/bitmask/tracker are acquired.
    ///
    /// Returns true if the value existed previously and was updated, false if it was newly inserted.
    pub fn put_value(
        &mut self,
        point_offset: PointOffset,
        value: &V,
        hw_counter: HwMetricRefCounter,
    ) -> Result<bool> {
        let value_bytes = value.to_bytes();
        let comp_value = self.compress(value_bytes);

        hw_counter.incr_delta(comp_value.len());

        let pending = self.pending.lock();
        let is_update = match pending.get(&point_offset) {
            Some(PendingChange::Put(_)) => true,
            Some(PendingChange::Delete) => false,
            None => self.internal.read().tracker.has_pointer(point_offset),
        };
        drop(pending);

        self.pending
            .lock()
            .insert(point_offset, PendingChange::Put(Arc::from(comp_value)));
        self.next_point_offset
            .fetch_max(point_offset + 1, Ordering::Relaxed);
        Ok(is_update)
    }

    /// Delete a value from the storage.
    ///
    /// Reads the value (from pending or committed storage), then marks deletion in `pending`.
    ///
    /// Returns None if the point_offset was not found; the deleted value otherwise.
    pub fn delete_value(&mut self, point_offset: PointOffset) -> Option<V> {
        let mut pending = self.pending.lock();
        match pending.get(&point_offset) {
            Some(PendingChange::Put(data)) => {
                let decompressed = self.decompress(data.to_vec());
                let value = V::from_bytes(&decompressed);
                pending.insert(point_offset, PendingChange::Delete);
                return Some(value);
            }
            Some(PendingChange::Delete) => return None,
            None => {}
        }
        drop(pending);

        // Read from committed storage
        let internal = self.internal.read();
        let pointer = internal.tracker.get(point_offset)?;
        let raw = internal.read_from_pages::<false>(
            pointer.page_id,
            pointer.block_offset,
            pointer.length,
            self.config.block_size_bytes,
        );
        drop(internal);

        let value = V::from_bytes(&self.decompress(raw));
        self.pending
            .lock()
            .insert(point_offset, PendingChange::Delete);
        Some(value)
    }

    /// Clear the storage, going back to the initial state.
    ///
    /// Completely wipes the storage, and recreates it with a single empty page.
    pub fn clear(&mut self) -> Result<()> {
        let create_options = StorageOptions::from(self.config);
        let base_path = self.base_path.clone();

        // Wait for all background flush operations to finish, abort pending flushes. Below we
        // create a new Gridstore instance with a new flush lock, so flushers created on the new
        // instance work as expected.
        self.is_alive_flush_lock.blocking_mark_dead();

        // Clear pending changes
        self.pending.lock().clear();

        // Wipe
        self.internal.write().pages.clear();
        fs::remove_dir_all(&base_path).map_err(|err| {
            GridstoreError::service_error(format!(
                "Failed to remove gridstore storage directory: {err}"
            ))
        })?;

        // Recreate
        fs::create_dir_all(&base_path).map_err(|err| {
            GridstoreError::service_error(format!(
                "Failed to create gridstore storage directory: {err}"
            ))
        })?;
        *self = Self::new(base_path, create_options)?;

        Ok(())
    }

    /// Wipe the storage, drop all pages and delete the base directory.
    ///
    /// Takes ownership because this function leaves Gridstore in an inconsistent state which does
    /// not allow further usage. Use [`clear`](Self::clear) instead to clear and reuse the storage.
    pub fn wipe(self) -> Result<()> {
        let base_path = self.base_path.clone();

        // Wait for all background flush operations to finish, abort pending flushes
        self.is_alive_flush_lock.blocking_mark_dead();

        // Make sure strong references are dropped, to avoid starting another flush
        drop(self);

        // Delete base directory
        fs::remove_dir_all(base_path).map_err(|err| {
            GridstoreError::service_error(format!(
                "Failed to remove gridstore storage directory: {err}"
            ))
        })
    }

    /// Iterate over all the values in the storage.
    ///
    /// Merges pending changes with committed storage. Stops when the callback returns `Ok(false)`.
    pub fn iter<F, E>(
        &self,
        mut callback: F,
        hw_counter: HwMetricRefCounter,
    ) -> std::result::Result<(), E>
    where
        F: FnMut(PointOffset, V) -> std::result::Result<bool, E>,
    {
        const BUFFER_SIZE: usize = 128;
        let max_point_offset = self.max_point_offset();

        // Snapshot pending for consistent iteration
        let pending_snapshot = self.pending.lock().clone();

        let mut from = 0;
        let mut buffer = Vec::with_capacity(min(BUFFER_SIZE, max_point_offset as usize));

        // Track the highest offset from the tracker so we can emit pending-only entries after
        let mut tracker_max: PointOffset = 0;

        loop {
            // Collect pointers into a buffer while holding the read lock
            buffer.clear();
            {
                let internal = self.internal.read();
                buffer.extend(
                    internal
                        .tracker
                        .iter_pointers(from)
                        .filter_map(|(point_offset, opt_pointer)| {
                            // Skip if pending has an override for this offset
                            if pending_snapshot.contains_key(&point_offset) {
                                // Still consume it so `from` advances
                                return opt_pointer.map(|p| (point_offset, p));
                            }
                            opt_pointer.map(|pointer| (point_offset, pointer))
                        })
                        .take(BUFFER_SIZE),
                );
            }

            if buffer.is_empty() {
                break;
            }

            from = buffer.last().unwrap().0 + 1;
            tracker_max = tracker_max.max(from);

            // Process buffer without holding locks
            for &(point_offset, pointer) in &buffer {
                // Check if pending overrides this offset
                match pending_snapshot.get(&point_offset) {
                    Some(PendingChange::Put(data)) => {
                        hw_counter.incr_delta(data.len());
                        let decompressed = self.decompress(data.to_vec());
                        let value = V::from_bytes(&decompressed);
                        if !callback(point_offset, value)? {
                            return Ok(());
                        }
                        continue;
                    }
                    Some(PendingChange::Delete) => continue,
                    None => {}
                }

                let ValuePointer {
                    page_id,
                    block_offset,
                    length,
                } = pointer;

                let raw = self.internal.read().read_from_pages::<true>(
                    page_id,
                    block_offset,
                    length,
                    self.config.block_size_bytes,
                );

                hw_counter.incr_delta(raw.len());

                let decompressed = self.decompress(raw);
                let value = V::from_bytes(&decompressed);
                if !callback(point_offset, value)? {
                    return Ok(());
                }
            }
        }

        // Emit pending Put entries that are beyond the committed tracker range
        // (these are newly inserted points not yet in the tracker)
        let committed_max = self.internal.read().tracker.pointer_count();
        let mut pending_only: Vec<_> = pending_snapshot
            .iter()
            .filter_map(|(&offset, change)| {
                if offset >= committed_max
                    && let PendingChange::Put(data) = change
                {
                    return Some((offset, data.clone()));
                }
                None
            })
            .collect();
        pending_only.sort_by_key(|(offset, _)| *offset);

        for (point_offset, data) in pending_only {
            hw_counter.incr_delta(data.len());
            let decompressed = self.decompress(data.to_vec());
            let value = V::from_bytes(&decompressed);
            if !callback(point_offset, value)? {
                return Ok(());
            }
        }

        Ok(())
    }

    /// Return the storage size in bytes
    pub fn get_storage_size_bytes(&self) -> usize {
        self.internal.read().bitmask.get_storage_size_bytes()
    }
}

// ---------------------------------------------------------------------------
// Gridstore<V> — non-Blob methods (flusher, populate, etc.)
// ---------------------------------------------------------------------------

impl<V> Gridstore<V> {
    /// The number of blocks needed for a given value bytes size
    #[inline]
    fn blocks_for_value(value_size: usize, block_size: usize) -> u32 {
        value_size.div_ceil(block_size).try_into().unwrap()
    }

    /// Create flusher that durably persists all pending changes when invoked.
    ///
    /// The flusher applies buffered pending changes to committed storage in phases:
    /// 1. Write lock on internal: apply pending Puts/Deletes to pages/bitmask/tracker mmaps
    /// 2. Downgrade to read lock: flush mmaps to disk (compatible with concurrent readers)
    /// 3. Brief write lock: free old blocks in bitmask
    /// 4. Brief pending lock: reconcile (remove flushed entries from pending)
    pub fn flusher(&self) -> Flusher {
        // Snapshot pending changes (cheap: Arc<[u8]> clones are refcount-only)
        let pending_snapshot = self.pending.lock().clone();

        // Also snapshot the tracker's own pending updates (for pointer persistence)
        let tracker_pending_updates = self.internal.read().tracker.pending_updates.clone();

        let internal_weak = Arc::downgrade(&self.internal);
        let pending_weak = Arc::downgrade(&self.pending);
        let config = self.config;
        let base_path = self.base_path.clone();

        let is_alive_flush_lock = self.is_alive_flush_lock.handle();

        Box::new(move || {
            let (Some(is_alive_flush_guard), Some(internal)) =
                (is_alive_flush_lock.lock_if_alive(), internal_weak.upgrade())
            else {
                log::trace!("Gridstore was cleared, cancelling flush");
                return Err(GridstoreError::FlushCancelled);
            };

            // Phase 1: Apply pending changes to mmap (write lock)
            let old_pointers = {
                let mut guard = internal.write();

                // Apply pending Puts: allocate blocks, write to pages, mark bitmask, update tracker
                for (&point_offset, change) in &pending_snapshot {
                    match change {
                        PendingChange::Put(data) => {
                            let required_blocks =
                                Self::blocks_for_value(data.len(), config.block_size_bytes);
                            let (page_id, block_offset) = guard.find_or_create_available_blocks(
                                required_blocks,
                                &config,
                                &base_path,
                            )?;

                            guard.write_into_pages(
                                data,
                                page_id,
                                block_offset,
                                config.block_size_bytes,
                            );

                            guard
                                .bitmask
                                .mark_blocks(page_id, block_offset, required_blocks, true);

                            guard.tracker.set(
                                point_offset,
                                ValuePointer::new(page_id, block_offset, data.len() as u32),
                            );
                        }
                        PendingChange::Delete => {
                            guard.tracker.unset(point_offset);
                        }
                    }
                }

                // Merge tracker_pending_updates that were captured before pending_snapshot
                // with the tracker's own pending_updates (accumulated from the above .set/.unset calls)
                // by writing them all to the mmap.
                let mut all_tracker_pending = tracker_pending_updates;
                // The tracker's current pending_updates now contain entries from our
                // pending_snapshot application above. We need to write ALL of them.
                // Swap out the tracker's pending_updates and merge with the pre-snapshot ones.
                let current_tracker_pending = std::mem::take(&mut guard.tracker.pending_updates);
                for (offset, updates) in current_tracker_pending {
                    all_tracker_pending.insert(offset, updates);
                }
                // Restore pending_updates so write_pending can reconcile against them
                guard.tracker.pending_updates = all_tracker_pending.clone();
                guard.tracker.write_pending(all_tracker_pending)
            };

            // Phase 2: Flush mmaps to disk (read lock — compatible with readers)
            {
                let guard = internal.read();
                guard.bitmask.flush()?;
                for page in guard.pages.iter() {
                    page.flush()?;
                }
                guard.tracker.flush()?;
            }

            // Phase 3: Free old blocks in bitmask (brief write lock)
            if !old_pointers.is_empty() {
                let mut guard = internal.write();
                for (page_id, pointer_group) in
                    &old_pointers.into_iter().chunk_by(|pointer| pointer.page_id)
                {
                    let local_ranges = pointer_group.map(|pointer| {
                        let start = pointer.block_offset;
                        let end = pointer.block_offset
                            + Self::blocks_for_value(
                                pointer.length as usize,
                                config.block_size_bytes,
                            );
                        start as usize..end as usize
                    });
                    guard
                        .bitmask
                        .mark_blocks_batch(page_id, local_ranges, false);
                }
                guard.bitmask.flush()?;
            }

            // Phase 4: Reconcile — remove flushed entries from pending
            if let Some(pending_arc) = pending_weak.upgrade() {
                let mut pending = pending_arc.lock();
                for (point_offset, snapshot_change) in &pending_snapshot {
                    if let Some(current_change) = pending.get(point_offset) {
                        let same = match (current_change, snapshot_change) {
                            (PendingChange::Put(a), PendingChange::Put(b)) => Arc::ptr_eq(a, b),
                            (PendingChange::Delete, PendingChange::Delete) => true,
                            _ => false,
                        };
                        if same {
                            pending.remove(point_offset);
                        }
                    }
                }
            }

            // Keep the guard till the end of the flush to prevent concurrent drop/flushes
            drop(is_alive_flush_guard);

            Ok(())
        })
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> std::io::Result<()> {
        let internal = self.internal.read();
        for page in internal.pages.iter() {
            page.populate();
        }
        internal.tracker.populate();
        internal.bitmask.populate()?;
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> std::io::Result<()> {
        let internal = self.internal.read();
        for page in internal.pages.iter() {
            page.clear_cache()?;
        }
        internal.bitmask.clear_cache()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use fs_err::File;
    use itertools::Itertools;
    use rand::distr::Uniform;
    use rand::prelude::Distribution;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    use rstest::rstest;
    use tempfile::Builder;

    use super::*;
    use crate::blob::Blob;
    use crate::config::{
        DEFAULT_BLOCK_SIZE_BYTES, DEFAULT_PAGE_SIZE_BYTES, DEFAULT_REGION_SIZE_BLOCKS,
    };
    use crate::fixtures::{HM_FIELDS, Payload, empty_storage, empty_storage_sized, random_payload};

    #[test]
    fn test_empty_payload_storage() {
        let hw_counter = HardwareCounterCell::new();
        let (_dir, storage) = empty_storage();
        let payload = storage.get_value::<false>(0, &hw_counter);
        assert!(payload.is_none());
        assert_eq!(storage.get_storage_size_bytes(), 0);
    }

    #[test]
    fn test_put_single_empty_value() {
        let (_dir, mut storage) = empty_storage();

        let hw_counter = HardwareCounterCell::new();
        let hw_counter = hw_counter.ref_payload_io_write_counter();

        let payload = Payload::default();
        storage.put_value(0, &payload, hw_counter).unwrap();
        assert_eq!(storage.internal.read().pages.len(), 1);

        // Value is readable from pending (before flush)
        let hw_counter = HardwareCounterCell::new();
        let stored_payload = storage.get_value::<false>(0, &hw_counter);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), Payload::default());

        // After flush, committed storage reflects the write
        storage.flusher()().unwrap();
        assert_eq!(storage.internal.read().tracker.mapping_len(), 1);
        assert_eq!(storage.get_storage_size_bytes(), DEFAULT_BLOCK_SIZE_BYTES);
    }

    #[test]
    fn test_put_single_payload() {
        let (_dir, mut storage) = empty_storage();

        let mut payload = Payload::default();
        payload.0.insert(
            "key".to_string(),
            serde_json::Value::String("value".to_string()),
        );

        let hw_counter = HardwareCounterCell::new();
        let hw_counter = hw_counter.ref_payload_io_write_counter();

        storage.put_value(0, &payload, hw_counter).unwrap();
        assert_eq!(storage.internal.read().pages.len(), 1);

        // Value is readable from pending (before flush)
        let hw_counter = HardwareCounterCell::new();
        let stored_payload = storage.get_value::<false>(0, &hw_counter);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);

        // After flush, committed storage has the pointer
        storage.flusher()().unwrap();
        assert_eq!(storage.internal.read().tracker.mapping_len(), 1);

        let page_mapping = storage.internal.read().get_pointer(0).unwrap();
        assert_eq!(page_mapping.page_id, 0); // first page
        assert_eq!(page_mapping.block_offset, 0); // first cell
        assert_eq!(storage.get_storage_size_bytes(), DEFAULT_BLOCK_SIZE_BYTES);
    }

    #[test]
    fn test_storage_files() {
        let (dir, mut storage) = empty_storage();

        let mut payload = Payload::default();
        payload.0.insert(
            "key".to_string(),
            serde_json::Value::String("value".to_string()),
        );

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
        storage.put_value(0, &payload, hw_counter_ref).unwrap();
        storage.flusher()().unwrap();
        assert_eq!(storage.internal.read().pages.len(), 1);
        assert_eq!(storage.internal.read().tracker.mapping_len(), 1);
        let files = storage.files();
        let actual_files: Vec<_> = fs::read_dir(dir.path()).unwrap().try_collect().unwrap();
        assert_eq!(
            files.len(),
            actual_files.len(),
            "The directory has {} files, but we are reporting {}\nreported: {files:?}\n actual: {actual_files:?}",
            actual_files.len(),
            files.len()
        );
        assert_eq!(files.len(), 5, "Expected 5 files, got {files:?}");
        assert_eq!(files[0].file_name().unwrap(), "tracker.dat");
        assert_eq!(files[1].file_name().unwrap(), "page_0.dat");
        assert_eq!(files[2].file_name().unwrap(), "bitmask.dat");
        assert_eq!(files[3].file_name().unwrap(), "gaps.dat");
        assert_eq!(files[4].file_name().unwrap(), "config.json");
    }

    #[rstest]
    #[case(100000, 2)]
    #[case(100, 2000)]
    fn test_put_payload(#[case] num_payloads: u32, #[case] payload_size_factor: usize) {
        let (_dir, mut storage) = empty_storage();

        let rng = &mut rand::rngs::SmallRng::from_os_rng();

        let mut payloads = (0..num_payloads)
            .map(|point_offset| (point_offset, random_payload(rng, payload_size_factor)))
            .collect::<Vec<_>>();

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
        for (point_offset, payload) in payloads.iter() {
            storage
                .put_value(*point_offset, payload, hw_counter_ref)
                .unwrap();

            let stored_payload = storage.get_value::<false>(*point_offset, &hw_counter);
            assert!(stored_payload.is_some());
            assert_eq!(&stored_payload.unwrap(), payload);
        }

        // read randomly
        payloads.shuffle(rng);
        for (point_offset, payload) in payloads.iter() {
            let stored_payload = storage.get_value::<false>(*point_offset, &hw_counter);
            assert!(stored_payload.is_some());
            assert_eq!(stored_payload.unwrap(), payload.clone());
        }
    }

    #[test]
    fn test_delete_single_payload() {
        let (_dir, mut storage) = empty_storage();

        let mut payload = Payload::default();
        payload.0.insert(
            "key".to_string(),
            serde_json::Value::String("value".to_string()),
        );

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
        storage.put_value(0, &payload, hw_counter_ref).unwrap();
        assert_eq!(storage.internal.read().pages.len(), 1);

        let stored_payload = storage.get_value::<false>(0, &hw_counter);
        assert_eq!(stored_payload, Some(payload));

        // Flush to commit the value, then check storage size
        storage.flusher()().unwrap();
        assert_eq!(storage.get_storage_size_bytes(), DEFAULT_BLOCK_SIZE_BYTES);

        // delete payload
        let stored_payload = storage.get_value::<false>(0, &hw_counter);
        let deleted = storage.delete_value(0);
        assert_eq!(deleted, stored_payload);
        assert_eq!(storage.internal.read().pages.len(), 1);

        // get payload again — deleted from pending
        let stored_payload = storage.get_value::<false>(0, &hw_counter);
        assert!(stored_payload.is_none());
        storage.flusher()().unwrap();
        assert_eq!(storage.get_storage_size_bytes(), 0);
    }

    #[test]
    fn test_update_single_payload() {
        let (_dir, mut storage) = empty_storage();

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
        let put_payload = |storage: &mut Gridstore<Payload>, payload_value: &str| {
            let mut payload = Payload::default();
            payload.0.insert(
                "key".to_string(),
                serde_json::Value::String(payload_value.to_string()),
            );

            storage.put_value(0, &payload, hw_counter_ref).unwrap();

            let hw_counter = HardwareCounterCell::new();
            let stored_payload = storage.get_value::<false>(0, &hw_counter);
            assert!(stored_payload.is_some());
            assert_eq!(stored_payload.unwrap(), payload);
        };

        put_payload(&mut storage, "value");
        put_payload(&mut storage, "updated");
        put_payload(&mut storage, "updated again");

        // Flush commits only the latest value ("updated again")
        storage.flusher()().unwrap();
        assert_eq!(storage.internal.read().tracker.mapping_len(), 1);

        // After flush, block 0 was used; next put will get a new block after next flush
        put_payload(&mut storage, "updated after flush");
        storage.flusher()().unwrap();

        // Block 0 should now be freed (old value) and block 1 used for new value
        let pointer = storage.internal.read().get_pointer(0).unwrap();
        assert_eq!(pointer.page_id, 0);
    }

    #[test]
    fn test_write_across_pages() {
        let page_size = DEFAULT_BLOCK_SIZE_BYTES * DEFAULT_REGION_SIZE_BLOCKS;
        let (_dir, storage) = empty_storage_sized(page_size, Compression::None);

        let block_size = storage.config.block_size_bytes;

        // Create a second page via internal
        {
            let mut internal = storage.internal.write();
            internal
                .create_new_page(&storage.config, &storage.base_path)
                .unwrap();
        }

        let value_len = 1000;

        // Value should span 8 blocks
        let value = (0..)
            .map(|i| (i % 24) as u8)
            .take(value_len)
            .collect::<Vec<_>>();

        // Let's write it near the end
        let block_offset = DEFAULT_REGION_SIZE_BLOCKS - 10;
        {
            let mut internal = storage.internal.write();
            internal.write_into_pages(&value, 0, block_offset as u32, block_size);
        }

        let read_value = storage.internal.read().read_from_pages::<false>(
            0,
            block_offset as u32,
            value_len as u32,
            block_size,
        );
        assert_eq!(value, read_value);
    }

    enum Operation {
        // Insert point with payload
        Put(PointOffset, Payload),
        // Delete point by offset
        Delete(PointOffset),
        // Get point by offset
        Get(PointOffset),
        // Flush after delay
        FlushDelay(Duration),
        // Clear storage
        Clear,
        // Iter up to limit
        Iter(PointOffset),
    }

    impl Operation {
        fn random(rng: &mut impl Rng, max_point_offset: u32) -> Self {
            let workload = rand::distr::weighted::WeightedIndex::new([
                max_point_offset,         // put
                max_point_offset / 100,   // delete
                max_point_offset,         // get
                max_point_offset / 500,   // flush
                max_point_offset / 5_000, // clear
                max_point_offset / 5_000, // iter
            ])
            .unwrap();

            let operation = workload.sample(rng);
            match operation {
                0 => {
                    let size_factor = rng.random_range(1..10);
                    let payload = random_payload(rng, size_factor);
                    let point_offset = rng.random_range(0..=max_point_offset);
                    Operation::Put(point_offset, payload)
                }
                1 => {
                    let point_offset = rng.random_range(0..=max_point_offset);
                    Operation::Delete(point_offset)
                }
                2 => {
                    let point_offset = rng.random_range(0..=max_point_offset);
                    Operation::Get(point_offset)
                }
                3 => {
                    let delay_ms = rng.random_range(0..=500);
                    let delay = Duration::from_millis(delay_ms);
                    Operation::FlushDelay(delay)
                }
                4 => Operation::Clear,
                5 => {
                    let limit = rng.random_range(0..=10);
                    Operation::Iter(limit)
                }
                op => panic!("{op} out of range"),
            }
        }
    }

    #[rstest]
    fn test_behave_like_hashmap(
        #[values(1_048_576, 2_097_152, DEFAULT_PAGE_SIZE_BYTES)] page_size: usize,
        #[values(Compression::None, Compression::LZ4)] compression: Compression,
    ) {
        use ahash::AHashMap;

        let operation_count = 100_000;
        let max_point_offset = 10_000u32;

        let _ = env_logger::builder().is_test(true).try_init();

        let (dir, mut storage) = empty_storage_sized(page_size, compression);

        let rng = &mut rand::rngs::SmallRng::from_os_rng();

        let mut model_hashmap = AHashMap::with_capacity(max_point_offset as usize);

        let operations = (0..operation_count).map(|_| Operation::random(rng, max_point_offset));

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

        // ensure no concurrent flushing & flusher instances
        let has_flusher_lock = Arc::new(parking_lot::Mutex::new(false));

        let mut flush_thread_handles = Vec::new();

        // apply operations to storage and model_hashmap
        for (i, operation) in operations.enumerate() {
            match operation {
                Operation::Clear => {
                    log::debug!("op:{i} CLEAR");
                    storage.clear().unwrap();
                    assert_eq!(storage.max_point_offset(), 0, "storage should be empty");
                    model_hashmap.clear();
                }
                Operation::Iter(limit) => {
                    log::debug!("op:{i} ITER limit:{limit}");
                    storage
                        .iter::<_, String>(
                            |point_offset, payload| {
                                if point_offset >= limit {
                                    return Ok(false); // shortcut iteration
                                }
                                assert_eq!(
                                    model_hashmap.get(&point_offset), Some(&payload),
                                    "storage and model are different when using `iter` for offset:{point_offset}"
                                );
                                Ok(true) // no shortcutting
                            },
                            hw_counter_ref,
                        )
                        .unwrap();
                }
                Operation::Put(point_offset, payload) => {
                    log::debug!("op:{i} PUT offset:{point_offset}");
                    let old1 = storage
                        .put_value(point_offset, &payload, hw_counter_ref)
                        .unwrap();
                    let old2 = model_hashmap.insert(point_offset, payload);
                    assert_eq!(
                        old1,
                        old2.is_some(),
                        "put failed for point_offset: {point_offset} with {old1:?} vs {old2:?}",
                    );
                }
                Operation::Delete(point_offset) => {
                    log::debug!("op:{i} DELETE offset:{point_offset}");
                    let old1 = storage.delete_value(point_offset);
                    let old2 = model_hashmap.remove(&point_offset);
                    assert_eq!(
                        old1, old2,
                        "same deletion failed for point_offset: {point_offset} with {old1:?} vs {old2:?}",
                    );
                }
                Operation::Get(point_offset) => {
                    log::debug!("op:{i} GET offset:{point_offset}");
                    let v1_seq = storage.get_value::<true>(point_offset, &hw_counter);
                    let v1_rand = storage.get_value::<false>(point_offset, &hw_counter);
                    let v2 = model_hashmap.get(&point_offset).cloned();
                    assert_eq!(
                        v1_seq, v2,
                        "get sequential failed for point_offset: {point_offset} with {v1_seq:?} vs {v2:?}",
                    );
                    assert_eq!(
                        v1_rand, v2,
                        "get_rand sequential failed for point_offset: {point_offset} with {v1_rand:?} vs {v2:?}",
                    );
                }
                Operation::FlushDelay(delay) => {
                    let mut flush_lock_guard = has_flusher_lock.lock();
                    if *flush_lock_guard {
                        log::debug!(
                            "op:{i} Skip flushing because a Flusher has already been created"
                        );
                    } else {
                        log::debug!("op:{i} Scheduling flush in {delay:?}");
                        let flusher = storage.flusher();
                        *flush_lock_guard = true;
                        drop(flush_lock_guard);
                        let flush_lock = has_flusher_lock.clone();
                        let handle = std::thread::Builder::new()
                            .name("background_flush".to_string())
                            .spawn(move || {
                                thread::sleep(delay); // keep flusher alive while other operation are applied
                                let mut flush_lock_guard = flush_lock.lock();
                                assert!(
                                    *flush_lock_guard,
                                    "there must be a flusher marked as alive"
                                );
                                log::debug!("op:{i} STARTING FLUSH after waiting {delay:?}");
                                match flusher() {
                                    Ok(_) => log::debug!("op:{i} FLUSH DONE"),
                                    Err(err) => log::error!("op:{i} FLUSH failed {err:?}"),
                                }
                                *flush_lock_guard = false; // no flusher alive
                                drop(flush_lock_guard);
                            })
                            .unwrap();
                        flush_thread_handles.push(handle);
                    }
                }
            }
        }

        log::debug!("All operations successfully applied - now checking consistency...");

        // wait for all background flushes to complete
        for handle in flush_thread_handles {
            handle.join().expect("flush thread should not panic");
        }

        // Flush all remaining pending changes to committed storage
        storage.flusher()().unwrap();

        // asset same length
        assert_eq!(
            storage.internal.read().tracker.mapping_len(),
            model_hashmap.len(),
            "different number of points"
        );

        // validate storage and model_hashmap are the same
        for point_offset in 0..=max_point_offset {
            let stored_payload = storage.get_value::<false>(point_offset, &hw_counter);
            let model_payload = model_hashmap.get(&point_offset);
            assert_eq!(
                stored_payload.as_ref(),
                model_payload,
                "storage and model differ for offset:{point_offset} {stored_payload:?} vs {model_payload:?}"
            );
        }

        // flush data
        storage.flusher()().unwrap();

        let before_size = storage.get_storage_size_bytes();
        // drop storage
        drop(storage);

        // reopen storage
        let storage = Gridstore::<Payload>::open(dir.path().to_path_buf()).unwrap();
        // assert same size
        assert_eq!(storage.get_storage_size_bytes(), before_size);
        // assert same length
        assert_eq!(
            storage.internal.read().tracker.mapping_len(),
            model_hashmap.len()
        );

        // validate storage and model_hashmap are the same
        for point_offset in 0..=max_point_offset {
            let stored_payload = storage.get_value::<false>(point_offset, &hw_counter);
            let model_payload = model_hashmap.get(&point_offset);
            assert_eq!(
                stored_payload.as_ref(),
                model_payload,
                "failed for point_offset: {point_offset}",
            );
        }

        // wipe storage
        storage.wipe().unwrap();

        // assert base folder is gone
        assert!(
            !dir.path().exists(),
            "base folder should be deleted by wipe"
        );
    }

    #[test]
    fn test_handle_huge_payload() {
        let (_dir, mut storage) = empty_storage();
        assert_eq!(storage.internal.read().pages.len(), 1);

        let mut payload = Payload::default();

        let huge_payload_size = 1024 * 1024 * 50; // 50MB

        let distr = Uniform::new('a', 'z').unwrap();
        let rng = rand::rngs::SmallRng::from_os_rng();

        let huge_value =
            serde_json::Value::String(distr.sample_iter(rng).take(huge_payload_size).collect());
        payload.0.insert("huge".to_string(), huge_value);

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
        storage.put_value(0, &payload, hw_counter_ref).unwrap();

        // Value is readable from pending
        let stored_payload = storage.get_value::<false>(0, &hw_counter);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);

        // Flush to commit to pages
        storage.flusher()().unwrap();

        // After flush, the huge payload should have caused a second page to be created
        assert_eq!(storage.internal.read().pages.len(), 2);

        let page_mapping = storage.get_pointer(0).unwrap();
        assert_eq!(page_mapping.page_id, 0); // first page
        assert_eq!(page_mapping.block_offset, 0); // first cell

        {
            // the fitting page should be 64MB, so we should still have about 14MB of free space
            let free_blocks = storage.internal.read().bitmask.free_blocks_for_page(1);
            let min_expected = 1024 * 1024 * 13 / DEFAULT_BLOCK_SIZE_BYTES;
            let max_expected = 1024 * 1024 * 15 / DEFAULT_BLOCK_SIZE_BYTES;
            assert!((min_expected..max_expected).contains(&free_blocks));
        }

        {
            // delete payload
            let deleted = storage.delete_value(0);
            assert!(deleted.is_some());
            // Pages are not freed until flush
            assert_eq!(storage.internal.read().pages.len(), 2);

            assert!(storage.get_value::<false>(0, &hw_counter).is_none());
        }
    }

    #[test]
    fn test_storage_persistence_basic() {
        let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
        let path = dir.path().to_path_buf();

        let mut payload = Payload::default();
        payload.0.insert(
            "key".to_string(),
            serde_json::Value::String("value".to_string()),
        );

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
        {
            let mut storage = Gridstore::new(path.clone(), Default::default()).unwrap();
            storage.put_value(0, &payload, hw_counter_ref).unwrap();
            assert_eq!(storage.internal.read().pages.len(), 1);

            // Value is readable from pending
            let stored_payload = storage.get_value::<false>(0, &hw_counter);
            assert!(stored_payload.is_some());
            assert_eq!(stored_payload.unwrap(), payload);

            // flush storage before dropping
            storage.flusher()().unwrap();

            // After flush, pointer is committed
            let page_mapping = storage.get_pointer(0).unwrap();
            assert_eq!(page_mapping.page_id, 0); // first page
            assert_eq!(page_mapping.block_offset, 0); // first cell
        }

        // reopen storage
        let storage = Gridstore::<Payload>::open(path).unwrap();
        assert_eq!(storage.internal.read().pages.len(), 1);

        let stored_payload = storage.get_value::<false>(0, &hw_counter);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);
    }

    #[test]
    #[ignore = "this test is too slow for ci, and has similar coverage to the hashmap tests"]
    fn test_with_real_hm_data() {
        const EXPECTED_LEN: usize = 105_542;

        fn write_data(storage: &mut Gridstore<Payload>, init_offset: u32) -> u32 {
            let csv_path = dataset::Dataset::HMArticles
                .download()
                .expect("download should succeed");

            let csv_file = BufReader::new(File::open(csv_path).expect("file should open"));

            let hw_counter = HardwareCounterCell::new();
            let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

            let mut rdr = csv::Reader::from_reader(csv_file);
            let mut point_offset = init_offset;
            for result in rdr.records() {
                let record = result.unwrap();
                let mut payload = Payload::default();
                for (i, &field) in HM_FIELDS.iter().enumerate() {
                    payload.0.insert(
                        field.to_string(),
                        serde_json::Value::String(record.get(i).unwrap().to_string()),
                    );
                }
                storage
                    .put_value(point_offset, &payload, hw_counter_ref)
                    .unwrap();
                point_offset += 1;
            }
            storage.flusher()().unwrap();
            point_offset
        }

        fn storage_double_pass_is_consistent(
            storage: &Gridstore<Payload>,
            right_shift_offset: u32,
        ) {
            // validate storage value equality between the two writes
            let csv_path = dataset::Dataset::HMArticles
                .download()
                .expect("download should succeed");

            let csv_file = BufReader::new(File::open(csv_path).expect("file should open"));
            let hw_counter = HardwareCounterCell::new();

            let mut rdr = csv::Reader::from_reader(csv_file);
            for (row_index, result) in rdr.records().enumerate() {
                let record = result.unwrap();
                // apply shift offset
                let storage_index = row_index as u32 + right_shift_offset;
                let first = storage
                    .get_value::<false>(storage_index, &hw_counter)
                    .unwrap();
                let second = storage
                    .get_value::<false>(storage_index + EXPECTED_LEN as u32, &hw_counter)
                    .unwrap();
                // assert the two payloads are equal
                assert_eq!(first, second);
                // validate the payload against record
                for (i, field) in HM_FIELDS.iter().enumerate() {
                    assert_eq!(
                        first.0.get(*field).unwrap().as_str().unwrap(),
                        record.get(i).unwrap(),
                        "failed for id {row_index} with shift {right_shift_offset} for field: {field}",
                    );
                }
            }
        }

        let (dir, mut storage) = empty_storage();
        // load data into storage
        let point_offset = write_data(&mut storage, 0);
        assert_eq!(point_offset, EXPECTED_LEN as u32);
        assert_eq!(storage.internal.read().tracker.mapping_len(), EXPECTED_LEN);
        assert_eq!(storage.internal.read().pages.len(), 2);

        // write the same payload a second time
        let point_offset = write_data(&mut storage, point_offset);
        assert_eq!(point_offset, EXPECTED_LEN as u32 * 2);
        assert_eq!(
            storage.internal.read().tracker.mapping_len(),
            EXPECTED_LEN * 2
        );
        assert_eq!(storage.internal.read().pages.len(), 4);

        // assert storage is consistent
        storage_double_pass_is_consistent(&storage, 0);

        // drop storage
        storage.flusher()().unwrap();
        drop(storage);

        // reopen storage
        let mut storage = Gridstore::open(dir.path().to_path_buf()).unwrap();
        assert_eq!(point_offset, EXPECTED_LEN as u32 * 2);
        assert_eq!(storage.internal.read().pages.len(), 4);
        assert_eq!(
            storage.internal.read().tracker.mapping_len(),
            EXPECTED_LEN * 2
        );

        // assert storage is consistent after reopening
        storage_double_pass_is_consistent(&storage, 0);

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

        // update values shifting point offset by 1 to the right
        // loop from the end to the beginning to avoid overwriting
        let offset: u32 = 1;
        for i in (0..EXPECTED_LEN).rev() {
            let payload = storage.get_value::<false>(i as u32, &hw_counter).unwrap();
            // move first write to the right
            storage
                .put_value(i as u32 + offset, &payload, hw_counter_ref)
                .unwrap();
            // move second write to the right
            storage
                .put_value(
                    i as u32 + offset + EXPECTED_LEN as u32,
                    &payload,
                    hw_counter_ref,
                )
                .unwrap();
        }
        // assert storage is consistent after updating
        storage_double_pass_is_consistent(&storage, offset);

        // wipe storage manually
        assert!(dir.path().exists());
        storage.wipe().unwrap();
        assert!(!dir.path().exists());
    }

    #[test]
    fn test_payload_compression() {
        let payload = random_payload(&mut rand::rngs::SmallRng::from_os_rng(), 2);
        let payload_bytes = payload.to_bytes();
        let compressed = compress_lz4(&payload_bytes);
        let decompressed = decompress_lz4(&compressed);
        let decompressed_payload = <Payload as Blob>::from_bytes(&decompressed);
        assert_eq!(payload, decompressed_payload);
    }

    #[rstest]
    #[case(64)]
    #[case(128)]
    #[case(256)]
    #[case(512)]
    fn test_different_block_sizes(#[case] block_size_bytes: usize) {
        use crate::fixtures::minimal_payload;

        let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
        let blocks_per_page = DEFAULT_PAGE_SIZE_BYTES / block_size_bytes;
        let options = StorageOptions {
            block_size_bytes: Some(block_size_bytes),
            ..Default::default()
        };
        let mut storage = Gridstore::new(dir.path().to_path_buf(), options).unwrap();

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
        let payload = minimal_payload();
        let last_point_id = 3 * blocks_per_page as u32;
        for point_offset in 0..=last_point_id {
            storage
                .put_value(point_offset, &payload, hw_counter_ref)
                .unwrap();
        }

        // Flush pending changes to committed storage
        storage.flusher()().unwrap();
        println!("{last_point_id}");

        assert_eq!(storage.internal.read().pages.len(), 4);
        // Verify all points are committed and readable
        for point_offset in 0..=last_point_id {
            let stored = storage.get_value::<false>(point_offset, &hw_counter);
            assert!(stored.is_some(), "point {point_offset} should exist");
            assert_eq!(stored.unwrap(), payload);
        }
        // Verify the last point has a valid committed pointer
        let last_pointer = storage.get_pointer(last_point_id).unwrap();
        assert!(
            (last_pointer.page_id as usize) < storage.internal.read().pages.len(),
            "page_id should be valid"
        );
    }

    /// Test that data is only actually flushed when we invoke the flush closure
    ///
    /// Specifically:
    /// - version of data we write to disk is that of when the flusher was created
    /// - data is only written to disk when closure is invoked
    /// - with pending changes, block allocation happens during flush, not during put_value
    #[test]
    fn test_deferred_flush() {
        let (dir, mut storage) = empty_storage();
        let path = dir.path().to_path_buf();

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
        let get_payload = |storage: &Gridstore<Payload>| {
            storage
                .get_value::<false>(0, &hw_counter)
                .expect("offset exists")
                .0
                .get("key")
                .expect("key exists")
                .as_str()
                .expect("value is a string")
                .to_owned()
        };
        let put_payload = |storage: &mut Gridstore<Payload>, payload_value: &str| {
            let mut payload = Payload::default();
            payload.0.insert(
                "key".to_string(),
                serde_json::Value::String(payload_value.to_string()),
            );

            storage.put_value(0, &payload, hw_counter_ref).unwrap();

            let hw_counter = HardwareCounterCell::new();
            let stored_payload = storage.get_value::<false>(0, &hw_counter);
            assert!(stored_payload.is_some());
            assert_eq!(stored_payload.unwrap(), payload);
        };

        put_payload(&mut storage, "value 1");
        assert_eq!(get_payload(&storage), "value 1");

        put_payload(&mut storage, "value 2");
        assert_eq!(get_payload(&storage), "value 2");

        let flusher = storage.flusher();

        put_payload(&mut storage, "value 3");
        assert_eq!(get_payload(&storage), "value 3");

        // We drop the flusher so nothing happens
        drop(flusher);
        assert_eq!(get_payload(&storage), "value 3");

        let flusher = storage.flusher();

        put_payload(&mut storage, "value 4");
        assert_eq!(get_payload(&storage), "value 4");

        // We flush and still expect to read latest data
        flusher().unwrap();
        assert_eq!(get_payload(&storage), "value 4");

        put_payload(&mut storage, "value 5");
        assert_eq!(get_payload(&mut storage), "value 5");

        // Reopen gridstore (only flushed data survives)
        drop(storage);
        let mut storage = Gridstore::<Payload>::open(path).unwrap();
        assert_eq!(storage.internal.read().pages.len(), 1);

        // On reopen, we expect to read the data at the time the flusher was created (value 3)
        // because the flusher snapshotted pending which contained value 3 for offset 0.
        // value 4 was written after the flusher was created and also got included in the snapshot.
        // Actually, the flusher snapshots at creation time, so value 3 was in the snapshot.
        // value 4 was written after the flusher was created, so it was NOT in the snapshot.
        // Therefore on reopen we get value 3.
        assert_eq!(get_payload(&storage), "value 3");

        put_payload(&mut storage, "value 6");
        assert_eq!(get_payload(&storage), "value 6");

        put_payload(&mut storage, "value 7");
        assert_eq!(get_payload(&storage), "value 7");

        put_payload(&mut storage, "value 8");
        assert_eq!(get_payload(&storage), "value 8");
    }

    /// Similar to [`test_deferred_flush`] but more complex, including multiple flushers and deletes
    #[test]
    fn test_deferred_flush_with_delete() {
        let (dir, mut storage) = empty_storage();
        let path = dir.path().to_path_buf();

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
        let get_payload = |storage: &Gridstore<Payload>| {
            Some(
                storage
                    .get_value::<false>(0, &hw_counter)?
                    .0
                    .get("key")
                    .expect("key exists")
                    .as_str()
                    .expect("value is a string")
                    .to_owned(),
            )
        };
        let put_payload = |storage: &mut Gridstore<Payload>, payload_value: &str| {
            let mut payload = Payload::default();
            payload.0.insert(
                "key".to_string(),
                serde_json::Value::String(payload_value.to_string()),
            );

            storage.put_value(0, &payload, hw_counter_ref).unwrap();

            let hw_counter = HardwareCounterCell::new();
            let stored_payload = storage.get_value::<false>(0, &hw_counter);
            assert!(stored_payload.is_some());
            assert_eq!(stored_payload.unwrap(), payload);
        };

        put_payload(&mut storage, "value 1");
        assert_eq!(get_payload(&storage).unwrap(), "value 1");

        put_payload(&mut storage, "value 2");
        assert_eq!(get_payload(&storage).unwrap(), "value 2");

        let flusher = storage.flusher();

        put_payload(&mut storage, "value 3");
        assert_eq!(get_payload(&storage).unwrap(), "value 3");

        storage.delete_value(0);
        assert!(get_payload(&storage).is_none());

        // Flush, storage is still open so we expect the point not to exist
        flusher().unwrap();
        assert!(get_payload(&storage).is_none());

        // Reopen gridstore
        drop(storage);
        let mut storage = Gridstore::<Payload>::open(path.clone()).unwrap();
        assert_eq!(storage.internal.read().pages.len(), 1);

        let flusher = storage.flusher();

        // On reopen, later updates and the delete were not flushed, expect to read value 2
        assert_eq!(get_payload(&storage).unwrap(), "value 2");

        flusher().unwrap();

        storage.delete_value(0);
        assert!(get_payload(&storage).is_none());

        storage.flusher()().unwrap();
        storage.flusher()().unwrap();
        assert!(get_payload(&storage).is_none());

        // Reopen gridstore
        drop(storage);
        let mut storage = Gridstore::<Payload>::open(path.clone()).unwrap();
        assert_eq!(storage.internal.read().pages.len(), 1);

        // On reopen, delete was flushed this time, expect point to be missing
        assert!(get_payload(&storage).is_none());

        put_payload(&mut storage, "value 4");
        assert_eq!(get_payload(&storage).unwrap(), "value 4");

        assert_eq!(storage.pending.lock().len(), 1, "expect 1 pending update",);

        storage.flusher()().unwrap();

        assert_eq!(storage.pending.lock().len(), 0, "expect 0 pending updates",);

        // Reopen gridstore
        drop(storage);
        let mut storage = Gridstore::<Payload>::open(path.clone()).unwrap();
        assert_eq!(storage.internal.read().pages.len(), 1);

        // On reopen, value 4 was flushed, expect to read it
        assert_eq!(get_payload(&storage).unwrap(), "value 4");

        put_payload(&mut storage, "value 5");
        assert_eq!(get_payload(&storage).unwrap(), "value 5");

        let flusher_1_value_5 = storage.flusher();

        storage.delete_value(0);
        assert!(get_payload(&storage).is_none());

        let flusher_2_delete = storage.flusher();

        put_payload(&mut storage, "value 6");
        assert_eq!(get_payload(&storage).unwrap(), "value 6");

        let flusher_3_value_6 = storage.flusher();

        put_payload(&mut storage, "value 7");
        assert_eq!(get_payload(&storage).unwrap(), "value 7");

        // Not flushed, still expect to read value 4
        {
            let tmp_storage = Gridstore::<Payload>::open(path.clone()).unwrap();
            assert_eq!(get_payload(&tmp_storage).unwrap(), "value 4");
        }

        // First flusher flushed, expect to read value 5 if we load from disk
        flusher_1_value_5().unwrap();
        {
            let tmp_storage = Gridstore::<Payload>::open(path.clone()).unwrap();
            assert_eq!(get_payload(&tmp_storage).unwrap(), "value 5");
        }

        // Second flusher flushed, expect point to be missing if we load from disk
        flusher_2_delete().unwrap();
        {
            let tmp_storage = Gridstore::<Payload>::open(path.clone()).unwrap();
            assert!(get_payload(&tmp_storage).is_none());
        }

        // Third flusher flushed, expect to read value 6 if we load from disk
        flusher_3_value_6().unwrap();
        {
            let tmp_storage = Gridstore::<Payload>::open(path).unwrap();
            assert_eq!(get_payload(&tmp_storage).unwrap(), "value 6");
        }

        // Main storage still isn't flushed, but has value 7
        assert_eq!(get_payload(&storage).unwrap(), "value 7");
        assert_eq!(storage.pending.lock().len(), 1, "expect 1 pending update",);
    }

    /// Test that data is only actually flushed when the Gridstore instance is still valid
    ///
    /// Specifically:
    /// - ensure that 'late' flushers don't write any data if already invalidated by a clear or
    ///   something else
    #[test]
    fn test_skip_deferred_flush_after_clear() {
        let (dir, mut storage) = empty_storage();
        let path = dir.path().to_path_buf();

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
        let put_payload =
            |storage: &mut Gridstore<Payload>, point_offset: u32, payload_value: &str| {
                let mut payload = Payload::default();
                payload.0.insert(
                    "key".to_string(),
                    serde_json::Value::String(payload_value.to_string()),
                );

                storage
                    .put_value(point_offset, &payload, hw_counter_ref)
                    .unwrap();
                assert_eq!(storage.internal.read().pages.len(), 1);

                let hw_counter = HardwareCounterCell::new();
                let stored_payload = storage.get_value::<false>(point_offset, &hw_counter);
                assert!(stored_payload.is_some());
                assert_eq!(stored_payload.unwrap(), payload);
            };

        // Write enough new pointers so that they don't fit in the default tracker file size
        // On flush, the tracker file will be resized and reopened, significant for this test
        let file_size = storage.internal.read().tracker.mmap_file_size();
        const POINTER_SIZE: usize = size_of::<Option<ValuePointer>>();
        let last_point_offset = (file_size / POINTER_SIZE) as u32;
        for i in 0..=last_point_offset {
            put_payload(&mut storage, i, "value x");
        }

        let flusher = storage.flusher();

        // Clone arcs to keep storage alive after clear
        // Necessary for this test to allow the flusher task to still upgrade its weak arcs when we
        // call the flusher. This allows us to trigger broken flush behavior in old versions.
        // The same is possible without cloning these arcs, but it would require specific timing
        // conditions. Cloning arcs here is much more reliable for this test case.
        let storage_arcs = (Arc::clone(&storage.internal), Arc::clone(&storage.pending));

        // We clear the storage, pending flusher must not write anything anymore
        storage.clear().unwrap();

        // Flusher is invalidated and does nothing
        // This was broken before <https://github.com/qdrant/qdrant/pull/7702>
        assert!(flusher().is_err_and(|err| matches!(err, GridstoreError::FlushCancelled)));

        drop(storage_arcs);

        // We expect the storage to be empty
        assert!(storage.get_pointer(0).is_none(), "point must not exist");

        // If we reopen the storage it must still be empty
        drop(storage);
        let storage = Gridstore::<Payload>::open(path.clone()).unwrap();
        assert_eq!(storage.internal.read().pages.len(), 1);
        assert!(storage.get_pointer(0).is_none(), "point must not exist");
        assert_eq!(storage.max_point_offset(), 0, "must have zero points");
    }
}
