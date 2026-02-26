mod reader;
#[cfg(test)]
mod tests;
pub(crate) mod view;

use std::path::PathBuf;
use std::sync::Arc;

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::referenced_counter::HwMetricRefCounter;
use common::fs::atomic_save_json;
use common::is_alive_lock::IsAliveLock;
use common::universal_io::mmap::MmapUniversal;
use fs_err as fs;
use itertools::Itertools;
use parking_lot::RwLock;
use reader::CONFIG_FILENAME;
pub use reader::GridstoreReader;
pub use view::GridstoreView;

use crate::Result;
use crate::bitmask::Bitmask;
use crate::blob::Blob;
use crate::config::{StorageConfig, StorageOptions};
use crate::error::GridstoreError;
use crate::page::Page;
use crate::tracker::{BlockOffset, PageId, PointOffset, PointerUpdates, Tracker, ValuePointer};

pub type Flusher = Box<dyn FnOnce() -> std::result::Result<(), GridstoreError> + Send>;

/// Read-write storage for values of type `V`.
///
/// Uses `Arc<RwLock<...>>` for pages and tracker to support concurrent flushing.
/// Assumes sequential IDs to the values (0, 1, 2, 3, ...)
#[derive(Debug)]
pub struct Gridstore<V> {
    pub(super) config: StorageConfig,
    pub(super) tracker: Arc<RwLock<Tracker>>,
    pub(super) pages: Arc<RwLock<Vec<Page<MmapUniversal<u8>>>>>,
    /// Bitmask to represent which "blocks" of data in the pages are used and which are free.
    ///
    /// 0 is free, 1 is used.
    pub(super) bitmask: Arc<RwLock<Bitmask>>,
    pub(super) base_path: PathBuf,
    pub(super) _value_type: std::marker::PhantomData<V>,
    /// Lock to prevent concurrent flushes and used for waiting for ongoing flushes to finish.
    is_alive_flush_lock: IsAliveLock,
}

impl<V: Blob> Gridstore<V> {
    /// Create a [`GridstoreView`] by locking pages and tracker, then call `f` with the view.
    fn with_view<R>(&self, f: impl FnOnce(GridstoreView<'_, V, MmapUniversal<u8>>) -> R) -> R {
        let pages = self.pages.read();
        let tracker = self.tracker.read();
        f(GridstoreView::new(&self.config, &tracker, &pages))
    }

    /// List all files belonging to this storage (tracker, pages, bitmask, config).
    pub fn files(&self) -> Vec<PathBuf> {
        let tracker = self.tracker.read();
        let num_pages = self.pages.read().len();

        let mut paths = Vec::with_capacity(num_pages + 2);
        for tracker_file in tracker.files() {
            paths.push(tracker_file);
        }
        for page_id in 0..num_pages as PageId {
            paths.push(self.page_path(page_id));
        }
        paths.push(self.base_path.join(CONFIG_FILENAME));
        for bitmask_file in self.bitmask.read().files() {
            paths.push(bitmask_file);
        }
        paths
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        vec![self.base_path.join(CONFIG_FILENAME)]
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

        let bitmask = Bitmask::create(&base_path, config.clone())?;

        let storage = Self {
            tracker: Arc::new(RwLock::new(Tracker::new(&base_path, None))),
            pages: Default::default(),
            base_path,
            config,
            _value_type: std::marker::PhantomData,
            bitmask: Arc::new(RwLock::new(bitmask)),
            is_alive_flush_lock: IsAliveLock::new(),
        };

        let new_page_id = storage.next_page_id();
        let path = storage.page_path(new_page_id);
        let page = Page::<MmapUniversal<u8>>::new(&path, storage.config.page_size_bytes)?;
        storage.pages.write().push(page);

        atomic_save_json(&config_path, &storage.config)
            .map_err(|err| GridstoreError::service_error(err.to_string()))?;

        Ok(storage)
    }

    /// Open an existing storage at the given path.
    ///
    /// Uses the bitmask to infer page count for consistency with the write path.
    pub fn open(base_path: PathBuf) -> Result<Self> {
        let (config, tracker) = reader::read_config_and_tracker(&base_path)?;
        let bitmask = Bitmask::open(&base_path, config.clone())?;
        let num_pages = bitmask.infer_num_pages();

        let mut pages = Vec::with_capacity(num_pages);
        for page_id in 0..num_pages as PageId {
            let page_path = base_path.join(format!("page_{page_id}.dat"));
            let page = Page::<MmapUniversal<u8>>::open(&page_path)?;
            pages.push(page);
        }

        Ok(Self {
            config,
            tracker: Arc::new(RwLock::new(tracker)),
            pages: Arc::new(RwLock::new(pages)),
            bitmask: Arc::new(RwLock::new(bitmask)),
            base_path,
            _value_type: std::marker::PhantomData,
            is_alive_flush_lock: IsAliveLock::new(),
        })
    }

    /// Create a new page and return its id.
    #[allow(clippy::needless_pass_by_ref_mut)]
    fn create_new_page(&mut self) -> Result<u32> {
        let new_page_id = self.next_page_id();
        let path = self.page_path(new_page_id);
        let page = Page::<MmapUniversal<u8>>::new(&path, self.config.page_size_bytes)?;
        self.pages.write().push(page);

        self.bitmask.write().cover_new_page()?;

        Ok(new_page_id)
    }

    fn find_or_create_available_blocks(
        &mut self,
        num_blocks: u32,
    ) -> Result<(PageId, BlockOffset)> {
        debug_assert!(num_blocks > 0, "num_blocks must be greater than 0");

        let bitmask_guard = self.bitmask.read();
        if let Some((page_id, block_offset)) = bitmask_guard.find_available_blocks(num_blocks) {
            return Ok((page_id, block_offset));
        }
        let trailing_free_blocks = bitmask_guard.trailing_free_blocks();

        drop(bitmask_guard);

        let missing_blocks = num_blocks.saturating_sub(trailing_free_blocks) as usize;

        let num_pages =
            (missing_blocks * self.config.block_size_bytes).div_ceil(self.config.page_size_bytes);
        for _ in 0..num_pages {
            self.create_new_page()?;
        }

        let available = self
            .bitmask
            .read()
            .find_available_blocks(num_blocks)
            .unwrap();

        Ok(available)
    }

    /// Write value into a new cell, considering that it can span more than one page.
    #[allow(clippy::needless_pass_by_ref_mut)]
    fn write_into_pages(
        &mut self,
        value: &[u8],
        start_page_id: PageId,
        mut block_offset: BlockOffset,
    ) -> Result<()> {
        let value_size = value.len();

        let mut unwritten_tail = value_size;

        let mut pages = self.pages.write();
        for page_id in start_page_id.. {
            let page = &mut pages[page_id as usize];

            let range = (value_size - unwritten_tail)..;
            unwritten_tail =
                page.write_value(block_offset, &value[range], self.config.block_size_bytes)?;
            if unwritten_tail == 0 {
                break;
            }

            block_offset = 0;
        }

        Ok(())
    }

    /// Put a value in the storage.
    ///
    /// Returns true if the value existed previously and was updated, false if it was newly inserted.
    pub fn put_value(
        &mut self,
        point_offset: PointOffset,
        value: &V,
        hw_counter: HwMetricRefCounter,
    ) -> Result<bool> {
        // This function needs to NOT corrupt data in case of a crash.
        //
        // Since we cannot know deterministically when a write is persisted without flushing explicitly,
        // and we don't want to flush on every write, we decided to follow this approach:
        // ┌─────────┐           ┌───────┐┌────┐┌────┐   ┌───────┐                            ┌─────┐
        // │put_value│           │Tracker││Page││Gaps│   │Bitmask│                            │flush│
        // └────┬────┘           └───┬───┘└─┬──┘└─┬──┘   └───┬───┘                            └──┬──┘
        //      │                    │      │     │          │                                   │
        //      │            Find a spot to write │          │                                   │
        //      │───────────────────────────────────────────>│                                   │
        //      │                    │      │     │          │                                   │
        //      │              Page id + offset   │          │                                   │
        //      │<───────────────────────────────────────────│                                   │
        //      │                    │      │     │          │                                   │
        //      │    Write data to page     │     │          │                                   │
        //      │──────────────────────────>│     │          │                                   │
        //      │                    │      │     │          │                                   │
        //      │                Mark as used     │          │                                   │
        //      │───────────────────────────────────────────>│                                   │
        //      │                    │      │     │          │                                   │
        //      │                    │      │     │Update gap│                                   │
        //      │                    │      │     │<─────────│                                   │
        //      │                    │      │     │          │                                   │
        //      │Set pointer (in-ram)│      │     │          │                                   │
        //      │───────────────────>│      │     │          │                                   │
        //      │                    │      │     │          │                                   │
        //      │                    │      │     │          │               flush               │
        //      │                    │      │     │          │<──────────────────────────────────│
        //      │                    │      │     │          │                                   │
        //      │                    │      │     │  flush   │                                   │
        //      │                    │      │     │<─────────│                                   │
        //      │                    │      │     │          │                                   │
        //      │                    │      │     │          │      flush                        │
        //      │                    │      │<───────────────────────────────────────────────────│
        //      │                    │      │     │          │                                   │
        //      │                    │      │     │   Write from memory AND flush                │
        //      │                    │<──────────────────────────────────────────────────────────│
        //      │                    │      │     │          │                                   │
        //      │                    │      │     │          │Mark all old as available and flush│
        //      │                    │      │     │          │<──────────────────────────────────│
        // ┌────┴────┐           ┌───┴───┐┌─┴──┐┌─┴──┐   ┌───┴───┐                            ┌──┴──┐
        // │put_value│           │Tracker││Page││Gaps│   │Bitmask│                            │flush│
        // └─────────┘           └───────┘└────┘└────┘   └───────┘                            └─────┘
        //
        // In case of crashing somewhere in the middle of this operation, the worst
        // that should happen is that we mark more cells as used than they actually are,
        // so will never reuse such space, but data will not be corrupted.

        let value_bytes = value.to_bytes();
        let comp_value = self.with_view(|view| view.compress(value_bytes));
        let value_size = comp_value.len();

        hw_counter.incr_delta(value_size);

        let required_blocks = Self::blocks_for_value(value_size, self.config.block_size_bytes);
        let (start_page_id, block_offset) =
            self.find_or_create_available_blocks(required_blocks)?;

        self.write_into_pages(&comp_value, start_page_id, block_offset)?;

        self.bitmask
            .write()
            .mark_blocks(start_page_id, block_offset, required_blocks, true);

        let mut tracker_guard = self.tracker.write();
        let is_update = tracker_guard.has_pointer(point_offset);
        tracker_guard.set(
            point_offset,
            ValuePointer::new(start_page_id, block_offset, value_size as u32),
        );

        Ok(is_update)
    }

    /// Delete a value from the storage.
    ///
    /// Returns None if the point_offset, page, or value was not found.
    /// Returns the deleted value otherwise.
    pub fn delete_value(&mut self, point_offset: PointOffset) -> Result<Option<V>> {
        let Some(pointer) = self.tracker.write().unset(point_offset) else {
            return Ok(None);
        };

        let ValuePointer {
            page_id,
            block_offset,
            length,
        } = pointer;

        let raw =
            self.with_view(|view| view.read_from_pages::<false>(page_id, block_offset, length))?;
        let decompressed = self.with_view(|view| view.decompress(raw));
        let value = V::from_bytes(&decompressed);

        Ok(Some(value))
    }

    /// Clear the storage, going back to the initial state.
    ///
    /// Completely wipes the storage, and recreates it with a single empty page.
    pub fn clear(&mut self) -> Result<()> {
        let create_options = StorageOptions::from(&self.config);
        let base_path = self.base_path.clone();

        self.is_alive_flush_lock.blocking_mark_dead();

        self.pages.write().clear();
        fs::remove_dir_all(&base_path).map_err(|err| {
            GridstoreError::service_error(format!(
                "Failed to remove gridstore storage directory: {err}"
            ))
        })?;

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

        self.is_alive_flush_lock.blocking_mark_dead();

        drop(self);

        fs::remove_dir_all(base_path).map_err(|err| {
            GridstoreError::service_error(format!(
                "Failed to remove gridstore storage directory: {err}"
            ))
        })
    }

    /// Return the storage size in bytes (precise, based on bitmask occupancy).
    pub fn get_storage_size_bytes(&self) -> usize {
        self.bitmask.read().get_storage_size_bytes()
    }

    pub fn get_value<const READ_SEQUENTIAL: bool>(
        &self,
        point_offset: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Option<V>> {
        self.with_view(|view| view.get_value::<READ_SEQUENTIAL>(point_offset, hw_counter))
    }

    #[cfg(test)]
    pub fn get_pointer(&self, point_offset: PointOffset) -> Option<ValuePointer> {
        self.tracker.read().get(point_offset)
    }

    pub fn max_point_offset(&self) -> PointOffset {
        self.tracker.read().pointer_count()
    }

    pub fn iter<F, E>(
        &self,
        callback: F,
        hw_counter: HwMetricRefCounter,
    ) -> std::result::Result<(), E>
    where
        F: FnMut(PointOffset, V) -> std::result::Result<bool, E>,
        E: From<GridstoreError>,
    {
        self.with_view(|view| view.iter(callback, hw_counter))
    }
}

impl<V> Gridstore<V> {
    fn next_page_id(&self) -> PageId {
        self.pages.read().len() as PageId
    }

    fn page_path(&self, page_id: u32) -> PathBuf {
        self.base_path.join(format!("page_{page_id}.dat"))
    }

    #[inline]
    fn blocks_for_value(value_size: usize, block_size: usize) -> u32 {
        value_size.div_ceil(block_size).try_into().unwrap()
    }

    /// Create flusher that durably persists all pending changes when invoked.
    pub fn flusher(&self) -> Flusher {
        let pending_updates = self.tracker.read().pending_updates.clone();

        let pages = Arc::downgrade(&self.pages);
        let tracker = Arc::downgrade(&self.tracker);
        let bitmask = Arc::downgrade(&self.bitmask);
        let block_size_bytes = self.config.block_size_bytes;

        let is_alive_flush_lock = self.is_alive_flush_lock.handle();

        Box::new(move || {
            let (Some(is_alive_flush_guard), Some(pages), Some(tracker), Some(bitmask)) = (
                is_alive_flush_lock.lock_if_alive(),
                pages.upgrade(),
                tracker.upgrade(),
                bitmask.upgrade(),
            ) else {
                log::trace!("Gridstore was cleared, cancelling flush");
                return Err(GridstoreError::FlushCancelled);
            };

            let bitmask_flusher = bitmask.read().flusher();
            bitmask_flusher()?;

            let page_flushers: Vec<_> = pages.read().iter().map(|p| p.flusher()).collect();
            for flusher in page_flushers {
                flusher()?;
            }

            let old_pointers = Self::flush_tracker(&tracker, pending_updates)?;

            if old_pointers.is_empty() {
                return Ok(());
            }

            Self::flush_free_blocks(&bitmask, old_pointers, block_size_bytes)?;

            drop(is_alive_flush_guard);

            Ok(())
        })
    }

    /// Write pending updates to the tracker and flush it.
    fn flush_tracker(
        tracker: &Arc<RwLock<Tracker>>,
        pending_updates: AHashMap<PointOffset, PointerUpdates>,
    ) -> crate::Result<Vec<ValuePointer>> {
        let (old_pointers, tracker_flusher) = {
            let mut guard = tracker.write();
            let old_pointers = guard.write_pending(pending_updates);
            let flusher = guard.flusher();
            (old_pointers, flusher)
        };
        tracker_flusher()?;
        Ok(old_pointers)
    }

    /// Update all free blocks in the bitmask for old pointers and flush it.
    fn flush_free_blocks(
        bitmask: &Arc<RwLock<Bitmask>>,
        old_pointers: Vec<ValuePointer>,
        block_size_bytes: usize,
    ) -> crate::Result<()> {
        let bitmask_flusher = {
            let mut guard = bitmask.write();
            for (page_id, pointer_group) in
                &old_pointers.into_iter().chunk_by(|pointer| pointer.page_id)
            {
                let local_ranges = pointer_group.map(|pointer| {
                    let start = pointer.block_offset;
                    let end = pointer.block_offset
                        + Self::blocks_for_value(pointer.length as usize, block_size_bytes);
                    start as usize..end as usize
                });
                guard.mark_blocks_batch(page_id, local_ranges, false);
            }
            guard.flusher()
        };
        bitmask_flusher()?;
        Ok(())
    }

    /// Populate all pages, tracker, and bitmask in the mmap.
    pub fn populate(&self) -> Result<()> {
        for page in self.pages.read().iter() {
            page.populate()?;
        }
        self.tracker.read().populate()?;
        self.bitmask.read().populate()?;
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> std::io::Result<()> {
        for page in self.pages.read().iter() {
            page.clear_cache()?;
        }
        self.bitmask.read().clear_cache()?;
        Ok(())
    }
}
