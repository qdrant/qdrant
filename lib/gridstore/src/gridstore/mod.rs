mod reader;
pub(crate) mod view;

#[cfg(test)]
mod tests;

use std::cmp;
use std::path::PathBuf;
use std::sync::Arc;

use ahash::AHashMap;
use common::counter::counter_cell::CounterCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::referenced_counter::HwMetricRefCounter;
use common::generic_consts::{AccessPattern, Random, Sequential};
use common::is_alive_lock::IsAliveLock;
use common::universal_io::{MmapFile, Populate, UniversalWrite, UniversalWriteFileOps, UserData};
use itertools::Itertools;
use parking_lot::RwLock;
use reader::CONFIG_FILENAME;
pub use reader::GridstoreReader;
pub use view::GridstoreView;

use crate::Result;
use crate::bitmask::Bitmask;
use crate::blob::Blob;
use crate::config::{Mode, StorageConfig, StorageOptions};
use crate::error::GridstoreError;
use crate::pages::{Pages, page_path};
use crate::tracker::{BlockOffset, PageId, PointOffset, PointerUpdates, Tracker, ValuePointer};

pub type Flusher = Box<dyn FnOnce() -> std::result::Result<(), GridstoreError> + Send>;

/// Read-write storage for values of type `V`.
///
/// Uses `Arc<RwLock<...>>` for pages and tracker to support concurrent flushing.
/// Assumes sequential IDs to the values (0, 1, 2, 3, ...)
#[derive(Debug)]
pub struct Gridstore<V, S = MmapFile>
where
    S: UniversalWrite + 'static,
{
    pub(super) fs: S::Fs,
    pub(super) config: StorageConfig,
    pub(super) tracker: Arc<RwLock<Tracker<S>>>,
    pub(super) pages: Arc<RwLock<Pages<S>>>,
    /// MmapBitmask to represent which "blocks" of data in the pages are used and which are free.
    ///
    /// 0 is free, 1 is used.
    pub(super) bitmask: Arc<RwLock<Bitmask<S>>>,
    pub(super) base_path: PathBuf,
    pub(super) _value_type: std::marker::PhantomData<V>,
    /// Lock to prevent concurrent flushes and used for waiting for ongoing flushes to finish.
    is_alive_flush_lock: IsAliveLock,
}

impl<V, S> Gridstore<V, S>
where
    V: Blob,
    S: UniversalWrite + 'static,
{
    /// Create a [`GridstoreView`] by locking pages and tracker, then call `f` with the view.
    fn with_view<R>(&self, f: impl FnOnce(GridstoreView<'_, V, S, Tracker<S>>) -> R) -> R {
        let pages = self.pages.read();
        let tracker = self.tracker.read();
        f(GridstoreView::new(&self.config, &tracker, &pages))
    }

    /// List all files belonging to this storage (tracker, pages, bitmask, config).
    pub fn files(&self) -> Vec<PathBuf> {
        let tracker = self.tracker.read();
        let pages = self.pages.read();
        let num_pages = pages.num_pages();

        let mut paths = Vec::with_capacity(num_pages + 2);
        for tracker_file in tracker.files() {
            paths.push(tracker_file);
        }
        for page_id in 0..num_pages as PageId {
            paths.push(pages.page_path(page_id));
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
    pub fn open_or_create(
        fs: S::Fs,
        base_path: PathBuf,
        create_options: StorageOptions,
        populate: Populate,
    ) -> Result<Self> {
        let config_path = base_path.join(CONFIG_FILENAME);
        if config_path.exists() {
            Self::open(fs, base_path, populate)
        } else {
            fs.create_dir(&base_path)?;
            Self::new(fs, base_path, create_options)
        }
    }

    /// Initializes a new storage with a single empty page.
    ///
    /// `base_path` is the directory where the storage files will be stored.
    /// It should exist already.
    pub fn new(fs: S::Fs, base_path: PathBuf, options: StorageOptions) -> Result<Self> {
        let config = StorageConfig::try_from(options).map_err(GridstoreError::service_error)?;
        if config.mode == Mode::Serverless {
            // TODO: dispatch to the serverless variant once it is implemented
            return Err(GridstoreError::service_error(format!(
                "Serverless mode is not supported yet: {}",
                base_path.display(),
            )));
        }
        let config_path = base_path.join(CONFIG_FILENAME);

        let bitmask = Bitmask::create(&fs, &base_path, config.clone())?;

        let storage = Self {
            tracker: Arc::new(RwLock::new(Tracker::new(&fs, &base_path, None)?)),
            pages: Arc::new(RwLock::new(Pages::new(base_path.clone(), true))),
            base_path,
            config,
            fs,
            _value_type: std::marker::PhantomData,
            bitmask: Arc::new(RwLock::new(bitmask)),
            is_alive_flush_lock: IsAliveLock::new(),
        };

        let new_page_id = storage.next_page_id();
        let path = page_path(&storage.base_path, new_page_id);
        storage.create_page_file(&path)?;
        storage
            .pages
            .write()
            .attach_page(&storage.fs, &path, Populate::No)?;

        let config_bytes = serde_json::to_vec(&storage.config)?;
        storage.fs.atomic_save(&config_path, &config_bytes)?;

        Ok(storage)
    }

    /// Open an existing storage at the given path.
    ///
    /// Uses the bitmask to infer page count for consistency with the write path.
    pub fn open(fs: S::Fs, base_path: PathBuf, populate: Populate) -> Result<Self> {
        let config = reader::read_config(&fs, &base_path)?;
        if config.mode == Mode::Serverless {
            // TODO: dispatch to the serverless variant once it is implemented
            return Err(GridstoreError::service_error(format!(
                "Serverless mode is not supported yet: {}",
                base_path.display(),
            )));
        }

        // Writable store: open pages and tracker writable so it can append.
        let tracker = Tracker::open(&fs, &base_path, populate, true)?;
        let bitmask = Bitmask::open(&fs, &base_path, config.clone())?;
        let num_pages = bitmask.infer_num_pages();

        let pages = Pages::open(&fs, &base_path, true, populate)?;
        let loaded_pages = pages.num_pages();

        if loaded_pages != num_pages {
            return Err(GridstoreError::service_error(format!(
                "Inconsistent number of gridstore pages at {base_path:?}: expected {num_pages}, but found {loaded_pages}",
            )));
        }

        Ok(Self {
            fs,
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
        let path = page_path(&self.base_path, new_page_id);
        self.create_page_file(&path)?;
        self.pages
            .write()
            .attach_page(&self.fs, &path, Populate::No)?;

        self.bitmask.write().cover_new_page()?;

        Ok(new_page_id)
    }

    fn create_page_file(&self, path: &std::path::Path) -> Result<()> {
        self.fs.create(path, self.config.page_size_bytes)?;
        Ok(())
    }

    fn find_or_create_available_blocks(
        &mut self,
        num_blocks: u32,
    ) -> Result<(PageId, BlockOffset)> {
        debug_assert!(num_blocks > 0, "num_blocks must be greater than 0");

        let bitmask_guard = self.bitmask.read();
        if let Some((page_id, block_offset)) = bitmask_guard.find_available_blocks(num_blocks)? {
            return Ok((page_id, block_offset));
        }
        let trailing_free_blocks = bitmask_guard.trailing_free_blocks()?;

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
            .find_available_blocks(num_blocks)?
            .expect("New page has just been created");

        Ok(available)
    }

    /// Write value into a new cell, considering that it can span more than one page.
    #[allow(clippy::needless_pass_by_ref_mut)]
    fn write_into_pages(
        &mut self,
        value: &[u8],
        start_page_id: PageId,
        block_offset: BlockOffset,
    ) -> Result<()> {
        let pointer = ValuePointer::new(start_page_id, block_offset, value.len() as u32);
        self.pages
            .write()
            .write_to_pages(pointer, value, &self.config)
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
            .mark_blocks(start_page_id, block_offset, required_blocks, true)?;

        let mut tracker_guard = self.tracker.write();
        let is_update = tracker_guard.has_pointer(point_offset)?;
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
        let Some(pointer) = self.tracker.write().unset(point_offset)? else {
            return Ok(None);
        };

        self.with_view(|view| {
            let raw = view.read_from_pages::<Random>(pointer)?;
            let decompressed = view.decompress(raw);
            let value = V::from_bytes(&decompressed);
            Ok(Some(value))
        })
    }

    /// Clear the storage, going back to the initial state.
    ///
    /// Completely wipes the storage, and recreates it with a single empty page.
    pub fn clear(&mut self) -> Result<()> {
        self.is_alive_flush_lock.blocking_mark_dead();
        self.pages.write().clear();

        self.fs.remove_dir(&self.base_path)?;
        self.fs.create_dir(&self.base_path)?;

        *self = Self::new(
            self.fs.clone(),
            self.base_path.clone(),
            StorageOptions::from(&self.config),
        )?;

        Ok(())
    }

    /// Wipe the storage, drop all pages and delete the base directory.
    ///
    /// Takes ownership because this function leaves Gridstore in an inconsistent state which does
    /// not allow further usage. Use [`clear`](Self::clear) instead to clear and reuse the storage.
    pub fn wipe(self) -> Result<()> {
        let Self {
            fs,
            tracker,
            pages,
            bitmask,
            base_path,
            config: _,
            _value_type,
            is_alive_flush_lock,
        } = self;

        is_alive_flush_lock.blocking_mark_dead();
        drop((tracker, pages, bitmask));

        fs.remove_dir(&base_path)?;
        Ok(())
    }

    /// Return the storage size in bytes (precise, based on bitmask occupancy).
    pub fn get_storage_size_bytes(&self) -> Result<usize> {
        self.bitmask.read().get_storage_size_bytes()
    }

    pub fn get_value<P: AccessPattern>(
        &self,
        point_offset: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Option<V>> {
        self.with_view(|view| view.get_value::<P>(point_offset, hw_counter))
    }

    /// Iterate over all given values and execute callback for each one.
    ///
    /// Return `false` from the callback to stop iteration early.
    pub fn read_values<P, U, E>(
        &self,
        point_offsets: impl Iterator<Item = (U, PointOffset)>,
        mut callback: impl FnMut(U, PointOffset, Option<V>) -> Result<(), E>,
        hw_counter_cell: &CounterCell,
    ) -> Result<(), E>
    where
        P: AccessPattern,
        U: UserData,
        E: From<GridstoreError>,
    {
        self.with_view(|view| {
            view.read_values::<P, _, _>(
                point_offsets,
                move |user_data, point_offset, value| -> Result<_, E> {
                    callback(user_data, point_offset, value)?;
                    Ok(true)
                },
                hw_counter_cell,
            )
        })?;

        Ok(())
    }

    #[cfg(test)]
    pub fn get_pointer(&self, point_offset: PointOffset) -> Option<ValuePointer> {
        self.tracker.read().get(point_offset).ok().flatten()
    }

    pub fn max_point_offset(&self) -> PointOffset {
        self.tracker.read().pointer_count()
    }

    /// Iterate over all values and execute callback for each one. Missing values are skipped.
    ///
    /// Return `false` from the callback to stop iteration early.
    pub fn iter<F, E>(&self, mut callback: F, hw_counter: HwMetricRefCounter) -> Result<(), E>
    where
        F: FnMut(PointOffset, V) -> Result<bool, E>,
        E: From<GridstoreError>,
    {
        let mut current_offset = 0;
        let mut max_offset = PointOffset::MAX;

        let mut should_continue = true;

        while current_offset < max_offset && should_continue {
            // Iterate in batches to allow releasing read locks
            //
            // See:
            // - https://github.com/qdrant/qdrant/pull/7983
            // - https://github.com/qdrant/qdrant/pull/8248
            self.with_view(|view| -> Result<_, E> {
                max_offset = view.max_point_offset()?;

                if current_offset >= max_offset {
                    return Ok(());
                }

                const BATCH_SIZE: u32 = 256;
                let end_offset = current_offset.saturating_add(BATCH_SIZE);
                let end_offset = cmp::min(end_offset, max_offset);

                let point_offsets =
                    (current_offset..end_offset).map(|point_offset| ((), point_offset));

                should_continue = view.read_values::<Sequential, _, _>(
                    point_offsets,
                    |_, point_offset, value| -> Result<_, E> {
                        let Some(value) = value else {
                            return Ok(true);
                        };

                        callback(point_offset, value)
                    },
                    &hw_counter,
                )?;

                if should_continue {
                    current_offset = end_offset;
                }

                Ok(())
            })?;
        }

        Ok(())
    }
}

impl<V, S: UniversalWrite + 'static> Gridstore<V, S> {
    fn next_page_id(&self) -> PageId {
        self.pages.read().num_pages() as PageId
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

            let pages_flusher = pages.read().flusher();
            pages_flusher()?;

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
        tracker: &Arc<RwLock<Tracker<S>>>,
        pending_updates: AHashMap<PointOffset, PointerUpdates>,
    ) -> crate::Result<Vec<ValuePointer>> {
        let (old_pointers, tracker_flusher) = {
            let mut guard = tracker.write();
            let old_pointers = guard.write_pending(pending_updates)?;
            let flusher = guard.flusher();
            (old_pointers, flusher)
        };
        tracker_flusher()?;
        Ok(old_pointers)
    }

    /// Update all free blocks in the bitmask for old pointers and flush it.
    fn flush_free_blocks(
        bitmask: &Arc<RwLock<Bitmask<S>>>,
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
                guard.mark_blocks_batch(page_id, local_ranges, false)?;
            }
            guard.flusher()
        };
        bitmask_flusher()?;
        Ok(())
    }

    /// Populate all pages, tracker, and bitmask in the mmap.
    pub fn populate(&self) -> Result<()> {
        self.pages.read().populate()?;
        self.tracker.read().populate()?;
        self.bitmask.read().populate()?;
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> crate::Result<()> {
        let Self {
            fs: _,
            config: _,
            tracker: _,
            pages,
            bitmask,
            base_path: _,
            _value_type,
            is_alive_flush_lock: _,
        } = self;
        pages.read().clear_cache()?;
        bitmask.read().clear_cache()?;
        Ok(())
    }
}
