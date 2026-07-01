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
use common::universal_io::{MmapFile, Populate, UniversalReadFileOps, UniversalWrite, UserData};
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
use crate::pages::{Pages, page_path};
use crate::tracker::{BlockOffset, PageId, PointOffset, PointerUpdates, Tracker, ValuePointer};

pub type Flusher = Box<dyn FnOnce() -> std::result::Result<(), GridstoreError> + Send>;

#[derive(Debug)]
enum StorageEngine<S = MmapFile>
where
    S: UniversalWrite + 'static,
{
    Dynamic {
        /// MmapBitmask to represent which "blocks" of data in the pages are used and which are free.
        /// Only used in dynamic storage engine where we want to reuse freed blocks in the middle of
        /// pages.
        ///
        /// 0 is free, 1 is used.
        bitmask: Arc<RwLock<Bitmask<S>>>,
    },
    Serverless {},
}

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
    mode: Mode,
    pub(super) config: StorageConfig,
    pub(super) tracker: Arc<RwLock<Tracker<S>>>,
    pub(super) pages: Arc<RwLock<Pages<S>>>,
    storage_engine: StorageEngine<S>,
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
    fn with_view<R>(&self, f: impl FnOnce(GridstoreView<'_, V, S>) -> R) -> R {
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
        match &self.storage_engine {
            StorageEngine::Dynamic { bitmask } => {
                for bitmask_file in bitmask.read().files() {
                    paths.push(bitmask_file);
                }
            }
            StorageEngine::Serverless {} => {}
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
        mode: Mode,
    ) -> Result<Self> {
        let config_path = base_path.join(CONFIG_FILENAME);
        if config_path.exists() {
            Self::open(fs, base_path, populate, mode)
        } else {
            fs.create_dir(&base_path)?;
            Self::new(fs, base_path, create_options, mode)
        }
    }

    /// Initializes a new storage with a single empty page.
    ///
    /// `base_path` is the directory where the storage files will be stored.
    /// It should exist already.
    pub fn new(fs: S::Fs, base_path: PathBuf, options: StorageOptions, mode: Mode) -> Result<Self> {
        let config = StorageConfig::try_from(options).map_err(GridstoreError::service_error)?;
        let config_path = base_path.join(CONFIG_FILENAME);

        // Serverless storage is append-only and has no bitmask, so don't create
        // its files.
        let storage_engine = match mode {
            Mode::Regular => StorageEngine::Dynamic {
                bitmask: Arc::new(RwLock::new(Bitmask::create(
                    &fs,
                    &base_path,
                    config.clone(),
                )?)),
            },
            Mode::Serverless => StorageEngine::Serverless {},
        };

        let storage = Self {
            mode,
            tracker: Arc::new(RwLock::new(Tracker::new(&fs, &base_path, None)?)),
            pages: Arc::new(RwLock::new(Pages::new(base_path.clone(), true))),
            storage_engine,
            base_path,
            config,
            _value_type: std::marker::PhantomData,
            is_alive_flush_lock: IsAliveLock::new(),
            fs,
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
    /// In dynamic mode the bitmask infers the page count, which is cross-checked
    /// against the page files on disk. Serverless mode has no bitmask and infers
    /// the page count purely from the page files (like [`GridstoreReader`]).
    pub fn open(fs: S::Fs, base_path: PathBuf, populate: Populate, mode: Mode) -> Result<Self> {
        // Writable store: open pages and tracker writable so it can append.
        let (config, tracker) = reader::read_config_and_tracker(&fs, &base_path, true)?;

        let pages = Pages::open(&fs, &base_path, true, populate)?;
        let loaded_pages = pages.num_pages();

        let storage_engine = match mode {
            Mode::Regular => {
                // The bitmask needs one flag per block, `pages * blocks_per_page`
                // bits. Compare that to what it covers: a storage created or
                // extended in serverless mode has no bitmask, or a stale one
                // covering too few blocks. Either way, reconstruct all-used.
                let blocks_per_page = config.page_size_bytes / config.block_size_bytes;
                let expected_blocks = loaded_pages * blocks_per_page;
                let bitmask = if Bitmask::<S>::exists(&base_path) {
                    let bitmask = Bitmask::open(&fs, &base_path, config.clone())?;
                    let covered_blocks = bitmask.covered_blocks();
                    if covered_blocks == expected_blocks {
                        bitmask
                    } else if covered_blocks < expected_blocks {
                        // Missing flags for serverless-appended blocks; rebuild.
                        drop(bitmask);
                        Bitmask::create_all_used(&fs, &base_path, config.clone(), loaded_pages)?
                    } else {
                        // More blocks than pages hold: corruption, not a mode switch.
                        return Err(GridstoreError::service_error(format!(
                            "Inconsistent gridstore blocks at {base_path:?}: bitmask covers {covered_blocks}, but pages hold {expected_blocks}",
                        )));
                    }
                } else {
                    // No bitmask: created in serverless mode.
                    Bitmask::create_all_used(&fs, &base_path, config.clone(), loaded_pages)?
                };
                debug_assert_eq!(bitmask.covered_blocks(), expected_blocks);
                StorageEngine::Dynamic {
                    bitmask: Arc::new(RwLock::new(bitmask)),
                }
            }
            Mode::Serverless => StorageEngine::Serverless {},
        };

        Ok(Self {
            fs,
            mode,
            config,
            tracker: Arc::new(RwLock::new(tracker)),
            pages: Arc::new(RwLock::new(pages)),
            storage_engine,
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

        match &self.storage_engine {
            StorageEngine::Dynamic { bitmask } => {
                bitmask.write().cover_new_page()?;
            }
            StorageEngine::Serverless {} => {}
        }

        Ok(new_page_id)
    }

    fn create_page_file(&self, path: &std::path::Path) -> Result<()> {
        // Serverless pages are created empty and grown as data is appended, so
        // their byte length reflects the appended data, block-aligned (see
        // `write_into_pages` and `next_append_block`).
        // Dynamic pages are pre-sized to the full page so the bitmask can use
        // any block within them.
        let expected_length = if self.mode.is_serverless() {
            0
        } else {
            self.config.page_size_bytes
        };
        self.fs.create(path, expected_length)?;
        Ok(())
    }

    fn find_or_create_available_blocks(
        &mut self,
        num_blocks: u32,
    ) -> Result<(PageId, BlockOffset)> {
        debug_assert!(num_blocks > 0, "num_blocks must be greater than 0");
        debug_assert!(
            !self.mode.is_serverless(),
            "must not use find_or_create_available_blocks in serverless mode",
        );

        let bitmask = match &self.storage_engine {
            StorageEngine::Dynamic { bitmask } => Arc::clone(bitmask),
            StorageEngine::Serverless {} => {
                unreachable!(
                    "find_or_create_available_blocks should not be called in serverless mode"
                );
            }
        };

        let bitmask_guard = bitmask.read();
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

        let available = bitmask
            .read()
            .find_available_blocks(num_blocks)?
            .expect("New page has just been created");

        Ok(available)
    }

    /// Find the next block to write to if we're only appending to storage.
    ///
    /// Append-only storage has no bitmask, so the cursor is derived from the
    /// byte length of the last page: because every value is padded to whole
    /// blocks on write (see [`Self::write_into_pages`]), each page's byte length
    /// is a block multiple and the number of used blocks in the last page is
    /// `last_page_len / block_size` (computed as `ceil` to stay robust). This
    /// holds for both single-page and page-spanning values.
    ///
    /// This only computes the position; it must never create new pages. Use
    /// [`Self::ensure_append_pages`] to materialize the page files the value
    /// needs.
    fn next_append_block(&self, num_blocks: u32) -> Result<(PageId, BlockOffset)> {
        debug_assert!(num_blocks > 0, "num_blocks must be greater than 0");

        let blocks_per_page = (self.config.page_size_bytes / self.config.block_size_bytes) as u64;

        let pages = self.pages.read();
        let num_pages = pages.num_pages();
        debug_assert!(num_pages > 0, "storage always has at least one page");
        let used_in_last = pages
            .last_page_len()?
            .div_ceil(self.config.block_size_bytes as u64);
        drop(pages);

        // Global block index of the append cursor. When the last page is exactly
        // full (used == blocks_per_page) this rolls over to a fresh page start.
        let start_global = (num_pages as u64 - 1) * blocks_per_page + used_in_last;
        let start_page = (start_global / blocks_per_page) as PageId;
        let start_offset = (start_global % blocks_per_page) as BlockOffset;

        Ok((start_page, start_offset))
    }

    /// Ensure the page files backing an append of `num_blocks` blocks starting
    /// at the position returned by [`Self::next_append_block`] exist (are
    /// created empty and attached).
    ///
    /// Serverless storage is append-only and does not pre-size pages: the
    /// page files are created empty here, and the write itself grows each one
    /// by the appended bytes, padded to a whole number of blocks (see
    /// [`Self::write_into_pages`] / [`Pages::write_to_pages_grow`]). This only
    /// materializes the files; it must not size them.
    fn ensure_append_pages(
        &mut self,
        start_page_id: PageId,
        block_offset: BlockOffset,
        num_blocks: u32,
    ) -> Result<()> {
        let blocks_per_page = (self.config.page_size_bytes / self.config.block_size_bytes) as u64;
        let start_global = u64::from(start_page_id) * blocks_per_page + u64::from(block_offset);
        let pages_required =
            (start_global + u64::from(num_blocks)).div_ceil(blocks_per_page) as usize;
        while self.pages.read().num_pages() < pages_required {
            self.create_new_page()?;
        }
        Ok(())
    }

    /// Write a value into a cell, considering that it can span more than one
    /// page.
    ///
    /// In serverless (append-only) mode the write grows each page file as a
    /// single operation, so the pages must not be pre-sized. The appended bytes
    /// are padded with zeroes up to a whole number of blocks, so every value
    /// occupies whole blocks and the next append starts on a block boundary —
    /// matching the block-aligned, pre-sized pages of dynamic mode. In dynamic
    /// mode the pages are already allocated to their full size and are written
    /// in place.
    ///
    /// The padding is physical only: the tracker still stores the value's exact
    /// (unpadded) length, so reads return just the value bytes.
    #[allow(clippy::needless_pass_by_ref_mut)]
    fn write_into_pages(
        &mut self,
        value: &[u8],
        start_page_id: PageId,
        block_offset: BlockOffset,
    ) -> Result<()> {
        let mut pages = self.pages.write();
        if self.mode.is_serverless() {
            // Append-only pages grow per write, so pad with zeroes up to the
            // block boundary to keep every value block-aligned.
            let padded_len = value.len().next_multiple_of(self.config.block_size_bytes);
            let pointer = ValuePointer::new(start_page_id, block_offset, padded_len as u32);
            if padded_len == value.len() {
                pages.write_to_pages_grow(pointer, value, &self.config)
            } else {
                let mut padded = Vec::with_capacity(padded_len);
                padded.extend_from_slice(value);
                padded.resize(padded_len, 0);
                pages.write_to_pages_grow(pointer, &padded, &self.config)
            }
        } else {
            let pointer = ValuePointer::new(start_page_id, block_offset, value.len() as u32);
            pages.write_to_pages(pointer, value, &self.config)
        }
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

        let (start_page_id, block_offset) = if self.mode.is_serverless() {
            // Append-only: derive the position, then ensure the (empty) page
            // files exist. The write itself grows each page by appending. Page
            // creation is intentionally kept out of `next_append_block`.
            let (page_id, block_offset) = self.next_append_block(required_blocks)?;
            self.ensure_append_pages(page_id, block_offset, required_blocks)?;
            (page_id, block_offset)
        } else {
            self.find_or_create_available_blocks(required_blocks)?
        };

        self.write_into_pages(&comp_value, start_page_id, block_offset)?;

        // We only mark used blocks in dynamic mode
        match &self.storage_engine {
            StorageEngine::Dynamic { bitmask } => {
                bitmask
                    .write()
                    .mark_blocks(start_page_id, block_offset, required_blocks, true)?;
            }
            StorageEngine::Serverless {} => {}
        }

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
            self.mode,
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
            mode: _,
            tracker,
            pages,
            storage_engine,
            base_path,
            config: _,
            _value_type,
            is_alive_flush_lock,
        } = self;

        is_alive_flush_lock.blocking_mark_dead();
        drop((tracker, pages, storage_engine));

        fs.remove_dir(&base_path)?;
        Ok(())
    }

    /// Return the storage size in bytes (precise, based on bitmask occupancy).
    pub fn get_storage_size_bytes(&self) -> Result<usize> {
        match &self.storage_engine {
            StorageEngine::Dynamic { bitmask } => bitmask.read().get_storage_size_bytes(),
            StorageEngine::Serverless {} => {
                let mut total = 0;
                for page in &self.pages.read().pages {
                    total += page.len::<u8>()? as usize;
                }
                Ok(total)
            }
        }
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
                max_offset = view.max_point_offset();

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

    /// Test-only access to the dynamic-mode bitmask. Panics in serverless mode,
    /// which has no bitmask.
    #[cfg(test)]
    pub(crate) fn bitmask_for_test(&self) -> &Arc<RwLock<Bitmask<S>>> {
        match &self.storage_engine {
            StorageEngine::Dynamic { bitmask } => bitmask,
            StorageEngine::Serverless {} => panic!("no bitmask in serverless mode"),
        }
    }

    #[inline]
    fn blocks_for_value(value_size: usize, block_size: usize) -> u32 {
        value_size.div_ceil(block_size).try_into().unwrap()
    }

    /// Create flusher that durably persists all pending changes when invoked.
    pub fn flusher(&self) -> Flusher {
        match &self.storage_engine {
            StorageEngine::Dynamic { bitmask } => self.flusher_dynamic(bitmask),
            StorageEngine::Serverless {} => self.flusher_serverless(),
        }
    }

    /// Create flusher that durably persists all pending changes when invoked.
    fn flusher_dynamic(&self, bitmask: &Arc<RwLock<Bitmask<S>>>) -> Flusher {
        debug_assert!(
            !self.mode.is_serverless(),
            "flusher_dynamic must only be called in dynamic mode",
        );

        let pending_updates = self.tracker.read().pending_updates.clone();

        let pages = Arc::downgrade(&self.pages);
        let tracker = Arc::downgrade(&self.tracker);
        let bitmask = Arc::downgrade(bitmask);
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

    /// Create flusher that durably persists all pending changes when invoked.
    fn flusher_serverless(&self) -> Flusher {
        debug_assert!(
            self.mode.is_serverless(),
            "flusher_dynamic may only be called in serverless mode",
        );

        let pending_updates = self.tracker.read().pending_updates.clone();

        let pages = Arc::downgrade(&self.pages);
        let tracker = Arc::downgrade(&self.tracker);
        let _block_size_bytes = self.config.block_size_bytes;

        let is_alive_flush_lock = self.is_alive_flush_lock.handle();

        Box::new(move || {
            let (Some(is_alive_flush_guard), Some(pages), Some(tracker)) = (
                is_alive_flush_lock.lock_if_alive(),
                pages.upgrade(),
                tracker.upgrade(),
            ) else {
                log::trace!("Gridstore was cleared, cancelling flush");
                return Err(GridstoreError::FlushCancelled);
            };

            let pages_flusher = pages.read().flusher();
            pages_flusher()?;

            // In serverless we don't clean up old points
            let _old_pointers = Self::flush_tracker(&tracker, pending_updates)?;

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
        match &self.storage_engine {
            StorageEngine::Dynamic { bitmask } => {
                bitmask.read().populate()?;
            }
            StorageEngine::Serverless {} => {}
        }
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> crate::Result<()> {
        let Self {
            fs: _,
            mode: _,
            config: _,
            tracker: _,
            pages,
            storage_engine,
            base_path: _,
            _value_type,
            is_alive_flush_lock: _,
        } = self;
        pages.read().clear_cache()?;

        match storage_engine {
            StorageEngine::Dynamic { bitmask } => {
                bitmask.read().clear_cache()?;
            }
            StorageEngine::Serverless {} => {}
        }

        Ok(())
    }
}

/// Operating mode
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
pub enum Mode {
    #[default]
    Regular,
    Serverless,
}

impl Mode {
    pub fn is_serverless(self) -> bool {
        match self {
            Self::Regular => false,
            Self::Serverless => true,
        }
    }
}
