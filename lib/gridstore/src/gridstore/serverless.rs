use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::counter::counter_cell::CounterCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::referenced_counter::HwMetricRefCounter;
use common::generic_consts::AccessPattern;
use common::is_alive_lock::IsAliveLock;
use common::universal_io::{UniversalWrite, UserData};
use fs_err as fs;
use fs_err::File;
use parking_lot::RwLock;

use super::Flusher;
use super::reader::CONFIG_FILENAME;
use super::view::{compress_lz4, decompress_lz4};
use crate::blob::Blob;
use crate::config::{Compression, StorageConfig, StorageOptions};
use crate::error::GridstoreError;
use crate::tracker::serverless::ServerlessTracker;
use crate::tracker::{BlockOffset, PointOffset, ValuePointer};
use crate::{Result, direct_io};

/// File name of the serverless page file
///
/// Deliberately different from the dynamic page file names (`page_{id}.dat`), so that one mode
/// never attempts to load the incompatible file format of the other. Keeps a page number for
/// forward compatibility, even though the serverless mode always uses a single page for now.
const PAGE_FILE_NAME: &str = "serverless_page_0.dat";

/// Append-only page of value data for the serverless storage mode.
///
/// A single file holding the raw (compressed) value bytes. Every value starts at a block aligned
/// offset. The file starts empty and only ever grows by appending; existing bytes are never
/// rewritten. The file length always matches the end of the last appended value, there is no
/// preallocation and no trailing padding.
///
/// The file is read and written directly, it is never memory mapped.
#[derive(Debug)]
struct ServerlessPage {
    /// Path to the page file
    path: PathBuf,
    /// Open handle to the page file
    file: File,
    /// Length of the page file in bytes, tracked in memory
    len: u64,
}

impl ServerlessPage {
    fn page_file_name(dir: &Path) -> PathBuf {
        dir.join(PAGE_FILE_NAME)
    }

    /// Create a new empty page in the given directory.
    ///
    /// The directory must exist already.
    fn new(dir: &Path) -> Result<Self> {
        let path = Self::page_file_name(dir);
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        Ok(Self { path, file, len: 0 })
    }

    /// Open an existing page in the given directory.
    ///
    /// If the file does not exist, return an error.
    fn open(dir: &Path, writeable: bool) -> Result<Self> {
        let path = Self::page_file_name(dir);
        let file = fs::OpenOptions::new()
            .read(true)
            .write(writeable)
            .open(&path)
            .map_err(|err| {
                if err.kind() == std::io::ErrorKind::NotFound {
                    // If config exists and the page doesn't,
                    // it should be treated as inconsistent storage rather than a missing one
                    GridstoreError::service_error(format!(
                        "Serverless page file does not exist: {}",
                        path.display(),
                    ))
                } else {
                    GridstoreError::from(err)
                }
            })?;
        let len = file.metadata()?.len();
        Ok(Self { path, file, len })
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    /// Length of the page file in bytes, which is the end of the last appended value.
    fn len(&self) -> u64 {
        self.len
    }

    /// Append a value at the next block aligned offset, returning the block offset it landed at.
    ///
    /// The write starts exactly at the current end of the file and includes the zero padding up
    /// to the next block boundary, so that it is a pure append.
    fn append_value(&mut self, value: &[u8], block_size_bytes: u64) -> Result<BlockOffset> {
        let start = self.len.next_multiple_of(block_size_bytes);
        let pad = (start - self.len) as usize;

        if pad == 0 {
            direct_io::write_all_at(&self.file, value, self.len)?;
        } else {
            // Prefix the write with the padding, so that it lands at the end of the file
            let mut buf = vec![0; pad + value.len()];
            buf[pad..].copy_from_slice(value);
            direct_io::write_all_at(&self.file, &buf, self.len)?;
        }
        self.len = start + value.len() as u64;

        BlockOffset::try_from(start / block_size_bytes).map_err(|_| {
            GridstoreError::service_error(format!(
                "serverless page file {} exceeds the maximum addressable size",
                self.path.display(),
            ))
        })
    }

    /// Read the raw value bytes at the given pointer.
    fn read_value(&self, pointer: ValuePointer, block_size_bytes: u64) -> Result<Vec<u8>> {
        let start = u64::from(pointer.block_offset) * block_size_bytes;
        let mut buf = vec![0; pointer.length as usize];
        direct_io::read_exact_at(&self.file, &mut buf, start)?;
        Ok(buf)
    }

    /// Reload the length from the file, making newly appended value data visible in the reported
    /// storage size.
    ///
    /// Reads themselves always go directly to the file and need no reload.
    fn refresh_len(&mut self) -> Result<()> {
        self.len = self.file.metadata()?.len();
        Ok(())
    }

    /// Create a closure that syncs all written value data in the page file to disk.
    fn flusher(&self) -> Result<Flusher> {
        let file = self.file.try_clone()?;
        Ok(Box::new(move || {
            file.sync_data()?;
            Ok(())
        }))
    }
}

fn compress(compression: Compression, value: Vec<u8>) -> Vec<u8> {
    match compression {
        Compression::None => value,
        Compression::LZ4 => compress_lz4(&value),
    }
}

fn decompress(compression: Compression, value: Vec<u8>) -> Vec<u8> {
    match compression {
        Compression::None => value,
        Compression::LZ4 => decompress_lz4(&value),
    }
}

/// A non-owning view into gridstore data in serverless mode.
///
/// Holds borrowed references to the tracker and page, and contains all reading logic.
///
/// The serverless mode does not use the universal io backend `S`, it reads files directly. The
/// parameter is kept so this view fits in the generic [`super::GridstoreView`].
pub(super) struct ServerlessGridstoreView<'a, V, S> {
    config: &'a StorageConfig,
    tracker: &'a ServerlessTracker,
    page: &'a ServerlessPage,
    _phantom: PhantomData<(V, S)>,
}

impl<'a, V, S> ServerlessGridstoreView<'a, V, S> {
    fn new(
        config: &'a StorageConfig,
        tracker: &'a ServerlessTracker,
        page: &'a ServerlessPage,
    ) -> Self {
        Self {
            config,
            tracker,
            page,
            _phantom: PhantomData,
        }
    }

    pub(super) fn max_point_offset(&self) -> PointOffset {
        self.tracker.pointer_count()
    }

    /// Return the storage size in bytes (precise, the exact amount of appended value data).
    pub(super) fn get_storage_size_bytes(&self) -> usize {
        self.page.len() as usize
    }

    /// Read the raw value bytes at the given pointer.
    pub(super) fn read_from_page(&self, pointer: ValuePointer) -> Result<Vec<u8>> {
        self.page
            .read_value(pointer, self.config.block_size_bytes as u64)
    }
}

impl<'a, V: Blob, S> ServerlessGridstoreView<'a, V, S> {
    /// Get the value for a given point offset.
    ///
    /// The access pattern `P` is ignored, the serverless mode always reads the file directly.
    #[allow(clippy::extra_unused_type_parameters)]
    pub(super) fn get_value<P: AccessPattern>(
        &self,
        point_offset: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Option<V>> {
        let Some(pointer) = self.tracker.get(point_offset)? else {
            return Ok(None);
        };

        let raw = self.read_from_page(pointer)?;
        hw_counter.payload_io_read_counter().incr_delta(raw.len());

        let decompressed = decompress(self.config.compression, raw);
        Ok(Some(V::from_bytes(&decompressed)))
    }

    /// Iterate over all given values and execute callback for each one.
    ///
    /// Return `false` from the callback to stop iteration early.
    ///
    /// The access pattern `P` is ignored, the serverless mode always reads the file directly.
    #[allow(clippy::extra_unused_type_parameters)]
    pub(super) fn read_values<P, U, E>(
        &self,
        point_offsets: impl Iterator<Item = (U, PointOffset)>,
        mut callback: impl FnMut(U, PointOffset, Option<V>) -> Result<bool, E>,
        hw_counter_cell: &CounterCell,
    ) -> Result<bool, E>
    where
        P: AccessPattern,
        U: UserData,
        E: From<GridstoreError>,
    {
        for (user_data, point_offset) in point_offsets {
            let value = match self.tracker.get(point_offset).map_err(E::from)? {
                None => None,
                Some(pointer) => {
                    let raw = self.read_from_page(pointer).map_err(E::from)?;
                    hw_counter_cell.incr_delta(raw.len());

                    let decompressed = decompress(self.config.compression, raw);
                    Some(V::from_bytes(&decompressed))
                }
            };

            if !callback(user_data, point_offset, value)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Iterate over a contiguous range of point offsets and execute callback for each existing
    /// value. Missing values are skipped.
    ///
    /// The mappings for the whole range are fetched with a single batched read.
    ///
    /// Return `false` from the callback to stop iteration early. Returns whether iteration should
    /// continue.
    pub(super) fn iter_range<F, E>(
        &self,
        point_offsets: std::ops::Range<PointOffset>,
        mut callback: F,
        hw_counter: HwMetricRefCounter,
    ) -> Result<bool, E>
    where
        F: FnMut(PointOffset, V) -> Result<bool, E>,
        E: From<GridstoreError>,
    {
        let start = point_offsets.start;
        let pointers = self.tracker.get_range(point_offsets).map_err(E::from)?;

        for (index, pointer) in pointers.into_iter().enumerate() {
            let Some(pointer) = pointer else {
                continue;
            };

            let raw = self.read_from_page(pointer).map_err(E::from)?;
            hw_counter.incr_delta(raw.len());

            let decompressed = decompress(self.config.compression, raw);
            let value = V::from_bytes(&decompressed);

            if !callback(start + index as PointOffset, value)? {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

/// Read-write storage for values of type `V`, operating in serverless mode.
///
/// Append-only variant for serverless deployments, which restrict IO to appending to files:
/// existing bytes can never be rewritten. To use as few files as possible, all value data is
/// stored in a single page file, next to a single tracker file and the storage config.
///
/// Values cannot be updated or deleted, and must be put at monotonically increasing point
/// offsets. All files are read and written directly, they are never memory mapped.
///
/// Uses `Arc<RwLock<...>>` for the page and tracker to support concurrent flushing.
#[derive(Debug)]
pub(super) struct ServerlessGridstore<V, S>
where
    S: UniversalWrite + 'static,
{
    pub(super) config: StorageConfig,
    tracker: Arc<RwLock<ServerlessTracker>>,
    page: Arc<RwLock<ServerlessPage>>,
    base_path: PathBuf,
    /// Lock to prevent concurrent flushes and used for waiting for ongoing flushes to finish.
    is_alive_flush_lock: IsAliveLock,
    /// The serverless mode does not use the universal io backend `S`, it reads and writes files
    /// directly. The parameter is kept so this variant fits in the generic [`super::Gridstore`].
    _phantom: PhantomData<(V, S)>,
}

impl<V, S> ServerlessGridstore<V, S>
where
    V: Blob,
    S: UniversalWrite + 'static,
{
    /// List all files belonging to this storage (tracker, page, config).
    pub(super) fn files(&self) -> Vec<PathBuf> {
        let mut paths = self.tracker.read().files();
        paths.extend(self.page.read().files());
        paths.push(self.base_path.join(CONFIG_FILENAME));
        paths
    }

    pub(super) fn immutable_files(&self) -> Vec<PathBuf> {
        vec![self.base_path.join(CONFIG_FILENAME)]
    }

    /// Initializes a new storage with an empty tracker and page.
    ///
    /// `base_path` is the directory where the storage files will be stored.
    /// It should exist already.
    pub(super) fn new(base_path: PathBuf, options: StorageOptions) -> Result<Self> {
        let config = StorageConfig::try_from(options).map_err(GridstoreError::service_error)?;

        let tracker = ServerlessTracker::new(&base_path)?;
        let page = ServerlessPage::new(&base_path)?;

        let config_path = base_path.join(CONFIG_FILENAME);
        common::fs::atomic_save_json(&config_path, &config)?;

        Ok(Self {
            config,
            tracker: Arc::new(RwLock::new(tracker)),
            page: Arc::new(RwLock::new(page)),
            base_path,
            is_alive_flush_lock: IsAliveLock::new(),
            _phantom: PhantomData,
        })
    }

    /// Open an existing storage at the given path, with the already read config.
    pub(super) fn open(base_path: PathBuf, config: StorageConfig) -> Result<Self> {
        let tracker = ServerlessTracker::open(&base_path, true)?;
        let page = ServerlessPage::open(&base_path, true)?;

        Ok(Self {
            config,
            tracker: Arc::new(RwLock::new(tracker)),
            page: Arc::new(RwLock::new(page)),
            base_path,
            is_alive_flush_lock: IsAliveLock::new(),
            _phantom: PhantomData,
        })
    }

    /// Create a [`ServerlessGridstoreView`] by locking tracker and page, then call `f` with the
    /// view.
    pub(super) fn with_view<R>(&self, f: impl FnOnce(ServerlessGridstoreView<'_, V, S>) -> R) -> R {
        let tracker = self.tracker.read();
        let page = self.page.read();
        f(ServerlessGridstoreView::new(&self.config, &tracker, &page))
    }

    /// Put a value in the storage.
    ///
    /// The value data is appended to the page file right away, its mapping is buffered in memory
    /// until the next flush.
    ///
    /// Values must be put at monotonically increasing point offsets: each offset must be larger
    /// than every offset put before it. Putting a value twice or at an old point offset is
    /// rejected, the storage is append-only.
    ///
    /// Always returns false on success, as values can never be updated.
    // Takes &mut self for signature parity with the dynamic variant
    #[allow(clippy::needless_pass_by_ref_mut)]
    pub(super) fn put_value(
        &mut self,
        point_offset: PointOffset,
        value: &V,
        hw_counter: HwMetricRefCounter,
    ) -> Result<bool> {
        // Validate before writing anything, a rejected put must not leave data behind
        let next = self.tracker.read().pointer_count();
        if point_offset < next {
            return Err(GridstoreError::validation_error(format!(
                "cannot put value at point offset {point_offset}: the serverless gridstore is \
                 append-only, values cannot be overwritten and must be put at monotonically \
                 increasing point offsets, the next allowed point offset is {next}",
            )));
        }

        let value_bytes = value.to_bytes();
        let comp_value = compress(self.config.compression, value_bytes);
        let value_size = comp_value.len();

        hw_counter.incr_delta(value_size);

        let value_size = u32::try_from(value_size)
            .map_err(|_| GridstoreError::service_error("value is too large"))?;

        let block_size_bytes = self.config.block_size_bytes as u64;
        let block_offset = self
            .page
            .write()
            .append_value(&comp_value, block_size_bytes)?;

        self.tracker
            .write()
            .set(point_offset, ValuePointer::new(0, block_offset, value_size))?;

        Ok(false)
    }

    /// Deleting values is not supported in serverless mode.
    // Takes &mut self for signature parity with the dynamic variant
    #[allow(clippy::unused_self, clippy::needless_pass_by_ref_mut)]
    pub(super) fn delete_value(&mut self, _point_offset: PointOffset) -> Result<Option<V>> {
        Err(GridstoreError::unsupported_operation("deleting values"))
    }

    /// Clear the storage, going back to the initial state.
    ///
    /// Completely wipes the storage, and recreates it in serverless mode.
    pub(super) fn clear(&mut self) -> Result<()> {
        self.is_alive_flush_lock.blocking_mark_dead();

        fs::remove_dir_all(&self.base_path)?;
        fs::create_dir_all(&self.base_path)?;

        *self = Self::new(self.base_path.clone(), StorageOptions::from(&self.config))?;

        Ok(())
    }

    /// Wipe the storage, drop the tracker and page and delete the base directory.
    ///
    /// Takes ownership because this function leaves the storage in an inconsistent state which
    /// does not allow further usage. Use [`clear`](Self::clear) instead to clear and reuse the
    /// storage.
    pub(super) fn wipe(self) -> Result<()> {
        let Self {
            config: _,
            tracker,
            page,
            base_path,
            is_alive_flush_lock,
            _phantom,
        } = self;

        is_alive_flush_lock.blocking_mark_dead();
        drop((tracker, page));

        fs::remove_dir_all(&base_path)?;
        Ok(())
    }

    /// Return the storage size in bytes (precise, the exact amount of appended value data).
    // Wrapped in Result for signature parity with the dynamic variant
    #[allow(clippy::unnecessary_wraps)]
    pub(super) fn get_storage_size_bytes(&self) -> Result<usize> {
        Ok(self.with_view(|view| view.get_storage_size_bytes()))
    }

    /// Get the value for a given point offset.
    pub(super) fn get_value<P: AccessPattern>(
        &self,
        point_offset: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Option<V>> {
        self.with_view(|view| view.get_value::<P>(point_offset, hw_counter))
    }

    /// Iterate over all given values and execute callback for each one.
    pub(super) fn read_values<P, U, E>(
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
    pub(super) fn get_pointer(&self, point_offset: PointOffset) -> Option<ValuePointer> {
        self.tracker.read().get(point_offset).ok().flatten()
    }

    pub(super) fn max_point_offset(&self) -> PointOffset {
        self.tracker.read().pointer_count()
    }

    /// Iterate over all values and execute callback for each one. Missing values are skipped.
    ///
    /// Return `false` from the callback to stop iteration early.
    pub(super) fn iter<F, E>(
        &self,
        mut callback: F,
        hw_counter: HwMetricRefCounter,
    ) -> Result<(), E>
    where
        F: FnMut(PointOffset, V) -> Result<bool, E>,
        E: From<GridstoreError>,
    {
        let mut current_offset = 0;
        let mut max_offset = PointOffset::MAX;

        let mut should_continue = true;

        while current_offset < max_offset && should_continue {
            // Iterate in batches to allow releasing read locks
            const BATCH_SIZE: PointOffset = 256;

            self.with_view(|view| -> Result<_, E> {
                max_offset = view.max_point_offset();

                if current_offset >= max_offset {
                    return Ok(());
                }

                let end_offset = current_offset.saturating_add(BATCH_SIZE).min(max_offset);

                should_continue =
                    view.iter_range(current_offset..end_offset, &mut callback, hw_counter)?;

                if should_continue {
                    current_offset = end_offset;
                }

                Ok(())
            })?;
        }

        Ok(())
    }
}

impl<V, S: UniversalWrite + 'static> ServerlessGridstore<V, S> {
    /// Create flusher that durably persists all pending changes when invoked.
    ///
    /// Syncs the page file first, then appends all pending mappings to the tracker file with a
    /// single write and syncs it. This order guarantees that a mapping on disk never points at
    /// value data that is not durable yet.
    pub(super) fn flusher(&self) -> Flusher {
        // Only mappings up to this point are persisted, mappings put during the flush stay
        // pending for the next flush
        let target = self.tracker.read().pointer_count();

        let tracker = Arc::downgrade(&self.tracker);
        let page = Arc::downgrade(&self.page);
        let is_alive_flush_lock = self.is_alive_flush_lock.handle();

        Box::new(move || {
            let (Some(is_alive_flush_guard), Some(tracker), Some(page)) = (
                is_alive_flush_lock.lock_if_alive(),
                tracker.upgrade(),
                page.upgrade(),
            ) else {
                log::trace!("Gridstore was cleared, cancelling flush");
                return Err(GridstoreError::FlushCancelled);
            };

            let page_flusher = page.read().flusher()?;
            page_flusher()?;

            let tracker_flusher = {
                let mut tracker_guard = tracker.write();
                tracker_guard.write_pending(target)?;
                tracker_guard.flusher()?
            };
            tracker_flusher()?;

            drop(is_alive_flush_guard);

            Ok(())
        })
    }

    /// Populating is a no-op in serverless mode.
    ///
    /// Files are read directly without memory mapping, the OS page cache manages caching.
    // Signature parity with the dynamic variant
    #[allow(clippy::unused_self, clippy::unnecessary_wraps)]
    pub(super) fn populate(&self) -> Result<()> {
        Ok(())
    }

    /// Dropping disk cache is a no-op in serverless mode.
    ///
    /// Files are read directly without memory mapping, the OS page cache manages caching.
    // Signature parity with the dynamic variant
    #[allow(clippy::unused_self, clippy::unnecessary_wraps)]
    pub(super) fn clear_cache(&self) -> crate::Result<()> {
        Ok(())
    }
}

/// Read-only storage for values of type `V`, operating in serverless mode.
///
/// Holds the tracker and page directly (no locks) since it provides only read access.
/// For read-write access, use [`ServerlessGridstore`].
///
/// The serverless mode does not use the universal io backend `S`, it reads files directly. The
/// parameter is kept so this reader fits in the generic [`super::GridstoreReader`].
#[derive(Debug)]
pub(super) struct ServerlessGridstoreReader<V, S> {
    config: StorageConfig,
    tracker: ServerlessTracker,
    page: ServerlessPage,
    base_path: PathBuf,
    _phantom: PhantomData<(V, S)>,
}

impl<V: Blob, S> ServerlessGridstoreReader<V, S> {
    /// Open an existing read-only storage at the given path, with the already read config.
    pub(super) fn open(base_path: PathBuf, config: StorageConfig) -> Result<Self> {
        let tracker = ServerlessTracker::open(&base_path, false)?;
        let page = ServerlessPage::open(&base_path, false)?;

        Ok(Self {
            config,
            tracker,
            page,
            base_path,
            _phantom: PhantomData,
        })
    }

    /// Create a [`ServerlessGridstoreView`] borrowing this reader's data.
    pub(super) fn view(&self) -> ServerlessGridstoreView<'_, V, S> {
        ServerlessGridstoreView::new(&self.config, &self.tracker, &self.page)
    }

    /// List all files belonging to this reader (tracker, page, config).
    pub(super) fn files(&self) -> Vec<PathBuf> {
        let mut paths = self.tracker.files();
        paths.extend(self.page.files());
        paths.push(self.base_path.join(CONFIG_FILENAME));
        paths
    }

    pub(super) fn max_point_offset(&self) -> PointOffset {
        self.tracker.pointer_count()
    }

    pub(super) fn get_value<P: AccessPattern>(
        &self,
        point_offset: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Option<V>> {
        self.view().get_value::<P>(point_offset, hw_counter)
    }

    /// Iterate over all values with point offsets below `max_id` and execute callback for each
    /// one. Missing values are skipped.
    ///
    /// Return `false` from the callback to stop iteration early.
    pub(super) fn iter<F, E>(
        &self,
        max_id: PointOffset,
        callback: F,
        hw_counter: HwMetricRefCounter,
    ) -> Result<(), E>
    where
        F: FnMut(PointOffset, V) -> Result<bool, E>,
        E: From<GridstoreError>,
    {
        let max_id = max_id.min(self.max_point_offset());
        self.view().iter_range(0..max_id, callback, hw_counter)?;
        Ok(())
    }

    pub(super) fn read_values<P, U, E>(
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
        self.view().read_values::<P, _, _>(
            point_offsets,
            move |user_data, point_offset, value| -> Result<_, E> {
                callback(user_data, point_offset, value)?;
                Ok(true)
            },
            hw_counter_cell,
        )?;

        Ok(())
    }

    /// Return the storage size in bytes (precise, the exact amount of appended value data).
    pub(super) fn get_storage_size_bytes(&self) -> usize {
        self.view().get_storage_size_bytes()
    }

    /// This method reloads the storage from "disk", so that it makes newly appended data
    /// readable.
    ///
    /// Important assumptions:
    ///
    /// - Data is append-only, existing mappings and value data never change.
    /// - Partial writes are possible, but ignored: a trailing partial tracker entry is not
    ///   counted.
    pub(super) fn live_reload(&mut self) -> Result<()> {
        let has_new_data = self.tracker.live_reload()?;

        if !has_new_data {
            return Ok(());
        }

        // Value reads always go directly to the file; refreshing the page length only updates
        // the reported storage size
        self.page.refresh_len()?;

        Ok(())
    }
}

impl<V, S> ServerlessGridstoreReader<V, S> {
    /// Returns `true`: serverless storage always reads from disk, it is never memory mapped or
    /// populated into RAM.
    #[allow(clippy::unused_self)]
    pub(super) fn is_on_disk(&self) -> bool {
        true
    }

    /// Dropping disk cache is a no-op in serverless mode.
    ///
    /// Files are read directly without memory mapping, the OS page cache manages caching.
    // Signature parity with the dynamic variant
    #[allow(clippy::unused_self, clippy::unnecessary_wraps)]
    pub(super) fn clear_cache(&self) -> crate::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::generic_consts::Random;
    use common::universal_io::{MmapFile, MmapFs, Populate};
    use tempfile::TempDir;

    use super::*;
    use crate::config::Mode;
    use crate::fixtures::{Payload, empty_storage_serverless, random_payload};
    use crate::{Gridstore, GridstoreReader};

    /// Size in bytes of a single mapping entry in the tracker file
    const TRACKER_ENTRY_SIZE: u64 = 16;

    /// Create an empty serverless storage of raw byte values, for precise size assertions.
    fn empty_byte_storage(compression: Compression) -> (TempDir, Gridstore<Vec<u8>>) {
        let dir = TempDir::new().unwrap();
        let options = StorageOptions {
            compression: Some(compression),
            mode: Some(Mode::Serverless),
            ..Default::default()
        };
        let storage = Gridstore::new(MmapFs, dir.path().to_path_buf(), options).unwrap();
        (dir, storage)
    }

    fn tracker_file_len(dir: &TempDir) -> u64 {
        fs::metadata(dir.path().join("serverless_tracker.dat"))
            .unwrap()
            .len()
    }

    #[test]
    fn test_empty_storage() {
        let (dir, storage) = empty_storage_serverless();

        let hw_counter = HardwareCounterCell::new();
        assert_eq!(storage.get_value::<Random>(0, &hw_counter).unwrap(), None);
        assert_eq!(storage.max_point_offset(), 0);
        assert_eq!(storage.get_storage_size_bytes().unwrap(), 0);

        // Only three files: tracker, page and config
        let files = storage.files();
        assert_eq!(files.len(), 3, "Expected 3 files, got {files:?}");
        assert_eq!(files[0].file_name().unwrap(), "serverless_tracker.dat");
        assert_eq!(files[1].file_name().unwrap(), "serverless_page_0.dat");
        assert_eq!(files[2].file_name().unwrap(), "config.json");
        let actual_files = fs::read_dir(dir.path()).unwrap().count();
        assert_eq!(files.len(), actual_files);

        let immutable_files = storage.immutable_files();
        assert_eq!(immutable_files.len(), 1);
        assert_eq!(immutable_files[0].file_name().unwrap(), "config.json");

        // Both files start empty, they are not preallocated
        assert_eq!(tracker_file_len(&dir), 0);
        assert_eq!(
            fs::metadata(dir.path().join("serverless_page_0.dat"))
                .unwrap()
                .len(),
            0,
        );
    }

    #[rstest::rstest]
    #[case(Compression::None)]
    #[case(Compression::LZ4)]
    fn test_put_get_roundtrip(#[case] compression: Compression) {
        let dir = TempDir::new().unwrap();
        let options = StorageOptions {
            compression: Some(compression),
            mode: Some(Mode::Serverless),
            ..Default::default()
        };
        let mut storage =
            Gridstore::<Payload>::new(MmapFs, dir.path().to_path_buf(), options).unwrap();

        let rng = &mut rand::make_rng::<rand::rngs::SmallRng>();
        let payloads = (0..100)
            .map(|point_offset| (point_offset, random_payload(rng, 2)))
            .collect::<Vec<_>>();

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
        for (point_offset, payload) in &payloads {
            let is_update = storage
                .put_value(*point_offset, payload, hw_counter_ref)
                .unwrap();
            assert!(!is_update);
        }
        assert!(hw_counter.payload_io_write_counter().get() > 0);
        assert_eq!(storage.max_point_offset(), 100);

        for (point_offset, payload) in &payloads {
            let stored = storage
                .get_value::<Random>(*point_offset, &hw_counter)
                .unwrap();
            assert_eq!(stored.as_ref(), Some(payload));
        }

        storage.flusher()().unwrap();

        // The tracker file length matches the exact number of mappings
        assert_eq!(tracker_file_len(&dir), 100 * TRACKER_ENTRY_SIZE);

        // Everything is still there after reopening, the mode is selected automatically
        drop(storage);
        let storage =
            Gridstore::<Payload>::open(MmapFs, dir.path().to_path_buf(), Populate::No).unwrap();
        storage.as_serverless();
        assert_eq!(storage.max_point_offset(), 100);
        for (point_offset, payload) in &payloads {
            let stored = storage
                .get_value::<Random>(*point_offset, &hw_counter)
                .unwrap();
            assert_eq!(stored.as_ref(), Some(payload));
        }
    }

    #[test]
    fn test_put_rejects_out_of_order_point_offsets() {
        let (_dir, mut storage) = empty_byte_storage(Compression::None);

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

        storage.put_value(0, &vec![1; 100], hw_counter_ref).unwrap();
        assert_eq!(storage.get_storage_size_bytes().unwrap(), 100);

        // Putting the same point offset twice is rejected
        let err = storage
            .put_value(0, &vec![2; 100], hw_counter_ref)
            .unwrap_err();
        assert!(matches!(err, GridstoreError::ValidationError { .. }));

        storage.put_value(2, &vec![3; 100], hw_counter_ref).unwrap();

        // Putting a lower point offset is rejected, even if it was never set
        let err = storage
            .put_value(1, &vec![4; 100], hw_counter_ref)
            .unwrap_err();
        assert!(matches!(err, GridstoreError::ValidationError { .. }));

        // Rejected puts must not append any value data
        assert_eq!(storage.get_storage_size_bytes().unwrap(), 128 + 100);

        // The stored values are unaffected
        assert_eq!(
            storage.get_value::<Random>(0, &hw_counter).unwrap(),
            Some(vec![1; 100]),
        );
        assert_eq!(
            storage.get_value::<Random>(2, &hw_counter).unwrap(),
            Some(vec![3; 100]),
        );
    }

    #[test]
    fn test_delete_is_rejected() {
        let (_dir, mut storage) = empty_storage_serverless();

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
        storage
            .put_value(0, &Payload::default(), hw_counter_ref)
            .unwrap();

        let err = storage.delete_value(0).unwrap_err();
        assert!(matches!(err, GridstoreError::UnsupportedOperation { .. }));

        // The value is still there
        assert_eq!(
            storage.get_value::<Random>(0, &hw_counter).unwrap(),
            Some(Payload::default()),
        );
    }

    #[test]
    fn test_skipped_point_offsets_read_as_none() {
        let (_dir, mut storage) = empty_byte_storage(Compression::None);

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

        for point_offset in [0, 3, 4, 10] {
            storage
                .put_value(point_offset, &vec![point_offset as u8; 10], hw_counter_ref)
                .unwrap();
        }
        assert_eq!(storage.max_point_offset(), 11);

        for point_offset in [1, 2, 5, 6, 7, 8, 9, 11] {
            assert_eq!(
                storage
                    .get_value::<Random>(point_offset, &hw_counter)
                    .unwrap(),
                None,
            );
        }

        // Iteration skips the gaps
        let mut collected = Vec::new();
        storage
            .iter(
                |point_offset, value: Vec<u8>| {
                    collected.push((point_offset, value));
                    Ok::<_, GridstoreError>(true)
                },
                hw_counter.ref_payload_io_read_counter(),
            )
            .unwrap();
        assert_eq!(
            collected,
            vec![
                (0, vec![0; 10]),
                (3, vec![3; 10]),
                (4, vec![4; 10]),
                (10, vec![10; 10]),
            ]
        );

        // Iteration can stop early
        let mut count = 0;
        storage
            .iter(
                |_, _: Vec<u8>| {
                    count += 1;
                    Ok::<_, GridstoreError>(false)
                },
                hw_counter.ref_payload_io_read_counter(),
            )
            .unwrap();
        assert_eq!(count, 1);

        // Batched reads yield None for gaps and out of range point offsets
        let mut collected = Vec::new();
        storage
            .read_values::<Random, _, GridstoreError>(
                [0, 1, 5, 10, 3, 20].iter().map(|&offset| ((), offset)),
                |_, point_offset, value| {
                    collected.push((point_offset, value));
                    Ok(())
                },
                hw_counter.payload_io_read_counter(),
            )
            .unwrap();
        assert_eq!(
            collected,
            vec![
                (0, Some(vec![0; 10])),
                (1, None),
                (5, None),
                (10, Some(vec![10; 10])),
                (3, Some(vec![3; 10])),
                (20, None),
            ]
        );
    }

    #[test]
    fn test_values_are_block_aligned() {
        let (_dir, mut storage) = empty_byte_storage(Compression::None);

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

        // Each value starts at a block boundary (128 byte default), without trailing padding
        storage.put_value(0, &vec![1; 100], hw_counter_ref).unwrap();
        assert_eq!(storage.get_storage_size_bytes().unwrap(), 100);
        storage.put_value(1, &vec![2; 50], hw_counter_ref).unwrap();
        assert_eq!(storage.get_storage_size_bytes().unwrap(), 128 + 50);
        storage.put_value(2, &vec![3; 300], hw_counter_ref).unwrap();
        assert_eq!(storage.get_storage_size_bytes().unwrap(), 256 + 300);

        let pointer = storage.get_pointer(1).unwrap();
        assert_eq!(pointer.page_id, 0);
        assert_eq!(pointer.block_offset, 1);
        assert_eq!(pointer.length, 50);
        let pointer = storage.get_pointer(2).unwrap();
        assert_eq!(pointer.block_offset, 2);

        for (point_offset, value) in [(0, vec![1; 100]), (1, vec![2; 50]), (2, vec![3; 300])] {
            assert_eq!(
                storage
                    .get_value::<Random>(point_offset, &hw_counter)
                    .unwrap(),
                Some(value),
            );
        }
    }

    #[test]
    fn test_empty_and_huge_values() {
        let (_dir, mut storage) = empty_byte_storage(Compression::None);

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

        // An empty value takes no space at all
        storage.put_value(0, &vec![], hw_counter_ref).unwrap();
        assert_eq!(storage.get_storage_size_bytes().unwrap(), 0);
        assert_eq!(
            storage.get_value::<Random>(0, &hw_counter).unwrap(),
            Some(vec![]),
        );

        // A huge value simply grows the single page file, values never span pages
        let huge = (0..2_000_000).map(|i| i as u8).collect::<Vec<u8>>();
        storage.put_value(1, &huge, hw_counter_ref).unwrap();
        assert_eq!(storage.get_storage_size_bytes().unwrap(), huge.len());
        assert_eq!(
            storage.get_value::<Random>(1, &hw_counter).unwrap(),
            Some(huge),
        );
    }

    #[test]
    fn test_unflushed_mappings_are_lost_after_reopen() {
        let dir = TempDir::new().unwrap();
        let options = StorageOptions {
            compression: Some(Compression::None),
            mode: Some(Mode::Serverless),
            ..Default::default()
        };
        let mut storage =
            Gridstore::<Vec<u8>>::new(MmapFs, dir.path().to_path_buf(), options).unwrap();

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

        // Value data is written through to the page file, but the mappings are only appended to
        // the tracker file on flush
        for point_offset in 0..3 {
            storage
                .put_value(point_offset, &vec![7; 100], hw_counter_ref)
                .unwrap();
        }
        drop(storage);

        // Without a flush, the mappings are gone after reopening
        let mut storage =
            Gridstore::<Vec<u8>>::open(MmapFs, dir.path().to_path_buf(), Populate::No).unwrap();
        assert_eq!(storage.max_point_offset(), 0);
        assert_eq!(storage.get_value::<Random>(0, &hw_counter).unwrap(), None);

        // The unreferenced value data is left behind in the page file, new appends land past it
        assert_eq!(storage.get_storage_size_bytes().unwrap(), 2 * 128 + 100);
        storage.put_value(0, &vec![9; 10], hw_counter_ref).unwrap();
        storage.flusher()().unwrap();
        assert_eq!(storage.get_storage_size_bytes().unwrap(), 3 * 128 + 10);
        assert_eq!(
            storage.get_value::<Random>(0, &hw_counter).unwrap(),
            Some(vec![9; 10]),
        );
    }

    #[test]
    fn test_stale_flusher_is_noop() {
        let dir = TempDir::new().unwrap();
        let options = StorageOptions {
            mode: Some(Mode::Serverless),
            ..Default::default()
        };
        let mut storage =
            Gridstore::<Payload>::new(MmapFs, dir.path().to_path_buf(), options).unwrap();

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

        for point_offset in 0..3 {
            storage
                .put_value(point_offset, &Payload::default(), hw_counter_ref)
                .unwrap();
        }
        let stale_flusher = storage.flusher();

        storage
            .put_value(3, &Payload::default(), hw_counter_ref)
            .unwrap();
        storage.flusher()().unwrap();
        assert_eq!(tracker_file_len(&dir), 4 * TRACKER_ENTRY_SIZE);

        // The stale flusher must not write anything again, its mappings are already persisted
        stale_flusher().unwrap();
        assert_eq!(tracker_file_len(&dir), 4 * TRACKER_ENTRY_SIZE);

        drop(storage);
        let storage =
            Gridstore::<Payload>::open(MmapFs, dir.path().to_path_buf(), Populate::No).unwrap();
        assert_eq!(storage.max_point_offset(), 4);
        for point_offset in 0..4 {
            assert_eq!(
                storage
                    .get_value::<Random>(point_offset, &hw_counter)
                    .unwrap(),
                Some(Payload::default()),
            );
        }
    }

    #[test]
    fn test_flusher_after_clear_is_cancelled() {
        let (_dir, mut storage) = empty_storage_serverless();

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
        storage
            .put_value(0, &Payload::default(), hw_counter_ref)
            .unwrap();

        let flusher = storage.flusher();
        storage.clear().unwrap();

        let err = flusher().unwrap_err();
        assert!(matches!(err, GridstoreError::FlushCancelled));
    }

    #[test]
    fn test_clear_preserves_serverless_mode() {
        let (dir, mut storage) = empty_storage_serverless();

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
        for point_offset in 0..3 {
            storage
                .put_value(point_offset, &Payload::default(), hw_counter_ref)
                .unwrap();
        }

        storage.clear().unwrap();
        assert_eq!(storage.max_point_offset(), 0);
        assert_eq!(storage.get_storage_size_bytes().unwrap(), 0);

        // The storage is usable again, and putting restarts at point offset zero
        storage
            .put_value(0, &Payload::default(), hw_counter_ref)
            .unwrap();
        storage.flusher()().unwrap();

        // The recreated storage is still in serverless mode after reopening
        drop(storage);
        let storage =
            Gridstore::<Payload>::open(MmapFs, dir.path().to_path_buf(), Populate::No).unwrap();
        storage.as_serverless();
        assert_eq!(storage.max_point_offset(), 1);
    }

    #[test]
    fn test_wipe_removes_all_files() {
        let (dir, mut storage) = empty_storage_serverless();

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
        storage
            .put_value(0, &Payload::default(), hw_counter_ref)
            .unwrap();

        storage.wipe().unwrap();
        assert!(!dir.path().exists());
    }

    #[test]
    fn test_open_or_create_keeps_mode_on_disk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("storage");

        let options = StorageOptions {
            mode: Some(Mode::Serverless),
            ..Default::default()
        };
        let storage =
            Gridstore::<Payload>::open_or_create(MmapFs, path.clone(), options, Populate::No)
                .unwrap();
        storage.as_serverless();
        drop(storage);

        // Opening again ignores the create options, the mode comes from the persisted config
        let storage = Gridstore::<Payload>::open_or_create(
            MmapFs,
            path,
            StorageOptions::default(),
            Populate::No,
        )
        .unwrap();
        storage.as_serverless();
    }

    #[test]
    fn test_reader_on_serverless_storage() {
        let dir = TempDir::new().unwrap();
        let options = StorageOptions {
            compression: Some(Compression::None),
            mode: Some(Mode::Serverless),
            ..Default::default()
        };
        let mut storage =
            Gridstore::<Vec<u8>>::new(MmapFs, dir.path().to_path_buf(), options).unwrap();

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
        for point_offset in [0, 1, 4] {
            storage
                .put_value(point_offset, &vec![point_offset as u8; 10], hw_counter_ref)
                .unwrap();
        }
        storage.flusher()().unwrap();

        // The reader selects the serverless mode automatically
        let reader = GridstoreReader::<Vec<u8>, MmapFile>::open(
            &MmapFs,
            dir.path().to_path_buf(),
            Populate::No,
        )
        .unwrap();
        assert_eq!(reader.max_point_offset().unwrap(), 5);
        assert!(reader.is_on_disk());
        // Three values, packed at consecutive blocks: point offset gaps take no page space
        assert_eq!(reader.get_storage_size_bytes(), 2 * 128 + 10);
        reader.clear_cache().unwrap();

        assert_eq!(
            reader.get_value::<Random>(0, &hw_counter).unwrap(),
            Some(vec![0; 10]),
        );
        assert_eq!(reader.get_value::<Random>(2, &hw_counter).unwrap(), None);

        // Reader files match the writer files
        let mut reader_files = reader.files();
        let mut writer_files = storage.files();
        reader_files.sort();
        writer_files.sort();
        assert_eq!(reader_files, writer_files);

        // Iteration is bounded by the given maximum id and skips gaps
        let mut collected = Vec::new();
        reader
            .iter(
                4,
                |point_offset, value: Vec<u8>| {
                    collected.push((point_offset, value));
                    Ok::<_, GridstoreError>(true)
                },
                hw_counter.ref_payload_io_read_counter(),
            )
            .unwrap();
        assert_eq!(collected, vec![(0, vec![0; 10]), (1, vec![1; 10])]);

        // Batched reads through the reader and its view
        let mut collected = Vec::new();
        reader
            .read_values::<Random, _, GridstoreError>(
                [(0, 0), (1, 3), (2, 4)].iter().copied(),
                |user_data, point_offset, value| {
                    collected.push((user_data, point_offset, value.is_some()));
                    Ok(())
                },
                hw_counter.payload_io_read_counter(),
            )
            .unwrap();
        assert_eq!(collected, vec![(0, 0, true), (1, 3, false), (2, 4, true)]);

        let view = reader.view();
        assert_eq!(view.max_point_offset().unwrap(), 5);
        assert_eq!(view.get_storage_size_bytes(), 2 * 128 + 10);
        assert_eq!(
            view.get_value::<Random>(4, &hw_counter).unwrap(),
            Some(vec![4; 10]),
        );
    }

    #[test]
    fn test_reader_live_reload() {
        let dir = TempDir::new().unwrap();
        let options = StorageOptions {
            compression: Some(Compression::None),
            mode: Some(Mode::Serverless),
            ..Default::default()
        };
        let mut storage =
            Gridstore::<Vec<u8>>::new(MmapFs, dir.path().to_path_buf(), options).unwrap();

        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
        for point_offset in 0..3 {
            storage
                .put_value(point_offset, &vec![point_offset as u8; 10], hw_counter_ref)
                .unwrap();
        }
        storage.flusher()().unwrap();

        let mut reader = GridstoreReader::<Vec<u8>, MmapFile>::open(
            &MmapFs,
            dir.path().to_path_buf(),
            Populate::No,
        )
        .unwrap();
        assert_eq!(reader.max_point_offset().unwrap(), 3);

        // Without new data, a live reload changes nothing
        reader.live_reload(&MmapFs).unwrap();
        assert_eq!(reader.max_point_offset().unwrap(), 3);

        // The writer appends more values, the reader picks them up after a live reload
        for point_offset in 3..6 {
            storage
                .put_value(point_offset, &vec![point_offset as u8; 10], hw_counter_ref)
                .unwrap();
        }
        storage.flusher()().unwrap();

        reader.live_reload(&MmapFs).unwrap();
        assert_eq!(reader.max_point_offset().unwrap(), 6);
        assert_eq!(reader.get_storage_size_bytes(), 5 * 128 + 10);
        for point_offset in 0..6 {
            assert_eq!(
                reader
                    .get_value::<Random>(point_offset, &hw_counter)
                    .unwrap(),
                Some(vec![point_offset as u8; 10]),
            );
        }
    }
}
