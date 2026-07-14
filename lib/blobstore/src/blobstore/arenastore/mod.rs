mod page;
mod reader;
#[cfg(test)]
mod tests;
mod view;

use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;

use common::counter::counter_cell::CounterCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::referenced_counter::HwMetricRefCounter;
use common::generic_consts::AccessPattern;
use common::is_alive_lock::IsAliveLock;
use common::universal_io::{UniversalRead, UniversalWrite, UserData};
use fs_err as fs;
use page::AppendOnlyPages;
use parking_lot::RwLock;
pub(super) use reader::ArenastoreReader;
pub(super) use view::ArenastoreView;

use super::Flusher;
use super::reader::CONFIG_FILENAME;
use crate::Result;
use crate::blob::Blob;
use crate::config::{StorageConfig, StorageOptions};
use crate::error::GridstoreError;
use crate::tracker::append_only::AppendOnlyTracker;
use crate::tracker::{PointOffset, ValuePointer};

/// Number of most recent mappings validated against the page file lengths when opening
const OPEN_CHECK_MAPPINGS: PointOffset = 256;

/// Check that the most recently persisted mappings point at value data within the page files.
///
/// Guards against page files that are missing or shorter than what the tracker references, for
/// example after a partial copy or restore of the storage directory. Only the most recent
/// mappings are checked to keep opening cheap.
fn validate_consistency<S: UniversalRead>(
    tracker: &AppendOnlyTracker,
    pages: &AppendOnlyPages<S>,
) -> Result<()> {
    let count = tracker.pointer_count();
    let start = count.saturating_sub(OPEN_CHECK_MAPPINGS);
    for pointer in tracker.get_range(start..count)?.into_iter().flatten() {
        let extent = u64::from(pointer.block_offset) + u64::from(pointer.length);
        match pages.page_len(pointer.page_id) {
            None => {
                return Err(GridstoreError::service_error(format!(
                    "Inconsistent Arenastore: a mapping references value data in page {}, \
                     but the page file does not exist",
                    pointer.page_id,
                )));
            }
            Some(page_len) if extent > page_len => {
                return Err(GridstoreError::service_error(format!(
                    "Inconsistent Arenastore: a mapping references value data up to byte \
                     {extent} in page {}, but the page file only holds {page_len} bytes",
                    pointer.page_id,
                )));
            }
            Some(_) => {}
        }
    }

    Ok(())
}

/// Read-write storage for values of type `V`, operating in append-only mode.
///
/// Append-only variant for serverless deployments, which restrict IO to appending to files:
/// existing bytes can never be rewritten. To use as few files as possible, value data is packed
/// back to back in page files, without blocks or alignment, next to a single tracker file and
/// the storage config. Once a page reaches the configured page size, a new page is started,
/// bounding the size of and the number of appends to each file (object stores limit appends per
/// object).
///
/// Values cannot be updated or deleted, and must be put at monotonically increasing point
/// offsets. Value data is read and written through the universal IO backend `S`; the tracker
/// file is read and written directly.
///
/// Uses `Arc<RwLock<...>>` for the pages and tracker to support concurrent flushing.
#[derive(Debug)]
pub(super) struct Arenastore<V, S>
where
    S: UniversalWrite + 'static,
{
    fs: S::Fs,
    pub(super) config: StorageConfig,
    tracker: Arc<RwLock<AppendOnlyTracker>>,
    pages: Arc<RwLock<AppendOnlyPages<S>>>,
    base_path: PathBuf,
    /// Lock to prevent concurrent flushes and used for waiting for ongoing flushes to finish.
    is_alive_flush_lock: IsAliveLock,
    _phantom: PhantomData<V>,
}

impl<V, S> Arenastore<V, S>
where
    V: Blob,
    S: UniversalWrite + 'static,
{
    /// List all files belonging to this storage (tracker, pages, config).
    pub(super) fn files(&self) -> Vec<PathBuf> {
        let mut paths = self.tracker.read().files();
        paths.extend(self.pages.read().files());
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
    pub(super) fn new(fs: S::Fs, base_path: PathBuf, options: StorageOptions) -> Result<Self> {
        let config = StorageConfig::try_from(options).map_err(GridstoreError::service_error)?;

        let tracker = AppendOnlyTracker::new(&base_path)?;
        let pages = AppendOnlyPages::new(&fs, &base_path)?;

        let config_path = base_path.join(CONFIG_FILENAME);
        common::fs::atomic_save_json(&config_path, &config)?;

        Ok(Self {
            fs,
            config,
            tracker: Arc::new(RwLock::new(tracker)),
            pages: Arc::new(RwLock::new(pages)),
            base_path,
            is_alive_flush_lock: IsAliveLock::new(),
            _phantom: PhantomData,
        })
    }

    /// Open an existing storage at the given path, with the already read config.
    pub(super) fn open(fs: S::Fs, base_path: PathBuf, config: StorageConfig) -> Result<Self> {
        let tracker = AppendOnlyTracker::open(&base_path, true)?;
        let pages = AppendOnlyPages::open(&fs, &base_path, true)?;
        validate_consistency(&tracker, &pages)?;

        Ok(Self {
            fs,
            config,
            tracker: Arc::new(RwLock::new(tracker)),
            pages: Arc::new(RwLock::new(pages)),
            base_path,
            is_alive_flush_lock: IsAliveLock::new(),
            _phantom: PhantomData,
        })
    }

    /// Create an [`ArenastoreView`] by locking tracker and pages, then call `f` with the
    /// view.
    pub(super) fn with_view<R>(&self, f: impl FnOnce(ArenastoreView<'_, V, S>) -> R) -> R {
        let tracker = self.tracker.read();
        let pages = self.pages.read();
        f(ArenastoreView::new(&self.config, &tracker, &pages))
    }

    /// Put a value in the storage.
    ///
    /// Both the value data and its mapping are buffered in memory until the next flush, which
    /// batches them into a single write per file. Reads transparently serve buffered values.
    /// Only a page rollover touches disk before the flush, by creating the new, empty page file.
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
        // Validate before buffering anything, a rejected put must not leave data behind
        let next = self.tracker.read().pointer_count();
        if point_offset < next {
            return Err(GridstoreError::unsupported_operation(format!(
                "cannot put value at point offset {point_offset}, the storage is append-only: \
                 values cannot be overwritten and must be put at monotonically increasing point \
                 offsets, the next allowed point offset is {next}",
            )));
        }

        let value_bytes = value.to_bytes();
        let comp_value = self.config.compression.compress(value_bytes);
        let value_size = comp_value.len();

        hw_counter.incr_delta(value_size);

        let value_size = u32::try_from(value_size)
            .map_err(|_| GridstoreError::service_error("value is too large"))?;

        let page_capacity_bytes = self.config.page_size_bytes as u64;
        let (page_id, offset) =
            self.pages
                .write()
                .append_value(&self.fs, &comp_value, page_capacity_bytes)?;

        self.tracker
            .write()
            .set(point_offset, ValuePointer::new(page_id, offset, value_size))?;

        Ok(false)
    }

    /// Deleting values is not supported in append-only mode.
    // Takes &mut self for signature parity with the dynamic variant
    #[allow(clippy::unused_self, clippy::needless_pass_by_ref_mut)]
    pub(super) fn delete_value(&mut self, _point_offset: PointOffset) -> Result<Option<V>> {
        Err(GridstoreError::unsupported_operation("deleting values"))
    }

    /// Clear the storage, going back to the initial state.
    ///
    /// Completely wipes the storage, and recreates it in append-only mode.
    pub(super) fn clear(&mut self) -> Result<()> {
        self.is_alive_flush_lock.blocking_mark_dead();

        fs::remove_dir_all(&self.base_path)?;
        fs::create_dir_all(&self.base_path)?;

        *self = Self::new(
            self.fs.clone(),
            self.base_path.clone(),
            StorageOptions::from(&self.config),
        )?;

        Ok(())
    }

    /// Wipe the storage, drop the tracker and pages and delete the base directory.
    ///
    /// Takes ownership because this function leaves the storage in an inconsistent state which
    /// does not allow further usage. Use [`clear`](Self::clear) instead to clear and reuse the
    /// storage.
    pub(super) fn wipe(self) -> Result<()> {
        let Self {
            fs: _,
            config: _,
            tracker,
            pages,
            base_path,
            is_alive_flush_lock,
            _phantom,
        } = self;

        is_alive_flush_lock.blocking_mark_dead();
        drop((tracker, pages));

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
            //
            // See:
            // - https://github.com/qdrant/qdrant/pull/7983
            // - https://github.com/qdrant/qdrant/pull/8248
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

impl<V, S: UniversalWrite + 'static> Arenastore<V, S> {
    /// Create flusher that durably persists all pending changes when invoked.
    ///
    /// Appends all buffered value data to the page files with a single write per page and syncs
    /// them, then does the same for the pending mappings in the tracker file. This order
    /// guarantees that a mapping on disk never points at value data that is not durable yet.
    pub(super) fn flusher(&self) -> Flusher {
        // Only values and mappings put up to this point are persisted, puts made during the
        // flush stay buffered for the next flush. The two watermarks are consistent with each
        // other: puts take &mut self, so no put can happen between capturing them.
        let target = self.tracker.read().pointer_count();
        let target_page_lens = self.pages.read().page_lens();

        let fs = self.fs.clone();
        let tracker = Arc::downgrade(&self.tracker);
        let pages = Arc::downgrade(&self.pages);
        let is_alive_flush_lock = self.is_alive_flush_lock.handle();

        Box::new(move || {
            let (Some(is_alive_flush_guard), Some(tracker), Some(pages)) = (
                is_alive_flush_lock.lock_if_alive(),
                tracker.upgrade(),
                pages.upgrade(),
            ) else {
                log::trace!("Arenastore was cleared, cancelling flush");
                return Err(GridstoreError::FlushCancelled);
            };

            let pages_flusher = {
                let mut pages_guard = pages.write();
                pages_guard.write_pending(&fs, &target_page_lens)?;
                pages_guard.flusher()
            };
            pages_flusher()?;

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

    /// Populating is a no-op in append-only mode.
    ///
    /// Files are never populated into RAM, the OS page cache manages caching.
    // Signature parity with the dynamic variant
    #[allow(clippy::unused_self, clippy::unnecessary_wraps)]
    pub(super) fn populate(&self) -> Result<()> {
        Ok(())
    }

    /// Dropping disk cache is a no-op in append-only mode.
    ///
    /// Files are never populated into RAM, the OS page cache manages caching.
    // Signature parity with the dynamic variant
    #[allow(clippy::unused_self, clippy::unnecessary_wraps)]
    pub(super) fn clear_cache(&self) -> crate::Result<()> {
        Ok(())
    }
}
