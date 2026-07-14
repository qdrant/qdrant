use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use common::counter::counter_cell::CounterCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::referenced_counter::HwMetricRefCounter;
use common::generic_consts::AccessPattern;
use common::universal_io::{CachedReadFs, UniversalRead, UniversalReadFs, UserData};

use super::page::AppendOnlyPages;
use super::validate_consistency;
use super::view::ArenastoreView;
use crate::Result;
use crate::blob::Blob;
use crate::blobstore::reader::CONFIG_FILENAME;
use crate::config::StorageConfig;
use crate::error::GridstoreError;
use crate::tracker::PointOffset;
use crate::tracker::append_only::AppendOnlyTracker;

/// Read-only storage for values of type `V`, operating in append-only mode.
///
/// Holds the tracker and pages directly (no locks) since it provides only read access.
/// For read-write access, use [`Arenastore`].
///
/// Value data is read through the universal IO backend `S`, the tracker file is read directly.
#[derive(Debug)]
pub(crate) struct ArenastoreReader<V, S: UniversalRead> {
    config: StorageConfig,
    tracker: AppendOnlyTracker,
    pages: AppendOnlyPages<S>,
    base_path: PathBuf,
    _phantom: PhantomData<V>,
}

impl<V: Blob, S: UniversalRead> ArenastoreReader<V, S> {
    /// Schedule prefetches for the files a subsequent [`open`](Self::open) reads through the
    /// universal IO backend, which is only the page files.
    ///
    /// The tracker file is read directly, not through the backend, so there is nothing to
    /// schedule for it. The config file is scheduled by the mode dispatching reader, which reads
    /// it first to select the operating mode.
    pub(crate) fn preopen<Fs: CachedReadFs<File = S>>(fs: &Fs, base_path: &Path) -> Result<()> {
        AppendOnlyPages::<S>::preopen(fs, base_path)
    }

    /// Open an existing read-only storage at the given path, with the already read config.
    pub(crate) fn open<Fs: UniversalReadFs<File = S>>(
        fs: &Fs,
        base_path: PathBuf,
        config: StorageConfig,
    ) -> Result<Self> {
        let tracker = AppendOnlyTracker::open(&base_path, false)?;
        let pages = AppendOnlyPages::open(fs, &base_path, false)?;
        validate_consistency(&tracker, &pages)?;

        Ok(Self {
            config,
            tracker,
            pages,
            base_path,
            _phantom: PhantomData,
        })
    }

    /// Create an [`ArenastoreView`] borrowing this reader's data.
    pub(crate) fn view(&self) -> ArenastoreView<'_, V, S> {
        ArenastoreView::new(&self.config, &self.tracker, &self.pages)
    }

    /// List all files belonging to this reader (tracker, pages, config).
    pub(crate) fn files(&self) -> Vec<PathBuf> {
        let mut paths = self.tracker.files();
        paths.extend(self.pages.files());
        paths.push(self.base_path.join(CONFIG_FILENAME));
        paths
    }

    pub(crate) fn max_point_offset(&self) -> PointOffset {
        self.tracker.pointer_count()
    }

    pub(crate) fn get_value<P: AccessPattern>(
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
    pub(crate) fn iter<F, E>(
        &self,
        max_id: PointOffset,
        mut callback: F,
        hw_counter: HwMetricRefCounter,
    ) -> Result<(), E>
    where
        F: FnMut(PointOffset, V) -> Result<bool, E>,
        E: From<GridstoreError>,
    {
        let max_id = max_id.min(self.max_point_offset());
        let view = self.view();

        // Iterate in batches to bound the size of the tracker reads
        const BATCH_SIZE: PointOffset = 256;

        let mut current_offset = 0;
        while current_offset < max_id {
            let end_offset = current_offset.saturating_add(BATCH_SIZE).min(max_id);

            if !view.iter_range(current_offset..end_offset, &mut callback, hw_counter)? {
                return Ok(());
            }

            current_offset = end_offset;
        }

        Ok(())
    }

    pub(crate) fn read_values<P, U, E>(
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
    pub(crate) fn get_storage_size_bytes(&self) -> usize {
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
    pub(crate) fn live_reload(&mut self, fs: &S::Fs) -> Result<()> {
        self.tracker.live_reload()?;

        // Reload the pages after the tracker: newly loaded mappings only reference value data
        // that was appended (and whose page file was created) before the mappings were written,
        // so reloading the pages last guarantees they cover all of them. Reload them even
        // without new mappings, unflushed value data may have been appended already and counts
        // towards the reported storage size.
        self.pages.live_reload(fs)?;

        Ok(())
    }
}

impl<V, S: UniversalRead> ArenastoreReader<V, S> {
    /// Returns `true`: append-only storage always reads from disk, it is never populated into
    /// RAM.
    #[allow(clippy::unused_self)]
    pub(crate) fn is_on_disk(&self) -> bool {
        true
    }

    /// Dropping disk cache is a no-op in append-only mode.
    ///
    /// Files are never populated into RAM, the OS page cache manages caching.
    // Signature parity with the dynamic variant
    #[allow(clippy::unused_self, clippy::unnecessary_wraps)]
    pub(crate) fn clear_cache(&self) -> crate::Result<()> {
        Ok(())
    }
}
