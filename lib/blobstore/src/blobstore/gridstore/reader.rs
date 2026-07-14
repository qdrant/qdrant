use std::cmp;
use std::path::PathBuf;

use common::counter::counter_cell::CounterCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::referenced_counter::HwMetricRefCounter;
use common::generic_consts::{AccessPattern, Sequential};
use common::universal_io::{Populate, UniversalRead, UniversalReadFs, UserData};

use super::view::GridstoreView;
use crate::Result;
use crate::blob::Blob;
use crate::blobstore::reader::CONFIG_FILENAME;
use crate::config::StorageConfig;
use crate::error::GridstoreError;
use crate::pages::Pages;
use crate::tracker::{PageId, PointOffset, ReadOnlyTracker};

/// Read-only storage for values of type `V`, operating in mutable mode.
///
/// Holds pages and tracker directly (no locks) since it provides only read access.
/// For read-write access, use [`super::Gridstore`].
#[derive(Debug)]
pub(crate) struct GridstoreReader<V, S: UniversalRead> {
    config: StorageConfig,
    tracker: ReadOnlyTracker<S>,
    pages: Pages<S>,
    base_path: PathBuf,
    _value_type: std::marker::PhantomData<V>,
    /// How to populate new attached pages
    populate: Populate,
}

impl<V: Blob, S: UniversalRead> GridstoreReader<V, S> {
    /// Create a [`GridstoreView`] borrowing this reader's data.
    pub(crate) fn view(&self) -> GridstoreView<'_, V, S, ReadOnlyTracker<S>> {
        GridstoreView::new(&self.config, &self.tracker, &self.pages)
    }

    /// List all files belonging to this reader (tracker, pages, config).
    pub(crate) fn files(&self) -> Vec<PathBuf> {
        let num_pages = self.pages.num_pages();
        let mut paths = Vec::with_capacity(num_pages + 2);
        for tracker_file in self.tracker.files() {
            paths.push(tracker_file);
        }
        for page_id in 0..num_pages as PageId {
            paths.push(self.page_path(page_id));
        }
        paths.push(self.base_path.join(CONFIG_FILENAME));
        paths
    }

    /// Exclusive upper bound of point offsets that may have a value.
    ///
    /// The writer-maintained count read from the stored tracker header, as of
    /// the last [`Self::live_reload`] — see
    /// [`TrackerRead::max_point_offset`](crate::tracker::TrackerRead::max_point_offset).
    pub(crate) fn max_point_offset(&self) -> Result<PointOffset> {
        self.view().max_point_offset()
    }

    /// Open an existing read-only storage at the given path, with the already read config.
    ///
    /// Infers page count by scanning for page files on disk.
    pub(crate) fn open<Fs: UniversalReadFs<File = S>>(
        fs: &Fs,
        base_path: PathBuf,
        config: StorageConfig,
        populate: Populate,
    ) -> Result<Self> {
        // A reader only reads, so open pages and tracker non-writable. This
        // lets the backend be write-enforced (e.g. `ReadOnly<MmapFile>`); the
        // writable `Gridstore` opens these same files writable instead.
        let tracker = ReadOnlyTracker::open(fs, &base_path, populate)?;

        let pages = Pages::<S>::open(fs, &base_path, false, populate)?;

        Ok(Self {
            tracker,
            config,
            pages,
            base_path,
            populate,
            _value_type: std::marker::PhantomData,
        })
    }

    fn page_path(&self, page_id: u32) -> PathBuf {
        self.base_path.join(format!("page_{page_id}.dat"))
    }

    pub(crate) fn get_value<P: AccessPattern>(
        &self,
        point_offset: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Option<V>> {
        self.view().get_value::<P>(point_offset, hw_counter)
    }

    /// Iterate over all values with point offsets below `max_id` and execute callback for each one.
    /// Missing values are skipped.
    ///
    /// Return `false` from the callback to stop iteration early.
    //
    // TODO: Unify with `read_values`?
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
        let max_id = cmp::min(max_id, self.max_point_offset()?);

        let point_offsets = (0..max_id).map(|point_offset| ((), point_offset));

        self.view().read_values::<Sequential, _, _>(
            point_offsets,
            |_, point_offset, value| {
                let Some(value) = value else {
                    return Ok(true);
                };

                callback(point_offset, value)
            },
            &hw_counter,
        )?;

        Ok(())
    }

    // TODO: Unify with `iter`?
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

    /// Return the storage size in bytes (approximate: total page capacity).
    ///
    /// For the precise used-space calculation, use [`crate::Blobstore::get_storage_size_bytes`].
    pub(crate) fn get_storage_size_bytes(&self) -> usize {
        self.view().get_storage_size_bytes()
    }

    /// This method reloads the Gridstore data from "disk", so that
    /// it should make newly written data is readable.
    ///
    /// Important assumptions:
    ///
    /// - Only appending new data is supported, for modifications of existing data there are no consistency guarantees.
    /// - Partial writes are possible, it is up to the caller to read only fully written data.
    ///
    /// Both the tracker and the pages are refreshed unconditionally: the
    /// tracker file is mutated in place and carries no reliable
    /// cheaply-readable change signal, so there is no "nothing changed"
    /// fast path. Both refreshes are cheap for non-populated readers.
    pub(crate) fn live_reload(&mut self, fs: &S::Fs) -> Result<()> {
        self.tracker.live_reload(fs)?;
        self.pages.live_reload(fs, self.populate)?;

        Ok(())
    }
}

impl<V, S: UniversalRead> GridstoreReader<V, S> {
    /// Returns `true` if the reader is on disk, i.e. not populated on start/reload
    pub(crate) fn is_on_disk(&self) -> bool {
        !self.populate.to_bool::<S>()
    }

    /// Drop disk cache for pages.
    pub(crate) fn clear_cache(&self) -> crate::Result<()> {
        let Self {
            config: _,
            tracker: _,
            pages,
            base_path: _,
            populate: _,
            _value_type,
        } = self;
        pages.clear_cache()?;
        Ok(())
    }
}
