use std::cmp;
use std::path::PathBuf;

use common::counter::counter_cell::CounterCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::referenced_counter::HwMetricRefCounter;
use common::generic_consts::{AccessPattern, Sequential};
use common::universal_io::{
    CachedReadFs, Populate, UniversalRead, UniversalReadFs, UserData, read_json_via,
};

use super::view::GridstoreView;
use crate::Result;
use crate::blob::Blob;
use crate::config::StorageConfig;
use crate::error::GridstoreError;
use crate::pages::Pages;
use crate::tracker::{PageId, PointOffset, Tracker};

pub(super) const CONFIG_FILENAME: &str = "config.json";

/// Read-only storage for values of type `V`.
///
/// Holds pages and tracker directly (no locks) since it provides only read access.
/// For read-write access, use [`super::Gridstore`].
#[derive(Debug)]
pub struct GridstoreReader<V, S: UniversalRead> {
    pub(super) config: StorageConfig,
    pub(super) tracker: Tracker<S>,
    pub(super) pages: Pages<S>,
    pub(super) base_path: PathBuf,
    pub(super) _value_type: std::marker::PhantomData<V>,
    /// How to populate new attached pages
    populate: Populate,
}

impl<V: Blob, S: UniversalRead> GridstoreReader<V, S> {
    /// Create a [`GridstoreView`] borrowing this reader's data.
    pub fn view(&self) -> GridstoreView<'_, V, S> {
        GridstoreView::new(&self.config, &self.tracker, &self.pages)
    }

    /// List all files belonging to this reader (tracker, pages, config).
    ///
    /// Note: does not include bitmask files. Use [`super::Gridstore::files`] for the full list.
    pub fn files(&self) -> Vec<PathBuf> {
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

    pub fn max_point_offset(&self) -> PointOffset {
        self.view().max_point_offset()
    }

    pub fn preopen<Fs: CachedReadFs<File = S>>(
        fs: &Fs,
        base_path: PathBuf,
        populate: Populate,
    ) -> Result<()> {
        // schedule config file
        let config_path = base_path.join(CONFIG_FILENAME);
        fs.schedule_prefetch(&config_path, None, None)?;

        // schedule tracker
        Tracker::<S>::preopen(fs, &base_path, populate)?;

        // schedule pages
        Pages::preopen(fs, &base_path, populate)
    }

    /// Open an existing read-only storage at the given path.
    ///
    /// Infers page count by scanning for page files on disk.
    ///
    /// `fs` may be the raw backend or a caching wrapper producing the same
    /// `S`-typed handles (e.g. `CachedReadFs`). Live reload keeps taking a
    /// raw `&S::Fs` — a caching wrapper's snapshot goes stale the moment
    /// the leader writes.
    pub fn open<Fs: UniversalReadFs<File = S>>(
        fs: &Fs,
        base_path: PathBuf,
        populate: Populate,
    ) -> Result<Self> {
        // A reader only reads, so open pages and tracker non-writable. This
        // lets the backend be write-enforced (e.g. `ReadOnly<MmapFile>`); the
        // writable `Gridstore` opens these same files writable instead.
        let (config, tracker) = read_config_and_tracker(fs, &base_path, populate, false)?;

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

    pub(super) fn page_path(&self, page_id: u32) -> PathBuf {
        self.base_path.join(format!("page_{page_id}.dat"))
    }

    pub fn get_value<P: AccessPattern>(
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
    pub fn iter<F, E>(
        &self,
        max_id: PointOffset,
        mut callback: F,
        hw_counter: HwMetricRefCounter,
    ) -> Result<(), E>
    where
        F: FnMut(PointOffset, V) -> Result<bool, E>,
        E: From<GridstoreError>,
    {
        let max_id = cmp::min(max_id, self.max_point_offset());

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
    /// For the precise used-space calculation, use [`super::Gridstore::get_storage_size_bytes`].
    pub fn get_storage_size_bytes(&self) -> usize {
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
    pub fn live_reload(&mut self, fs: &S::Fs) -> Result<()> {
        let has_new_data = self.tracker.live_reload()?;

        if !has_new_data {
            return Ok(());
        }

        self.pages.live_reload(fs, self.populate)?;

        Ok(())
    }
}

impl<V, S: UniversalRead> GridstoreReader<V, S> {
    /// Returns `true` if GridstoreReader is on disk, i.e. not populated on start/reload
    pub fn is_on_disk(&self) -> bool {
        !self.populate.to_bool::<S>()
    }

    /// Drop disk cache for pages.
    pub fn clear_cache(&self) -> crate::Result<()> {
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

/// Read config and open tracker from the base path.
///
/// Shared helper used by both [`GridstoreReader::open`] and [`super::Gridstore::open`].
pub(super) fn read_config_and_tracker<Fs, S>(
    fs: &Fs,
    base_path: &std::path::Path,
    populate: Populate,
    writeable: bool,
) -> Result<(StorageConfig, Tracker<S>)>
where
    Fs: UniversalReadFs<File = S>,
    S: UniversalRead,
{
    let config_path = base_path.join(CONFIG_FILENAME);
    let config: StorageConfig =
        read_json_via::<Fs, StorageConfig>(fs, &config_path).map_err(GridstoreError::from)?;

    let tracker = Tracker::<S>::open(fs, base_path, populate, writeable)?;

    Ok((config, tracker))
}
