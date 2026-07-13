use std::cmp;
use std::path::PathBuf;

use common::counter::counter_cell::CounterCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::referenced_counter::HwMetricRefCounter;
use common::generic_consts::{AccessPattern, Sequential};
use common::universal_io::{
    CachedReadFs, Populate, UniversalRead, UniversalReadFs, UserData, read_json_via,
};

use super::append_only::ArenastoreReader;
use super::view::{BlobstoreView, GridstoreView};
use crate::Result;
use crate::blob::Blob;
use crate::config::{Mode, StorageConfig};
use crate::error::GridstoreError;
use crate::pages::Pages;
use crate::tracker::{PageId, PointOffset, ReadOnlyTracker, Tracker};

pub(super) const CONFIG_FILENAME: &str = "config.json";

/// Read-only storage for values of type `V`.
///
/// Operates in one of two modes, automatically selected when opening, see [`Mode`].
///
/// Holds its data directly (no locks) since it provides only read access.
/// For read-write access, use [`super::Blobstore`].
#[derive(Debug)]
pub struct BlobstoreReader<V, S: UniversalRead> {
    variant: ReaderVariant<V, S>,
}

/// Mode specific implementation of the reader, see [`Mode`].
#[derive(Debug)]
enum ReaderVariant<V, S: UniversalRead> {
    Gridstore(GridstoreReader<V, S>),
    Arenastore(ArenastoreReader<V, S>),
}

impl<V: Blob, S: UniversalRead> BlobstoreReader<V, S> {
    /// Schedule prefetches for the files a subsequent [`open`](Self::open) reads.
    ///
    /// Reads the config first to select the operating mode, like `open`: the mode decides which
    /// files exist, and scheduling a prefetch for a missing file fails immediately.
    pub fn preopen<Fs: CachedReadFs<File = S>>(
        fs: &Fs,
        base_path: PathBuf,
        populate: Populate,
    ) -> Result<()> {
        let config = read_config(fs, &base_path)?;

        // schedule config file, so the config read in `open` is served from the prefetch pool
        let config_path = base_path.join(CONFIG_FILENAME);
        fs.schedule_prefetch(&config_path, None, None)?;

        match config.mode {
            Mode::Dynamic => {
                // schedule tracker
                Tracker::<S>::preopen(fs, &base_path, populate)?;

                // schedule pages
                Pages::preopen(fs, &base_path, populate)
            }
            Mode::AppendOnly => ArenastoreReader::<V, S>::preopen(fs, &base_path),
        }
    }

    /// Open an existing read-only storage at the given path.
    ///
    /// The operating mode is automatically selected based on the persisted config.
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
        let config = read_config(fs, &base_path)?;
        match config.mode {
            Mode::Dynamic => {
                let reader = GridstoreReader::open(fs, base_path, config, populate)?;
                Ok(Self {
                    variant: ReaderVariant::Gridstore(reader),
                })
            }
            Mode::AppendOnly => {
                let reader = ArenastoreReader::open(fs, base_path, config)?;
                Ok(Self {
                    variant: ReaderVariant::Arenastore(reader),
                })
            }
        }
    }

    /// Create a [`BlobstoreView`] borrowing this reader's data.
    pub fn view(&self) -> BlobstoreView<'_, V, S> {
        match &self.variant {
            ReaderVariant::Gridstore(reader) => BlobstoreView::from_gridstore(reader.view()),
            ReaderVariant::Arenastore(reader) => BlobstoreView::from_arenastore(reader.view()),
        }
    }

    /// List all files belonging to this reader.
    ///
    /// Note: in dynamic mode this does not include bitmask files. Use
    /// [`super::Blobstore::files`] for the full list.
    pub fn files(&self) -> Vec<PathBuf> {
        match &self.variant {
            ReaderVariant::Gridstore(reader) => reader.files(),
            ReaderVariant::Arenastore(reader) => reader.files(),
        }
    }

    /// Exclusive upper bound of point offsets that may have a value.
    ///
    /// In dynamic mode this is the writer-maintained count read from the stored tracker header,
    /// as of the last [`Self::live_reload`] — see
    /// [`TrackerRead::max_point_offset`](crate::tracker::TrackerRead::max_point_offset).
    pub fn max_point_offset(&self) -> Result<PointOffset> {
        match &self.variant {
            ReaderVariant::Gridstore(reader) => reader.max_point_offset(),
            ReaderVariant::Arenastore(reader) => Ok(reader.max_point_offset()),
        }
    }

    pub fn get_value<P: AccessPattern>(
        &self,
        point_offset: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Option<V>> {
        match &self.variant {
            ReaderVariant::Gridstore(reader) => reader.get_value::<P>(point_offset, hw_counter),
            ReaderVariant::Arenastore(reader) => reader.get_value::<P>(point_offset, hw_counter),
        }
    }

    /// Iterate over all values with point offsets below `max_id` and execute callback for each one.
    /// Missing values are skipped.
    ///
    /// Return `false` from the callback to stop iteration early.
    pub fn iter<F, E>(
        &self,
        max_id: PointOffset,
        callback: F,
        hw_counter: HwMetricRefCounter,
    ) -> Result<(), E>
    where
        F: FnMut(PointOffset, V) -> Result<bool, E>,
        E: From<GridstoreError>,
    {
        match &self.variant {
            ReaderVariant::Gridstore(reader) => reader.iter(max_id, callback, hw_counter),
            ReaderVariant::Arenastore(reader) => reader.iter(max_id, callback, hw_counter),
        }
    }

    pub fn read_values<P, U, E>(
        &self,
        point_offsets: impl Iterator<Item = (U, PointOffset)>,
        callback: impl FnMut(U, PointOffset, Option<V>) -> Result<(), E>,
        hw_counter_cell: &CounterCell,
    ) -> Result<(), E>
    where
        P: AccessPattern,
        U: UserData,
        E: From<GridstoreError>,
    {
        match &self.variant {
            ReaderVariant::Gridstore(reader) => {
                reader.read_values::<P, U, E>(point_offsets, callback, hw_counter_cell)
            }
            ReaderVariant::Arenastore(reader) => {
                reader.read_values::<P, U, E>(point_offsets, callback, hw_counter_cell)
            }
        }
    }

    /// Return the storage size in bytes.
    ///
    /// Approximate (total page capacity) in dynamic mode, exact in append-only mode.
    pub fn get_storage_size_bytes(&self) -> usize {
        match &self.variant {
            ReaderVariant::Gridstore(reader) => reader.get_storage_size_bytes(),
            ReaderVariant::Arenastore(reader) => reader.get_storage_size_bytes(),
        }
    }

    /// This method reloads the Blobstore data from "disk", so that
    /// it should make newly written data is readable.
    ///
    /// Important assumptions:
    ///
    /// - Only appending new data is supported, for modifications of existing data there are no consistency guarantees.
    /// - Partial writes are possible, it is up to the caller to read only fully written data.
    ///
    pub fn live_reload(&mut self, fs: &S::Fs) -> Result<()> {
        match &mut self.variant {
            ReaderVariant::Gridstore(reader) => reader.live_reload(fs),
            ReaderVariant::Arenastore(reader) => reader.live_reload(),
        }
    }
}

impl<V, S: UniversalRead> BlobstoreReader<V, S> {
    /// Returns `true` if BlobstoreReader is on disk, i.e. not populated on start/reload
    pub fn is_on_disk(&self) -> bool {
        match &self.variant {
            ReaderVariant::Gridstore(reader) => reader.is_on_disk(),
            ReaderVariant::Arenastore(reader) => reader.is_on_disk(),
        }
    }

    /// Drop disk cache for pages.
    pub fn clear_cache(&self) -> crate::Result<()> {
        match &self.variant {
            ReaderVariant::Gridstore(reader) => reader.clear_cache(),
            ReaderVariant::Arenastore(reader) => reader.clear_cache(),
        }
    }
}

/// Read-only storage for values of type `V`, operating in dynamic mode.
///
/// Holds pages and tracker directly (no locks) since it provides only read access.
/// For read-write access, use [`super::dynamic::Gridstore`].
#[derive(Debug)]
struct GridstoreReader<V, S: UniversalRead> {
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
    fn view(&self) -> GridstoreView<'_, V, S, ReadOnlyTracker<S>> {
        GridstoreView::new(&self.config, &self.tracker, &self.pages)
    }

    /// List all files belonging to this reader (tracker, pages, config).
    fn files(&self) -> Vec<PathBuf> {
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
    fn max_point_offset(&self) -> Result<PointOffset> {
        self.view().max_point_offset()
    }

    /// Open an existing read-only storage at the given path, with the already read config.
    ///
    /// Infers page count by scanning for page files on disk.
    fn open<Fs: UniversalReadFs<File = S>>(
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

    fn get_value<P: AccessPattern>(
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
    fn iter<F, E>(
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
    fn read_values<P, U, E>(
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
    /// For the precise used-space calculation, use [`super::Blobstore::get_storage_size_bytes`].
    fn get_storage_size_bytes(&self) -> usize {
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
    fn live_reload(&mut self, fs: &S::Fs) -> Result<()> {
        self.tracker.live_reload(fs)?;
        self.pages.live_reload(fs, self.populate)?;

        Ok(())
    }
}

impl<V, S: UniversalRead> GridstoreReader<V, S> {
    /// Returns `true` if the reader is on disk, i.e. not populated on start/reload
    fn is_on_disk(&self) -> bool {
        !self.populate.to_bool::<S>()
    }

    /// Drop disk cache for pages.
    fn clear_cache(&self) -> crate::Result<()> {
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

/// Read the storage config from the base path.
///
/// Shared helper used by the `open` paths of [`BlobstoreReader`] and [`super::Blobstore`], which
/// read the config first to select the operating mode.
pub(super) fn read_config<Fs: UniversalReadFs>(
    fs: &Fs,
    base_path: &std::path::Path,
) -> Result<StorageConfig> {
    let config_path = base_path.join(CONFIG_FILENAME);
    let config =
        read_json_via::<Fs, StorageConfig>(fs, &config_path).map_err(GridstoreError::from)?;
    config.validate().map_err(|message| {
        GridstoreError::service_error(format!(
            "Invalid gridstore config at {}: {message}",
            config_path.display(),
        ))
    })?;
    Ok(config)
}
