use std::path::PathBuf;

use common::counter::counter_cell::CounterCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::referenced_counter::HwMetricRefCounter;
use common::generic_consts::AccessPattern;
use common::universal_io::{
    CachedReadFs, Populate, UniversalRead, UniversalReadFs, UserData, read_json_via,
};

use super::arenastore::ArenastoreReader;
use super::gridstore::GridstoreReader;
use super::view::BlobstoreView;
use crate::Result;
use crate::blob::Blob;
use crate::config::{Mode, StorageConfig};
use crate::error::GridstoreError;
use crate::tracker::PointOffset;

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
            Mode::Mutable => GridstoreReader::<V, S>::preopen(fs, &base_path, populate),
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
            Mode::Mutable => {
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
    /// Note: in mutable mode this does not include bitmask files. Use
    /// [`super::Blobstore::files`] for the full list.
    pub fn files(&self) -> Vec<PathBuf> {
        match &self.variant {
            ReaderVariant::Gridstore(reader) => reader.files(),
            ReaderVariant::Arenastore(reader) => reader.files(),
        }
    }

    /// Exclusive upper bound of point offsets that may have a value.
    ///
    /// In mutable mode this is the writer-maintained count read from the stored tracker header,
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
    /// Approximate (total page capacity) in mutable mode, exact in append-only mode.
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
            ReaderVariant::Arenastore(reader) => reader.live_reload(fs),
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
            "Invalid blobstore config at {}: {message}",
            config_path.display(),
        ))
    })?;
    Ok(config)
}
