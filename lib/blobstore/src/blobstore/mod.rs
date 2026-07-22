pub(crate) mod gridstore;
mod logstore;
mod reader;
pub(crate) mod view;

#[cfg(test)]
mod tests;

use std::path::PathBuf;

use common::counter::counter_cell::CounterCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::referenced_counter::HwMetricRefCounter;
use common::generic_consts::AccessPattern;
use common::universal_io::{
    MmapFile, Populate, UniversalAppend, UniversalWrite, UniversalWriteFileOps, UserData,
};
use gridstore::Gridstore;
use logstore::Logstore;
pub use reader::BlobstoreReader;
use reader::CONFIG_FILENAME;
pub use view::BlobstoreView;

use crate::Result;
use crate::blob::Blob;
use crate::config::StorageConfig;
use crate::error::BlobstoreError;
use crate::tracker::PointOffset;
#[cfg(test)]
use crate::tracker::ValuePointer;

pub type Flusher = Box<dyn FnOnce() -> std::result::Result<(), BlobstoreError> + Send>;

/// Read-write storage for values of type `V`.
///
/// Operates in one of two modes, specified on creation and automatically selected when opening:
///
/// - [`Mode::Mutable`](crate::config::Mode::Mutable): backed by the inner `Gridstore` — values
///   can be updated and deleted, freed blocks are tracked and reused.
/// - [`Mode::AppendOnly`](crate::config::Mode::AppendOnly): backed by the inner `Logstore` —
///   append-only variant for serverless deployments, values cannot be updated or deleted, and
///   must be put in monotonically increasing point offset order.
///
/// Assumes sequential IDs to the values (0, 1, 2, 3, ...)
#[derive(Debug)]
pub enum Blobstore<V, S = MmapFile>
where
    S: UniversalWrite + UniversalAppend + 'static,
{
    Gridstore(Gridstore<V, S>),
    Logstore(Logstore<V, S>),
}

impl<V, S> Blobstore<V, S>
where
    V: Blob,
    S: UniversalWrite + UniversalAppend + 'static,
{
    /// List all files belonging to this storage.
    pub fn files(&self) -> Vec<PathBuf> {
        match self {
            Blobstore::Gridstore(storage) => storage.files(),
            Blobstore::Logstore(storage) => storage.files(),
        }
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            Blobstore::Gridstore(storage) => storage.immutable_files(),
            Blobstore::Logstore(storage) => storage.immutable_files(),
        }
    }

    /// Opens an existing storage, or initializes a new one.
    /// Depends on the existence of the config file at the `base_path`.
    ///
    /// In case of opening, it ignores the `create_options` parameter.
    pub fn open_or_create(
        fs: S::Fs,
        base_path: PathBuf,
        config_if_create: StorageConfig,
        populate: Populate,
    ) -> Result<Self> {
        let config_path = base_path.join(CONFIG_FILENAME);
        if config_path.exists() {
            Self::open(fs, base_path, populate)
        } else {
            fs.create_dir(&base_path)?;
            Self::new(fs, base_path, config_if_create)
        }
    }

    /// Initializes a new storage in the mode of the given config variant.
    ///
    /// `base_path` is the directory where the storage files will be stored.
    /// It should exist already.
    pub fn new(fs: S::Fs, base_path: PathBuf, config: StorageConfig) -> Result<Self> {
        config
            .validate()
            .map_err(BlobstoreError::validation_error)?;
        match config {
            StorageConfig::Mutable(config) => {
                let storage = Gridstore::new(fs, base_path, config)?;
                Ok(Self::Gridstore(storage))
            }
            StorageConfig::AppendOnly(config) => {
                let storage = Logstore::new(fs, base_path, config)?;
                Ok(Self::Logstore(storage))
            }
        }
    }

    /// Open an existing storage at the given path.
    ///
    /// The operating mode is automatically selected based on the persisted config.
    pub fn open(fs: S::Fs, base_path: PathBuf, populate: Populate) -> Result<Self> {
        match reader::read_config(&fs, &base_path)? {
            StorageConfig::Mutable(config) => {
                let storage = Gridstore::open(fs, base_path, config, populate)?;
                Ok(Self::Gridstore(storage))
            }
            StorageConfig::AppendOnly(config) => {
                let storage = Logstore::open(fs, base_path, config)?;
                Ok(Self::Logstore(storage))
            }
        }
    }

    /// Put a value in the storage.
    ///
    /// Returns true if the value existed previously and was updated, false if it was newly inserted.
    ///
    /// In append-only mode values must be put at monotonically increasing point offsets, and
    /// cannot be overwritten.
    pub fn put_value(
        &mut self,
        point_offset: PointOffset,
        value: &V,
        hw_counter: HwMetricRefCounter,
    ) -> Result<bool> {
        match self {
            Blobstore::Gridstore(storage) => storage.put_value(point_offset, value, hw_counter),
            Blobstore::Logstore(storage) => storage.put_value(point_offset, value, hw_counter),
        }
    }

    /// Delete a value from the storage.
    ///
    /// Returns None if the point_offset, page, or value was not found.
    /// Returns the deleted value otherwise.
    ///
    /// Not supported in append-only mode, returns an error.
    pub fn delete_value(&mut self, point_offset: PointOffset) -> Result<Option<V>> {
        match self {
            Blobstore::Gridstore(storage) => storage.delete_value(point_offset),
            Blobstore::Logstore(storage) => storage.delete_value(point_offset),
        }
    }

    /// Clear the storage, going back to the initial state.
    ///
    /// Completely wipes the storage, and recreates it in the same mode.
    pub fn clear(&mut self) -> Result<()> {
        match self {
            Blobstore::Gridstore(storage) => storage.clear(),
            Blobstore::Logstore(storage) => storage.clear(),
        }
    }

    /// Wipe the storage, drop all pages and delete the base directory.
    ///
    /// Takes ownership because this function leaves Blobstore in an inconsistent state which does
    /// not allow further usage. Use [`clear`](Self::clear) instead to clear and reuse the storage.
    pub fn wipe(self) -> Result<()> {
        match self {
            Blobstore::Gridstore(storage) => storage.wipe(),
            Blobstore::Logstore(storage) => storage.wipe(),
        }
    }

    /// Return the storage size in bytes.
    pub fn get_storage_size_bytes(&self) -> Result<usize> {
        match self {
            Blobstore::Gridstore(storage) => storage.get_storage_size_bytes(),
            Blobstore::Logstore(storage) => storage.get_storage_size_bytes(),
        }
    }

    pub fn get_value<P: AccessPattern>(
        &self,
        point_offset: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Option<V>> {
        match self {
            Blobstore::Gridstore(storage) => storage.get_value::<P>(point_offset, hw_counter),
            Blobstore::Logstore(storage) => storage.get_value::<P>(point_offset, hw_counter),
        }
    }

    /// Iterate over all given values and execute callback for each one.
    ///
    /// Return `false` from the callback to stop iteration early.
    pub fn read_values<P, U, E>(
        &self,
        point_offsets: impl Iterator<Item = (U, PointOffset)>,
        callback: impl FnMut(U, PointOffset, Option<V>) -> Result<(), E>,
        hw_counter_cell: &CounterCell,
    ) -> Result<(), E>
    where
        P: AccessPattern,
        U: UserData,
        E: From<BlobstoreError>,
    {
        match self {
            Blobstore::Gridstore(storage) => {
                storage.read_values::<P, U, E>(point_offsets, callback, hw_counter_cell)
            }
            Blobstore::Logstore(storage) => {
                storage.read_values::<P, U, E>(point_offsets, callback, hw_counter_cell)
            }
        }
    }

    #[cfg(test)]
    pub fn get_pointer(&self, point_offset: PointOffset) -> Option<ValuePointer> {
        match self {
            Blobstore::Gridstore(storage) => storage.get_pointer(point_offset),
            Blobstore::Logstore(storage) => storage.get_pointer(point_offset),
        }
    }

    pub fn max_point_offset(&self) -> PointOffset {
        match self {
            Blobstore::Gridstore(storage) => storage.max_point_offset(),
            Blobstore::Logstore(storage) => storage.max_point_offset(),
        }
    }

    /// Iterate over all values and execute callback for each one. Missing values are skipped.
    ///
    /// Return `false` from the callback to stop iteration early.
    pub fn iter<F, E>(&self, callback: F, hw_counter: HwMetricRefCounter) -> Result<(), E>
    where
        F: FnMut(PointOffset, V) -> Result<bool, E>,
        E: From<BlobstoreError>,
    {
        match self {
            Blobstore::Gridstore(storage) => storage.iter(callback, hw_counter),
            Blobstore::Logstore(storage) => storage.iter(callback, hw_counter),
        }
    }
}

impl<V, S: UniversalWrite + UniversalAppend + 'static> Blobstore<V, S> {
    /// Create flusher that durably persists all pending changes when invoked.
    pub fn flusher(&self) -> Flusher {
        match self {
            Blobstore::Gridstore(storage) => storage.flusher(),
            Blobstore::Logstore(storage) => storage.flusher(),
        }
    }

    /// Populate all parts of the storage in the mmap.
    ///
    /// No-op in append-only mode, which never populates its files into RAM.
    pub fn populate(&self) -> Result<()> {
        match self {
            Blobstore::Gridstore(storage) => storage.populate(),
            Blobstore::Logstore(storage) => storage.populate(),
        }
    }

    /// Drop disk cache.
    ///
    /// No-op in append-only mode, which never populates its files into RAM.
    pub fn clear_cache(&self) -> crate::Result<()> {
        match self {
            Blobstore::Gridstore(storage) => storage.clear_cache(),
            Blobstore::Logstore(storage) => storage.clear_cache(),
        }
    }
}

#[cfg(test)]
impl<V, S: UniversalWrite + UniversalAppend + 'static> Blobstore<V, S> {
    /// Get the inner Gridstore (mutable mode storage), panics if the storage is in another mode.
    fn as_gridstore(&self) -> &Gridstore<V, S> {
        match self {
            Blobstore::Gridstore(storage) => storage,
            Blobstore::Logstore(_) => panic!("storage is not in mutable mode"),
        }
    }

    /// Get the inner Gridstore (mutable mode storage), panics if the storage is in another mode.
    fn as_gridstore_mut(&mut self) -> &mut Gridstore<V, S> {
        match self {
            Blobstore::Gridstore(storage) => storage,
            Blobstore::Logstore(_) => panic!("storage is not in mutable mode"),
        }
    }

    /// Get the inner Logstore (append-only mode storage), panics if the storage is in another mode.
    fn as_logstore(&self) -> &Logstore<V, S> {
        match self {
            Blobstore::Gridstore(_) => panic!("storage is not in append-only mode"),
            Blobstore::Logstore(storage) => storage,
        }
    }
}
