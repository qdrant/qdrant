mod append_only;
mod dynamic;
mod reader;
pub(crate) mod view;

#[cfg(test)]
mod tests;

use std::path::PathBuf;

use append_only::AppendOnlyGridstore;
use common::counter::counter_cell::CounterCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::referenced_counter::HwMetricRefCounter;
use common::generic_consts::AccessPattern;
use common::universal_io::{MmapFile, Populate, UniversalWrite, UniversalWriteFileOps, UserData};
use dynamic::DynamicGridstore;
use reader::CONFIG_FILENAME;
pub use reader::GridstoreReader;
pub use view::GridstoreView;

use crate::Result;
use crate::blob::Blob;
use crate::config::{Mode, StorageOptions};
use crate::error::GridstoreError;
use crate::tracker::PointOffset;
#[cfg(test)]
use crate::tracker::ValuePointer;

pub type Flusher = Box<dyn FnOnce() -> std::result::Result<(), GridstoreError> + Send>;

/// Read-write storage for values of type `V`.
///
/// Operates in one of two modes, specified on creation and automatically selected when opening:
///
/// - [`Mode::Dynamic`]: values can be updated and deleted, freed blocks are tracked and reused.
/// - [`Mode::AppendOnly`]: append-only variant for serverless deployments, values cannot be
///   updated or deleted, and must be put in monotonically increasing point offset order.
///
/// Assumes sequential IDs to the values (0, 1, 2, 3, ...)
#[derive(Debug)]
pub struct Gridstore<V, S = MmapFile>
where
    S: UniversalWrite + 'static,
{
    variant: GridstoreVariant<V, S>,
}

/// Mode specific implementation of the storage, see [`Mode`].
#[derive(Debug)]
enum GridstoreVariant<V, S>
where
    S: UniversalWrite + 'static,
{
    Dynamic(DynamicGridstore<V, S>),
    AppendOnly(AppendOnlyGridstore<V, S>),
}

impl<V, S> Gridstore<V, S>
where
    V: Blob,
    S: UniversalWrite + 'static,
{
    /// List all files belonging to this storage.
    pub fn files(&self) -> Vec<PathBuf> {
        match &self.variant {
            GridstoreVariant::Dynamic(storage) => storage.files(),
            GridstoreVariant::AppendOnly(storage) => storage.files(),
        }
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        match &self.variant {
            GridstoreVariant::Dynamic(storage) => storage.immutable_files(),
            GridstoreVariant::AppendOnly(storage) => storage.immutable_files(),
        }
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

    /// Initializes a new storage in the mode given through the options.
    ///
    /// `base_path` is the directory where the storage files will be stored.
    /// It should exist already.
    pub fn new(fs: S::Fs, base_path: PathBuf, options: StorageOptions) -> Result<Self> {
        match options.mode.unwrap_or_default() {
            Mode::Dynamic => {
                let storage = DynamicGridstore::new(fs, base_path, options)?;
                Ok(Self {
                    variant: GridstoreVariant::Dynamic(storage),
                })
            }
            // The append-only mode does not use the universal io backend, it reads and writes
            // files directly
            Mode::AppendOnly => {
                let storage = AppendOnlyGridstore::new(base_path, options)?;
                Ok(Self {
                    variant: GridstoreVariant::AppendOnly(storage),
                })
            }
        }
    }

    /// Open an existing storage at the given path.
    ///
    /// The operating mode is automatically selected based on the persisted config.
    pub fn open(fs: S::Fs, base_path: PathBuf, populate: Populate) -> Result<Self> {
        let config = reader::read_config(&fs, &base_path)?;
        match config.mode {
            Mode::Dynamic => {
                let storage = DynamicGridstore::open(fs, base_path, config, populate)?;
                Ok(Self {
                    variant: GridstoreVariant::Dynamic(storage),
                })
            }
            // The append-only mode does not use the universal io backend, it reads and writes
            // files directly
            Mode::AppendOnly => {
                let storage = AppendOnlyGridstore::open(base_path, config)?;
                Ok(Self {
                    variant: GridstoreVariant::AppendOnly(storage),
                })
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
        match &mut self.variant {
            GridstoreVariant::Dynamic(storage) => {
                storage.put_value(point_offset, value, hw_counter)
            }
            GridstoreVariant::AppendOnly(storage) => {
                storage.put_value(point_offset, value, hw_counter)
            }
        }
    }

    /// Delete a value from the storage.
    ///
    /// Returns None if the point_offset, page, or value was not found.
    /// Returns the deleted value otherwise.
    ///
    /// Not supported in append-only mode, returns an error.
    pub fn delete_value(&mut self, point_offset: PointOffset) -> Result<Option<V>> {
        match &mut self.variant {
            GridstoreVariant::Dynamic(storage) => storage.delete_value(point_offset),
            GridstoreVariant::AppendOnly(storage) => storage.delete_value(point_offset),
        }
    }

    /// Clear the storage, going back to the initial state.
    ///
    /// Completely wipes the storage, and recreates it in the same mode.
    pub fn clear(&mut self) -> Result<()> {
        match &mut self.variant {
            GridstoreVariant::Dynamic(storage) => storage.clear(),
            GridstoreVariant::AppendOnly(storage) => storage.clear(),
        }
    }

    /// Wipe the storage, drop all pages and delete the base directory.
    ///
    /// Takes ownership because this function leaves Gridstore in an inconsistent state which does
    /// not allow further usage. Use [`clear`](Self::clear) instead to clear and reuse the storage.
    pub fn wipe(self) -> Result<()> {
        match self.variant {
            GridstoreVariant::Dynamic(storage) => storage.wipe(),
            GridstoreVariant::AppendOnly(storage) => storage.wipe(),
        }
    }

    /// Return the storage size in bytes.
    pub fn get_storage_size_bytes(&self) -> Result<usize> {
        match &self.variant {
            GridstoreVariant::Dynamic(storage) => storage.get_storage_size_bytes(),
            GridstoreVariant::AppendOnly(storage) => storage.get_storage_size_bytes(),
        }
    }

    pub fn get_value<P: AccessPattern>(
        &self,
        point_offset: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Option<V>> {
        match &self.variant {
            GridstoreVariant::Dynamic(storage) => storage.get_value::<P>(point_offset, hw_counter),
            GridstoreVariant::AppendOnly(storage) => {
                storage.get_value::<P>(point_offset, hw_counter)
            }
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
        E: From<GridstoreError>,
    {
        match &self.variant {
            GridstoreVariant::Dynamic(storage) => {
                storage.read_values::<P, U, E>(point_offsets, callback, hw_counter_cell)
            }
            GridstoreVariant::AppendOnly(storage) => {
                storage.read_values::<P, U, E>(point_offsets, callback, hw_counter_cell)
            }
        }
    }

    #[cfg(test)]
    pub fn get_pointer(&self, point_offset: PointOffset) -> Option<ValuePointer> {
        match &self.variant {
            GridstoreVariant::Dynamic(storage) => storage.get_pointer(point_offset),
            GridstoreVariant::AppendOnly(storage) => storage.get_pointer(point_offset),
        }
    }

    pub fn max_point_offset(&self) -> PointOffset {
        match &self.variant {
            GridstoreVariant::Dynamic(storage) => storage.max_point_offset(),
            GridstoreVariant::AppendOnly(storage) => storage.max_point_offset(),
        }
    }

    /// Iterate over all values and execute callback for each one. Missing values are skipped.
    ///
    /// Return `false` from the callback to stop iteration early.
    pub fn iter<F, E>(&self, callback: F, hw_counter: HwMetricRefCounter) -> Result<(), E>
    where
        F: FnMut(PointOffset, V) -> Result<bool, E>,
        E: From<GridstoreError>,
    {
        match &self.variant {
            GridstoreVariant::Dynamic(storage) => storage.iter(callback, hw_counter),
            GridstoreVariant::AppendOnly(storage) => storage.iter(callback, hw_counter),
        }
    }
}

impl<V, S: UniversalWrite + 'static> Gridstore<V, S> {
    /// Create flusher that durably persists all pending changes when invoked.
    pub fn flusher(&self) -> Flusher {
        match &self.variant {
            GridstoreVariant::Dynamic(storage) => storage.flusher(),
            GridstoreVariant::AppendOnly(storage) => storage.flusher(),
        }
    }

    /// Populate all parts of the storage in the mmap.
    ///
    /// No-op in append-only mode, which does not memory map its files.
    pub fn populate(&self) -> Result<()> {
        match &self.variant {
            GridstoreVariant::Dynamic(storage) => storage.populate(),
            GridstoreVariant::AppendOnly(storage) => storage.populate(),
        }
    }

    /// Drop disk cache.
    ///
    /// No-op in append-only mode, which does not memory map its files.
    pub fn clear_cache(&self) -> crate::Result<()> {
        match &self.variant {
            GridstoreVariant::Dynamic(storage) => storage.clear_cache(),
            GridstoreVariant::AppendOnly(storage) => storage.clear_cache(),
        }
    }
}

#[cfg(test)]
impl<V, S: UniversalWrite + 'static> Gridstore<V, S> {
    /// Get the inner dynamic storage, panics if the storage is in another mode.
    fn as_dynamic(&self) -> &DynamicGridstore<V, S> {
        match &self.variant {
            GridstoreVariant::Dynamic(storage) => storage,
            GridstoreVariant::AppendOnly(_) => panic!("storage is not in dynamic mode"),
        }
    }

    /// Get the inner dynamic storage, panics if the storage is in another mode.
    fn as_dynamic_mut(&mut self) -> &mut DynamicGridstore<V, S> {
        match &mut self.variant {
            GridstoreVariant::Dynamic(storage) => storage,
            GridstoreVariant::AppendOnly(_) => panic!("storage is not in dynamic mode"),
        }
    }

    /// Get the inner append-only storage, panics if the storage is in another mode.
    fn as_append_only(&self) -> &AppendOnlyGridstore<V, S> {
        match &self.variant {
            GridstoreVariant::Dynamic(_) => panic!("storage is not in append-only mode"),
            GridstoreVariant::AppendOnly(storage) => storage,
        }
    }
}
