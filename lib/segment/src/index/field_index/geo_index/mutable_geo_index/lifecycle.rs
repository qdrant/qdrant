use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{MmapFs, Populate};
use gridstore::config::StorageOptions;
use gridstore::{Gridstore, Mode};

use super::MutableGeoIndex;
use super::inner::InMemoryGeoIndex;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::{GeoPoint, RawGeoPoint};

/// Default options for Gridstore storage
const GRIDSTORE_OPTIONS: StorageOptions = StorageOptions {
    // Size of geo point values in index
    block_size_bytes: Some(size_of::<RawGeoPoint>()),
    // Compressing geo point values is unreasonable
    compression: Some(gridstore::config::Compression::None),
    // Scale page size down with block size, prevents overhead of first page when there's (almost) no values
    page_size_bytes: Some(size_of::<RawGeoPoint>() * 8192 * 32), // 4 to 8 MiB = block_size * region_blocks * regions,
    region_size_blocks: None,
};

impl MutableGeoIndex {
    /// Open and load mutable geo index from Gridstore storage
    ///
    /// The `create_if_missing` parameter indicates whether to create a new Gridstore if it does
    /// not exist. If false and files don't exist, the load function will indicate nothing could be
    /// loaded.
    pub fn open(path: PathBuf, create_if_missing: bool) -> OperationResult<Option<Self>> {
        let store = if create_if_missing {
            Gridstore::open_or_create(
                MmapFs,
                path,
                GRIDSTORE_OPTIONS,
                Populate::Blocking,
                Mode::default(),
            )
            .map_err(|err| {
                OperationError::service_error(format!(
                    "failed to open mutable geo index on gridstore: {err}"
                ))
            })?
        } else if path.exists() {
            Gridstore::open(MmapFs, path, Populate::Blocking, Mode::default()).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to open mutable geo index on gridstore: {err}"
                ))
            })?
        } else {
            // Files don't exist, cannot load
            return Ok(None);
        };

        // Load in-memory index from Gridstore
        let mut in_memory_index = InMemoryGeoIndex::new();
        let hw_counter = HardwareCounterCell::disposable();
        let hw_counter_ref = hw_counter.ref_payload_index_io_write_counter();
        store
            .iter::<_, OperationError>(
                |idx, values: Vec<RawGeoPoint>| {
                    let geo_points = values.into_iter().map(GeoPoint::from).collect::<Vec<_>>();
                    in_memory_index.add_many_geo_points(idx, geo_points, &hw_counter)?;
                    Ok(true)
                },
                hw_counter_ref,
            )
            .map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to load mutable geo index from gridstore: {err}"
                ))
            })?;

        Ok(Some(Self {
            in_memory_index,
            storage: store,
        }))
    }

    #[expect(dead_code)] // FIXME(rocksdb): leftover after removing rocksdb
    #[inline]
    pub(in super::super) fn clear(&mut self) -> OperationResult<()> {
        self.storage.clear().map_err(|err| {
            OperationError::service_error(format!("Failed to clear mutable geo index: {err}"))
        })
    }

    #[inline]
    pub(in super::super) fn wipe(self) -> OperationResult<()> {
        self.storage.wipe().map_err(|err| {
            OperationError::service_error(format!("Failed to wipe mutable geo index: {err}"))
        })
    }

    /// Clear cache
    ///
    /// Only clears cache of Gridstore storage if used. Does not clear in-memory representation of
    /// index.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_cache().map_err(|err| {
            OperationError::service_error(format!(
                "Failed to clear mutable geo index gridstore cache: {err}"
            ))
        })
    }

    #[inline]
    pub(in super::super) fn files(&self) -> Vec<PathBuf> {
        self.storage.files()
    }

    #[inline]
    pub(in super::super) fn flusher(&self) -> Flusher {
        let storage_flusher = self.storage.flusher();
        Box::new(move || storage_flusher().map_err(OperationError::from))
    }

    pub fn add_many_geo_points(
        &mut self,
        idx: PointOffsetType,
        values: Vec<GeoPoint>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // Update persisted storage
        if values.is_empty() {
            // We cannot store empty value, then delete instead
            self.storage.delete_value(idx)?;
        } else {
            let hw_counter_ref = hw_counter.ref_payload_index_io_write_counter();
            let raw_values = values
                .iter()
                .cloned()
                .map(RawGeoPoint::from)
                .collect::<Vec<_>>();
            self.storage
                .put_value(idx, &raw_values, hw_counter_ref)
                .map_err(|err| {
                    OperationError::service_error(format!(
                        "failed to put value in mutable geo index gridstore: {err}"
                    ))
                })?;
        }

        self.in_memory_index
            .add_many_geo_points(idx, values, hw_counter)
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        // Update persisted storage
        self.storage.delete_value(idx)?;

        self.in_memory_index.remove_point(idx)
    }

    pub fn into_in_memory_index(self) -> InMemoryGeoIndex {
        self.in_memory_index
    }
}
