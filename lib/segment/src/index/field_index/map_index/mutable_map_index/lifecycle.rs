use std::borrow::Borrow;
use std::collections::HashMap;
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::config::StorageOptions;
use gridstore::error::GridstoreError;
use gridstore::{Blob, Gridstore};
use roaring::RoaringBitmap;

use super::super::MapIndexKey;
use super::MutableMapIndex;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};

/// Default options for Gridstore storage
const fn default_gridstore_options(block_size: usize) -> StorageOptions {
    StorageOptions {
        // Size dependent on map value type
        block_size_bytes: Some(block_size),
        compression: Some(gridstore::config::Compression::None),
        page_size_bytes: Some(block_size * 8192 * 32), // 4 to 8 MiB = block_size * region_blocks * regions,
        region_size_blocks: None,
    }
}

impl<N: MapIndexKey + ?Sized> MutableMapIndex<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    /// Open and load mutable map index from Gridstore storage
    ///
    /// The `create_if_missing` parameter indicates whether to create a new Gridstore if it does
    /// not exist. If false and files don't exist, the load function will indicate nothing could be
    /// loaded.
    pub fn open_gridstore(path: PathBuf, create_if_missing: bool) -> OperationResult<Option<Self>> {
        let store = if create_if_missing {
            let options = default_gridstore_options(N::gridstore_block_size());
            Gridstore::open_or_create(path, options).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to open mutable map index on gridstore: {err}"
                ))
            })?
        } else if path.exists() {
            Gridstore::open(path).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to open mutable map index on gridstore: {err}"
                ))
            })?
        } else {
            // Files don't exist, cannot load
            return Ok(None);
        };

        // Load in-memory index from Gridstore
        let mut map = HashMap::<_, RoaringBitmap>::new();
        let mut point_to_values = Vec::new();
        let mut indexed_points = 0;
        let mut values_count = 0;

        let hw_counter = HardwareCounterCell::disposable();
        let hw_counter_ref = hw_counter.ref_payload_index_io_write_counter();
        store
            .iter::<_, GridstoreError>(
                |idx, values: Vec<_>| {
                    for value in values {
                        if point_to_values.len() <= idx as usize {
                            point_to_values.resize_with(idx as usize + 1, Vec::new)
                        }
                        let point_values = &mut point_to_values[idx as usize];

                        if point_values.is_empty() {
                            indexed_points += 1;
                        }
                        values_count += 1;

                        point_values.push(value.clone());
                        map.entry(value).or_default().insert(idx);
                    }

                    Ok(true)
                },
                hw_counter_ref,
            )
            // unwrap safety: never returns an error
            .unwrap();

        Ok(Some(Self {
            map,
            point_to_values,
            indexed_points,
            values_count,
            storage: store,
        }))
    }

    pub fn add_many_to_map<Q>(
        &mut self,
        idx: PointOffsetType,
        values: Vec<Q>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>
    where
        Q: Into<<N as MapIndexKey>::Owned> + Clone,
    {
        if values.is_empty() {
            return Ok(());
        }

        self.values_count += values.len();
        if self.point_to_values.len() <= idx as usize {
            self.point_to_values.resize_with(idx as usize + 1, Vec::new)
        }

        self.point_to_values[idx as usize] = Vec::with_capacity(values.len());

        let hw_counter_ref = hw_counter.ref_payload_index_io_write_counter();

        for value in values.clone() {
            let entry = self.map.entry(value.into());
            self.point_to_values[idx as usize].push(entry.key().clone());
            entry.or_default().insert(idx);
        }

        let values = values.into_iter().map(Into::into).collect::<Vec<_>>();
        self.storage
            .put_value(idx, &values, hw_counter_ref)
            .map_err(|err| {
                OperationError::service_error(format!(
                    "failed to put value in mutable map index gridstore: {err}"
                ))
            })?;

        self.indexed_points += 1;
        Ok(())
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        if self.point_to_values.len() <= idx as usize {
            return Ok(());
        }

        let removed_values = std::mem::take(&mut self.point_to_values[idx as usize]);

        if !removed_values.is_empty() {
            self.indexed_points -= 1;
        }
        self.values_count -= removed_values.len();

        for value in &removed_values {
            if let Some(vals) = self.map.get_mut(value.borrow()) {
                vals.remove(idx);
            }
        }

        self.storage.delete_value(idx)?;

        Ok(())
    }

    #[inline]
    pub(in super::super) fn clear(&mut self) -> OperationResult<()> {
        self.storage.clear().map_err(|err| {
            OperationError::service_error(format!("Failed to clear mutable map index: {err}"))
        })
    }

    #[inline]
    pub(in super::super) fn wipe(self) -> OperationResult<()> {
        self.storage.wipe().map_err(|err| {
            OperationError::service_error(format!("Failed to wipe mutable map index: {err}"))
        })
    }

    /// Clear gridstore disk cache. Does not affect the in-memory index.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_cache().map_err(|err| {
            OperationError::service_error(format!(
                "Failed to clear mutable map index gridstore cache: {err}"
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
}
