use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::iter;
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::config::StorageOptions;
use gridstore::error::GridstoreError;
use gridstore::{Blob, Gridstore};
use roaring::RoaringBitmap;

use super::{IdIter, MapIndexKey};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::payload_config::StorageType;

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

pub struct MutableMapIndex<N: MapIndexKey + ?Sized>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    pub(super) map: HashMap<<N as MapIndexKey>::Owned, RoaringBitmap>,
    pub(super) point_to_values: Vec<Vec<<N as MapIndexKey>::Owned>>,
    /// Amount of point which have at least one indexed payload value
    pub(super) indexed_points: usize,
    pub(super) values_count: usize,
    storage: Storage<<N as MapIndexKey>::Owned>,
}

enum Storage<T>
where
    Vec<T>: Blob + Send + Sync,
{
    Gridstore(Gridstore<Vec<T>>),
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
            storage: Storage::Gridstore(store),
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

        match &mut self.storage {
            Storage::Gridstore(store) => {
                let hw_counter_ref = hw_counter.ref_payload_index_io_write_counter();

                for value in values.clone() {
                    let entry = self.map.entry(value.into());
                    self.point_to_values[idx as usize].push(entry.key().clone());
                    entry.or_default().insert(idx);
                }

                let values = values.into_iter().map(Into::into).collect::<Vec<_>>();
                store
                    .put_value(idx, &values, hw_counter_ref)
                    .map_err(|err| {
                        OperationError::service_error(format!(
                            "failed to put value in mutable map index gridstore: {err}"
                        ))
                    })?;
            }
        }

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

        match &mut self.storage {
            Storage::Gridstore(store) => {
                store.delete_value(idx)?;
            }
        }

        Ok(())
    }

    #[inline]
    pub(super) fn clear(&mut self) -> OperationResult<()> {
        match &mut self.storage {
            Storage::Gridstore(store) => store.clear().map_err(|err| {
                OperationError::service_error(format!("Failed to clear mutable map index: {err}",))
            }),
        }
    }

    #[inline]
    pub(super) fn wipe(self) -> OperationResult<()> {
        match self.storage {
            Storage::Gridstore(store) => store.wipe().map_err(|err| {
                OperationError::service_error(format!("Failed to wipe mutable map index: {err}",))
            }),
        }
    }

    /// Clear cache
    ///
    /// Only clears cache of Gridstore storage if used. Does not clear in-memory representation of
    /// index.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match &self.storage {
            Storage::Gridstore(index) => index.clear_cache().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to clear mutable map index gridstore cache: {err}"
                ))
            }),
        }
    }

    #[inline]
    pub(super) fn files(&self) -> Vec<PathBuf> {
        match &self.storage {
            Storage::Gridstore(store) => store.files(),
        }
    }

    #[inline]
    pub(super) fn flusher(&self) -> Flusher {
        match &self.storage {
            Storage::Gridstore(store) => {
                let storage_flusher = store.flusher();
                Box::new(move || storage_flusher().map_err(OperationError::from))
            }
        }
    }

    pub fn check_values_any(&self, idx: PointOffsetType, check_fn: impl Fn(&N) -> bool) -> bool {
        self.point_to_values
            .get(idx as usize)
            .map(|values| values.iter().any(|v| check_fn(v.borrow())))
            .unwrap_or(false)
    }

    pub fn get_values(
        &self,
        idx: PointOffsetType,
    ) -> Option<impl Iterator<Item = Cow<'_, N>> + '_> {
        Some(
            self.point_to_values
                .get(idx as usize)?
                .iter()
                .map(|v| Cow::Borrowed(v.borrow())),
        )
    }

    pub fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        self.point_to_values.get(idx as usize).map(Vec::len)
    }

    pub fn get_indexed_points(&self) -> usize {
        self.indexed_points
    }

    pub fn get_values_count(&self) -> usize {
        self.values_count
    }

    pub fn get_unique_values_count(&self) -> usize {
        self.map.len()
    }

    pub fn get_count_for_value(&self, value: &N) -> Option<usize> {
        self.map.get(value).map(|p| p.len() as usize)
    }

    pub fn for_points_values(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        mut f: impl FnMut(PointOffsetType, &[<N as MapIndexKey>::Owned]),
    ) {
        points.for_each(|idx| {
            if let Some(values) = self.point_to_values.get(idx as usize) {
                f(idx, values);
            }
        });
    }

    pub fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        mut f: impl FnMut(&N, usize) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.map.iter().try_for_each(|(k, v)| {
            let count = match deferred_internal_id {
                Some(deferred_internal_id) => v.range_cardinality(..deferred_internal_id) as usize,
                None => v.len() as usize,
            };
            f(k.borrow(), count)
        })
    }

    pub fn for_each_value_map(
        &self,
        mut f: impl FnMut(&N, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.map
            .iter()
            .try_for_each(|(k, v)| f(k.borrow(), &mut v.iter()))
    }

    pub fn get_iterator(&self, value: &N) -> IdIter<'_> {
        self.map
            .get(value)
            .map(|ids| Box::new(ids.iter()) as IdIter)
            .unwrap_or_else(|| Box::new(iter::empty::<PointOffsetType>()))
    }

    pub fn for_each_value(
        &self,
        mut f: impl FnMut(&N) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.map.keys().try_for_each(|v| f(v.borrow()))
    }

    pub fn storage_type(&self) -> StorageType {
        match &self.storage {
            Storage::Gridstore(_) => StorageType::Gridstore,
        }
    }

    /// Approximate RAM usage in bytes for in-memory index structures.
    pub fn ram_usage_bytes(&self) -> usize {
        let Self {
            map,
            point_to_values,
            indexed_points: _,
            values_count: _,
            storage: _, // disk-backed, accounted via files
        } = self;

        let hashmap_entry_overhead = std::mem::size_of::<u64>() + std::mem::size_of::<usize>();
        let map_base_bytes = map.capacity()
            * (std::mem::size_of::<<N as MapIndexKey>::Owned>()
                + std::mem::size_of::<RoaringBitmap>()
                + hashmap_entry_overhead);
        // Account for heap-allocated key data (e.g., long strings)
        let map_key_heap_bytes: usize = map.keys().map(|k| N::owned_heap_bytes(k)).sum();
        let map_bitmap_bytes: usize = map.values().map(|bitmap| bitmap.serialized_size()).sum();
        let map_bytes = map_base_bytes + map_key_heap_bytes + map_bitmap_bytes;
        let ptv_bytes: usize = point_to_values.capacity()
            * std::mem::size_of::<Vec<<N as MapIndexKey>::Owned>>()
            + point_to_values
                .iter()
                .map(|v| v.capacity() * std::mem::size_of::<<N as MapIndexKey>::Owned>())
                .sum::<usize>();
        map_bytes + ptv_bytes
    }
}
