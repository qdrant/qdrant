use std::borrow::Borrow;
use std::collections::HashMap;
use std::iter;
use std::path::PathBuf;
#[cfg(feature = "rocksdb")]
use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::config::StorageOptions;
use gridstore::{Blob, Gridstore};
#[cfg(feature = "rocksdb")]
use parking_lot::RwLock;
use roaring::RoaringBitmap;
#[cfg(feature = "rocksdb")]
use rocksdb::DB;

#[cfg(feature = "rocksdb")]
use super::MapIndex;
use super::{IdIter, MapIndexKey};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
#[cfg(feature = "rocksdb")]
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
#[cfg(feature = "rocksdb")]
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
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
    Vec<N::Owned>: Blob + Send + Sync,
{
    pub(super) map: HashMap<N::Owned, RoaringBitmap>,
    pub(super) point_to_values: Vec<Vec<N::Owned>>,
    /// Amount of point which have at least one indexed payload value
    pub(super) indexed_points: usize,
    pub(super) values_count: usize,
    storage: Storage<N::Owned>,
}

enum Storage<T>
where
    Vec<T>: Blob + Send + Sync,
{
    #[cfg(feature = "rocksdb")]
    RocksDb(DatabaseColumnScheduledDeleteWrapper),
    Gridstore(Gridstore<Vec<T>>),
}

impl<N: MapIndexKey + ?Sized> MutableMapIndex<N>
where
    Vec<N::Owned>: Blob + Send + Sync,
{
    /// Open mutable map index from RocksDB storage
    #[cfg(feature = "rocksdb")]
    pub fn open_rocksdb(
        db: Arc<RwLock<DB>>,
        field_name: &str,
        create_if_missing: bool,
    ) -> OperationResult<Option<Self>> {
        let store_cf_name = MapIndex::<N>::storage_cf_name(field_name);
        let db_wrapper = DatabaseColumnScheduledDeleteWrapper::new(DatabaseColumnWrapper::new(
            db,
            &store_cf_name,
        ));
        Self::open_rocksdb_db_wrapper(db_wrapper, create_if_missing)
    }

    #[cfg(feature = "rocksdb")]
    pub fn open_rocksdb_db_wrapper(
        db_wrapper: DatabaseColumnScheduledDeleteWrapper,
        create_if_missing: bool,
    ) -> OperationResult<Option<Self>> {
        if !db_wrapper.has_column_family()? {
            if create_if_missing {
                db_wrapper.recreate_column_family()?;
            } else {
                // Column family doesn't exist, cannot load
                return Ok(None);
            }
        };

        // Load in-memory index from RocksDB
        let mut map = HashMap::<_, RoaringBitmap>::new();
        let mut point_to_values = Vec::new();
        let mut indexed_points = 0;
        let mut values_count = 0;
        for (record, _) in db_wrapper.lock_db().iter()? {
            let record = std::str::from_utf8(&record).map_err(|_| {
                OperationError::service_error("Index load error: UTF8 error while DB parsing")
            })?;
            let (value, idx) = MapIndex::<N>::decode_db_record(record)?;

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

        Ok(Some(Self {
            map,
            point_to_values,
            indexed_points,
            values_count,
            storage: Storage::RocksDb(db_wrapper),
        }))
    }

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
            .iter::<_, ()>(
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
        Q: Into<N::Owned> + Clone,
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
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => {
                let mut hw_cell_wb = hw_counter
                    .payload_index_io_write_counter()
                    .write_back_counter();

                for value in values {
                    let entry = self.map.entry(value.into());
                    self.point_to_values[idx as usize].push(entry.key().clone());
                    let db_record = MapIndex::encode_db_record(entry.key().borrow(), idx);
                    entry.or_default().insert(idx);
                    hw_cell_wb.incr_delta(db_record.len());
                    db_wrapper.put(db_record, [])?;
                }
            }
            Storage::Gridstore(store) => {
                let hw_counter_ref = hw_counter.ref_payload_index_io_write_counter();

                for value in values.clone() {
                    let entry = self.map.entry(value.into());
                    self.point_to_values[idx as usize].push(entry.key().clone());
                    entry.or_default().insert(idx);
                }

                let values = values.into_iter().map(|v| v.into()).collect::<Vec<_>>();
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
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => {
                for value in &removed_values {
                    let key = MapIndex::encode_db_record(value.borrow(), idx);
                    db_wrapper.remove(key)?;
                }
            }
            Storage::Gridstore(store) => {
                store.delete_value(idx);
            }
        }

        Ok(())
    }

    #[inline]
    pub(super) fn clear(&mut self) -> OperationResult<()> {
        match &mut self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => db_wrapper.recreate_column_family(),
            Storage::Gridstore(store) => store.clear().map_err(|err| {
                OperationError::service_error(format!("Failed to clear mutable map index: {err}",))
            }),
        }
    }

    #[inline]
    pub(super) fn wipe(self) -> OperationResult<()> {
        match self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => db_wrapper.remove_column_family(),
            Storage::Gridstore(mut store) => store.clear().map_err(|err| {
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
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(_) => Ok(()),
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
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(_) => vec![],
            Storage::Gridstore(store) => store.files(),
        }
    }

    #[inline]
    pub(super) fn flusher(&self) -> (Flusher, Flusher) {
        match &self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => db_wrapper.flusher(),
            Storage::Gridstore(store) => {
                let (stage_1_flusher, stage_2_flusher) = store.flusher();

                let stage_1_flusher = Box::new(move || {
                    stage_1_flusher().map_err(|err| {
                        OperationError::service_error(format!(
                            "Failed to flush mutable map index gridstore: {err}"
                        ))
                    })
                });
                let stage_2_flusher = Box::new(move || {
                    stage_2_flusher().map_err(|err| {
                        OperationError::service_error(format!(
                            "Failed to flush mutable map index gridstore deletes: {err}"
                        ))
                    })
                });

                (stage_1_flusher, stage_2_flusher)
            }
        }
    }

    pub fn check_values_any(&self, idx: PointOffsetType, check_fn: impl Fn(&N) -> bool) -> bool {
        self.point_to_values
            .get(idx as usize)
            .map(|values| values.iter().any(|v| check_fn(v.borrow())))
            .unwrap_or(false)
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<impl Iterator<Item = &N> + '_> {
        Some(
            self.point_to_values
                .get(idx as usize)?
                .iter()
                .map(|v| v.borrow()),
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

    pub fn iter_counts_per_value(&self) -> impl Iterator<Item = (&N, usize)> + '_ {
        self.map.iter().map(|(k, v)| (k.borrow(), v.len() as usize))
    }

    pub fn iter_values_map(&self) -> impl Iterator<Item = (&N, IdIter<'_>)> {
        self.map
            .iter()
            .map(move |(k, v)| (k.borrow(), Box::new(v.iter()) as IdIter))
    }

    pub fn get_iterator(&self, value: &N) -> IdIter<'_> {
        self.map
            .get(value)
            .map(|ids| Box::new(ids.iter()) as IdIter)
            .unwrap_or_else(|| Box::new(iter::empty::<PointOffsetType>()))
    }

    pub fn iter_values(&self) -> Box<dyn Iterator<Item = &N> + '_> {
        Box::new(self.map.keys().map(|v| v.borrow()))
    }

    pub fn storage_type(&self) -> StorageType {
        match &self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(_) => StorageType::RocksDb,
            Storage::Gridstore(_) => StorageType::Gridstore,
        }
    }

    #[cfg(feature = "rocksdb")]
    pub fn is_rocksdb(&self) -> bool {
        match self.storage {
            Storage::RocksDb(_) => true,
            Storage::Gridstore(_) => false,
        }
    }
}
