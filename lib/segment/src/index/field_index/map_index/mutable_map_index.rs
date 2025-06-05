use std::borrow::Borrow;
use std::collections::{BTreeSet, HashMap};
use std::iter;
use std::path::PathBuf;
use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::config::StorageOptions;
use gridstore::{Blob, Gridstore};
use parking_lot::RwLock;
use rocksdb::DB;

use super::{IdIter, IdRefIter, MapIndex, MapIndexKey};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;

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
    pub(super) map: HashMap<N::Owned, BTreeSet<PointOffsetType>>,
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
    RocksDb(DatabaseColumnScheduledDeleteWrapper),
    Gridstore(Arc<RwLock<Gridstore<Vec<T>>>>),
}

impl<N: MapIndexKey + ?Sized> MutableMapIndex<N>
where
    Vec<N::Owned>: Blob + Send + Sync,
{
    /// Open mutable map index from RocksDB storage
    ///
    /// Note: after opening, the data must be loaded into memory separately using [`load`].
    pub fn open_rocksdb(db: Arc<RwLock<DB>>, field_name: &str) -> Self {
        let store_cf_name = MapIndex::<N>::storage_cf_name(field_name);
        let db_wrapper = DatabaseColumnScheduledDeleteWrapper::new(DatabaseColumnWrapper::new(
            db,
            &store_cf_name,
        ));
        Self::open_rocksdb_db_wrapper(db_wrapper)
    }

    pub fn open_rocksdb_db_wrapper(db_wrapper: DatabaseColumnScheduledDeleteWrapper) -> Self {
        Self {
            map: Default::default(),
            point_to_values: Vec::new(),
            indexed_points: 0,
            values_count: 0,
            storage: Storage::RocksDb(db_wrapper),
        }
    }

    /// Open mutable map index from Gridstore storage
    ///
    /// Note: after opening, the data must be loaded into memory separately using [`load`].
    pub fn open_gridstore(path: PathBuf) -> OperationResult<Self> {
        let options = default_gridstore_options(N::gridstore_block_size());
        let store = Gridstore::open_or_create(path, options).map_err(|err| {
            OperationError::service_error(format!(
                "failed to open mutable map index on gridstore: {err}"
            ))
        })?;
        Ok(Self {
            map: Default::default(),
            point_to_values: Vec::new(),
            indexed_points: 0,
            values_count: 0,
            storage: Storage::Gridstore(Arc::new(RwLock::new(store))),
        })
    }

    /// Load storage
    ///
    /// Loads in-memory index from backing RocksDB or Gridstore storage.
    pub(super) fn load(&mut self) -> OperationResult<bool> {
        match self.storage {
            Storage::RocksDb(_) => self.load_rocksdb(),
            Storage::Gridstore(_) => self.load_gridstore(),
        }
    }

    /// Load from RocksDB storage
    ///
    /// Loads in-memory index from RocksDB storage.
    pub fn load_rocksdb(&mut self) -> OperationResult<bool> {
        let Storage::RocksDb(db_wrapper) = &self.storage else {
            return Err(OperationError::service_error(
                "Failed to load index from RocksDB, using different storage backend",
            ));
        };

        if !db_wrapper.has_column_family()? {
            return Ok(false);
        }

        self.indexed_points = 0;
        for (record, _) in db_wrapper.lock_db().iter()? {
            let record = std::str::from_utf8(&record).map_err(|_| {
                OperationError::service_error("Index load error: UTF8 error while DB parsing")
            })?;
            let (value, idx) = MapIndex::<N>::decode_db_record(record)?;

            if self.point_to_values.len() <= idx as usize {
                self.point_to_values.resize_with(idx as usize + 1, Vec::new)
            }
            let point_values = &mut self.point_to_values[idx as usize];

            if point_values.is_empty() {
                self.indexed_points += 1;
            }
            self.values_count += 1;

            point_values.push(value.clone());
            self.map.entry(value).or_default().insert(idx);
        }
        Ok(true)
    }

    /// Load from Gridstore storage
    ///
    /// Loads in-memory index from Gridstore storage.
    fn load_gridstore(&mut self) -> OperationResult<bool> {
        let Storage::Gridstore(store) = &self.storage else {
            return Err(OperationError::service_error(
                "Failed to load index from Gridstore, using different storage backend",
            ));
        };

        self.indexed_points = 0;

        let hw_counter = HardwareCounterCell::disposable();
        let hw_counter_ref = hw_counter.ref_payload_index_io_write_counter();
        store
            .read()
            .iter::<_, ()>(
                |idx, values| {
                    for value in values {
                        if self.point_to_values.len() <= idx as usize {
                            self.point_to_values.resize_with(idx as usize + 1, Vec::new)
                        }
                        let point_values = &mut self.point_to_values[idx as usize];

                        if point_values.is_empty() {
                            self.indexed_points += 1;
                        }
                        self.values_count += 1;

                        point_values.push(value.clone());
                        self.map.entry(value.clone()).or_default().insert(idx);
                    }

                    Ok(true)
                },
                hw_counter_ref,
            )
            // unwrap safety: never returns an error
            .unwrap();

        Ok(true)
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

        match &self.storage {
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
                    .write()
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
                vals.remove(&idx);
            }
        }

        match &self.storage {
            Storage::RocksDb(db_wrapper) => {
                for value in &removed_values {
                    let key = MapIndex::encode_db_record(value.borrow(), idx);
                    db_wrapper.remove(key)?;
                }
            }
            Storage::Gridstore(store) => {
                store.write().delete_value(idx);
            }
        }

        Ok(())
    }

    #[inline]
    pub(super) fn clear(&self) -> OperationResult<()> {
        match &self.storage {
            Storage::RocksDb(db_wrapper) => db_wrapper.recreate_column_family(),
            Storage::Gridstore(store) => store.write().clear().map_err(|err| {
                OperationError::service_error(format!("Failed to clear mutable map index: {err}",))
            }),
        }
    }

    /// Clear cache
    ///
    /// Only clears cache of Gridstore storage if used. Does not clear in-memory representation of
    /// index.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match &self.storage {
            Storage::RocksDb(_) => Ok(()),
            Storage::Gridstore(index) => index.read().clear_cache().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to clear mutable map index gridstore cache: {err}"
                ))
            }),
        }
    }

    #[inline]
    pub(super) fn files(&self) -> Vec<PathBuf> {
        match &self.storage {
            Storage::RocksDb(_) => vec![],
            Storage::Gridstore(store) => store.read().files(),
        }
    }

    #[inline]
    pub(super) fn flusher(&self) -> Flusher {
        match &self.storage {
            Storage::RocksDb(db_wrapper) => db_wrapper.flusher(),
            Storage::Gridstore(store) => {
                let store = store.clone();
                Box::new(move || {
                    store.read().flush().map_err(|err| {
                        OperationError::service_error(format!(
                            "Failed to flush mutable map index gridstore: {err}"
                        ))
                    })
                })
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
        self.map.get(value).map(|p| p.len())
    }

    pub fn iter_counts_per_value(&self) -> impl Iterator<Item = (&N, usize)> + '_ {
        self.map.iter().map(|(k, v)| (k.borrow(), v.len()))
    }

    pub fn iter_values_map(&self) -> impl Iterator<Item = (&N, IdIter)> {
        self.map
            .iter()
            .map(move |(k, v)| (k.borrow(), Box::new(v.iter().copied()) as IdIter))
    }

    pub fn get_iterator(&self, value: &N) -> IdRefIter<'_> {
        self.map
            .get(value)
            .map(|ids| Box::new(ids.iter()) as Box<dyn Iterator<Item = &PointOffsetType>>)
            .unwrap_or_else(|| Box::new(iter::empty::<&PointOffsetType>()))
    }

    pub fn iter_values(&self) -> Box<dyn Iterator<Item = &N> + '_> {
        Box::new(self.map.keys().map(|v| v.borrow()))
    }
}
