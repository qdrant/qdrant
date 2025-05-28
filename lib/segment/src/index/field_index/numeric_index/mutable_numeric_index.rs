use std::collections::BTreeSet;
use std::ops::Bound;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::PathBuf;
use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::config::StorageOptions;
use gridstore::{Blob, Gridstore};
use parking_lot::RwLock;
use rocksdb::DB;

use super::mmap_numeric_index::MmapNumericIndex;
use super::{
    Encodable, HISTOGRAM_MAX_BUCKET_SIZE, HISTOGRAM_PRECISION, numeric_index_storage_cf_name,
};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::index::field_index::histogram::{Histogram, Numericable, Point};
use crate::index::field_index::mmap_point_to_values::MmapValue;

pub struct MutableNumericIndex<T: Encodable + Numericable + Blob> {
    // Backing storage, source of state, persists deletions
    storage: Storage<T>,
    in_memory_index: InMemoryNumericIndex<T>,
}

enum Storage<T: Encodable + Numericable + Blob> {
    RocksDb(DatabaseColumnScheduledDeleteWrapper),
    Gridstore(Arc<RwLock<Gridstore<Vec<T>>>>),
}

// Numeric Index with insertions and deletions without persistence
pub struct InMemoryNumericIndex<T: Encodable + Numericable> {
    pub map: BTreeSet<Point<T>>,
    pub histogram: Histogram<T>,
    pub points_count: usize,
    pub max_values_per_point: usize,
    pub point_to_values: Vec<Vec<T>>,
}

impl<T: Encodable + Numericable> Default for InMemoryNumericIndex<T> {
    fn default() -> Self {
        Self {
            map: BTreeSet::new(),
            histogram: Histogram::new(HISTOGRAM_MAX_BUCKET_SIZE, HISTOGRAM_PRECISION),
            points_count: 0,
            max_values_per_point: 0,
            point_to_values: Default::default(),
        }
    }
}

impl<T: Encodable + Numericable + Default> FromIterator<(PointOffsetType, T)>
    for InMemoryNumericIndex<T>
{
    fn from_iter<I: IntoIterator<Item = (PointOffsetType, T)>>(iter: I) -> Self {
        let mut index = InMemoryNumericIndex::default();
        for pair in iter {
            let (idx, value) = pair;

            if index.point_to_values.len() <= idx as usize {
                index
                    .point_to_values
                    .resize_with(idx as usize + 1, Vec::new)
            }

            index.point_to_values[idx as usize].push(value);

            let key = Point::new(value, idx);
            InMemoryNumericIndex::add_to_map(&mut index.map, &mut index.histogram, key);
        }
        for values in &index.point_to_values {
            if !values.is_empty() {
                index.points_count += 1;
                index.max_values_per_point = index.max_values_per_point.max(values.len());
            }
        }
        index
    }
}

impl<T: Encodable + Numericable + Default + MmapValue> InMemoryNumericIndex<T> {
    /// Construct in-memroy index from given mmap index
    ///
    /// # Warning
    ///
    /// Expensive because this reads the full mmap index.
    pub(super) fn from_mmap(mmap_index: &MmapNumericIndex<T>) -> Self {
        (0..mmap_index.point_to_values.len() as PointOffsetType)
            .filter_map(|idx| mmap_index.get_values(idx).map(|values| (idx, values)))
            .flat_map(|(idx, values)| values.into_iter().map(move |value| (idx, value)))
            .collect::<InMemoryNumericIndex<T>>()
    }
}

impl<T: Encodable + Numericable + Default> InMemoryNumericIndex<T> {
    pub fn check_values_any(&self, idx: PointOffsetType, check_fn: impl Fn(&T) -> bool) -> bool {
        self.point_to_values
            .get(idx as usize)
            .map(|values| values.iter().any(check_fn))
            .unwrap_or(false)
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>> {
        Some(Box::new(
            self.point_to_values
                .get(idx as usize)
                .map(|v| v.iter().cloned())?,
        ))
    }

    pub fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        self.point_to_values.get(idx as usize).map(Vec::len)
    }

    pub fn total_unique_values_count(&self) -> usize {
        self.map.len()
    }

    pub fn values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> impl Iterator<Item = PointOffsetType> {
        self.map
            .range((start_bound, end_bound))
            .map(|point| point.idx)
    }

    pub fn orderable_values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> impl DoubleEndedIterator<Item = (T, PointOffsetType)> + '_ {
        self.map
            .range((start_bound, end_bound))
            .map(|point| (point.val, point.idx))
    }

    pub fn add_many_to_list(&mut self, idx: PointOffsetType, values: Vec<T>) {
        if self.point_to_values.len() <= idx as usize {
            self.point_to_values.resize_with(idx as usize + 1, Vec::new)
        }
        for value in &values {
            let key = Point::new(*value, idx);
            Self::add_to_map(&mut self.map, &mut self.histogram, key);
        }
        if !values.is_empty() {
            self.points_count += 1;
            self.max_values_per_point = self.max_values_per_point.max(values.len());
        }
        self.point_to_values[idx as usize] = values;
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) {
        if let Some(values) = self.point_to_values.get_mut(idx as usize) {
            if !values.is_empty() {
                self.points_count = self.points_count.checked_sub(1).unwrap_or_default();
            }
            for value in values.iter() {
                let key = Point::new(*value, idx);
                Self::remove_from_map(&mut self.map, &mut self.histogram, key);
            }
            *values = Default::default();
        }
    }

    fn add_to_map(map: &mut BTreeSet<Point<T>>, histogram: &mut Histogram<T>, key: Point<T>) {
        let was_added = map.insert(key.clone());
        // Histogram works with unique values (idx + value) only, so we need to
        // make sure that we don't add the same value twice.
        // key is a combination of value + idx, so we can use it to ensure than the pair is unique
        if was_added {
            histogram.insert(
                key,
                |x| Self::get_histogram_left_neighbor(map, x.clone()),
                |x| Self::get_histogram_right_neighbor(map, x.clone()),
            );
        }
    }

    fn remove_from_map(map: &mut BTreeSet<Point<T>>, histogram: &mut Histogram<T>, key: Point<T>) {
        let was_removed = map.remove(&key);
        if was_removed {
            histogram.remove(
                &key,
                |x| Self::get_histogram_left_neighbor(map, x.clone()),
                |x| Self::get_histogram_right_neighbor(map, x.clone()),
            );
        }
    }

    fn get_histogram_left_neighbor(map: &BTreeSet<Point<T>>, key: Point<T>) -> Option<Point<T>> {
        map.range((Unbounded, Excluded(key))).next_back().cloned()
    }

    fn get_histogram_right_neighbor(map: &BTreeSet<Point<T>>, key: Point<T>) -> Option<Point<T>> {
        map.range((Excluded(key), Unbounded)).next().cloned()
    }

    pub fn get_histogram(&self) -> &Histogram<T> {
        &self.histogram
    }

    pub fn get_points_count(&self) -> usize {
        self.points_count
    }

    pub fn get_max_values_per_point(&self) -> usize {
        self.max_values_per_point
    }
}

impl<T: Encodable + Numericable + Blob + Send + Sync + Default> MutableNumericIndex<T> {
    /// Open mutable numeric index from RocksDB storage
    ///
    /// Note: after opening, the data must be loaded into memory separately using [`load`].
    pub fn open_rocksdb(db: Arc<RwLock<DB>>, field: &str) -> Self {
        let store_cf_name = numeric_index_storage_cf_name(field);
        let db_wrapper = DatabaseColumnScheduledDeleteWrapper::new(DatabaseColumnWrapper::new(
            db,
            &store_cf_name,
        ));
        Self::open_rocksdb_db_wrapper(db_wrapper)
    }

    pub fn open_rocksdb_db_wrapper(db_wrapper: DatabaseColumnScheduledDeleteWrapper) -> Self {
        Self {
            storage: Storage::RocksDb(db_wrapper),
            in_memory_index: InMemoryNumericIndex::default(),
        }
    }

    /// Open mutable numeric index from Gridstore storage
    ///
    /// Note: after opening, the data must be loaded into memory separately using [`load`].
    pub fn open_gridstore(path: PathBuf) -> OperationResult<Self> {
        let options = StorageOptions::default();
        let store = Gridstore::open_or_create(path, options).map_err(|err| {
            OperationError::service_error(format!(
                "failed to open mutable numeric index on gridstore: {err}"
            ))
        })?;
        Ok(Self {
            storage: Storage::Gridstore(Arc::new(RwLock::new(store))),
            in_memory_index: InMemoryNumericIndex::default(),
        })
    }

    /// Load storage
    ///
    /// Loads in-memory index from backing RocksDB or Gridstore storage.
    pub(super) fn load(&mut self) -> OperationResult<bool> {
        match self.storage {
            Storage::RocksDb(_) => self.load_rocksdb(),
            Storage::Gridstore(_) => Ok(self.load_gridstore()),
        }
    }

    /// Load from RocksDB storage
    ///
    /// Loads in-memory index from RocksDB storage.
    fn load_rocksdb(&mut self) -> OperationResult<bool> {
        let Storage::RocksDb(db_wrapper) = &self.storage else {
            return Ok(false);
        };

        if !db_wrapper.has_column_family()? {
            return Ok(false);
        };

        self.in_memory_index = db_wrapper
            .lock_db()
            .iter()?
            .map(|(key, value)| {
                let value_idx =
                    u32::from_be_bytes(value.as_ref().try_into().map_err(|_| {
                        OperationError::service_error("incorrect numeric index value")
                    })?);
                let (idx, value) = T::decode_key(&key);
                if idx != value_idx {
                    return Err(OperationError::service_error(
                        "incorrect numeric index key-value pair",
                    ));
                }
                Ok((idx, value))
            })
            .collect::<Result<InMemoryNumericIndex<_>, OperationError>>()?;

        Ok(true)
    }

    /// Load from Gridstore storage
    ///
    /// Loads in-memory index from Gridstore storage.
    fn load_gridstore(&mut self) -> bool {
        let Storage::Gridstore(store) = &self.storage else {
            return false;
        };

        let hw_counter = HardwareCounterCell::disposable();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
        store
            .read()
            .iter(
                |idx, values| {
                    self.in_memory_index.add_many_to_list(idx, values.clone());
                    Ok(true)
                },
                hw_counter_ref,
            )
            // unwrap safety: never returns an error
            .unwrap();

        true
    }

    pub fn into_in_memory_index(self) -> InMemoryNumericIndex<T> {
        self.in_memory_index
    }

    #[cfg(test)]
    pub(super) fn db_wrapper(&self) -> Option<&DatabaseColumnScheduledDeleteWrapper> {
        match self.storage {
            Storage::RocksDb(ref db_wrapper) => Some(db_wrapper),
            Storage::Mmap(_) => None,
        }
    }

    #[inline]
    pub(super) fn clear(&mut self) -> OperationResult<()> {
        match self.storage {
            Storage::RocksDb(ref db_wrapper) => db_wrapper.recreate_column_family(),
            Storage::Gridstore(ref mut store) => store.write().clear().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to clear mutable numeric index: {err}",
                ))
            }),
        }
    }

    #[inline]
    pub(super) fn files(&self) -> Vec<PathBuf> {
        match self.storage {
            Storage::RocksDb(_) => vec![],
            Storage::Gridstore(ref store) => store.read().files(),
        }
    }

    #[inline]
    pub(super) fn flusher(&self) -> Flusher {
        match self.storage {
            Storage::RocksDb(ref db_wrapper) => db_wrapper.flusher(),
            Storage::Gridstore(ref store) => {
                let store = store.clone();
                Box::new(move || {
                    store.read().flush().map_err(|err| {
                        OperationError::service_error(format!(
                            "Failed to flush mutable numeric index gridstore: {err}"
                        ))
                    })
                })
            }
        }
    }

    pub fn add_many_to_list(
        &mut self,
        idx: PointOffsetType,
        values: Vec<T>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let mut hw_cell_wb = hw_counter
            .payload_index_io_write_counter()
            .write_back_counter();

        // Update persisted storage
        match self.storage {
            Storage::RocksDb(ref db_wrapper) => {
                for value in &values {
                    let key = value.encode_key(idx);
                    db_wrapper.put(&key, idx.to_be_bytes())?;
                    hw_cell_wb.incr_delta(size_of_val(&key) + size_of_val(&idx));
                }
            }
            Storage::Gridstore(ref mut store) => {
                let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
                store
                    .write()
                    .put_value(idx, &values, hw_counter_ref)
                    .map_err(|err| {
                        OperationError::service_error(format!(
                            "failed to put value in mutable numeric index gridstore: {err}"
                        ))
                    })?;
                hw_cell_wb.incr_delta(size_of_val(&idx) + size_of::<T>() * values.len());
            }
        }

        self.in_memory_index.add_many_to_list(idx, values);
        Ok(())
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        // Update persisted storage
        match self.storage {
            Storage::RocksDb(ref db_wrapper) => {
                self.in_memory_index
                    .get_values(idx)
                    .map(|mut values| {
                        values.try_for_each(|value| {
                            let key = value.encode_key(idx);
                            db_wrapper.remove(key)
                        })
                    })
                    .transpose()?;
            }
            Storage::Gridstore(ref mut store) => {
                store.write().delete_value(idx);
            }
        }

        self.in_memory_index.remove_point(idx);
        Ok(())
    }

    pub fn map(&self) -> &BTreeSet<Point<T>> {
        &self.in_memory_index.map
    }

    #[inline]
    pub fn total_unique_values_count(&self) -> usize {
        self.in_memory_index.total_unique_values_count()
    }
    #[inline]
    pub fn check_values_any(&self, idx: PointOffsetType, check_fn: impl Fn(&T) -> bool) -> bool {
        self.in_memory_index.check_values_any(idx, check_fn)
    }
    #[inline]
    pub fn get_points_count(&self) -> usize {
        self.in_memory_index.get_points_count()
    }
    #[inline]
    pub fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>> {
        self.in_memory_index.get_values(idx)
    }
    #[inline]
    pub fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        self.in_memory_index.values_count(idx)
    }
    #[inline]
    pub fn values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> impl Iterator<Item = PointOffsetType> {
        self.in_memory_index.values_range(start_bound, end_bound)
    }
    #[inline]
    pub fn orderable_values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> impl DoubleEndedIterator<Item = (T, PointOffsetType)> + '_ {
        self.in_memory_index
            .orderable_values_range(start_bound, end_bound)
    }
    #[inline]
    pub fn get_histogram(&self) -> &Histogram<T> {
        self.in_memory_index.get_histogram()
    }
    #[inline]
    pub fn get_max_values_per_point(&self) -> usize {
        self.in_memory_index.get_max_values_per_point()
    }
}
