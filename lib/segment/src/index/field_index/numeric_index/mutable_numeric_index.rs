use std::collections::BTreeSet;
use std::ops::Bound;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::PathBuf;
#[cfg(feature = "rocksdb")]
use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::config::StorageOptions;
use gridstore::{Blob, Gridstore};
#[cfg(feature = "rocksdb")]
use parking_lot::RwLock;
#[cfg(feature = "rocksdb")]
use rocksdb::DB;

use super::mmap_numeric_index::MmapNumericIndex;
use super::{Encodable, HISTOGRAM_MAX_BUCKET_SIZE, HISTOGRAM_PRECISION};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
#[cfg(feature = "rocksdb")]
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
#[cfg(feature = "rocksdb")]
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::index::field_index::histogram::{Histogram, Numericable, Point};
use crate::index::field_index::mmap_point_to_values::MmapValue;
use crate::index::payload_config::StorageType;

/// Default options for Gridstore storage
const fn default_gridstore_options<T: Sized>() -> StorageOptions {
    let block_size = size_of::<T>();
    StorageOptions {
        // Size of numeric values in index
        block_size_bytes: Some(block_size),
        // Compressing numeric values is unreasonable
        compression: Some(gridstore::config::Compression::None),
        // Scale page size down with block size, prevents overhead of first page when there's (almost) no values
        page_size_bytes: Some(block_size * 8192 * 32), // 4 to 8 MiB = block_size * region_blocks * regions,
        region_size_blocks: None,
    }
}

pub struct MutableNumericIndex<T: Encodable + Numericable>
where
    Vec<T>: Blob,
{
    // Backing storage, source of state, persists deletions
    storage: Storage<T>,
    in_memory_index: InMemoryNumericIndex<T>,
}

enum Storage<T: Encodable + Numericable>
where
    Vec<T>: Blob,
{
    #[cfg(feature = "rocksdb")]
    RocksDb(DatabaseColumnScheduledDeleteWrapper),
    Gridstore(Gridstore<Vec<T>>),
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
        let point_count = mmap_index.storage.point_to_values.len();

        (0..point_count as PointOffsetType)
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

impl<T: Encodable + Numericable + Send + Sync + Default> MutableNumericIndex<T>
where
    Vec<T>: Blob,
{
    /// Open and load mutable numeric index from RocksDB storage
    #[cfg(feature = "rocksdb")]
    pub fn open_rocksdb(
        db: Arc<RwLock<DB>>,
        field: &str,
        create_if_missing: bool,
    ) -> OperationResult<Option<Self>> {
        let store_cf_name = super::numeric_index_storage_cf_name(field);
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
        let in_memory_index = db_wrapper
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

        Ok(Some(Self {
            storage: Storage::RocksDb(db_wrapper),
            in_memory_index,
        }))
    }

    /// Open and load mutable numeric index from Gridstore storage
    ///
    /// The `create_if_missing` parameter indicates whether to create a new Gridstore if it does
    /// not exist. If false and files don't exist, this will return `None` to indicate nothing
    /// could be loaded.
    pub fn open_gridstore(path: PathBuf, create_if_missing: bool) -> OperationResult<Option<Self>> {
        let store = if create_if_missing {
            let options = default_gridstore_options::<T>();
            Gridstore::open_or_create(path, options).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to open mutable numeric index on gridstore: {err}"
                ))
            })?
        } else if path.exists() {
            Gridstore::open(path).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to open mutable numeric index on gridstore: {err}"
                ))
            })?
        } else {
            // Files don't exist, cannot load
            return Ok(None);
        };

        // Load in-memory index from Gridstore
        let mut in_memory_index = InMemoryNumericIndex::default();
        let hw_counter = HardwareCounterCell::disposable();
        let hw_counter_ref = hw_counter.ref_payload_index_io_write_counter();
        store
            .iter::<_, ()>(
                |idx, values: Vec<T>| {
                    in_memory_index.add_many_to_list(idx, values);
                    Ok(true)
                },
                hw_counter_ref,
            )
            // unwrap safety: never returns an error
            .unwrap();

        Ok(Some(Self {
            storage: Storage::Gridstore(store),
            in_memory_index,
        }))
    }

    pub fn into_in_memory_index(self) -> InMemoryNumericIndex<T> {
        self.in_memory_index
    }

    #[cfg(all(test, feature = "rocksdb"))]
    pub(super) fn db_wrapper(&self) -> Option<&DatabaseColumnScheduledDeleteWrapper> {
        match &self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => Some(db_wrapper),
            Storage::Gridstore(_) => None,
        }
    }

    #[inline]
    pub(super) fn clear(&mut self) -> OperationResult<()> {
        match &mut self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => db_wrapper.recreate_column_family(),
            Storage::Gridstore(store) => store.clear().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to clear mutable numeric index: {err}",
                ))
            }),
        }
    }

    #[inline]
    pub(super) fn wipe(self) -> OperationResult<()> {
        match self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => db_wrapper.remove_column_family(),
            Storage::Gridstore(store) => store.wipe().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to wipe mutable numeric index: {err}",
                ))
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
                    "Failed to clear mutable numeric index gridstore cache: {err}"
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
                            "Failed to flush mutable numeric index gridstore: {err}"
                        ))
                    })
                });
                let stage_2_flusher = Box::new(move || {
                    stage_2_flusher().map_err(|err| {
                        OperationError::service_error(format!(
                            "Failed to flush mutable numeric index gridstore deletes: {err}"
                        ))
                    })
                });

                (stage_1_flusher, stage_2_flusher)
            }
        }
    }

    pub fn add_many_to_list(
        &mut self,
        idx: PointOffsetType,
        values: Vec<T>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // Update persisted storage
        match &mut self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => {
                let mut hw_cell_wb = hw_counter
                    .payload_index_io_write_counter()
                    .write_back_counter();
                for value in &values {
                    let key = value.encode_key(idx);
                    db_wrapper.put(&key, idx.to_be_bytes())?;
                    hw_cell_wb.incr_delta(size_of_val(&key) + size_of_val(&idx));
                }
            }
            // We cannot store empty value, then delete instead
            Storage::Gridstore(store) if values.is_empty() => {
                store.delete_value(idx);
            }
            Storage::Gridstore(store) => {
                let hw_counter_ref = hw_counter.ref_payload_index_io_write_counter();
                store
                    .put_value(idx, &values, hw_counter_ref)
                    .map_err(|err| {
                        OperationError::service_error(format!(
                            "failed to put value in mutable numeric index gridstore: {err}"
                        ))
                    })?;
            }
        }

        self.in_memory_index.add_many_to_list(idx, values);
        Ok(())
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        // Update persisted storage
        match &mut self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => {
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
            Storage::Gridstore(store) => {
                store.delete_value(idx);
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
