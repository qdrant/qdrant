use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{MmapFs, Populate, UniversalRead};
use gridstore::error::GridstoreError;
use gridstore::{Blob, Gridstore};

use super::super::Encodable;
use super::super::lifecycle::{HISTOGRAM_MAX_BUCKET_SIZE, HISTOGRAM_PRECISION};
use super::super::numeric_index_read::NumericIndexRead;
use super::super::on_disk_numeric_index::OnDiskNumericIndex;
use super::{InMemoryNumericIndex, MutableNumericIndex, default_gridstore_options};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::histogram::Histogram;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::on_disk_point_to_values::StoredValue;

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

impl<T: Encodable + Numericable + Default + StoredValue> InMemoryNumericIndex<T> {
    /// Construct in-memory index from given on-disk index
    ///
    /// # Warning
    ///
    /// Expensive because this reads the full on-disk index.
    pub(in super::super) fn from_on_disk<S: UniversalRead>(
        on_disk_index: &OnDiskNumericIndex<T, S>,
    ) -> Self {
        let point_count = on_disk_index.storage.point_to_values.len();

        (0..point_count as PointOffsetType)
            .filter_map(|idx| on_disk_index.get_values(idx).map(|values| (idx, values)))
            .flat_map(|(idx, values)| values.into_iter().map(move |value| (idx, value)))
            .collect::<InMemoryNumericIndex<T>>()
    }
}

impl<T: Encodable + Numericable + Default> InMemoryNumericIndex<T> {
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
                self.points_count = self.points_count.saturating_sub(1);
            }
            for value in values.iter() {
                let key = Point::new(*value, idx);
                Self::remove_from_map(&mut self.map, &mut self.histogram, key);
            }
            *values = Default::default();
        }
    }

    pub(super) fn add_to_map(
        map: &mut BTreeSet<Point<T>>,
        histogram: &mut Histogram<T>,
        key: Point<T>,
    ) {
        let was_added = map.insert(key);
        // Histogram works with unique values (idx + value) only, so we need to
        // make sure that we don't add the same value twice.
        // key is a combination of value + idx, so we can use it to ensure than the pair is unique
        if was_added {
            histogram.insert(
                key,
                |x| Self::get_histogram_left_neighbor(map, *x),
                |x| Self::get_histogram_right_neighbor(map, *x),
            );
        }
    }

    fn remove_from_map(map: &mut BTreeSet<Point<T>>, histogram: &mut Histogram<T>, key: Point<T>) {
        let was_removed = map.remove(&key);
        if was_removed {
            histogram.remove(
                &key,
                |x| Self::get_histogram_left_neighbor(map, *x),
                |x| Self::get_histogram_right_neighbor(map, *x),
            );
        }
    }

    fn get_histogram_left_neighbor(map: &BTreeSet<Point<T>>, key: Point<T>) -> Option<Point<T>> {
        map.range((Unbounded, Excluded(key))).next_back().copied()
    }

    fn get_histogram_right_neighbor(map: &BTreeSet<Point<T>>, key: Point<T>) -> Option<Point<T>> {
        map.range((Excluded(key), Unbounded)).next().copied()
    }
}

impl<T: Encodable + Numericable + Send + Sync + Default> MutableNumericIndex<T>
where
    Vec<T>: Blob,
{
    /// Open and load mutable numeric index from Gridstore storage
    ///
    /// The `create_if_missing` parameter indicates whether to create a new Gridstore if it does
    /// not exist. If false and files don't exist, this will return `None` to indicate nothing
    /// could be loaded.
    pub fn open_gridstore(path: PathBuf, create_if_missing: bool) -> OperationResult<Option<Self>> {
        let store = if create_if_missing {
            let options = default_gridstore_options::<T>();
            Gridstore::open_or_create(MmapFs, path, options, Populate::Blocking).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to open mutable numeric index on gridstore: {err}"
                ))
            })?
        } else if path.exists() {
            Gridstore::open(MmapFs, path, Populate::Blocking).map_err(|err| {
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
            .iter::<_, GridstoreError>(
                |idx, values: Vec<T>| {
                    in_memory_index.add_many_to_list(idx, values);
                    Ok(true)
                },
                hw_counter_ref,
            )
            // unwrap safety: never returns an error
            .unwrap();

        Ok(Some(Self {
            storage: store,
            in_memory_index,
        }))
    }

    pub fn into_in_memory_index(self) -> InMemoryNumericIndex<T> {
        self.in_memory_index
    }

    #[inline]
    pub(in super::super) fn clear(&mut self) -> OperationResult<()> {
        self.storage.clear().map_err(|err| {
            OperationError::service_error(format!("Failed to clear mutable numeric index: {err}",))
        })
    }

    #[inline]
    pub(in super::super) fn wipe(self) -> OperationResult<()> {
        self.storage.wipe().map_err(|err| {
            OperationError::service_error(format!("Failed to wipe mutable numeric index: {err}",))
        })
    }

    /// Clear cache
    ///
    /// Only clears cache of Gridstore storage if used. Does not clear in-memory representation of
    /// index.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_cache().map_err(|err| {
            OperationError::service_error(format!(
                "Failed to clear mutable numeric index gridstore cache: {err}"
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

    pub fn add_many_to_list(
        &mut self,
        idx: PointOffsetType,
        values: Vec<T>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // Update persisted storage
        if values.is_empty() {
            // We cannot store empty value, then delete instead
            self.storage.delete_value(idx)?;
        } else {
            let hw_counter_ref = hw_counter.ref_payload_index_io_write_counter();
            self.storage
                .put_value(idx, &values, hw_counter_ref)
                .map_err(|err| {
                    OperationError::service_error(format!(
                        "failed to put value in mutable numeric index gridstore: {err}"
                    ))
                })?;
        }

        self.in_memory_index.add_many_to_list(idx, values);
        Ok(())
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        // Update persisted storage
        self.storage.delete_value(idx)?;

        self.in_memory_index.remove_point(idx);
        Ok(())
    }
}
