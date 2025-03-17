use std::borrow::Borrow;
use std::collections::{BTreeSet, HashMap};
use std::iter;
use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::iterator_hw_measurement::HwMeasurementIteratorExt;
use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;

use super::{IdIter, IdRefIter, MapIndex, MapIndexKey};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::index::field_index::mmap_point_to_values::{MMAP_PTV_ACCESS_OVERHEAD, MmapValue};

pub struct MutableMapIndex<N: MapIndexKey + ?Sized> {
    pub(super) map: HashMap<N::Owned, BTreeSet<PointOffsetType>>,
    pub(super) point_to_values: Vec<Vec<N::Owned>>,
    /// Amount of point which have at least one indexed payload value
    pub(super) indexed_points: usize,
    pub(super) values_count: usize,
    pub(super) db_wrapper: DatabaseColumnScheduledDeleteWrapper,
}

impl<N: MapIndexKey + ?Sized> MutableMapIndex<N> {
    pub fn new(db: Arc<RwLock<DB>>, field_name: &str) -> Self {
        let store_cf_name = MapIndex::<N>::storage_cf_name(field_name);
        let db_wrapper = DatabaseColumnScheduledDeleteWrapper::new(DatabaseColumnWrapper::new(
            db,
            &store_cf_name,
        ));
        Self {
            map: Default::default(),
            point_to_values: Vec::new(),
            indexed_points: 0,
            values_count: 0,
            db_wrapper,
        }
    }

    pub fn add_many_to_map<Q>(
        &mut self,
        idx: PointOffsetType,
        values: Vec<Q>,
    ) -> OperationResult<()>
    where
        Q: Into<N::Owned>,
    {
        if values.is_empty() {
            return Ok(());
        }

        self.values_count += values.len();
        if self.point_to_values.len() <= idx as usize {
            self.point_to_values.resize_with(idx as usize + 1, Vec::new)
        }

        self.point_to_values[idx as usize] = Vec::with_capacity(values.len());
        for value in values {
            let entry = self.map.entry(value.into());
            self.point_to_values[idx as usize].push(entry.key().clone());
            let db_record = MapIndex::encode_db_record(entry.key().borrow(), idx);
            entry.or_default().insert(idx);
            self.db_wrapper.put(db_record, [])?;
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
            let key = MapIndex::encode_db_record(value.borrow(), idx);
            self.db_wrapper.remove(key)?;
        }

        Ok(())
    }

    pub fn get_db_wrapper(&self) -> &DatabaseColumnScheduledDeleteWrapper {
        &self.db_wrapper
    }

    pub fn load_from_db(&mut self) -> OperationResult<bool> {
        if !self.db_wrapper.has_column_family()? {
            return Ok(false);
        }
        self.indexed_points = 0;
        for (record, _) in self.db_wrapper.lock_db().iter()? {
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

    pub fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_acc: &HardwareCounterCell,
        check_fn: impl Fn(&N) -> bool,
    ) -> bool {
        // Overhead of accessing the index.
        hw_acc
            .payload_index_io_read_counter()
            .incr_delta(MMAP_PTV_ACCESS_OVERHEAD);

        self.point_to_values
            .get(idx as usize)
            .map(|values| {
                values.iter().any(|v| {
                    let item: &N = v.borrow();
                    let size = <N as MmapValue>::mmapped_size(item.as_referenced());
                    hw_acc.payload_index_io_read_counter().incr_delta(size);
                    check_fn(item)
                })
            })
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

    pub fn get_count_for_value(
        &self,
        value: &N,
        hw_counter: &HardwareCounterCell,
    ) -> Option<usize> {
        let counter = hw_counter.payload_index_io_read_counter();
        self.map.get(value).map(|p| {
            counter.incr_delta(size_of_val(p));
            p.len()
        })
    }

    pub fn iter_counts_per_value(&self) -> impl Iterator<Item = (&N, usize)> + '_ {
        self.map.iter().map(|(k, v)| (k.borrow(), v.len()))
    }

    pub fn iter_values_map<'a>(
        &'a self,
        hw_counter: &'a HardwareCounterCell,
    ) -> impl Iterator<Item = (&'a N, IdIter<'a>)> + 'a {
        self.map.iter().map(move |(k, v)| {
            hw_counter
                .payload_index_io_read_counter()
                .incr_delta(N::mmapped_size(MmapValue::as_referenced(k.borrow())));

            (
                k.borrow(),
                Box::new(v.iter().copied().measure_hw_with_cell(
                    hw_counter,
                    size_of::<PointOffsetType>(),
                    |i| i.payload_index_io_read_counter(),
                )) as IdIter,
            )
        })
    }

    pub fn get_iterator(&self, value: &N, hw_counter: &HardwareCounterCell) -> IdRefIter<'_> {
        self.map
            .get(value)
            .map(|ids| {
                hw_counter
                    .payload_index_io_read_counter()
                    .incr_delta(size_of_val(ids));
                Box::new(ids.iter()) as Box<dyn Iterator<Item = &PointOffsetType>>
            })
            .unwrap_or_else(|| Box::new(iter::empty::<&PointOffsetType>()))
    }

    pub fn iter_values(&self) -> Box<dyn Iterator<Item = &N> + '_> {
        Box::new(self.map.keys().map(|v| v.borrow()))
    }
}
