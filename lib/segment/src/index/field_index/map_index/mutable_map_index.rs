use std::collections::{BTreeSet, HashMap};
use std::fmt::Display;
use std::hash::Hash;
use std::iter;
use std::str::FromStr;
use std::sync::Arc;

use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;

use super::MapIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;

pub struct MutableMapIndex<N: Hash + Eq + Clone + Display + FromStr> {
    pub(super) map: HashMap<N, BTreeSet<PointOffsetType>>,
    pub(super) point_to_values: Vec<Vec<N>>,
    /// Amount of point which have at least one indexed payload value
    pub(super) indexed_points: usize,
    pub(super) values_count: usize,
    pub(super) db_wrapper: DatabaseColumnWrapper,
}

impl<N: Hash + Eq + Clone + Display + FromStr + Default> MutableMapIndex<N> {
    pub fn new(db: Arc<RwLock<DB>>, field_name: &str) -> Self {
        let store_cf_name = MapIndex::<N>::storage_cf_name(field_name);
        let db_wrapper = DatabaseColumnWrapper::new(db, &store_cf_name);
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
        Q: Into<N>,
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
            let db_record = MapIndex::encode_db_record(entry.key(), idx);
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
            if let Some(vals) = self.map.get_mut(value) {
                vals.remove(&idx);
            }
            let key = MapIndex::encode_db_record(value, idx);
            self.db_wrapper.remove(key)?;
        }

        Ok(())
    }

    pub fn get_db_wrapper(&self) -> &DatabaseColumnWrapper {
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
            let (value, idx) = MapIndex::decode_db_record(record)?;
            if self.point_to_values.len() <= idx as usize {
                self.point_to_values.resize_with(idx as usize + 1, Vec::new)
            }
            if self.point_to_values[idx as usize].is_empty() {
                self.indexed_points += 1;
            }
            self.values_count += 1;

            let entry = self.map.entry(value);
            self.point_to_values[idx as usize].push(entry.key().clone());
            entry.or_default().insert(idx);
        }
        Ok(true)
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<&[N]> {
        self.point_to_values.get(idx as usize).map(|v| v.as_slice())
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

    pub fn get_points_with_value_count<Q>(&self, value: &Q) -> Option<usize>
    where
        Q: ?Sized,
        N: std::borrow::Borrow<Q>,
        Q: Hash + Eq,
    {
        self.map.get(value).map(|p| p.len())
    }

    pub fn get_iterator<Q>(&self, value: &Q) -> Box<dyn Iterator<Item = PointOffsetType> + '_>
    where
        Q: ?Sized,
        N: std::borrow::Borrow<Q>,
        Q: Hash + Eq,
    {
        self.map
            .get(value)
            .map(|ids| Box::new(ids.iter().copied()) as Box<dyn Iterator<Item = PointOffsetType>>)
            .unwrap_or_else(|| Box::new(iter::empty::<PointOffsetType>()))
    }

    pub fn get_values_iterator(&self) -> Box<dyn Iterator<Item = &N> + '_> {
        Box::new(self.map.keys())
    }
}
