use std::collections::{BTreeSet, HashMap};
use std::fmt::Display;
use std::hash::Hash;
use std::iter;
use std::str::FromStr;
use std::sync::Arc;

use parking_lot::RwLock;
use rocksdb::DB;
use serde_json::Value;

use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PrimaryCondition, ValueIndexer,
};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    FieldCondition, IntPayloadType, Match, MatchValue, PayloadKeyType, PointOffsetType,
    ValueVariants,
};

/// HashMap-based type of index
pub struct MapIndex<N: Hash + Eq + Clone + Display> {
    map: HashMap<N, BTreeSet<PointOffsetType>>,
    point_to_values: Vec<Vec<N>>,
    /// Amount of point which have at least one indexed payload value
    indexed_points: usize,
    values_count: usize,
    db_wrapper: DatabaseColumnWrapper,
}

impl<N: Hash + Eq + Clone + Display + FromStr> MapIndex<N> {
    pub fn new(db: Arc<RwLock<DB>>, field_name: &str) -> MapIndex<N> {
        let store_cf_name = Self::storage_cf_name(field_name);
        let db_wrapper = DatabaseColumnWrapper::new(db, &store_cf_name);
        MapIndex {
            map: Default::default(),
            point_to_values: Vec::new(),
            indexed_points: 0,
            values_count: 0,
            db_wrapper,
        }
    }

    fn storage_cf_name(field: &str) -> String {
        format!("{field}_map")
    }

    pub fn recreate(&self) -> OperationResult<()> {
        self.db_wrapper.recreate_column_family()
    }

    fn load(&mut self) -> OperationResult<bool> {
        if !self.db_wrapper.has_column_family()? {
            return Ok(false);
        }
        self.indexed_points = 0;
        for (record, _) in self.db_wrapper.lock_db().iter()? {
            let record = std::str::from_utf8(&record).map_err(|_| {
                OperationError::service_error("Index load error: UTF8 error while DB parsing")
            })?;
            let (value, idx) = Self::decode_db_record(record)?;
            if self.point_to_values.len() <= idx as usize {
                self.point_to_values.resize(idx as usize + 1, Vec::new())
            }
            if self.point_to_values[idx as usize].is_empty() {
                self.indexed_points += 1;
            }
            self.values_count += 1;
            self.point_to_values[idx as usize].push(value.clone());
            self.map.entry(value).or_default().insert(idx);
        }
        Ok(true)
    }

    pub fn flusher(&self) -> Flusher {
        self.db_wrapper.flusher()
    }

    pub fn match_cardinality(&self, value: &N) -> CardinalityEstimation {
        let values_count = match self.map.get(value) {
            None => 0,
            Some(points) => points.len(),
        };

        CardinalityEstimation {
            primary_clauses: vec![],
            min: values_count,
            exp: values_count,
            max: values_count,
        }
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<&Vec<N>> {
        self.point_to_values.get(idx as usize)
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: self.indexed_points,
            points_values_count: self.values_count,
            histogram_bucket_size: None,
        }
    }

    fn add_many_to_map(&mut self, idx: PointOffsetType, values: Vec<N>) -> OperationResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        self.values_count += values.len();
        if self.point_to_values.len() <= idx as usize {
            self.point_to_values.resize(idx as usize + 1, Vec::new())
        }
        self.point_to_values[idx as usize] = values.into_iter().collect();
        for value in &self.point_to_values[idx as usize] {
            let entry = self.map.entry(value.clone()).or_default();
            entry.insert(idx);

            let db_record = Self::encode_db_record(value, idx);
            self.db_wrapper.put(db_record, [])?;
        }
        self.indexed_points += 1;
        Ok(())
    }

    fn get_iterator(&self, value: &N) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        self.map
            .get(value)
            .map(|ids| Box::new(ids.iter().copied()) as Box<dyn Iterator<Item = PointOffsetType>>)
            .unwrap_or_else(|| Box::new(iter::empty::<PointOffsetType>()))
    }

    fn encode_db_record(value: &N, idx: PointOffsetType) -> String {
        format!("{}/{}", value, idx)
    }

    fn decode_db_record(s: &str) -> OperationResult<(N, PointOffsetType)> {
        const DECODE_ERR: &str = "Index db parsing error: wrong data format";
        let separator_pos = s
            .rfind('/')
            .ok_or_else(|| OperationError::service_error(DECODE_ERR))?;
        if separator_pos == s.len() - 1 {
            return Err(OperationError::service_error(DECODE_ERR));
        }
        let value_str = &s[..separator_pos];
        let value =
            N::from_str(value_str).map_err(|_| OperationError::service_error(DECODE_ERR))?;
        let idx_str = &s[separator_pos + 1..];
        let idx = PointOffsetType::from_str(idx_str)
            .map_err(|_| OperationError::service_error(DECODE_ERR))?;
        Ok((value, idx))
    }

    fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
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
}

impl PayloadFieldIndex for MapIndex<String> {
    fn indexed_points(&self) -> usize {
        self.indexed_points
    }

    fn load(&mut self) -> OperationResult<bool> {
        MapIndex::load(self)
    }

    fn clear(self) -> OperationResult<()> {
        self.db_wrapper.recreate_column_family()
    }

    fn flusher(&self) -> Flusher {
        MapIndex::flusher(self)
    }

    fn filter(
        &self,
        condition: &FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Keyword(keyword),
            })) => Some(self.get_iterator(keyword)),
            _ => None,
        }
    }

    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Keyword(keyword),
            })) => {
                let mut estimation = self.match_cardinality(keyword);
                estimation
                    .primary_clauses
                    .push(PrimaryCondition::Condition(condition.clone()));
                Some(estimation)
            }
            _ => None,
        }
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        let iter = self
            .map
            .iter()
            .filter(move |(_value, point_ids)| point_ids.len() > threshold)
            .map(move |(value, point_ids)| PayloadBlockCondition {
                condition: FieldCondition::new_match(key.clone(), value.to_owned().into()),
                cardinality: point_ids.len(),
            });
        Box::new(iter)
    }

    fn count_indexed_points(&self) -> usize {
        self.indexed_points
    }
}

impl PayloadFieldIndex for MapIndex<IntPayloadType> {
    fn indexed_points(&self) -> usize {
        self.indexed_points
    }

    fn load(&mut self) -> OperationResult<bool> {
        MapIndex::load(self)
    }

    fn clear(self) -> OperationResult<()> {
        self.db_wrapper.recreate_column_family()
    }

    fn flusher(&self) -> Flusher {
        MapIndex::flusher(self)
    }

    fn filter(
        &self,
        condition: &FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Integer(integer),
            })) => Some(self.get_iterator(integer)),
            _ => None,
        }
    }

    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Integer(integer),
            })) => {
                let mut estimation = self.match_cardinality(integer);
                estimation
                    .primary_clauses
                    .push(PrimaryCondition::Condition(condition.clone()));
                Some(estimation)
            }
            _ => None,
        }
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        let iter = self
            .map
            .iter()
            .filter(move |(_value, point_ids)| point_ids.len() >= threshold)
            .map(move |(value, point_ids)| PayloadBlockCondition {
                condition: FieldCondition::new_match(key.clone(), (*value).into()),
                cardinality: point_ids.len(),
            });
        Box::new(iter)
    }

    fn count_indexed_points(&self) -> usize {
        self.indexed_points
    }
}

impl ValueIndexer<String> for MapIndex<String> {
    fn add_many(&mut self, id: PointOffsetType, values: Vec<String>) -> OperationResult<()> {
        self.add_many_to_map(id, values)
    }

    fn get_value(&self, value: &Value) -> Option<String> {
        if let Value::String(keyword) = value {
            return Some(keyword.to_owned());
        }
        None
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.remove_point(id)
    }
}

impl ValueIndexer<IntPayloadType> for MapIndex<IntPayloadType> {
    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<IntPayloadType>,
    ) -> OperationResult<()> {
        self.add_many_to_map(id, values)
    }

    fn get_value(&self, value: &Value) -> Option<IntPayloadType> {
        if let Value::Number(num) = value {
            return num.as_i64();
        }
        None
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.remove_point(id)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fmt::Debug;
    use std::iter::FromIterator;
    use std::path::Path;

    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::open_db_with_existing_cf;

    const FIELD_NAME: &str = "test";

    fn save_map_index<N: Hash + Eq + Clone + Display + FromStr + Debug>(
        data: &[Vec<N>],
        path: &Path,
    ) {
        let mut index = MapIndex::<N>::new(open_db_with_existing_cf(path).unwrap(), FIELD_NAME);
        index.recreate().unwrap();
        for (idx, values) in data.iter().enumerate() {
            index
                .add_many_to_map(idx as PointOffsetType, values.clone())
                .unwrap();
        }
        index.flusher()().unwrap();
    }

    fn load_map_index<N: Hash + Eq + Clone + Display + FromStr + Debug>(
        data: &[Vec<N>],
        path: &Path,
    ) {
        let mut index = MapIndex::<N>::new(open_db_with_existing_cf(path).unwrap(), FIELD_NAME);
        index.load().unwrap();
        for (idx, values) in data.iter().enumerate() {
            let index_values: HashSet<N> = HashSet::from_iter(
                index
                    .get_values(idx as PointOffsetType)
                    .unwrap()
                    .iter()
                    .cloned(),
            );
            let check_values: HashSet<N> = HashSet::from_iter(values.iter().cloned());
            assert_eq!(index_values, check_values);
        }
    }

    #[test]
    fn test_int_disk_map_index() {
        let data = vec![
            vec![1, 2, 3, 4, 5, 6],
            vec![1, 2, 3, 4, 5, 6],
            vec![13, 14, 15, 16, 17, 18],
            vec![19, 20, 21, 22, 23, 24],
            vec![25],
        ];

        let tmp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
        save_map_index(&data, tmp_dir.path());
        load_map_index(&data, tmp_dir.path());
    }

    #[test]
    fn test_string_disk_map_index() {
        let data = vec![
            vec![
                String::from("AABB"),
                String::from("UUFF"),
                String::from("IIBB"),
            ],
            vec![
                String::from("PPMM"),
                String::from("QQXX"),
                String::from("YYBB"),
            ],
            vec![
                String::from("FFMM"),
                String::from("IICC"),
                String::from("IIBB"),
            ],
            vec![
                String::from("AABB"),
                String::from("UUFF"),
                String::from("IIBB"),
            ],
            vec![String::from("PPGG")],
        ];

        let tmp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
        save_map_index(&data, tmp_dir.path());
        load_map_index(&data, tmp_dir.path());
    }
}
