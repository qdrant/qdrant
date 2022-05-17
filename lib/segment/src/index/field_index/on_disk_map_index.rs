use std::collections::HashMap;
use std::hash::Hash;
use std::iter;

use serde_json::Value;

use crate::common::rocksdb_operations::db_write_options;
use crate::entry::entry_point::OperationResult;
use crate::index::field_index::PayloadFieldIndex;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PrimaryCondition, ValueIndexer,
};
use crate::types::{
    FieldCondition, IntPayloadType, Match, MatchValue, PayloadKeyType, PointOffsetType,
    ValueVariants,
};
use atomic_refcell::AtomicRefCell;
use rocksdb::{IteratorMode, DB};
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

/// HashMap-based type of index
pub struct OnDiskMapIndex<N: Hash + Eq + Clone + Display> {
    map: HashMap<N, Vec<PointOffsetType>>,
    point_to_values: Vec<Vec<N>>,
    total_points: usize,
    store_cf_name: String,
    store: Arc<AtomicRefCell<DB>>,
}

impl<N: Hash + Eq + Clone + Display + FromStr> OnDiskMapIndex<N> {
    pub fn new(store: Arc<AtomicRefCell<DB>>, store_cf_name: &str) -> OnDiskMapIndex<N> {
        let mut index = OnDiskMapIndex {
            map: HashMap::new(),
            point_to_values: Vec::new(),
            total_points: 0,
            store_cf_name: String::from(store_cf_name),
            store,
        };
        index.load_from_db();
        index
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

    fn add_many_to_map(&mut self, idx: PointOffsetType, values: Vec<N>) {
        let store_ref = self.store.borrow();
        let cf_handle = store_ref.cf_handle(&self.store_cf_name).unwrap();

        if self.point_to_values.len() <= idx as usize {
            self.point_to_values.resize(idx as usize + 1, Vec::new())
        }
        self.point_to_values[idx as usize] = values.into_iter().collect();
        let mut empty = true;
        for value in &self.point_to_values[idx as usize] {
            let entry = self.map.entry(value.clone()).or_default();
            entry.push(idx);
            empty = false;

            let db_record = Self::encode_db_record(value, idx);
            store_ref
                .put_cf_opt(cf_handle, &db_record, &[], &db_write_options())
                .unwrap();
        }
        if !empty {
            self.total_points += 1;
        }
    }

    fn get_iterator(&self, value: &N) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        self.map
            .get(value)
            .map(|ids| Box::new(ids.iter().copied()) as Box<dyn Iterator<Item = PointOffsetType>>)
            .unwrap_or_else(|| Box::new(iter::empty::<PointOffsetType>()))
    }

    fn load_from_db(&mut self) {
        let store_ref = self.store.borrow();
        let cf_handle = store_ref.cf_handle(&self.store_cf_name).unwrap();
        for (record, _) in store_ref.iterator_cf(cf_handle, IteratorMode::Start) {
            let record = std::str::from_utf8(&record).unwrap();
            let (value, idx) = Self::decode_db_record(record);
            if self.point_to_values.len() <= idx as usize {
                self.point_to_values.resize(idx as usize + 1, Vec::new())
            }
            if self.point_to_values[idx as usize].is_empty() {
                self.total_points += 1;
            }
            self.point_to_values[idx as usize].push(value.clone());
            self.map.entry(value).or_default().push(idx);
        }
    }

    fn flush(&self) -> OperationResult<()> {
        let store_ref = self.store.borrow();
        let cf_handle = store_ref.cf_handle(&self.store_cf_name).unwrap();
        Ok(store_ref.flush_cf(cf_handle)?)
    }

    fn encode_db_record(value: &N, idx: PointOffsetType) -> String {
        format!("{}/{}", value, idx)
    }

    fn decode_db_record(s: &str) -> (N, PointOffsetType) {
        let separator_pos = s.rfind('/').unwrap();
        if separator_pos == s.len() - 1 {
            panic!("");
        }
        let value_str = &s[..separator_pos];
        let value = N::from_str(value_str).map_err(|_| {}).unwrap();
        let idx_str = &s[separator_pos + 1..];
        let idx = PointOffsetType::from_str(idx_str).unwrap();
        (value, idx)
    }
}

impl PayloadFieldIndex for OnDiskMapIndex<String> {
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
        self.total_points
    }
}

impl PayloadFieldIndex for OnDiskMapIndex<IntPayloadType> {
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
        self.total_points
    }
}

impl ValueIndexer<String> for OnDiskMapIndex<String> {
    fn add_many(&mut self, id: PointOffsetType, values: Vec<String>) {
        self.add_many_to_map(id, values);
    }

    fn get_value(&self, value: &Value) -> Option<String> {
        if let Value::String(keyword) = value {
            return Some(keyword.to_owned());
        }
        None
    }
}

impl ValueIndexer<IntPayloadType> for OnDiskMapIndex<IntPayloadType> {
    fn add_many(&mut self, id: PointOffsetType, values: Vec<IntPayloadType>) {
        self.add_many_to_map(id, values);
    }

    fn get_value(&self, value: &Value) -> Option<IntPayloadType> {
        if let Value::Number(num) = value {
            return num.as_i64();
        }
        None
    }
}
