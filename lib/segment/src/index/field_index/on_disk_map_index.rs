use std::collections::HashMap;
use std::hash::Hash;
use std::{iter, mem};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::common::rocksdb_operations::db_write_options;
use crate::entry::entry_point::OperationResult;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PrimaryCondition, ValueIndexer,
};
use crate::index::field_index::{FieldIndex, PayloadFieldIndex, PayloadFieldIndexBuilder};
use crate::types::{
    FieldCondition, IntPayloadType, Match, MatchValue, PayloadKeyType, PointOffsetType,
    ValueVariants,
};
use atomic_refcell::AtomicRefCell;
use rocksdb::DB;
use std::fmt::Display;
use std::sync::Arc;

/// HashMap-based type of index
pub struct OnDiskMapIndex<N: Hash + Eq + Clone + Display> {
    map: HashMap<N, Vec<PointOffsetType>>,
    point_to_values: Vec<Vec<N>>,
    total_points: usize,
    store_cf_name: String,
    store: Arc<AtomicRefCell<DB>>,
}

impl<N: Hash + Eq + Clone + Display> OnDiskMapIndex<N> {
    pub fn new(store: Arc<AtomicRefCell<DB>>, store_cf_name: &str) -> OnDiskMapIndex<N> {
        OnDiskMapIndex {
            map: HashMap::new(),
            point_to_values: Vec::new(),
            total_points: 0,
            store_cf_name: String::from(store_cf_name),
            store,
        }
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

    pub fn check_value(&self, idx: PointOffsetType, reference: &N) -> bool {
        self.get_values(idx)
            .map(|values| values.iter().any(|x| x == reference))
            .unwrap_or(false)
    }

    fn add_many_to_map(&mut self, idx: PointOffsetType, values: impl IntoIterator<Item = N>) {
        let store_ref = self.store.borrow();
        let cf_handle = store_ref.cf_handle(&self.store_cf_name).unwrap();

        if self.point_to_values.len() <= idx as usize {
            self.point_to_values.resize(idx as usize + 1, vec![])
        }
        self.point_to_values[idx as usize] = values.into_iter().collect();
        let mut empty = true;
        for value in &self.point_to_values[idx as usize] {
            let entry = self.map.entry(value.clone()).or_default();
            entry.push(idx);
            empty = false;

            let db_key = format!("{}/{}", value, entry.len());
            store_ref
                .put_cf_opt(
                    cf_handle,
                    bincode::serialize(&db_key).unwrap(),
                    bincode::serialize(&idx).unwrap(),
                    &db_write_options(),
                )
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

    fn flush_impl(&self) -> OperationResult<()> {
        let store_ref = self.store.borrow();
        let cf_handle = store_ref.cf_handle(&self.store_cf_name).unwrap();
        Ok(store_ref.flush_cf(cf_handle)?)
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

    fn flush(&self) -> OperationResult<()> {
        self.flush_impl()
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

    fn flush(&self) -> OperationResult<()> {
        self.flush_impl()
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

impl PayloadFieldIndexBuilder for OnDiskMapIndex<String> {
    fn add(&mut self, id: PointOffsetType, value: &Value) {
        self.add_point(id, value)
    }

    fn build(&mut self) -> FieldIndex {
        let map = mem::take(&mut self.map);
        let column = mem::take(&mut self.point_to_values);
        FieldIndex::KeywordDiskIndex(OnDiskMapIndex {
            map,
            point_to_values: column,
            total_points: self.total_points,
            store_cf_name: self.store_cf_name.clone(),
            store: self.store.clone(),
        })
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

impl PayloadFieldIndexBuilder for OnDiskMapIndex<IntPayloadType> {
    fn add(&mut self, id: PointOffsetType, value: &Value) {
        self.add_point(id, value)
    }

    fn build(&mut self) -> FieldIndex {
        let map = mem::take(&mut self.map);
        let column = mem::take(&mut self.point_to_values);
        FieldIndex::IntMapDiskIndex(OnDiskMapIndex {
            map,
            point_to_values: column,
            total_points: self.total_points,
            store_cf_name: self.store_cf_name.clone(),
            store: self.store.clone(),
        })
    }
}

impl<N: Hash + Eq + Clone + Display> Serialize for OnDiskMapIndex<N> {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        unimplemented!()
    }
}

impl<'de, N: Hash + Eq + Clone + Display> Deserialize<'de> for OnDiskMapIndex<N> {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        unimplemented!()
    }
}
