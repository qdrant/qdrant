use std::collections::HashMap;
use std::hash::Hash;
use std::{iter, mem};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PrimaryCondition, ValueIndexer,
};
use crate::index::field_index::{FieldIndex, PayloadFieldIndex, PayloadFieldIndexBuilder};
use crate::types::{
    FieldCondition, IntPayloadType, Match, MatchInteger, MatchKeyword, PayloadKeyType,
    PointOffsetType,
};

/// HashMap-based type of index
#[derive(Serialize, Deserialize, Default)]
pub struct PersistedMapIndex<N: Hash + Eq + Clone> {
    map: HashMap<N, Vec<PointOffsetType>>,
}

impl<N: Hash + Eq + Clone> PersistedMapIndex<N> {
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

    fn add_many_to_map(&mut self, idx: PointOffsetType, values: impl IntoIterator<Item = N>) {
        for value in values {
            let entry = self.map.entry(value).or_default();
            entry.push(idx);
        }
    }

    fn get_iterator(&self, value: &N) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        self.map
            .get(value)
            .map(|ids| Box::new(ids.iter().copied()) as Box<dyn Iterator<Item = PointOffsetType>>)
            .unwrap_or_else(|| Box::new(iter::empty::<PointOffsetType>()))
    }
}

impl PayloadFieldIndex for PersistedMapIndex<String> {
    fn filter(
        &self,
        condition: &FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        match &condition.r#match {
            Some(Match::Keyword(MatchKeyword { keyword })) => Some(self.get_iterator(keyword)),
            _ => None,
        }
    }

    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        match &condition.r#match {
            Some(Match::Keyword(MatchKeyword { keyword })) => {
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
                condition: FieldCondition {
                    key: key.clone(),
                    r#match: Some(value.to_owned().into()),
                    range: None,
                    geo_bounding_box: None,
                    geo_radius: None,
                },
                cardinality: point_ids.len(),
            });
        Box::new(iter)
    }
}

impl PayloadFieldIndex for PersistedMapIndex<IntPayloadType> {
    fn filter(
        &self,
        condition: &FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        match &condition.r#match {
            Some(Match::Integer(MatchInteger { integer })) => Some(self.get_iterator(integer)),
            _ => None,
        }
    }

    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        match &condition.r#match {
            Some(Match::Integer(MatchInteger { integer })) => {
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
                condition: FieldCondition {
                    key: key.clone(),
                    r#match: Some((*value).into()),
                    range: None,
                    geo_bounding_box: None,
                    geo_radius: None,
                },
                cardinality: point_ids.len(),
            });
        Box::new(iter)
    }
}

impl ValueIndexer<String> for PersistedMapIndex<String> {
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

impl PayloadFieldIndexBuilder for PersistedMapIndex<String> {
    fn add(&mut self, id: PointOffsetType, value: &Value) {
        self.add_point(id, value)
    }

    fn build(&mut self) -> FieldIndex {
        let data = mem::take(&mut self.map);
        FieldIndex::KeywordIndex(PersistedMapIndex { map: data })
    }
}

impl ValueIndexer<IntPayloadType> for PersistedMapIndex<IntPayloadType> {
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

impl PayloadFieldIndexBuilder for PersistedMapIndex<IntPayloadType> {
    fn add(&mut self, id: PointOffsetType, value: &Value) {
        self.add_point(id, value)
    }

    fn build(&mut self) -> FieldIndex {
        let data = mem::take(&mut self.map);
        FieldIndex::IntMapIndex(PersistedMapIndex { map: data })
    }
}
