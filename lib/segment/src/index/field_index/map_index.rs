use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::collections::HashMap;
use crate::types::{PointOffsetType, IntPayloadType, Condition, PayloadType};
use crate::index::field_index::{CardinalityEstimation, FieldIndex};
use std::cmp::{max, min};
use crate::index::field_index::index_builder::IndexBuilder;
use crate::index::field_index::field_index::{PayloadFieldIndex, PayloadFieldIndexBuilder};
use std::mem;

#[derive(Serialize, Deserialize)]
pub struct PersistedMapIndex<N: Hash + Eq + Clone> {
    points_count: usize,
    values_count: usize,
    map: HashMap<N, Vec<PointOffsetType>>,
}

impl<N: Hash + Eq + Clone> PersistedMapIndex<N> {
    pub fn new() -> PersistedMapIndex<N> {
        PersistedMapIndex {
            points_count: 0,
            values_count: 0,
            map: Default::default(),
        }
    }

    pub fn match_cardinality(&self, value: &N) -> CardinalityEstimation {
        let values_count = match self.map.get(value) {
            None => 0,
            Some(points) => points.len()
        };
        let value_per_point = self.values_count as f64 / self.points_count as f64;

        CardinalityEstimation {
            min: max(1, values_count as i64 - (self.values_count as i64 - self.points_count as i64)) as usize,
            exp: (values_count as f64 / value_per_point) as usize,
            max: min(self.points_count as i64, values_count as i64) as usize,
        }
    }

    fn add_many(&mut self, idx: PointOffsetType, values: &Vec<N>)
    {
        for value in values {
            let vec = match self.map.get_mut(&value) {
                None => {
                    let new_vec = vec![];
                    self.map.insert(value.clone(), new_vec);
                    self.map.get_mut(&value).unwrap()
                }
                Some(vec) => vec
            };
            vec.push(idx);
        }
    }
}

impl PayloadFieldIndex for PersistedMapIndex<String> {
    fn filter(&self, condition: &Condition) -> Box<dyn Iterator<Item=usize>> {
        unimplemented!()
    }

    fn estimate_condition_cardinality(&self, condition: &Condition) -> Option<CardinalityEstimation> {
        match condition {
            Condition::Match(match_condition) => {
                match_condition.keyword.as_ref().map(|keyword| self.match_cardinality(keyword))
            }
            _ => None
        }
    }
}

impl PayloadFieldIndexBuilder for PersistedMapIndex<String> {
    fn add(&mut self, id: usize, value: &PayloadType) {
        match value {
            PayloadType::Keyword(keywords) => self.add_many(id, keywords),
            _ => panic!("Unexpected payload type: {:?}", value)
        }
    }

    fn build(&mut self) -> FieldIndex {
        let data = mem::replace(&mut self.map, Default::default());

        FieldIndex::KeywordIndex(PersistedMapIndex {
            points_count: self.points_count,
            values_count: self.values_count,
            map: data,
        })
    }
}

impl PayloadFieldIndexBuilder for PersistedMapIndex<IntPayloadType> {
    fn add(&mut self, id: usize, value: &PayloadType) {
        match value {
            PayloadType::Integer(numbers) => self.add_many(id, numbers),
            _ => panic!("Unexpected payload type: {:?}", value)
        }
    }

    fn build(&mut self) -> FieldIndex {
        let data = mem::replace(&mut self.map, Default::default());

        FieldIndex::IntMapIndex(PersistedMapIndex {
            points_count: self.points_count,
            values_count: self.values_count,
            map: data,
        })
    }
}
