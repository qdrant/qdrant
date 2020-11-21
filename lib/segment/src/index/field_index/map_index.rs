use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::collections::HashMap;
use crate::types::{PointOffsetType, IntPayloadType};
use crate::index::field_index::Estimation;
use std::cmp::{max, min};
use crate::index::field_index::index_builder::IndexBuilder;

impl From<&IndexBuilder<String>> for PersistedMapIndex<String> {
    fn from(builder: &IndexBuilder<String>) -> Self {
        let mut map: HashMap<String, Vec<PointOffsetType>> = HashMap::new();
        for element in builder.elements.iter() {
            let vec = match map.get_mut(&element.value) {
                None => {
                    let new_vec = vec![];
                    map.insert(element.value.clone(), new_vec);
                    map.get_mut(&element.value).unwrap()
                }
                Some(vec) => vec
            };
            vec.push(element.id);
        }

        PersistedMapIndex {
            points_count: builder.ids.len(),
            values_count: builder.elements.len(),
            map
        }
    }
}

impl From<&IndexBuilder<IntPayloadType>> for PersistedMapIndex<IntPayloadType> {
    fn from(builder: &IndexBuilder<IntPayloadType>) -> Self {
        let mut map: HashMap<IntPayloadType, Vec<PointOffsetType>> = HashMap::new();
        for element in builder.elements.iter() {
            let vec = match map.get_mut(&element.value) {
                None => {
                    let new_vec = vec![];
                    map.insert(element.value.clone(), new_vec);
                    map.get_mut(&element.value).unwrap()
                }
                Some(vec) => vec
            };
            vec.push(element.id);
        }

        PersistedMapIndex {
            points_count: builder.ids.len(),
            values_count: builder.elements.len(),
            map
        }
    }
}


#[derive(Serialize, Deserialize)]
pub struct PersistedMapIndex<N: Hash + Eq + Clone> {
    points_count: usize,
    values_count: usize,
    map: HashMap<N, Vec<PointOffsetType>>,
}

impl<N: Hash + Eq + Clone> PersistedMapIndex<N> {
    pub fn match_cardinality(&self, value: &N) -> Estimation {
        let values_count = match self.map.get(value) {
            None => 0,
            Some(points) => points.len()
        };
        let value_per_point = self.values_count as f64 / self.points_count as f64;

        Estimation {
            min: max(1, values_count as i64 - (self.values_count as i64 - self.points_count as i64)) as usize,
            exp: (values_count as f64 / value_per_point) as usize,
            max: min(self.points_count as i64, values_count as i64) as usize,
        }
    }
}
