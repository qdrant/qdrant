use std::cmp::Reverse;
use std::collections::HashMap;

use crate::json_path::JsonPath;
use crate::types::Filter;

pub struct FacetRequest {
    pub key: JsonPath,
    pub limit: usize,
    pub filter: Option<Filter>,
}

#[derive(PartialOrd, Ord, PartialEq, Eq, Hash, Clone, Debug)]
pub enum FacetValue {
    Keyword(String),
    // other types to add?
    // Bool(bool),
    // Integer(IntPayloadType),
    // FloatRange(FloatRange),
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct FacetValueHit {
    pub value: FacetValue,
    pub count: usize,
}

impl Ord for FacetValueHit {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.count
            .cmp(&other.count)
            // Reverse so that descending order has ascending values when having the same count
            .then_with(|| Reverse(&self.value).cmp(&Reverse(&other.value)))
    }
}

impl PartialOrd for FacetValueHit {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub fn merge_facet_hits(
    this: impl IntoIterator<Item = FacetValueHit>,
    other: impl IntoIterator<Item = FacetValueHit>,
) -> impl Iterator<Item = FacetValueHit> {
    this.into_iter()
        .chain(other)
        .fold(HashMap::new(), |mut map, FacetValueHit { value, count }| {
            match map.get_mut(&value) {
                Some(existing_count) => *existing_count += count,
                None => {
                    map.insert(value, count);
                }
            }
            map
        })
        .into_iter()
        .map(|(value, count)| FacetValueHit { value, count })
}
