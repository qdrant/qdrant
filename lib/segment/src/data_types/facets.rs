use std::cmp::Reverse;
use std::collections::HashMap;
use std::hash::Hash;

use crate::json_path::JsonPath;
use crate::types::Filter;

pub struct FacetRequest {
    pub key: JsonPath,
    pub limit: usize,
    pub filter: Option<Filter>,
}

#[derive(Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum FacetValueRef<'a> {
    Keyword(&'a str),
}

impl<'a> FacetValueRef<'a> {
    pub fn to_owned(&self) -> FacetValue {
        match self {
            FacetValueRef::Keyword(s) => FacetValue::Keyword((*s).to_string()),
        }
    }
}

#[derive(PartialOrd, Ord, PartialEq, Eq, Hash, Clone, Debug)]
pub enum FacetValue {
    Keyword(String),
    // other types to add?
    // Bool(bool),
    // Integer(IntPayloadType),
    // FloatRange(FloatRange),
}

pub trait FacetValueTrait: Clone + PartialEq + Eq + Hash + Ord {}

impl FacetValueTrait for FacetValue {}
impl FacetValueTrait for FacetValueRef<'_> {}

pub type FacetValueHit = FacetHit<FacetValue>;

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct FacetHit<T: FacetValueTrait> {
    pub value: T,
    pub count: usize,
}

impl<T: FacetValueTrait> Ord for FacetHit<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.count
            .cmp(&other.count)
            // Reverse so that descending order has ascending values when having the same count
            .then_with(|| Reverse(&self.value).cmp(&Reverse(&other.value)))
    }
}

impl<T: FacetValueTrait> PartialOrd for FacetHit<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub fn merge_facet_hits<T: FacetValueTrait>(
    this: impl IntoIterator<Item = FacetHit<T>>,
    other: impl IntoIterator<Item = FacetHit<T>>,
) -> impl Iterator<Item = FacetHit<T>> {
    this.into_iter()
        .chain(other)
        .fold(HashMap::new(), |mut map, FacetHit { value, count }| {
            match map.get_mut(&value) {
                Some(existing_count) => *existing_count += count,
                None => {
                    map.insert(value, count);
                }
            }
            map
        })
        .into_iter()
        .map(|(value, count)| FacetHit { value, count })
}
