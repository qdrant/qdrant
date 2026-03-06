use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::hash::Hash;

use itertools::Itertools;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator::Validate;

use crate::json_path::JsonPath;
use crate::types::{Filter, IntPayloadType, UuidIntType, ValueVariants};

#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize, Validate, Hash)]
pub struct FacetParams {
    pub key: JsonPath,

    #[validate(range(min = 1))]
    pub limit: usize,
    #[validate(nested)]
    pub filter: Option<Filter>,
    #[serde(default)]
    pub exact: bool,
}

impl FacetParams {
    pub const DEFAULT_LIMIT: usize = 10;
    pub const DEFAULT_EXACT: bool = false;
}

#[derive(Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum FacetValueRef<'a> {
    Keyword(Cow<'a, str>),
    Int(IntPayloadType),
    Uuid(UuidIntType),
    Bool(bool),
}

impl FacetValueRef<'_> {
    pub fn to_owned(&self) -> FacetValue {
        match self {
            FacetValueRef::Keyword(str) => FacetValue::Keyword(str.to_string()),
            &FacetValueRef::Int(int) => FacetValue::Int(int),
            &FacetValueRef::Uuid(uuid) => FacetValue::Uuid(uuid),
            &FacetValueRef::Bool(bool) => FacetValue::Bool(bool),
        }
    }
}

impl<'a> From<Cow<'a, str>> for FacetValueRef<'a> {
    fn from(str: Cow<'a, str>) -> Self {
        FacetValueRef::Keyword(str)
    }
}

impl<'a> From<&'a str> for FacetValueRef<'a> {
    fn from(str: &'a str) -> Self {
        FacetValueRef::Keyword(Cow::Borrowed(str))
    }
}

impl<'a> From<Cow<'_, IntPayloadType>> for FacetValueRef<'a> {
    fn from(int: Cow<'_, IntPayloadType>) -> Self {
        FacetValueRef::Int(int.into_owned())
    }
}

impl<'a> From<&IntPayloadType> for FacetValueRef<'a> {
    fn from(int: &IntPayloadType) -> Self {
        FacetValueRef::Int(*int)
    }
}
impl<'a> From<Cow<'_, UuidIntType>> for FacetValueRef<'a> {
    fn from(uuid: Cow<'_, UuidIntType>) -> Self {
        FacetValueRef::Uuid(uuid.into_owned())
    }
}

impl<'a> From<&'_ UuidIntType> for FacetValueRef<'a> {
    fn from(uuid: &'_ UuidIntType) -> Self {
        FacetValueRef::Uuid(*uuid)
    }
}

#[derive(PartialOrd, Ord, PartialEq, Eq, Hash, Clone, Debug)]
pub enum FacetValue {
    Keyword(String),
    Int(IntPayloadType),
    Uuid(UuidIntType),
    Bool(bool),
    // TODO: Other types to add?
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

#[derive(Clone, Debug, Default)]
pub struct FacetResponse {
    pub hits: Vec<FacetValueHit>,
}

impl FacetResponse {
    /// Convert a count map to top `limit` hits sorted by count descending.
    ///
    /// Shared utility used by Edge and Collection facet implementations.
    pub fn top_hits(counts: HashMap<FacetValue, usize>, limit: usize) -> Self {
        let hits = counts
            .into_iter()
            .map(|(value, count)| FacetValueHit { value, count })
            .k_largest(limit)
            .collect();

        Self { hits }
    }
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

impl From<FacetValue> for ValueVariants {
    fn from(value: FacetValue) -> Self {
        match value {
            FacetValue::Keyword(s) => ValueVariants::String(s),
            FacetValue::Int(i) => ValueVariants::Integer(i),
            FacetValue::Uuid(uuid) => ValueVariants::String(Uuid::from_u128(uuid).to_string()),
            FacetValue::Bool(b) => ValueVariants::Bool(b),
        }
    }
}
