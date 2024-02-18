use std::fmt::Display;
use std::str::FromStr;

use serde_json::Value;
use validator::ValidationError;

use crate::common::utils::MultiValue;

mod string;
mod v2;

pub use string::JsonPathString;

pub type JsonPath = string::JsonPathString;

pub trait JsonPathInterface: Sized + Clone + FromStr<Err = ()> + Display {
    fn value_get<'a>(&self, json_map: &'a serde_json::Map<String, Value>) -> MultiValue<&'a Value>;
    fn value_set<'a>(
        path: Option<&Self>,
        dest: &'a mut serde_json::Map<String, Value>,
        src: &'a serde_json::Map<String, Value>,
    );
    fn value_remove(&self, json_map: &mut serde_json::Map<String, Value>) -> MultiValue<Value>;
    fn value_filter(
        json_map: &serde_json::Map<String, Value>,
        filter: impl Fn(&Self, &Value) -> bool,
    ) -> serde_json::Map<String, Value>;
    fn validate_not_empty(&self) -> Result<(), ValidationError>;

    fn strip_wildcard_suffix(&self) -> Self;
    fn strip_prefix(&self, prefix: &Self) -> Option<Self>;
    fn array_key(&self) -> Self;
    fn extend(&self, other: &Self) -> Self;
    fn check_include_pattern(&self, pattern: &Self) -> bool;
    fn check_exclude_pattern(&self, pattern: &Self) -> bool;

    fn extend_or_new(base: Option<&Self>, other: &Self) -> Self {
        base.map_or_else(|| other.clone(), |base| base.extend(other))
    }
}

/// Create a new `JsonPath` from a string.
///
/// # Panics
///
/// Panics if the string is not a valid path. Thus, this function should only be used in tests.
#[cfg(test)]
pub fn path<P: JsonPathInterface>(p: &str) -> P {
    p.parse().unwrap()
}
