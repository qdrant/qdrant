use std::fmt::Display;
use std::str::FromStr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use validator::ValidationError;

use super::JsonPathInterface;
use crate::common::anonymize::Anonymize;
use crate::common::utils::MultiValue;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, JsonSchema)]
pub struct JsonPathV2;

impl JsonPathInterface for JsonPathV2 {
    fn value_get<'a>(
        &self,
        _json_map: &'a serde_json::Map<String, Value>,
    ) -> MultiValue<&'a Value> {
        unimplemented!()
    }

    fn value_set<'a>(
        _path: Option<&Self>,
        _dest: &'a mut serde_json::Map<String, Value>,
        _src: &'a serde_json::Map<String, Value>,
    ) {
        unimplemented!()
    }

    fn value_remove(&self, _json_map: &mut serde_json::Map<String, Value>) -> MultiValue<Value> {
        unimplemented!()
    }

    fn value_filter(
        _json_map: &serde_json::Map<String, Value>,
        _filter: impl Fn(&Self, &Value) -> bool,
    ) -> serde_json::Map<String, Value> {
        unimplemented!()
    }

    fn validate_not_empty(&self) -> Result<(), ValidationError> {
        Ok(())
    }

    fn strip_wildcard_suffix(&self) -> Self {
        unimplemented!()
    }

    fn strip_prefix(&self, _prefix: &Self) -> Option<Self> {
        unimplemented!()
    }

    fn extend(&self, _other: &Self) -> Self {
        unimplemented!()
    }

    fn array_key(&self) -> Self {
        unimplemented!()
    }

    fn check_include_pattern(&self, _pattern: &Self) -> bool {
        unimplemented!()
    }

    fn check_exclude_pattern(&self, _pattern: &Self) -> bool {
        unimplemented!()
    }
}

impl Display for JsonPathV2 {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}

impl TryFrom<&str> for JsonPathV2 {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl FromStr for JsonPathV2 {
    type Err = ();

    fn from_str(_value: &str) -> Result<Self, Self::Err> {
        unimplemented!()
    }
}

impl Anonymize for JsonPathV2 {
    fn anonymize(&self) -> Self {
        self.clone()
    }
}
