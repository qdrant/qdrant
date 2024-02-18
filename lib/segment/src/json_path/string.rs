use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use validator::ValidationError;

use super::JsonPathInterface;
use crate::common::anonymize::Anonymize;
use crate::common::utils::MultiValue;

#[derive(Debug, PartialEq, Clone, Hash, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct JsonPathString(pub String);

impl Display for JsonPathString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl JsonPathInterface for JsonPathString {
    fn value_get<'a>(&self, json_map: &'a serde_json::Map<String, Value>) -> MultiValue<&'a Value> {
        #[allow(deprecated)]
        crate::common::utils::get_value_from_json_map(&self.0, json_map)
    }
    fn value_set<'a>(
        path: Option<&Self>,
        dest: &'a mut serde_json::Map<String, Value>,
        src: &'a serde_json::Map<String, Value>,
    ) {
        #[allow(deprecated)]
        crate::common::utils::set_value_to_json_map(
            path.map(|p| p.0.as_str()).unwrap_or(""),
            dest,
            src,
        )
    }
    fn value_remove(&self, json_map: &mut serde_json::Map<String, Value>) -> MultiValue<Value> {
        #[allow(deprecated)]
        crate::common::utils::remove_value_from_json_map(&self.0, json_map)
    }
    fn value_filter(
        json_map: &serde_json::Map<String, Value>,
        filter: impl Fn(&Self, &Value) -> bool,
    ) -> serde_json::Map<String, Value> {
        #[allow(deprecated)]
        crate::common::utils::filter_json_values(json_map, filter)
    }
    fn validate_not_empty(&self) -> Result<(), ValidationError> {
        if self.0.is_empty() {
            let mut err = ValidationError::new("length");
            err.add_param(Cow::from("min"), &1u64);
            err.add_param(Cow::from("value"), &self.0.as_str());
            return Err(err);
        }
        Ok(())
    }

    fn strip_wildcard_suffix(&self) -> JsonPathString {
        match self.0.strip_suffix("[]") {
            Some(s) => JsonPathString(s.to_string()),
            None => JsonPathString(self.0.clone()),
        }
    }

    fn strip_prefix(&self, prefix: &JsonPathString) -> Option<JsonPathString> {
        Some(JsonPathString(
            self.0
                .strip_prefix(&prefix.0)?
                .strip_prefix('.')?
                .to_string(),
        ))
    }

    fn array_key(&self) -> Self {
        let mut result = self.clone();
        if !result.0.ends_with("[]") {
            result.0.push_str("[]");
        }
        result
    }

    fn extend(&self, other: &JsonPathString) -> JsonPathString {
        JsonPathString(format!("{}.{}", self.0, other.0))
    }

    fn check_include_pattern(&self, pattern: &Self) -> bool {
        crate::common::utils::check_include_pattern(&self.0, &pattern.0)
    }

    fn check_exclude_pattern(&self, pattern: &Self) -> bool {
        crate::common::utils::check_exclude_pattern(&self.0, &pattern.0)
    }
}

impl TryFrom<&str> for JsonPathString {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl FromStr for JsonPathString {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(JsonPathString(value.to_string()))
    }
}

impl Anonymize for JsonPathString {
    fn anonymize(&self) -> JsonPathString {
        self.clone()
    }
}
