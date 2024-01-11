use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IntegerIndexType {
    #[default]
    Integer,
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub struct IntegerParams {
    // Required for OpenAPI pattern matching
    pub r#type: IntegerIndexType,
    /// If true - support direct lookups.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookup: Option<bool>,
    /// If true - support ranges filters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<bool>,
}
