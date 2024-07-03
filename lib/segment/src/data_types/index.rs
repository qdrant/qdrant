use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// Keyword

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum KeywordIndexType {
    #[default]
    Keyword,
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub struct KeywordIndexParams {
    // Required for OpenAPI schema without anonymous types, versus #[serde(tag = "type")]
    pub r#type: KeywordIndexType,

    /// If true, store the index on disk. Default: false.
    #[cfg(any())]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
}

// Integer

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IntegerIndexType {
    #[default]
    Integer,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub struct IntegerIndexParams {
    // Required for OpenAPI schema without anonymous types, versus #[serde(tag = "type")]
    pub r#type: IntegerIndexType,

    /// If true - support direct lookups.
    pub lookup: bool,

    /// If true - support ranges filters.
    pub range: bool,

    /// If true, store the index on disk. Default: false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg(any())]
    pub on_disk: Option<bool>,
}

impl Default for IntegerIndexParams {
    fn default() -> Self {
        Self {
            r#type: Default::default(),
            lookup: true,
            range: true,
        }
    }
}

// Float

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FloatIndexType {
    #[default]
    Float,
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub struct FloatIndexParams {
    // Required for OpenAPI schema without anonymous types, versus #[serde(tag = "type")]
    pub r#type: FloatIndexType,

    /// If true, store the index on disk. Default: false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg(any())]
    pub on_disk: Option<bool>,
}

// Geo

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GeoIndexType {
    #[default]
    Geo,
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub struct GeoIndexParams {
    // Required for OpenAPI schema without anonymous types, versus #[serde(tag = "type")]
    pub r#type: GeoIndexType,

    /// If true, store the index on disk. Default: false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg(any())]
    pub on_disk: Option<bool>,
}

// Text

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TextIndexType {
    #[default]
    Text,
}

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TokenizerType {
    Prefix,
    Whitespace,
    #[default]
    Word,
    Multilingual,
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub struct TextIndexParams {
    // Required for OpenAPI schema without anonymous types, versus #[serde(tag = "type")]
    pub r#type: TextIndexType,

    #[serde(default)]
    pub tokenizer: TokenizerType,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_token_len: Option<usize>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_token_len: Option<usize>,

    /// If true, lowercase all tokens. Default: true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lowercase: Option<bool>,
}

// Bool

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BoolIndexType {
    #[default]
    Bool,
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub struct BoolIndexParams {
    // Required for OpenAPI schema without anonymous types, versus #[serde(tag = "type")]
    pub r#type: BoolIndexType,

    /// If true, store the index on disk. Default: false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg(any())]
    pub on_disk: Option<bool>,
}

// Datetime

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DatetimeIndexType {
    #[default]
    Datetime,
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DatetimeIndexParams {
    // Required for OpenAPI schema without anonymous types, versus #[serde(tag = "type")]
    pub r#type: DatetimeIndexType,

    /// If true, store the index on disk. Default: false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg(any())]
    pub on_disk: Option<bool>,
}
