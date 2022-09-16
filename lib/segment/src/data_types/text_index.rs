use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// #[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
// #[serde(rename_all = "snake_case")]
// pub enum TextFieldType {
//     Text
// }

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TokenizerType {
    Prefix,
    Whitespace,
    Word,
}

impl Default for TokenizerType {
    fn default() -> Self {
        TokenizerType::Word
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TextIndexType {
    Text,
}

impl Default for TextIndexType {
    fn default() -> Self {
        TextIndexType::Text
    }
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub struct TextIndexParams {
    // Required for OpenAPI pattern matching
    pub r#type: TextIndexType,
    #[serde(default)]
    pub tokenizer: TokenizerType,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_token_len: Option<usize>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_token_len: Option<usize>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    /// If true, lowercase all tokens. Default: true
    pub lowercase: Option<bool>,
}
