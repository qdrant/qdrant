use std::collections::HashMap;
use std::fmt::Display;

use serde::Serialize;
use serde_json::Value;

/// One input of the inference service's `/api/v1/infer` request.
#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct InferenceInput {
    pub data: Value,
    pub data_type: InferenceDataType,
    pub model: String,
    pub options: Option<HashMap<String, Value>>,
}

#[derive(Debug, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InferenceDataType {
    Text,
    Image,
    Object,
}

/// Whether the inputs are embedded for storing (documents) or for searching (queries).
#[derive(Debug, Serialize, Default, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum InferenceType {
    #[default]
    Update,
    Search,
}

impl Display for InferenceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InferenceType::Update => f.write_str("update"),
            InferenceType::Search => f.write_str("search"),
        }
    }
}
