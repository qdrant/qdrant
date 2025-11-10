use std::collections::HashMap;

use api::rest::{Bm25Config, Document, DocumentOptions, Image, InferenceObject};
use serde::de::IntoDeserializer;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage::content_manager::errors::StorageError;

use super::service::InferenceData;

#[derive(Debug, Serialize, Clone)]
pub struct InferenceInput {
    pub data: Value,
    pub data_type: InferenceDataType,
    pub model: String,
    pub options: Option<HashMap<String, Value>>,
}

impl InferenceInput {
    /// Attempts to parse the input's options into a local model config.
    pub fn parse_bm25_config(
        options: Option<HashMap<String, Value>>,
    ) -> Result<Bm25Config, StorageError> {
        let options = options.unwrap_or_default();
        Bm25Config::deserialize(options.into_deserializer())
            .map_err(|err| StorageError::bad_input(format!("Invalid BM25 config: {err:#?}")))
    }
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum InferenceDataType {
    Text,
    Image,
    Object,
}

impl From<InferenceData> for InferenceInput {
    fn from(value: InferenceData) -> Self {
        match value {
            InferenceData::Document(doc) => {
                let Document {
                    text,
                    model,
                    options,
                } = doc;
                InferenceInput {
                    data: Value::String(text),
                    data_type: InferenceDataType::Text,
                    model: model.clone(),
                    options: options.map(DocumentOptions::into_options),
                }
            }
            InferenceData::Image(img) => {
                let Image {
                    image,
                    model,
                    options,
                } = img;
                InferenceInput {
                    data: image,
                    data_type: InferenceDataType::Image,
                    model: model.clone(),
                    options: options.options,
                }
            }
            InferenceData::Object(obj) => {
                let InferenceObject {
                    object,
                    model,
                    options,
                } = obj;
                InferenceInput {
                    data: object,
                    data_type: InferenceDataType::Object,
                    model: model.clone(),
                    options: options.options,
                }
            }
        }
    }
}
