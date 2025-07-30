use std::collections::HashMap;

use api::rest::{Document, Image, InferenceObject};
use serde::de::IntoDeserializer;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage::content_manager::errors::StorageError;

use super::bm25::Bm25Config;
use super::service::InferenceData;

#[derive(Debug, Serialize, Clone)]
pub struct InferenceInput {
    pub data: Value,
    pub data_type: String,
    pub model: String,
    pub options: Option<HashMap<String, Value>>,
}

impl InferenceInput {
    /// Attempts to parse the input's options into a local model config.
    pub fn parse_bm25_config(&self) -> Result<Bm25Config, StorageError> {
        let options = self.options.clone().unwrap_or_default();
        Bm25Config::deserialize(options.into_deserializer())
            .map_err(|err| StorageError::bad_input(format!("Invalid BM25 config: {err:#?}")))
    }
}

const DOCUMENT_DATA_TYPE: &str = "text";
const IMAGE_DATA_TYPE: &str = "image";
const OBJECT_DATA_TYPE: &str = "object";

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
                    data_type: DOCUMENT_DATA_TYPE.to_string(),
                    model: model.to_string(),
                    options: options.options,
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
                    data_type: IMAGE_DATA_TYPE.to_string(),
                    model: model.to_string(),
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
                    data_type: OBJECT_DATA_TYPE.to_string(),
                    model: model.to_string(),
                    options: options.options,
                }
            }
        }
    }
}
