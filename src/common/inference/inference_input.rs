use std::collections::HashMap;

use api::rest::{Document, Image, InferenceObject};
use serde::de::IntoDeserializer;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage::content_manager::errors::StorageError;

use super::bm25::Bm25Config;
use super::local_model::LocalModelConfig;
use super::service::InferenceData;

#[derive(Debug, Serialize, Clone)]
pub struct InferenceInput {
    pub data: Value,
    pub data_type: String,
    pub model: String,
    pub options: Option<HashMap<String, Value>>,
}

const LOCAL_MODEL_KEY: &str = "local_model";
const BM_25_MODEL: &str = "bm25";

impl InferenceInput {
    /// Tries to parse the given options as `Bm25Config`. Returns an error if the options can't be parsed.
    fn try_parse_bm25_config(options: &HashMap<String, Value>) -> Result<Bm25Config, StorageError> {
        Bm25Config::deserialize(options.clone().into_deserializer())
            .map_err(|err| StorageError::bad_input(format!("Invalid BM25 config: {err:#?}")))
    }

    /// Attempts to parse the input's options into a local model config.
    pub fn try_parse_local_model_input(&self) -> Result<Option<LocalModelConfig>, StorageError> {
        let Some(options) = self.options.as_ref() else {
            return Ok(None);
        };

        let Some(local_model_field) = options.get(LOCAL_MODEL_KEY) else {
            return Ok(None);
        };

        let Value::String(local_model_field) = local_model_field else {
            return Err(StorageError::bad_input(format!(
                "The field {LOCAL_MODEL_KEY:} must be of type String"
            )));
        };

        let config = match local_model_field.as_str() {
            BM_25_MODEL => LocalModelConfig::Bm25(Self::try_parse_bm25_config(options)?),
            _ => {
                return Err(StorageError::bad_input(format!(
                    "Invalid local model {local_model_field:?}"
                )));
            }
        };

        Ok(Some(config))
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
