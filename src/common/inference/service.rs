use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use api::rest::{Document, Image, InferenceObject};
use collection::operations::point_ops::VectorPersisted;
use parking_lot::RwLock;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage::content_manager::errors::StorageError;

use crate::common::inference::config::InferenceConfig;

const DOCUMENT_DATA_TYPE: &str = "text";
const IMAGE_DATA_TYPE: &str = "image";
const OBJECT_DATA_TYPE: &str = "object";

#[derive(Debug, Serialize, Default, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum InferenceType {
    #[default]
    Update,
    Search,
}

impl Display for InferenceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("{self:?}").to_lowercase())
    }
}

#[derive(Debug, Serialize)]
pub struct InferenceRequest {
    pub(crate) inputs: Vec<InferenceInput>,
    pub(crate) inference: Option<InferenceType>,
    #[serde(default)]
    pub(crate) token: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InferenceInput {
    data: Value,
    data_type: String,
    model: String,
    options: Option<HashMap<String, Value>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct InferenceResponse {
    pub(crate) embeddings: Vec<VectorPersisted>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum InferenceData {
    Document(Document),
    Image(Image),
    Object(InferenceObject),
}

impl InferenceData {
    pub(crate) fn type_name(&self) -> &'static str {
        match self {
            InferenceData::Document(_) => "document",
            InferenceData::Image(_) => "image",
            InferenceData::Object(_) => "object",
        }
    }
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

pub struct InferenceService {
    pub(crate) config: InferenceConfig,
    pub(crate) client: Client,
}

static INFERENCE_SERVICE: RwLock<Option<Arc<InferenceService>>> = RwLock::new(None);

impl InferenceService {
    pub fn new(config: InferenceConfig) -> Self {
        let timeout = Duration::from_secs(config.timeout);
        Self {
            config,
            client: Client::builder()
                .timeout(timeout)
                .build()
                .expect("Invalid timeout value for HTTP client"),
        }
    }

    pub fn init_global(config: InferenceConfig) -> Result<(), StorageError> {
        let mut inference_service = INFERENCE_SERVICE.write();

        if config.token.is_none() {
            return Err(StorageError::service_error(
                "Cannot initialize InferenceService: token is required but not provided in config",
            ));
        }

        if config.address.is_none() || config.address.as_ref().unwrap().is_empty() {
            return Err(StorageError::service_error(
                "Cannot initialize InferenceService: address is required but not provided or empty in config"
            ));
        }

        *inference_service = Some(Arc::new(Self::new(config)));
        Ok(())
    }

    pub fn get_global() -> Option<Arc<InferenceService>> {
        INFERENCE_SERVICE.read().as_ref().cloned()
    }

    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        if self
            .config
            .address
            .as_ref()
            .map_or(true, |url| url.is_empty())
        {
            return Err(StorageError::service_error(
                "InferenceService configuration error: address is missing or empty",
            ));
        }
        Ok(())
    }

    pub async fn infer(
        &self,
        inference_inputs: Vec<InferenceInput>,
        inference_type: InferenceType,
    ) -> Result<Vec<VectorPersisted>, StorageError> {
        let request = InferenceRequest {
            inputs: inference_inputs,
            inference: Some(inference_type),
            token: self.config.token.clone(),
        };

        let url = self.config.address.as_ref().ok_or_else(|| {
            StorageError::service_error(
                "InferenceService URL not configured - please provide valid address in config",
            )
        })?;

        let response = self
            .client
            .post(url)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                let error_body = e.to_string();
                StorageError::service_error(format!(
                    "Failed to send inference request to {url}: {e}, error details: {error_body}",
                ))
            })?;

        let status = response.status();
        let response_body = response.text().await.map_err(|e| {
            StorageError::service_error(format!("Failed to read inference response body: {e}",))
        })?;

        Self::handle_inference_response(status, &response_body)
    }

    pub(crate) fn handle_inference_response(
        status: reqwest::StatusCode,
        response_body: &str,
    ) -> Result<Vec<VectorPersisted>, StorageError> {
        match status {
            reqwest::StatusCode::OK => {
                let inference_response: InferenceResponse = serde_json::from_str(response_body)
                    .map_err(|e| {
                        StorageError::service_error(format!(
                            "Failed to parse successful inference response: {e}. Response body: {response_body}",
                        ))
                    })?;

                if inference_response.embeddings.is_empty() {
                    Err(StorageError::service_error(
                        "Inference response contained no embeddings - this may indicate an issue with the model or input"
                    ))
                } else {
                    Ok(inference_response.embeddings)
                }
            }
            reqwest::StatusCode::BAD_REQUEST => {
                let error_json: Value = serde_json::from_str(response_body).map_err(|e| {
                    StorageError::service_error(format!(
                        "Failed to parse error response: {e}. Raw response: {response_body}",
                    ))
                })?;

                if let Some(error_message) = error_json["error"].as_str() {
                    Err(StorageError::bad_request(format!(
                        "Inference request validation failed: {error_message}",
                    )))
                } else {
                    Err(StorageError::bad_request(format!(
                        "Invalid inference request: {response_body}",
                    )))
                }
            }
            status @ (reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN) => {
                Err(StorageError::service_error(format!(
                    "Authentication failed for inference service ({status}): {response_body}",
                )))
            }
            status @ (reqwest::StatusCode::INTERNAL_SERVER_ERROR
            | reqwest::StatusCode::SERVICE_UNAVAILABLE
            | reqwest::StatusCode::GATEWAY_TIMEOUT) => Err(StorageError::service_error(format!(
                "Inference service error ({status}): {response_body}",
            ))),
            _ => Err(StorageError::service_error(format!(
                "Unexpected inference service response ({status}): {response_body}"
            ))),
        }
    }
}
