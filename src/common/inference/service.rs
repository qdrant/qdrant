use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::Duration;

use api::rest::Document;
use collection::operations::point_ops::VectorPersisted;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage::content_manager::errors::StorageError;

use crate::common::inference::config::InferenceConfig;

const DOCUMENT_DATA_TYPE: &str = "text";
const IMAGE_DATA_TYPE: &str = "image";
const OBJECT_DATA_TYPE: &str = "object";
const AUDIO_DATA_TYPE: &str = "audio";

#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
enum InferenceType {
    #[default]
    Document,
    Query,
}

impl Display for InferenceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("{self:?}").to_lowercase())
    }
}

#[derive(Debug, Serialize, Default)]
struct InferenceRequest {
    inputs: Vec<InferenceInput>,
    inference: InferenceType,
    #[serde(default)]
    token: Option<String>,
}

#[derive(Debug, Serialize)]
struct InferenceInput {
    data: String,
    data_type: String,
    model: String,
    options: Option<HashMap<String, Value>>,
}

#[derive(Debug, Deserialize)]
struct InferenceResponse {
    embeddings: Vec<VectorPersisted>,
}

impl From<Document> for InferenceInput {
    fn from(doc: Document) -> Self {
        let Document {
            text,
            model,
            options,
        } = doc;

        InferenceInput {
            data: text,
            data_type: DOCUMENT_DATA_TYPE.to_string(), // Always "text" for Document
            model: model.unwrap_or_default(),
            options,
        }
    }
}

pub struct InferenceService {
    config: InferenceConfig,
    client: Client,
}

static INFERENCE_SERVICE: RwLock<Option<Arc<InferenceService>>> = RwLock::new(None);

impl InferenceService {
    pub fn new(config: InferenceConfig) -> Self {
        Self {
            config: config.clone(),
            client: Client::builder()
                .timeout(Duration::from_secs(config.timeout))
                .build()
                .expect("Failed to create HTTP client"),
        }
    }

    pub fn init(config: InferenceConfig) -> Result<(), StorageError> {
        let mut inference_service = INFERENCE_SERVICE
            .write()
            .map_err(|_| StorageError::service_error("Failed to acquire write lock"))?;
        *inference_service = Some(Arc::new(Self::new(config)));
        Ok(())
    }

    pub fn global() -> RwLockReadGuard<'static, Option<Arc<InferenceService>>> {
        INFERENCE_SERVICE.read().unwrap()
    }

    pub(crate) fn expect(&self) -> Result<InferenceService, StorageError> {
        if self
            .config
            .address
            .as_ref()
            .map_or(true, |url| url.is_empty())
        {
            Err(StorageError::inference_error(
                "Expected 'address' not found in configuration",
            ))
        } else {
            Ok(InferenceService {
                config: self.config.clone(),
                client: self.client.clone(),
            })
        }
    }

    pub async fn infer(&self, document: &Document) -> Result<Vec<VectorPersisted>, StorageError> {
        let url = self
            .config
            .address
            .as_ref()
            .ok_or_else(|| StorageError::inference_error("Inference URL is not configured"))?;

        let request = InferenceRequest {
            inputs: vec![InferenceInput::from(document.clone())],
            inference: InferenceType::Document,
            token: Option::from("todo: token will be here".to_string()),
        };

        let response = self
            .client
            .post(url)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                let error_body = e.to_string();
                StorageError::inference_error(format!(
                    "Failed to send inference request: {e}, body: {error_body}"
                ))
            })?;

        let status = response.status();
        let response_body = response.text().await.map_err(|e| {
            StorageError::inference_error(format!("Failed to read response body: {e}"))
        })?;

        Self::handle_inference_response(status, &response_body)
    }

    fn handle_inference_response(
        status: reqwest::StatusCode,
        response_body: &str,
    ) -> Result<Vec<VectorPersisted>, StorageError> {
        match status {
            reqwest::StatusCode::BAD_REQUEST => {
                let error_json: Value = serde_json::from_str(response_body).map_err(|e| {
                    StorageError::inference_error(format!("Failed to parse error response: {e}"))
                })?;

                if let Some(error_message) = error_json["error"].as_str() {
                    Err(StorageError::inference_error(error_message))
                } else {
                    Err(StorageError::inference_error("Unknown error"))
                }
            }
            reqwest::StatusCode::NOT_FOUND => Err(StorageError::inference_error(response_body)),
            reqwest::StatusCode::FORBIDDEN => Err(StorageError::inference_error(response_body)),
            reqwest::StatusCode::UNAUTHORIZED => Err(StorageError::inference_error(response_body)),
            reqwest::StatusCode::OK => {
                let inference_response: InferenceResponse = serde_json::from_str(response_body)
                    .map_err(|e| {
                        StorageError::inference_error(format!(
                            "Failed to parse inference response: {e}"
                        ))
                    })?;

                if inference_response.embeddings.is_empty() {
                    Err(StorageError::inference_error(
                        "Inference response contained no embeddings",
                    ))
                } else {
                    Ok(inference_response.embeddings)
                }
            }
            reqwest::StatusCode::INTERNAL_SERVER_ERROR => {
                Err(StorageError::inference_error(response_body))
            }
            reqwest::StatusCode::SERVICE_UNAVAILABLE => {
                Err(StorageError::inference_error(response_body))
            }
            reqwest::StatusCode::GATEWAY_TIMEOUT => {
                Err(StorageError::inference_error(response_body))
            }
            _ => Err(StorageError::inference_error(format!(
                "Unexpected status code: {status}",
            ))),
        }
    }
}
