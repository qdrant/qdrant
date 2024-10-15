use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use api::rest::Document;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage::content_manager::errors::StorageError;

use crate::common::inference::config::InferenceConfig;
use crate::settings::Settings;

#[derive(Debug, Serialize, Default)]
struct InferenceRequest {
    inputs: Vec<InferenceInput>,
    inference: String,
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
    embeddings: Vec<Embedding>,
}

#[derive(Debug, Deserialize)]
struct Embedding {
    indices: Vec<i32>,
    values: Vec<f32>,
}

impl From<Document> for InferenceInput {
    fn from(doc: Document) -> Self {
        InferenceInput {
            data: doc.text,
            data_type: "text".to_string(), // Always "text" for Document
            model: doc.model.ok_or("").unwrap(),
            options: doc.options.clone(),
        }
    }
}

pub struct InferenceService {
    config: InferenceConfig,
    client: Client,
}

static INFERENCE_SERVICE: RwLock<Option<Arc<InferenceService>>> = RwLock::new(None);

impl InferenceService {
    pub fn new(settings: Settings) -> Self {
        Self {
            config: InferenceConfig {
                url: settings.inference_address.clone(),
            },
            client: Client::new(),
        }
    }

    pub fn init(settings: Settings) {
        let mut inference_service = INFERENCE_SERVICE.write().unwrap();
        *inference_service = Some(Arc::new(Self::new(settings)));
    }

    pub fn global() -> RwLockReadGuard<'static, Option<Arc<InferenceService>>> {
        INFERENCE_SERVICE.read().unwrap()
    }

    pub(crate) fn expect(&self) -> Result<InferenceService, StorageError> {
        if self.config.url.as_ref().map_or(true, |url| url.is_empty()) {
            Err(StorageError::service_error(
                "Expected 'url' not found in configuration",
            ))
        } else {
            Ok(InferenceService {
                config: self.config.clone(),
                client: self.client.clone(),
            })
        }
    }

    pub async fn infer(&self, document: &Document) -> Result<Vec<f32>, StorageError> {
        let url = self
            .config
            .url
            .as_ref()
            .ok_or_else(|| StorageError::service_error("Inference URL is not configured"))?;

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
                StorageError::service_error(format!(
                    "Failed to send inference request: {e}, body: {error_body}"
                ))
            })?;

        let status = response.status();
        let response_body = response.text().await.map_err(|e| {
            StorageError::service_error(format!("Failed to read response body: {e}"))
        })?;

        if status == reqwest::StatusCode::BAD_REQUEST {
            let error_json: serde_json::Value =
                serde_json::from_str(&response_body).map_err(|e| {
                    StorageError::service_error(format!("Failed to parse error response: {e}"))
                })?;

            if let Some(error_message) = error_json["error"].as_str() {
                return Err(StorageError::service_error(error_message.to_string()));
            }
        }

        let inference_response: InferenceResponse =
            serde_json::from_str(&response_body).map_err(|e| {
                StorageError::service_error(format!("Failed to parse inference response: {e}"))
            })?;

        if inference_response.embeddings.is_empty() {
            return Err(StorageError::service_error(
                "Inference response contained no embeddings".to_string(),
            ));
        }

        Ok(inference_response.embeddings[0].values.clone())
    }
}
