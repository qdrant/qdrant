use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use api::rest::{Document, Image, InferenceObject};
use collection::operations::point_ops::VectorPersisted;
use log::{error, info, warn};
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

pub struct ConnectionPool {
    connections: RwLock<Vec<Client>>,
    size: usize,
}

impl ConnectionPool {
    pub fn new(size: usize, client_builder: reqwest::ClientBuilder) -> Self {
        let client = match client_builder.build() {
            Ok(client) => client,
            Err(e) => {
                error!("Failed to build client: {}", e);
                // Any inference attempt will fail with "No available clients..." error
                return Self {
                    connections: RwLock::new(Vec::new()),
                    size: 0,
                };
            }
        };

        let connections = vec![client; size];
        info!("Connection pool: initialized with {size} connections");
        Self {
            connections: RwLock::new(connections),
            size,
        }
    }

    pub fn get(&self) -> Option<Client> {
        let mut conns = self.connections.write();
        let client = conns.pop();
        if client.is_none() {
            warn!("Connection pool: no connections available");
        }
        client
    }

    pub fn put(&self, client: Client) {
        let mut conns = self.connections.write();
        if conns.len() < self.size {
            conns.push(client);
        } else {
            warn!(
                "Connection pool: discarding connection, pool is full at size {}",
                self.size
            );
        }
    }
}

pub struct InferenceService {
    pub(crate) config: InferenceConfig,
    pub(crate) connection_pool: ConnectionPool,
}

static INFERENCE_SERVICE: RwLock<Option<Arc<InferenceService>>> = RwLock::new(None);

impl InferenceService {
    pub fn new(config: InferenceConfig) -> Self {
        let timeout = Duration::from_secs(config.timeout);
        let client_builder = Client::builder().timeout(timeout);
        let pool_size = config.connection_pool_size;
        let connection_pool = ConnectionPool::new(pool_size, client_builder);
        Self {
            config,
            connection_pool,
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
                "Cannot initialize InferenceService: address is required but not provided or empty in config",
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

        let client = self.connection_pool.get().ok_or_else(|| {
            StorageError::service_error("No available clients in the connection pool")
        })?;

        let result = async {
            let response = client.post(url).json(&request).send().await.map_err(|e| {
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
        .await;

        self.connection_pool.put(client);
        result
    }

    pub(crate) fn handle_inference_response(
        status: reqwest::StatusCode,
        response_body: &str,
    ) -> Result<Vec<VectorPersisted>, StorageError> {
        match status {
            reqwest::StatusCode::OK => {
                let inference_response: InferenceResponse =
                    serde_json::from_str(response_body).map_err(|e| {
                        StorageError::service_error(format!(
                            "Failed to parse successful inference response: {e}. Response body: {response_body}",
                        ))
                    })?;

                if inference_response.embeddings.is_empty() {
                    Err(StorageError::service_error(
                        "Inference response contained no embeddings - this may indicate an issue with the model or input",
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_pool_creation_success() {
        let size = 5;
        let client_builder = Client::builder().timeout(Duration::from_secs(1));
        let pool = ConnectionPool::new(size, client_builder);

        assert_eq!(pool.size, size);
        assert_eq!(pool.connections.read().len(), size);
    }

    #[tokio::test]
    async fn test_get_and_put() {
        let size = 2;
        let client_builder = Client::builder().timeout(Duration::from_secs(1));
        let pool = ConnectionPool::new(size, client_builder);

        let client1 = pool.get();
        assert!(client1.is_some());
        assert_eq!(pool.connections.read().len(), 1);

        let client2 = pool.get();
        assert!(client2.is_some());
        assert_eq!(pool.connections.read().len(), 0);

        let client3 = pool.get();
        assert!(client3.is_none());
        assert_eq!(pool.connections.read().len(), 0);

        if let Some(client) = client1 {
            pool.put(client);
            assert_eq!(pool.connections.read().len(), 1);
        }
    }

    #[tokio::test]
    async fn test_pool_max_size() {
        let size = 1;
        let client_builder = Client::builder().timeout(Duration::from_secs(1));
        let pool = ConnectionPool::new(size, client_builder);

        let extra_client = Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap();

        assert_eq!(pool.connections.read().len(), 1);
        pool.put(extra_client);
        assert_eq!(pool.connections.read().len(), 1);
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        let size = 50;
        let client_builder = Client::builder().timeout(Duration::from_secs(1));
        let pool = Arc::new(ConnectionPool::new(size, client_builder));
        let tasks: Vec<_> = (0..100)
            .map(|_| {
                let pool = Arc::clone(&pool);
                tokio::spawn(async move {
                    if let Some(client) = pool.get() {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        pool.put(client);
                    }
                })
            })
            .collect();

        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(pool.connections.read().len(), size);
    }

    #[tokio::test]
    async fn test_empty_pool_behavior() {
        let size = 1;
        let client_builder = Client::builder().timeout(Duration::from_secs(1));
        let pool = ConnectionPool::new(size, client_builder);

        let client = pool.get();
        assert!(client.is_some());

        let no_client = pool.get();
        assert!(no_client.is_none());
    }

    #[tokio::test]
    async fn test_put_after_size_reduction() {
        let original_size = 2;
        let client_builder = Client::builder().timeout(Duration::from_secs(1));
        let pool = ConnectionPool::new(original_size, client_builder);

        let client1 = pool.get().unwrap();
        let client2 = pool.get().unwrap();

        let smaller_pool = ConnectionPool::new(1, Client::builder());
        smaller_pool.put(client1);
        smaller_pool.put(client2);

        assert_eq!(smaller_pool.connections.read().len(), 1);
    }
}
