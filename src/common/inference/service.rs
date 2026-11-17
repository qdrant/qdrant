use std::sync::Arc;
use std::time::Duration;

use api::rest::models::{InferenceUsage, ModelUsage};
use api::rest::{Document, Image, InferenceObject};
use collection::operations::point_ops::VectorPersisted;
use common::defaults::APP_USER_AGENT;
use inference::{
    InferenceClient, InferenceClientConfig, InferenceError, Rejection, RequestParams,
    TransportError, TransportStage, UnknownLanguage,
};
pub use inference::{InferenceRequest, InferenceType};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use storage::content_manager::errors::StorageError;

pub use super::inference_input::InferenceInput;
use crate::common::inference::api_keys::{InferenceApiKeys, convert_to_reqwest_headers};
use crate::common::inference::config::InferenceConfig;
use crate::common::inference::params::InferenceParams;

#[derive(Debug)]
pub struct InferenceResponse {
    pub embeddings: Vec<VectorPersisted>,
    pub usage: Option<InferenceUsage>,
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

pub struct InferenceService {
    pub(crate) config: InferenceConfig,
    client: InferenceClient,
    default_timeout: Duration,
}

static INFERENCE_SERVICE: RwLock<Option<Arc<InferenceService>>> = RwLock::new(None);

/// We assume that the inference provider will handle timeouts itself, if
/// not provided by the user or configured. But we need ensurance, that we don't
/// wait forever for a response.
static DEFAULT_INFERENCE_TIMEOUT_SECS: u64 = 10 * 60; // 10 minutes

impl InferenceService {
    pub fn new(config: Option<InferenceConfig>) -> Self {
        let config = config.unwrap_or_default();
        let InferenceConfig {
            address,
            timeout,
            token,
        } = &config;

        let timeout = Duration::from_secs(timeout.unwrap_or(DEFAULT_INFERENCE_TIMEOUT_SECS));
        let client = InferenceClient::new(InferenceClientConfig {
            address: address.clone(),
            timeout,
            token: token.clone(),
            user_agent: Some(APP_USER_AGENT.to_string()),
            // The server keeps accepting the deprecated BM25 `language: "none"`.
            unknown_bm25_language: UnknownLanguage::Legacy,
        })
        .expect("Invalid timeout value for HTTP client");

        Self {
            config,
            client,
            default_timeout: timeout,
        }
    }

    pub fn init_global(config: Option<InferenceConfig>) -> Result<(), StorageError> {
        let mut inference_service = INFERENCE_SERVICE.write();

        let service = Self::new(config);

        if !service.is_address_valid() {
            return Err(StorageError::service_error(
                "Cannot initialize InferenceService: address is required but not provided or empty in config",
            ));
        }

        *inference_service = Some(Arc::new(service));
        Ok(())
    }

    pub fn get_global() -> Option<Arc<InferenceService>> {
        INFERENCE_SERVICE.read().as_ref().cloned()
    }

    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        if !self.is_address_valid() {
            return Err(StorageError::service_error(
                "InferenceService configuration error: address is missing or empty",
            ));
        }
        Ok(())
    }

    /// One embedding per input, in order: local models (BM25) in-process, the rest through the
    /// inference service.
    pub async fn infer(
        &self,
        inference_inputs: Vec<InferenceInput>,
        inference_type: InferenceType,
        inference_params: InferenceParams,
    ) -> Result<InferenceResponse, StorageError> {
        // Assume that either:
        // - User doesn't have access to generating random JWT tokens (like in serverless)
        // - Inference server checks validity of the tokens.
        let InferenceParams { api_keys, timeout } = inference_params;
        let InferenceApiKeys {
            keys: ext_api_keys,
            token,
        } = api_keys;

        let request_timeout = timeout.unwrap_or(self.default_timeout);
        let params = RequestParams {
            token,
            headers: convert_to_reqwest_headers(&ext_api_keys),
            timeout,
        };

        let inference::InferenceResponse { embeddings, usage } = self
            .client
            .infer(inference_inputs, inference_type, params)
            .await
            .map_err(|err| storage_error(err, request_timeout))?;

        Ok(InferenceResponse {
            embeddings,
            usage: usage.map(inference_usage),
        })
    }

    fn is_address_valid(&self) -> bool {
        self.config.address.is_none() // In BM25 we don't need an address so we allow InferenceService to have an empty address.
            || self.config.address.as_ref().is_some_and(|i| !i.is_empty())
    }
}

fn inference_usage(usage: inference::InferenceUsage) -> InferenceUsage {
    let inference::InferenceUsage { models } = usage;
    InferenceUsage {
        models: models
            .into_iter()
            .map(|(model, inference::ModelUsage { tokens })| (model, ModelUsage { tokens }))
            .collect(),
    }
}

fn storage_error(err: InferenceError, request_timeout: Duration) -> StorageError {
    match err {
        InferenceError::NotConfigured => StorageError::service_error(
            "InferenceService URL not configured - please provide valid address in config",
        ),
        InferenceError::InvalidInput(message) => StorageError::bad_input(message),
        InferenceError::Transport(TransportError { timeout: true, .. }) => {
            StorageError::timeout(request_timeout, "inference request")
        }
        InferenceError::Transport(TransportError {
            stage,
            timeout: false,
            message,
        }) => match stage {
            TransportStage::Send => {
                StorageError::service_error(format!("Failed to send inference request: {message}"))
            }
            TransportStage::ReadBody => StorageError::service_error(format!(
                "Failed to read inference response body: {message}"
            )),
        },
        InferenceError::Rejected(rejection) => rejection_error(rejection),
        InferenceError::InvalidResponse(message) | InferenceError::Internal(message) => {
            StorageError::service_error(message)
        }
    }
}

fn rejection_error(rejection: Rejection) -> StorageError {
    let Rejection {
        status,
        body,
        error,
        retry_after,
    } = rejection;
    match status {
        reqwest::StatusCode::BAD_REQUEST => match error {
            // Error description extracted from a JSON body
            Some(error) => {
                StorageError::bad_request(format!("Inference request validation failed: {error}",))
            }
            None => StorageError::bad_request(format!("Invalid inference request: {body}")),
        },
        reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN => {
            StorageError::service_error(format!(
                "Authentication failed for inference service ({status}): {body}",
            ))
        }
        reqwest::StatusCode::TOO_MANY_REQUESTS => StorageError::rate_limit_exceeded(
            format!("Too many requests for inference service ({status}): {body}"),
            retry_after,
        ),
        status if status.is_server_error() => {
            StorageError::service_error(format!("Inference service error ({status}): {body}"))
        }
        status if status.is_client_error() => StorageError::bad_request(format!(
            "Inference can't process request ({status}): {body}",
        )),
        status => {
            StorageError::service_error(format!("Unexpected inference error ({status}): {body}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use inference::InferenceDataType;
    use serde_json::Value;
    use tokio::net::TcpListener;

    use super::*;

    #[tokio::test]
    async fn test_request_timeout_is_a_timeout_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}/", listener.local_addr().unwrap());
        // Accepts the request and never replies.
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });

        let service = InferenceService::new(Some(InferenceConfig::new(Some(address))));
        let input = InferenceInput {
            data: Value::String("some text".to_string()),
            data_type: InferenceDataType::Text,
            model: "some-model".to_string(),
            options: None,
        };
        let params = InferenceParams::new(
            InferenceApiKeys::default(),
            Some(Duration::from_millis(100)),
        );

        let err = service
            .infer(vec![input], InferenceType::Search, params)
            .await
            .unwrap_err();
        server.abort();

        assert!(
            matches!(
                &err,
                StorageError::Timeout { description }
                    if description == "Operation 'inference request' timed out after 100ms"
            ),
            "{err}",
        );
    }
}
