use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use actix_web::http::header::HttpDate;
use api::rest::models::InferenceUsage;
use api::rest::{Document, Image, InferenceObject};
use collection::operations::point_ops::VectorPersisted;
use itertools::{Either, Itertools};
use parking_lot::RwLock;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use storage::content_manager::errors::StorageError;

pub use super::inference_input::InferenceInput;
use super::local_model;
use crate::common::inference::InferenceToken;
use crate::common::inference::config::InferenceConfig;

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

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(Serialize))]
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

#[derive(Debug, Deserialize)]
struct InferenceError {
    pub error: String,
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
    pub(crate) client: Client,
}

static INFERENCE_SERVICE: RwLock<Option<Arc<InferenceService>>> = RwLock::new(None);

impl InferenceService {
    pub fn new(config: Option<InferenceConfig>) -> Self {
        let config = config.unwrap_or_default();
        let timeout = Duration::from_secs(config.timeout);
        Self {
            config,
            client: Client::builder()
                .timeout(timeout)
                .build()
                .expect("Invalid timeout value for HTTP client"),
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

    pub async fn infer(
        &self,
        inference_inputs: Vec<InferenceInput>,
        inference_type: InferenceType,
        inference_token: InferenceToken,
    ) -> Result<InferenceResponse, StorageError> {
        let (
            (local_inference_inputs, local_inference_positions),
            (remote_inference_inputs, remote_inference_positions),
        ): ((Vec<_>, Vec<_>), (Vec<_>, Vec<_>)) = inference_inputs
            .into_iter()
            // Keep track of the input's positions so we can properly merge them together later.
            .enumerate()
            .partition_map(|(pos, input)| {
                // Check if input is targeting a local model or the configured remote server.
                if local_model::is_local_model(&input.model) {
                    Either::Left((input, pos))
                } else {
                    Either::Right((input, pos))
                }
            });

        // Run inference on local models
        let local_model_results = local_model::infer_local(local_inference_inputs, inference_type)?;

        // Early return with the local model's results if no other inference_inputs were passed.
        // If local models is also empty, we automatically return an empty response here.
        if remote_inference_inputs.is_empty() {
            return Ok(InferenceResponse {
                embeddings: local_model_results,
                usage: None, // No usage since everything was processed locally.
            });
        }

        let remote_result = self
            .infer_remote(remote_inference_inputs, inference_type, inference_token)
            .await?;

        Ok(Self::merge_local_and_remote_result(
            local_model_results,
            local_inference_positions,
            remote_result,
            remote_inference_positions,
        ))
    }

    async fn infer_remote(
        &self,
        inference_inputs: Vec<InferenceInput>,
        inference_type: InferenceType,
        inference_token: InferenceToken,
    ) -> Result<InferenceResponse, StorageError> {
        // Assume that either:
        // - User doesn't have access to generating random JWT tokens (like in serverless)
        // - Inference server checks validity of the tokens.

        let token = inference_token.0.or_else(|| self.config.token.clone());

        let Some(url) = self.config.address.as_ref() else {
            return Err(StorageError::service_error(
                "InferenceService URL not configured - please provide valid address in config",
            ));
        };

        let request = InferenceRequest {
            inputs: inference_inputs,
            inference: Some(inference_type),
            token,
        };

        let response = self.client.post(url).json(&request).send().await;

        let (response_body, status, retry_after) = match response {
            Ok(response) => {
                let status = response.status();
                let retry_after = Self::parse_retry_after(response.headers());
                match response.text().await {
                    Ok(body) => (body, status, retry_after),
                    Err(err) => {
                        return Err(StorageError::service_error(format!(
                            "Failed to read inference response body: {err}"
                        )));
                    }
                }
            }
            Err(error) => {
                if let Some(status) = error.status() {
                    (error.to_string(), status, None)
                } else {
                    return Err(StorageError::service_error(format!(
                        "Failed to send inference request: {error}"
                    )));
                }
            }
        };

        Self::handle_inference_response(status, &response_body, retry_after)
    }

    fn merge_local_and_remote_result(
        local_results: Vec<VectorPersisted>,
        local_pos: Vec<usize>,
        remote_res: InferenceResponse,
        remote_pos: Vec<usize>,
    ) -> InferenceResponse {
        // Skip merging with local results if we only have inference results from remote.
        if local_results.is_empty() {
            return remote_res;
        }

        // Merge remote results and local results together in the exact same order they have been passed.
        let merged = merge_position_items(
            local_results,
            local_pos,
            remote_res.embeddings,
            remote_pos,
        )
        .expect(
            "Expected local results and remote items being contiguous. This is an internal bug!",
        );

        InferenceResponse {
            embeddings: merged,
            usage: remote_res.usage, // Only account for usage of remote.
        }
    }

    fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
        headers
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| {
                // Check if the value is a valid duration in seconds
                if let Ok(seconds) = value.parse::<u64>() {
                    return Some(Duration::from_secs(seconds));
                }

                // Check if the value is a valid Date
                if let Ok(http_date) = value.parse::<HttpDate>() {
                    let ts = SystemTime::from(http_date);
                    return ts
                        .duration_since(SystemTime::now())
                        .ok()
                        .map(|d| d.max(Duration::ZERO));
                }

                None
            })
    }

    pub(crate) fn handle_inference_response(
        status: reqwest::StatusCode,
        response_body: &str,
        retry_after: Option<Duration>,
    ) -> Result<InferenceResponse, StorageError> {
        match status {
            reqwest::StatusCode::OK => {
                serde_json::from_str(response_body)
                    .map_err(|e| {
                        StorageError::service_error(format!(
                            "Failed to parse successful inference response: {e}. Response body: {response_body}",
                        ))
                    })
            }
            reqwest::StatusCode::BAD_REQUEST => {
                // Try to extract error description from the response body, if it is a valid JSON
                let parsed_body: Result<InferenceError, _> = serde_json::from_str(response_body);
                match parsed_body {
                    Ok(InferenceError { error}) => {
                        Err(StorageError::bad_request(format!(
                            "Inference request validation failed: {error}",
                        )))
                    }
                    Err(_) => {
                        Err(StorageError::bad_request(format!(
                            "Invalid inference request: {response_body}",
                        )))
                    }
                }
            }
            status @ (reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN) => {
                Err(StorageError::service_error(format!(
                    "Authentication failed for inference service ({status}): {response_body}",
                )))
            }
            status @ reqwest::StatusCode::TOO_MANY_REQUESTS => {
                Err(StorageError::rate_limit_exceeded(
                    format!("Too many requests for inference service ({status}): {response_body}"),
                    retry_after,
                ))
            }
            status @ (reqwest::StatusCode::INTERNAL_SERVER_ERROR
            | reqwest::StatusCode::SERVICE_UNAVAILABLE
            | reqwest::StatusCode::GATEWAY_TIMEOUT) => Err(StorageError::service_error(format!(
                "Inference service error ({status}): {response_body}",
            ))),
            _ => {
                if status.is_server_error() {
                    Err(StorageError::service_error(format!(
                        "Inference service error ({status}): {response_body}",
                    )))
                } else if status.is_client_error() {
                    Err(StorageError::bad_request(format!(
                        "Inference can't process request ({status}): {response_body}",
                    )))
                } else {
                    Err(StorageError::service_error(format!(
                        "Unexpected inference error ({status}): {response_body}",
                    )))
                }
            },
        }
    }

    fn is_address_valid(&self) -> bool {
        self.config.address.is_none() // In BM25 we don't need an address so we allow InferenceService to have an empty address.
        || self.config.address.as_ref().is_some_and(|i| !i.is_empty())
    }
}

/// 2-way merge of lists with `PositionItems`. Also checks for skipped items and returns `None` in case an item is left out.
fn merge_position_items<I>(
    left: impl IntoIterator<Item = I>,
    left_pos: Vec<usize>,
    right: impl IntoIterator<Item = I>,
    right_pos: Vec<usize>,
) -> Option<Vec<I>> {
    let left_iter = left.into_iter().zip(left_pos);
    let right_iter = right.into_iter().zip(right_pos);

    let mut i = 0; // Check that we cover all items and don't skip any.
    left_iter
        .merge_by(right_iter, |l: &(I, usize), r: &(I, usize)| l.1 < r.1)
        .map(|item| {
            if item.1 == i {
                i += 1;
                Some(item.0)
            } else {
                None
            }
        })
        .collect::<Option<Vec<_>>>()
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use api::rest::Bm25Config;
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    use serde_json::{Value, json};

    use super::*;
    use crate::common::inference::bm25::Bm25;
    use crate::common::inference::inference_input::InferenceDataType;

    const BM25_LOCAL_MODEL_NAME: &str = "bm25";

    #[test]
    fn test_merge_position_items() {
        let (left, right): ((Vec<_>, Vec<_>), (Vec<_>, Vec<_>)) =
            (0..1000).map(|i| (i, i)).partition(|i| i.0 % 7 == 0);
        let merged = merge_position_items(left.0, left.1, right.0, right.1);
        assert_eq!(merged, Some((0..1000).collect::<Vec<_>>()));
    }

    #[test]
    fn test_merge_position_items_fail() {
        let (left, mut right): ((Vec<_>, Vec<_>), (Vec<_>, Vec<_>)) =
            (0..1000).map(|i| (i, i)).partition(|i| i.0 % 7 == 0);

        right.0.remove(5);
        right.1.remove(5);

        let merged = merge_position_items(left.0, left.1, right.0, right.1);

        // We were missing an item and therefore expect `None`.
        assert_eq!(merged, None);
    }

    #[tokio::test]
    async fn test_bm25_end_to_end() {
        let mut rng = StdRng::seed_from_u64(42);

        // Test without any BM25
        let only_inference_inputs: Vec<_> = (0..rng.random_range(30..100))
            .map(|_| make_normal_inference_input("this is some input", &mut rng))
            .collect();
        let res = run_inference_with_mocked_remote(only_inference_inputs.clone()).await;
        check_inference_response(only_inference_inputs, res);

        // Test with only BM25
        let only_bm25_inputs: Vec<_> = (0..rng.random_range(30..100))
            .map(|_| make_bm25_inference_input("this is some input"))
            .collect();
        let res = run_inference_with_mocked_remote(only_bm25_inputs.clone()).await;
        check_inference_response(only_bm25_inputs, res);

        // Test BM25 and inference mixed.
        let mut inputs: Vec<InferenceInput> = vec![];
        inputs.extend(
            (0..rng.random_range(30..100)).map(|_| make_bm25_inference_input("this is some input")),
        );
        inputs.extend(
            (0..rng.random_range(30..100))
                .map(|_| make_normal_inference_input("this is some input", &mut rng)),
        );
        inputs.shuffle(&mut rng);
        let res = run_inference_with_mocked_remote(inputs.clone()).await;
        check_inference_response(inputs, res);
    }

    fn make_normal_inference_input(input: &str, rand: &mut StdRng) -> InferenceInput {
        let options = if rand.random_bool(0.3) {
            let mut opts = HashMap::default();
            let value = rand.random_iter::<char>().take(10).collect::<String>(); // Test utf8
            opts.insert("some-key".to_string(), Value::String(value));
            Some(opts)
        } else {
            None
        };

        InferenceInput {
            data: Value::String(input.to_string()),
            data_type: InferenceDataType::Text,
            model: "anyModel".to_string(),
            options,
        }
    }

    fn make_bm25_inference_input(input: &str) -> InferenceInput {
        let bm25_config = Bm25Config::default();

        let options: HashMap<String, Value> =
            serde_json::from_str(&serde_json::to_string(&bm25_config).unwrap()).unwrap();

        InferenceInput {
            data: Value::String(input.to_string()),
            data_type: InferenceDataType::Text,
            model: BM25_LOCAL_MODEL_NAME.to_string(),
            options: Some(options),
        }
    }

    fn check_inference_response(inputs: Vec<InferenceInput>, response: InferenceResponse) {
        assert_eq!(inputs.len(), response.embeddings.len());

        for (idx, (input, response)) in inputs.into_iter().zip(response.embeddings).enumerate() {
            if input.model == BM25_LOCAL_MODEL_NAME {
                // In our test-setup, only BM25 returns sparse vectors. Normal inference is mocked
                // and always returns dense vectors.
                assert!(matches!(response, VectorPersisted::Sparse(..)));
                let bm25_config = InferenceInput::parse_bm25_config(input.options).unwrap();

                // Re-run bm25 and check that response is correct.
                let bm25 = Bm25::new(bm25_config).doc_embed(input.data.as_str().unwrap());
                assert_eq!(response, bm25);
            } else {
                let expected_vector = VectorPersisted::Dense(vec![0.0; idx]);
                assert_eq!(response, expected_vector);
            }
        }
    }

    async fn run_inference_with_mocked_remote(
        inference_inputs: Vec<InferenceInput>,
    ) -> InferenceResponse {
        // Request a new server from the pool
        let mut server = mockito::Server::new_async().await;

        // Create dummy dense vectors for non-bm25 inputs with the length of the index.
        // The dummy dense vector have the dimension of the position they appeared in `inference_inputs`,
        // so we can easily check for correct ordering later, although it is a bit hacky.
        let expected_embeddings: Vec<_> = inference_inputs
            .iter()
            .enumerate()
            .filter(|(_, item)| item.model != BM25_LOCAL_MODEL_NAME)
            .map(|(index, _)| {
                let values = vec![0.0; index];
                VectorPersisted::Dense(values)
            })
            .collect();

        // Create an HTTP mock
        let mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "text/json")
            .with_body(
                json!(InferenceResponse {
                    embeddings: expected_embeddings,
                    usage: None,
                })
                .to_string(),
            )
            .create_async()
            .await;

        let config = InferenceConfig {
            address: Some(server.url()), // Use mock's URL as address when doing inference.
            timeout: 5,                  // Mock should answer fast enough.
            token: Some(String::default()),
        };

        let service = InferenceService::new(Some(config));

        let has_remote_inference_items = inference_inputs
            .iter()
            .any(|i| i.model != BM25_LOCAL_MODEL_NAME);

        let res = service
            .infer(
                inference_inputs,
                InferenceType::Update,
                InferenceToken::new("key".to_string()),
            )
            .await
            .expect("Failed to do inference");

        // We expect exactly 1 request if there is any inference (non-bm25) request
        // and 0 if all inputs are bm25.
        if has_remote_inference_items {
            mock.expect(1).assert_async().await;
        } else {
            mock.expect(0).assert_async().await;
        }

        res
    }
}
