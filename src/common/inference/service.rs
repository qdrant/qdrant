use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use api::rest::models::InferenceUsage;
use api::rest::{Document, Image, InferenceObject};
use collection::operations::point_ops::VectorPersisted;
use itertools::{Either, Itertools};
use parking_lot::RwLock;
use reqwest::Client;
use serde::de::IntoDeserializer;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage::content_manager::errors::StorageError;

use super::bm25::{Bm25, Bm25Config};
use crate::common::inference::InferenceToken;
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

impl InferenceInput {
    /// Tries to parse the input's options for BM25 config and returns Ok(Some(..)) if found and Ok(None) if not.
    ///
    /// Returns an error if bm25 has been explicitly enabled but config could not be deserialized properly.
    pub fn try_parse_bm25_config(&self) -> Result<Option<Bm25Config>, StorageError> {
        let Some(options) = self.options.as_ref() else {
            return Ok(None);
        };

        if options.get("use_bm25") != Some(&Value::Bool(true))
            && options.get("use_bm25") != Some(&Value::String("true".to_string()))
        {
            return Ok(None);
        }

        Bm25Config::deserialize(options.clone().into_deserializer())
            .map_err(|err| StorageError::bad_input(format!("Invalid BM25 config: {err:#?}")))
            .map(Some)
    }
}

#[derive(Debug, Deserialize)]
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
            .is_none_or(|url| url.is_empty())
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
        inference_token: InferenceToken,
    ) -> Result<InferenceResponse, StorageError> {
        let (bm25_inference_inputs, inference_inputs): (Vec<_>, Vec<_>) = inference_inputs
            .into_iter()
            // Keep track of the input's positions so we can properly merge them together later.
            .enumerate()
            .partition_map(|(pos, input)| {
                // Check if input is targeting a bm25 model or the configured remote server.
                if let Some(bm_25_config) = input.try_parse_bm25_config().transpose() {
                    Either::Left(PositionItem::new((input, bm_25_config), pos))
                } else {
                    Either::Right(PositionItem::new(input, pos))
                }
            });

        let bm25_results: Vec<_> = bm25_inference_inputs
            .into_iter()
            .map(|item| -> Result<_, StorageError> {
                let (input, bm25_config) = item.item;
                let bm25_config = bm25_config?;
                let input_str = input.data.as_str().ok_or_else(|| {
                    StorageError::service_error(
                        "Only strings supported as text type in BM25 inference!",
                    )
                })?;
                let embedding = Bm25::new(bm25_config).embed(input_str);
                Ok(PositionItem::new(embedding, item.position))
            })
            .collect::<Result<_, _>>()?;

        // Early return with bm25 results if no other inference_inputs were passed.
        // If Bm25 is also empty, we automatically return an empty response here.
        if inference_inputs.is_empty() {
            let embeddings = bm25_results.into_iter().map(|i| i.item).collect();

            return Ok(InferenceResponse {
                embeddings,
                usage: None, // No usage since everything was processed locally.
            });
        }

        // Assume that either:
        // - User doesn't have access to generating random JWT tokens (like in serverless)
        // - Inference server checks validity of the tokens.

        let token = inference_token.0.or_else(|| self.config.token.clone());

        let url = self.config.address.as_ref().ok_or_else(|| {
            StorageError::service_error(
                "InferenceService URL not configured - please provide valid address in config",
            )
        })?;

        let (remote_pos, inference_inputs): (Vec<_>, Vec<_>) = inference_inputs
            .into_iter()
            .map(|i| (i.position, i.item))
            .unzip();

        let request = InferenceRequest {
            inputs: inference_inputs,
            inference: Some(inference_type),
            token,
        };

        let response = self.client.post(url).json(&request).send().await;

        let (response_body, status) = match response {
            Ok(response) => {
                let status = response.status();
                match response.text().await {
                    Ok(body) => (body, status),
                    Err(err) => {
                        return Err(StorageError::service_error(format!(
                            "Failed to read inference response body: {err}"
                        )));
                    }
                }
            }
            Err(error) => {
                if let Some(status) = error.status() {
                    (error.to_string(), status)
                } else {
                    return Err(StorageError::service_error(format!(
                        "Failed to send inference request: {error}"
                    )));
                }
            }
        };

        let remote_res = Self::handle_inference_response(status, &response_body)?;
        Self::merge_bm25_and_remote_result(bm25_results, remote_res, remote_pos)
    }

    fn merge_bm25_and_remote_result(
        bm25_results: Vec<PositionItem<VectorPersisted>>,
        remote_res: InferenceResponse,
        remote_pos: Vec<usize>,
    ) -> Result<InferenceResponse, StorageError> {
        // Skip merging with bm25 if we only have inference results from remote.
        if bm25_results.is_empty() {
            return Ok(remote_res);
        }

        // Wrap remote items in `PositionItem`s because they need to be merged with bm25 results in the same order they have initially been passed.
        let remote_items_iter = remote_res
            .embeddings
            .into_iter()
            .zip(remote_pos)
            .map(|(item, pos)| PositionItem::new(item, pos));

        // Merge remote results and local (bm25) results together in the exact same order they have been passed.
        let merged = merge_position_items(bm25_results, remote_items_iter)
            .expect("Expected bm25 and remote items being continguous. This is an internal bug");

        Ok(InferenceResponse {
            embeddings: merged,
            usage: remote_res.usage, // Only account for usage of remote since BM25 is processed locally and doesn't need to be measured.
        })
    }

    pub(crate) fn handle_inference_response(
        status: reqwest::StatusCode,
        response_body: &str,
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
}

/// Any kind of value that has an index assigned.
struct PositionItem<I> {
    item: I,
    position: usize,
}

impl<I> PositionItem<I> {
    pub fn new(item: I, position: usize) -> Self {
        Self { item, position }
    }
}

/// 2-way merge of lists with `PositionItems`. Also checks for skipped items and returns `None` in case an item is left out.
fn merge_position_items<I>(
    left: Vec<PositionItem<I>>,
    right: impl IntoIterator<Item = PositionItem<I>>,
) -> Option<Vec<I>> {
    let mut i = 0; // Check that we cover all items and don't skip any.
    left.into_iter()
        .merge_by(right, |l, r| l.position < r.position)
        .map(|item| {
            if item.position == i {
                i += 1;
                Some(item.item)
            } else {
                None
            }
        })
        .collect::<Option<Vec<_>>>()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_merge_position_items() {
        let (left, right): (Vec<_>, Vec<_>) = (0..1000)
            .map(|i| PositionItem::new(i, i))
            .partition(|i| i.item % 7 == 0);
        let merged = merge_position_items(left, right);
        assert_eq!(merged, Some((0..1000).collect::<Vec<_>>()));
    }

    #[test]
    fn test_merge_position_items_fail() {
        let (left, mut right): (Vec<_>, Vec<_>) = (0..1000)
            .map(|i| PositionItem::new(i, i))
            .partition(|i| i.item % 7 == 0);
        right.remove(5);
        let merged = merge_position_items(left, right);
        // We were missing an item and therefore expect `None`.
        assert_eq!(merged, None);
    }
}
