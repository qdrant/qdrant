use std::time::{Duration, SystemTime};

use itertools::{Either, Itertools};
use reqwest::header::HeaderMap;
use reqwest::{Client, StatusCode};
use segment::index::field_index::full_text_index::bm25_embedder::UnknownLanguage;
use tokio_util::task::AbortOnDropHandle;

use crate::error::{InferenceError, Rejection, TransportStage};
use crate::input::{InferenceInput, InferenceType};
use crate::local_model::{infer_local, is_local_model};
use crate::merge::merge_position_items;
use crate::wire::{ErrorBody, InferenceRequest, InferenceResponse};

#[derive(Debug, Clone)]
pub struct InferenceClientConfig {
    /// The inference service's full `/api/v1/infer` URL. Without it only local models work.
    pub address: Option<String>,
    /// Upper bound of a request, unless [`RequestParams::timeout`] sets another one.
    pub timeout: Duration,
    /// Token sent with every request that doesn't bring its own.
    pub token: Option<String>,
    pub user_agent: Option<String>,
    /// What the local BM25 model does with an unrecognized `language` option: the qdrant server
    /// keeps the deprecated lenient behavior, other callers can reject it.
    pub unknown_bm25_language: UnknownLanguage,
}

/// What a request brings to its inference call besides the inputs.
#[derive(Debug, Clone, Default)]
pub struct RequestParams {
    /// Sent instead of the configured token.
    pub token: Option<String>,
    /// Extra request headers: the caller's provider keys (see
    /// [`is_provider_api_key`](crate::is_provider_api_key)), accounting scope, …
    pub headers: HeaderMap,
    /// Replaces the configured timeout for this request.
    pub timeout: Option<Duration>,
}

pub struct InferenceClient {
    address: Option<String>,
    token: Option<String>,
    unknown_bm25_language: UnknownLanguage,
    http: Client,
}

impl InferenceClient {
    pub fn new(config: InferenceClientConfig) -> Result<Self, reqwest::Error> {
        let InferenceClientConfig {
            address,
            timeout,
            token,
            user_agent,
            unknown_bm25_language,
        } = config;
        let mut builder = Client::builder().timeout(timeout);
        if let Some(user_agent) = user_agent {
            builder = builder.user_agent(user_agent);
        }
        Ok(Self {
            address,
            token,
            unknown_bm25_language,
            http: builder.build()?,
        })
    }

    pub fn address(&self) -> Option<&str> {
        self.address.as_deref()
    }

    /// One embedding per input, in the inputs' order: local models (BM25) in this process, the
    /// rest through the inference service in a single request.
    pub async fn infer(
        &self,
        inference_inputs: Vec<InferenceInput>,
        inference_type: InferenceType,
        params: RequestParams,
    ) -> Result<InferenceResponse, InferenceError> {
        let (
            (local_inference_inputs, local_inference_positions),
            (remote_inference_inputs, remote_inference_positions),
        ): ((Vec<_>, Vec<_>), (Vec<_>, Vec<_>)) = inference_inputs
            .into_iter()
            // Keep track of the input's positions so we can properly merge them together later.
            .enumerate()
            .partition_map(|(pos, input)| {
                if is_local_model(&input.model) {
                    Either::Left((input, pos))
                } else {
                    Either::Right((input, pos))
                }
            });

        // Local models are synchronous CPU work (tokenization, stemming, hashing), so move it
        // off the async worker. Skip the round-trip through the blocking pool when every input
        // is remote, which is the common case.
        let local_results = if local_inference_inputs.is_empty() {
            vec![]
        } else {
            let unknown_bm25_language = self.unknown_bm25_language;
            AbortOnDropHandle::new(tokio::task::spawn_blocking(move || {
                infer_local(
                    local_inference_inputs,
                    inference_type,
                    unknown_bm25_language,
                )
            }))
            .await
            .map_err(|err| InferenceError::Internal(format!("local inference failed: {err}")))??
        };

        // If local models is also empty, we automatically return an empty response here.
        if remote_inference_inputs.is_empty() {
            return Ok(InferenceResponse {
                embeddings: local_results,
                usage: None, // No usage since everything was processed locally.
            });
        }

        let remote = self
            .infer_remote(remote_inference_inputs, inference_type, params)
            .await?;

        if local_results.is_empty() {
            return Ok(remote);
        }

        let InferenceResponse { embeddings, usage } = remote;
        let embeddings = merge_position_items(
            local_results,
            local_inference_positions,
            embeddings,
            remote_inference_positions,
        )
        .ok_or_else(|| {
            InferenceError::Internal("local and remote inference results don't line up".into())
        })?;

        Ok(InferenceResponse {
            embeddings,
            usage, // Only account for usage of remote.
        })
    }

    async fn infer_remote(
        &self,
        inputs: Vec<InferenceInput>,
        inference_type: InferenceType,
        params: RequestParams,
    ) -> Result<InferenceResponse, InferenceError> {
        let RequestParams {
            token,
            headers,
            timeout,
        } = params;

        let Some(url) = self.address.as_ref() else {
            return Err(InferenceError::NotConfigured);
        };

        let input_count = inputs.len();
        let body = InferenceRequest {
            inputs,
            inference: Some(inference_type),
            token: token.or_else(|| self.token.clone()),
        };

        let mut request = self.http.post(url).json(&body);
        if let Some(timeout) = timeout {
            request = request.timeout(timeout);
        }
        if !headers.is_empty() {
            request = request.headers(headers);
        }

        let (status, body, retry_after) = match request.send().await {
            Ok(response) => {
                let status = response.status();
                let retry_after = parse_retry_after(response.headers());
                let body = response
                    .text()
                    .await
                    .map_err(|err| InferenceError::transport(TransportStage::ReadBody, &err))?;
                (status, body, retry_after)
            }
            Err(err) => match err.status() {
                Some(status) => (status, err.to_string(), None),
                None => return Err(InferenceError::transport(TransportStage::Send, &err)),
            },
        };

        if status != StatusCode::OK {
            let error = serde_json::from_str::<ErrorBody>(&body)
                .ok()
                .map(|ErrorBody { error }| error);
            return Err(InferenceError::Rejected(Rejection {
                status,
                body,
                error,
                retry_after,
            }));
        }

        let response: InferenceResponse = serde_json::from_str(&body).map_err(|err| {
            InferenceError::InvalidResponse(format!(
                "Failed to parse successful inference response: {err}. Response body: {body}"
            ))
        })?;

        if response.embeddings.len() != input_count {
            return Err(InferenceError::InvalidResponse(format!(
                "Inference service returned {} embeddings for {input_count} inputs",
                response.embeddings.len(),
            )));
        }

        Ok(response)
    }
}

/// `Retry-After`: seconds or an HTTP date.
fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    httpdate::parse_http_date(value)
        .ok()?
        .duration_since(SystemTime::now())
        .ok()
}
