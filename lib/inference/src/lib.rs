//! Inference for `document` / `image` / `object` inputs, shared by qdrant and qdrant-serverless.
//!
//! [`InferenceClient::infer`] runs the local models (BM25) in-process and sends everything else
//! to the cloud inference service in one request, then returns one vector per input in order.
//!
//! The crate knows nothing about the server's REST/gRPC request types, its error types or its
//! configuration: callers build [`InferenceInput`]s from their own requests and map
//! [`InferenceError`] into their own errors.

mod api_keys;
mod client;
mod error;
mod input;
mod local_model;
mod merge;
mod wire;

pub use api_keys::{PROVIDER_API_KEY_HEADER_SUFFIX, is_provider_api_key};
pub use client::{InferenceClient, InferenceClientConfig, RequestParams};
pub use error::{InferenceError, Rejection, TransportError, TransportStage};
pub use input::{InferenceDataType, InferenceInput, InferenceType};
pub use local_model::{is_local_model, parse_bm25_config};
pub use segment::index::field_index::full_text_index::bm25_embedder::UnknownLanguage;
pub use wire::{InferenceRequest, InferenceResponse, InferenceUsage, ModelUsage};

#[cfg(test)]
mod tests;
