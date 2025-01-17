#![allow(dead_code)]

use std::convert::Infallible;
use std::fmt;
use std::future::{ready, Ready};

use actix_web::{FromRequest, HttpMessage};

mod batch_processing;
mod batch_processing_grpc;
pub(crate) mod config;
mod infer_processing;
pub mod query_requests_grpc;
pub mod query_requests_rest;
pub mod service;
pub mod update_requests;

#[derive(Debug, Clone, PartialEq)]
pub struct InferenceToken(pub Option<String>);

impl InferenceToken {
    pub fn new(key: String) -> Self {
        InferenceToken(Option::from(key))
    }
    pub fn as_str(&self) -> &Option<String> {
        &self.0
    }
}

impl FromRequest for InferenceToken {
    type Error = Infallible;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(
        req: &actix_web::HttpRequest,
        _payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        let api_key = req.extensions().get::<InferenceToken>().cloned();
        ready(Ok(api_key.unwrap_or(InferenceToken(None))))
    }
}

impl fmt::Display for InferenceToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

pub fn extract_token<R>(req: &tonic::Request<R>) -> InferenceToken {
    req.extensions()
        .get::<InferenceToken>()
        .cloned()
        .unwrap_or(InferenceToken(None))
}
