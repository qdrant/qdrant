use std::convert::Infallible;
use std::future::{Ready, ready};

use actix_web::{FromRequest, HttpMessage};

#[derive(Debug, Clone, PartialEq, Default)]
pub struct InferenceToken(pub Option<String>);

impl InferenceToken {
    pub fn new(key: impl Into<String>) -> Self {
        InferenceToken(Some(key.into()))
    }

    pub fn as_str(&self) -> Option<&str> {
        self.0.as_deref()
    }
}

impl From<&str> for InferenceToken {
    fn from(s: &str) -> Self {
        InferenceToken::new(s)
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
        ready(Ok(api_key.unwrap_or_default()))
    }
}

pub fn extract_token<R>(req: &tonic::Request<R>) -> InferenceToken {
    req.extensions()
        .get::<InferenceToken>()
        .cloned()
        .unwrap_or(InferenceToken(None))
}
