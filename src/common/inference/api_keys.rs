use std::collections::HashMap;
use std::convert::Infallible;
use std::str::FromStr;

use actix_web::{FromRequest, HttpMessage};
use futures::future::{Ready, ready};

pub const EMBEDDING_API_KEY_HEADER_SUFFIX: &str = "-api-key";

/// Combined inference authentication containing both the inference token (from JWT)
/// and external API keys (from headers like `openai-api-key`).
#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct InferenceApiKeys {
    /// Token extracted from JWT claims (sub field), used for inference tracking
    pub token: Option<String>,
    /// External provider API keys extracted from headers ending with `-api-key`
    pub keys: HashMap<String, String>,
}

impl InferenceApiKeys {
    pub fn new(token: Option<String>) -> Self {
        Self {
            token,
            keys: HashMap::new(),
        }
    }

    /// Get the token as a string slice
    pub fn token_as_str(&self) -> Option<&str> {
        self.token.as_deref()
    }

    /// Single source of truth: extracts API keys from any iterator of (key, value) string pairs
    fn extract_keys_from_pairs<'a>(&mut self, iter: impl Iterator<Item = (&'a str, &'a str)>) {
        for (k, v) in iter {
            if k.ends_with(EMBEDDING_API_KEY_HEADER_SUFFIX) {
                self.keys.insert(k.to_string(), v.to_string());
            }
        }
    }

    pub fn from_http_request(req: &actix_web::HttpRequest) -> Self {
        // Extract token from request extensions (set by auth middleware)
        let token = req
            .extensions()
            .get::<InferenceToken>()
            .and_then(|t| t.0.clone());

        let mut api_keys = Self::new(token);
        api_keys.extract_keys_from_pairs(
            req.headers()
                .iter()
                .filter_map(|(k, v)| v.to_str().ok().map(|v| (k.as_str(), v))),
        );
        api_keys
    }

    pub fn from_grpc_request<R>(req: &tonic::Request<R>) -> Self {
        // Extract token from request extensions (set by auth middleware)
        let token = req
            .extensions()
            .get::<InferenceToken>()
            .and_then(|t| t.0.clone());

        let mut api_keys = Self::new(token);
        api_keys.extract_keys_from_pairs(req.metadata().iter().filter_map(|kv| match kv {
            tonic::metadata::KeyAndValueRef::Ascii(k, v) => {
                v.to_str().ok().map(|v| (k.as_str(), v))
            }
            tonic::metadata::KeyAndValueRef::Binary(_, _) => None,
        }));
        api_keys
    }
}

/// Legacy type alias for backward compatibility during transition.
/// This will be removed once all usages are updated.
#[derive(Debug, Clone, Eq, PartialEq, Default)]
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

impl FromRequest for InferenceApiKeys {
    type Error = Infallible;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(
        req: &actix_web::HttpRequest,
        _payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        ready(Ok(InferenceApiKeys::from_http_request(req)))
    }
}

/// Extract combined inference auth from a gRPC request
pub fn extract_inference_auth<R>(req: &tonic::Request<R>) -> InferenceApiKeys {
    InferenceApiKeys::from_grpc_request(req)
}

pub fn convert_to_reqwest_headers(
    api_keys: &HashMap<String, String>,
) -> reqwest::header::HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();

    for (k, v) in api_keys {
        let k = reqwest::header::HeaderName::from_str(k).ok();
        let v = reqwest::header::HeaderValue::from_str(v).ok();
        if let Some(k) = k
            && let Some(v) = v
        {
            headers.insert(k, v);
        }
    }

    headers
}
