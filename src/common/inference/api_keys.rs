use std::collections::HashMap;
use std::convert::Infallible;
use std::str::FromStr;

use actix_web::FromRequest;
use actix_web::http::header::HeaderMap;
use futures::future::{Ready, ready};

pub const EMBEDDING_API_KEY_HEADER_SUFFIX: &str = "-api-key";

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct InferenceApiKeys {
    pub keys: HashMap<String, String>,
}

impl InferenceApiKeys {
    /// Single source of truth: extracts API keys from any iterator of (key, value) string pairs
    fn from_key_value_pairs<'a>(iter: impl Iterator<Item = (&'a str, &'a str)>) -> Self {
        let mut api_keys = Self::default();

        for (k, v) in iter {
            if k.ends_with(EMBEDDING_API_KEY_HEADER_SUFFIX) {
                api_keys.keys.insert(k.to_string(), v.to_string());
            }
        }

        api_keys
    }

    pub fn from_http_headers(headers: &HeaderMap) -> Self {
        Self::from_key_value_pairs(
            headers
                .iter()
                .filter_map(|(k, v)| v.to_str().ok().map(|v| (k.as_str(), v))),
        )
    }

    pub fn from_grpc_metadata(metadata: &tonic::metadata::MetadataMap) -> Self {
        Self::from_key_value_pairs(metadata.iter().filter_map(|kv| match kv {
            tonic::metadata::KeyAndValueRef::Ascii(k, v) => {
                v.to_str().ok().map(|v| (k.as_str(), v))
            }
            tonic::metadata::KeyAndValueRef::Binary(_, _) => None,
        }))
    }
}

impl FromRequest for InferenceApiKeys {
    type Error = Infallible;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(
        req: &actix_web::HttpRequest,
        _payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        ready(Ok(InferenceApiKeys::from_http_headers(req.headers())))
    }
}

pub fn extract_api_key(metadata: &tonic::metadata::MetadataMap) -> InferenceApiKeys {
    InferenceApiKeys::from_grpc_metadata(metadata)
}

impl From<InferenceApiKeys> for reqwest::header::HeaderMap {
    fn from(api_keys: InferenceApiKeys) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();

        for (k, v) in api_keys.keys {
            let k = reqwest::header::HeaderName::from_str(&k).expect("invalid header key");
            let v = reqwest::header::HeaderValue::from_str(&v).expect("invalid header value");
            headers.insert(k, v);
        }

        headers
    }
}
