use std::collections::HashMap;
use std::convert::Infallible;
use std::str::FromStr;

use actix_web::FromRequest;
use actix_web::http::header::HeaderMap;
use futures::future::{Ready, ready};

pub const EMBEDDING_API_KEY_HEADER_SUFFIX: &str = "-api-key";

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct ApiKeys {
    pub keys: HashMap<String, String>,
}

impl ApiKeys {
    fn from_headers(headers: &HeaderMap) -> ApiKeys {
        let mut api_keys = Self::default();

        for (k, v) in headers {
            if k.as_str().ends_with(EMBEDDING_API_KEY_HEADER_SUFFIX)
                && let Some(v) = v.to_str().ok()
            {
                api_keys.keys.insert(k.to_string(), v.to_string());
            }
        }

        api_keys
    }

    pub fn from_http_headers(headers: &HeaderMap) -> Self {
        Self::from_headers(headers)
    }

    pub fn from_grpc_metadata(metadata: &tonic::metadata::MetadataMap) -> Self {
        Self::from_headers(&metadata.clone().into_headers().into())
    }
}

impl FromRequest for ApiKeys {
    type Error = Infallible;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(
        req: &actix_web::HttpRequest,
        _payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        ready(Ok(ApiKeys::from_http_headers(req.headers())))
    }
}

pub fn extract_api_key(metadata: &tonic::metadata::MetadataMap) -> ApiKeys {
    ApiKeys::from_grpc_metadata(metadata)
}

impl From<ApiKeys> for reqwest::header::HeaderMap {
    fn from(api_keys: ApiKeys) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();

        for (k, v) in api_keys.keys {
            let k = reqwest::header::HeaderName::from_str(&k).expect("invalid header key");
            let v = reqwest::header::HeaderValue::from_str(&v).expect("invalid header value");
            headers.insert(k, v);
        }

        headers
    }
}
