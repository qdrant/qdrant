use http::{HeaderMap, HeaderValue, Method, Uri};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize, Serialize)]
pub struct Solution {
    pub message: String,
    pub action: Option<Action>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Action {
    #[serde(with = "http_serde::method")]
    pub method: Method,

    #[serde(with = "http_serde::uri")]
    pub uri: Uri,

    #[serde(with = "http_serde::header_map")]
    #[serde(skip_serializing_if = "HeaderMap::is_empty")]
    pub headers: HeaderMap<HeaderValue>,

    pub body: Option<Value>,
}
