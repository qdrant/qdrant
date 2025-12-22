use std::collections::HashMap;
use std::convert::Infallible;

use actix_web::{FromRequest, http::header::HeaderMap};
use futures::future::{Ready, ready};

const PROVIDERS: [Provider; 4] = [
    Provider::OpenAI,
    Provider::JinaAI,
    Provider::Cohere,
    Provider::OpenRouter,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Provider {
    OpenAI,
    JinaAI,
    Cohere,
    OpenRouter,
}

impl Provider {
    pub fn as_api_key(&self) -> &str {
        match self {
            Self::OpenAI => "openai-api-key",
            Self::JinaAI => "jina-api-key",
            Self::Cohere => "cohere-api-key",
            Self::OpenRouter => "openrouter-api-key",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct ApiKeys(pub Option<HashMap<Provider, String>>);

impl ApiKeys {
    pub fn into_inner(self) -> HashMap<Provider, String> {
        self.0.unwrap_or_default()
    }

    pub fn get(&self, p: Provider) -> Option<&str> {
        self.0.as_ref()?.get(&p).map(String::as_str)
    }

    fn from_fn<F>(f: F) -> Self
    where
        F: Fn(&str) -> Option<String>,
    {
        let mut map = HashMap::<Provider, String>::new();

        for p in PROVIDERS {
            if let Some(v) = f(p.as_api_key()) {
                map.insert(p, v);
            }
        }

        Self((!map.is_empty()).then_some(map))
    }

    pub fn from_http_headers(headers: &HeaderMap) -> Self {
        Self::from_fn(|key| {
            headers
                .get(key)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_owned())
        })
    }

    pub fn from_grpc_metadata(metadata: &tonic::metadata::MetadataMap) -> Self {
        Self::from_fn(|key| {
            metadata
                .get(key)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_owned())
        })
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
