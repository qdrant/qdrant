use std::{collections::HashMap, convert::Infallible};

use actix_web::FromRequest;
use futures::future::{Ready, ready};

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
    pub fn into_inner(&self) -> HashMap<Provider, String> {
        self.0.clone().unwrap_or_default()
    }

    pub fn get(&self, p: Provider) -> Option<&str> {
        if self.0.as_ref().is_none() {
            return None;
        }
        let map = self.0.as_ref().unwrap();
        map.get(&p).map(|s| s.as_str())
    }
}

impl FromRequest for ApiKeys {
    type Error = Infallible;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(
        req: &actix_web::HttpRequest,
        _payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        let headers = req.headers();
        let mut map = HashMap::<Provider, String>::new();

        for (k, v) in headers {
            if k.as_str() == Provider::OpenAI.as_api_key()
                && let Some(v) = v.to_str().ok()
            {
                map.insert(Provider::OpenAI, v.to_string());
            }

            if k.as_str() == Provider::JinaAI.as_api_key()
                && let Some(v) = v.to_str().ok()
            {
                map.insert(Provider::JinaAI, v.to_string());
            }

            if k.as_str() == Provider::Cohere.as_api_key()
                && let Some(v) = v.to_str().ok()
            {
                map.insert(Provider::Cohere, v.to_string());
            }

            if k.as_str() == Provider::OpenRouter.as_api_key()
                && let Some(v) = v.to_str().ok()
            {
                map.insert(Provider::OpenRouter, v.to_string());
            }
        }

        ready(Ok(ApiKeys(Some(map))))
    }
}

pub fn extract_api_key<R>(req: &tonic::Request<R>) -> ApiKeys {
    req.extensions()
        .get::<ApiKeys>()
        .cloned()
        .unwrap_or(ApiKeys(None))
}
