use std::future::{ready, Ready};

use actix_web::body::{BoxBody, EitherBody};
use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::http::header::Header;
use actix_web::{Error, HttpResponse};
use actix_web_httpauth::headers::authorization::{Authorization, Bearer};
use constant_time_eq::constant_time_eq;
use futures_util::future::LocalBoxFuture;

pub struct ApiKey {
    api_key: String,
    whitelist: Vec<WhitelistItem>,
}

impl ApiKey {
    pub fn new(api_key: &str, whitelist: Vec<WhitelistItem>) -> Self {
        Self {
            api_key: api_key.to_string(),
            whitelist,
        }
    }
}

impl<S, B> Transform<S, ServiceRequest> for ApiKey
where
    S: Service<ServiceRequest, Response = ServiceResponse<EitherBody<B, BoxBody>>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B, BoxBody>>;
    type Error = Error;
    type InitError = ();
    type Transform = ApiKeyMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(ApiKeyMiddleware {
            api_key: self.api_key.clone(),
            whitelist: self.whitelist.clone(),
            service,
        }))
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct WhitelistItem(pub String, pub PathMode);

impl WhitelistItem {
    pub fn exact<S: Into<String>>(path: S) -> Self {
        Self(path.into(), PathMode::Exact)
    }

    pub fn prefix<S: Into<String>>(path: S) -> Self {
        Self(path.into(), PathMode::Prefix)
    }

    pub fn matches(&self, other: &str) -> bool {
        self.1.check(&self.0, other)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub enum PathMode {
    /// Path must match exactly
    Exact,
    /// Path must have given prefix
    Prefix,
}

impl PathMode {
    fn check(&self, key: &str, other: &str) -> bool {
        match self {
            Self::Exact => key == other,
            Self::Prefix => other.starts_with(key),
        }
    }
}

pub struct ApiKeyMiddleware<S> {
    api_key: String,
    /// List of items whitelisted from authentication.
    whitelist: Vec<WhitelistItem>,
    service: S,
}

impl<S> ApiKeyMiddleware<S> {
    pub fn is_path_whitelisted(&self, path: &str) -> bool {
        self.whitelist.iter().any(|item| item.matches(path))
    }
}

impl<S, B> Service<ServiceRequest> for ApiKeyMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<EitherBody<B, BoxBody>>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B, BoxBody>>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let path = req.path();

        if self.is_path_whitelisted(path) {
            return Box::pin(self.service.call(req));
        }

        // Grab API key from request
        let key =
            // Request header
            req.headers().get("api-key").and_then(|key| key.to_str().ok()).map(|key| key.to_string())
            // Fall back to authentication header with bearer token
            .or_else(|| {
                Authorization::<Bearer>::parse(&req).ok().map(|auth| auth.as_ref().token().into())
            });

        // If we have an API key, compare in constant time
        if let Some(key) = key {
            if constant_time_eq(self.api_key.as_bytes(), key.as_bytes()) {
                return Box::pin(self.service.call(req));
            }
        }

        Box::pin(async {
            Ok(req
                .into_response(HttpResponse::Forbidden().body("Invalid api-key"))
                .map_into_right_body())
        })
    }
}
