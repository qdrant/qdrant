use std::future::{ready, Ready};

use actix_web::body::{BoxBody, EitherBody};
use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::http::header::Header;
use actix_web::http::Method;
use actix_web::{Error, HttpMessage as _, HttpResponse};
use actix_web_httpauth::headers::authorization::{Authorization, Bearer};
use futures_util::future::LocalBoxFuture;

use crate::common::auth::AuthKeys;

/// List of read-only POST request paths. List MUST be sorted.
const READ_ONLY_POST_PATTERNS: [&str; 11] = [
    "/collections/{name}/points",
    "/collections/{name}/points/count",
    "/collections/{name}/points/discover",
    "/collections/{name}/points/discover/batch",
    "/collections/{name}/points/recommend",
    "/collections/{name}/points/recommend/batch",
    "/collections/{name}/points/recommend/groups",
    "/collections/{name}/points/scroll",
    "/collections/{name}/points/search",
    "/collections/{name}/points/search/batch",
    "/collections/{name}/points/search/groups",
];

pub struct Auth {
    auth_keys: AuthKeys,
    whitelist: Vec<WhitelistItem>,
}

impl Auth {
    pub fn new(auth_keys: AuthKeys, whitelist: Vec<WhitelistItem>) -> Self {
        Self {
            auth_keys,
            whitelist,
        }
    }
}

impl<S, B> Transform<S, ServiceRequest> for Auth
where
    S: Service<ServiceRequest, Response = ServiceResponse<EitherBody<B, BoxBody>>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B, BoxBody>>;
    type Error = Error;
    type InitError = ();
    type Transform = AuthMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(AuthMiddleware {
            auth_keys: self.auth_keys.clone(),
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

pub struct AuthMiddleware<S> {
    auth_keys: AuthKeys,
    /// List of items whitelisted from authentication.
    whitelist: Vec<WhitelistItem>,
    service: S,
}

impl<S> AuthMiddleware<S> {
    pub fn is_path_whitelisted(&self, path: &str) -> bool {
        self.whitelist.iter().any(|item| item.matches(path))
    }
}

impl<S, B> Service<ServiceRequest> for AuthMiddleware<S>
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

        let invalid_api_key = |req: ServiceRequest| -> Self::Future {
            Box::pin(async {
                Ok(req
                    .into_response(HttpResponse::Forbidden().body("Invalid api-key"))
                    .map_into_right_body())
            })
        };

        let auth_keys = &self.auth_keys;

        let api_key = req
            .headers()
            .get("api-key")
            .and_then(|key| key.to_str().ok());

        let bearer_key = Authorization::<Bearer>::parse(&req);
        let bearer_key = bearer_key.as_ref().ok().map(|auth| auth.as_ref().token());

        if let Some(key) = api_key.or(bearer_key) {
            if auth_keys.can_write(key) || (is_read_only(&req) && auth_keys.can_read(key)) {
                return Box::pin(self.service.call(req));
            }
        };

        if let Some(claims) = auth_keys
            .jwt_parser()
            .and_then(|p| p.decode(bearer_key?).ok())
        {
            if !claims.w.unwrap_or(false) && !is_read_only(&req) {
                return invalid_api_key(req);
            }
            req.extensions_mut().insert(claims);
            return Box::pin(self.service.call(req));
        }

        invalid_api_key(req)
    }
}

fn is_read_only(req: &ServiceRequest) -> bool {
    match *req.method() {
        Method::GET => true,
        Method::POST => req
            .match_pattern()
            .map(|pattern| {
                READ_ONLY_POST_PATTERNS
                    .binary_search(&pattern.as_str())
                    .is_ok()
            })
            .unwrap_or_default(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_post_patterns_sorted() {
        let mut sorted = READ_ONLY_POST_PATTERNS;
        sorted.sort_unstable();
        assert_eq!(
            READ_ONLY_POST_PATTERNS, sorted,
            "The READ_ONLY_POST_PATTERNS list must be sorted"
        );
    }
}
