use std::convert::Infallible;
use std::future::{ready, Ready};
use std::sync::Arc;

use actix_web::body::{BoxBody, EitherBody};
use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::{Error, FromRequest, HttpMessage, HttpResponse, ResponseError};
use futures_util::future::LocalBoxFuture;
use storage::rbac::Access;

use super::helpers::HttpError;
use crate::common::auth::{AuthError, AuthKeys};

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
    S: Service<ServiceRequest, Response = ServiceResponse<EitherBody<B, BoxBody>>, Error = Error>
        + 'static,
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
            auth_keys: Arc::new(self.auth_keys.clone()),
            whitelist: self.whitelist.clone(),
            service: Arc::new(service),
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
    auth_keys: Arc<AuthKeys>,
    /// List of items whitelisted from authentication.
    whitelist: Vec<WhitelistItem>,
    service: Arc<S>,
}

impl<S> AuthMiddleware<S> {
    pub fn is_path_whitelisted(&self, path: &str) -> bool {
        self.whitelist.iter().any(|item| item.matches(path))
    }
}

impl<S, B> Service<ServiceRequest> for AuthMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<EitherBody<B, BoxBody>>, Error = Error>
        + 'static,
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

        let auth_keys = self.auth_keys.clone();
        let service = self.service.clone();
        Box::pin(async move {
            match auth_keys
                .validate_request(|key| req.headers().get(key).and_then(|val| val.to_str().ok()))
                .await
            {
                Ok(access) => {
                    let _previous = req.extensions_mut().insert::<Access>(access);
                    debug_assert!(
                        _previous.is_none(),
                        "Previous access object should not exist in the request"
                    );
                    service.call(req).await
                }
                Err(e) => {
                    let resp = match e {
                        AuthError::Unauthorized(e) => HttpResponse::Unauthorized().body(e),
                        AuthError::Forbidden(e) => HttpResponse::Forbidden().body(e),
                        AuthError::StorageError(e) => HttpError::from(e).error_response(),
                    };
                    Ok(req.into_response(resp).map_into_right_body())
                }
            }
        })
    }
}

pub struct ActixAccess(pub Access);

impl FromRequest for ActixAccess {
    type Error = Infallible;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(
        req: &actix_web::HttpRequest,
        _payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        let access = req.extensions_mut().remove::<Access>().unwrap_or_else(|| {
            Access::full("All requests have full by default access when API key is not configured")
        });
        ready(Ok(ActixAccess(access)))
    }
}
