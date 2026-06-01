use std::convert::Infallible;
use std::future::{Ready, ready};
use std::sync::Arc;

use actix_web::body::EitherBody;
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform, forward_ready};
use actix_web::{Error, FromRequest, HttpMessage, HttpResponse, ResponseError};
use futures_util::future::LocalBoxFuture;
use storage::audit::{audit_trust_forwarded_headers, extract_tracing_id};
use storage::rbac::{Access, AccessRequirements, CollectionMultipass};

use super::forwarded;
use super::helpers::HttpError;
use crate::common::auth::{Auth, AuthError, AuthKeys, AuthType, log_denied_auth};

/// Actix middleware factory that validates API keys / JWTs and inserts an
/// [`Auth`] object into request extensions.
///
/// Renamed from the previous `Auth` to avoid confusion with the new
/// per-request [`Auth`] context type.
pub struct AuthTransform {
    auth_keys: AuthKeys,
    whitelist: Vec<WhitelistItem>,
}

impl AuthTransform {
    pub fn new(auth_keys: AuthKeys, whitelist: Vec<WhitelistItem>) -> Self {
        Self {
            auth_keys,
            whitelist,
        }
    }
}

impl<S, B> Transform<S, ServiceRequest> for AuthTransform
where
    S: Service<ServiceRequest, Response = ServiceResponse<EitherBody<B>>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
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
    pub fn is_path_whitelisted(&self, req: &ServiceRequest) -> bool {
        // Resolve request path and find route pattern
        // If none, we don't know this path
        let Some(pattern) = req.match_pattern() else {
            return false;
        };

        self.whitelist.iter().any(|item| item.matches(&pattern))
    }
}

impl<S, B> Service<ServiceRequest> for AuthMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<EitherBody<B>>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let is_whitelisted = self.is_path_whitelisted(&req);
        if is_whitelisted {
            return Box::pin(self.service.call(req));
        }

        let auth_keys = self.auth_keys.clone();
        let service = self.service.clone();
        Box::pin(async move {
            let remote = if audit_trust_forwarded_headers() {
                forwarded::forwarded_for(&req)
            } else {
                None
            }
            .or_else(|| req.peer_addr().map(|a| a.ip().to_string()));

            let tracing_id = extract_tracing_id(|h| {
                req.headers()
                    .get(h)
                    .and_then(|val| val.to_str().ok())
                    .map(str::to_string)
            });

            match auth_keys
                .validate_request(|key| req.headers().get(key).and_then(|val| val.to_str().ok()))
                .await
            {
                Ok((access, inference_token, auth_type, subject)) => {
                    let api_path = req.path().to_string();
                    let auth = Auth::new(access, subject, remote, auth_type, tracing_id)
                        .with_api(api_path);
                    let previous = req.extensions_mut().insert(auth);
                    req.extensions_mut().insert(inference_token);
                    debug_assert!(
                        previous.is_none(),
                        "Previous auth object should not exist in the request"
                    );
                    service.call(req).await
                }
                Err(e) => {
                    log_denied_auth(req.path(), remote.clone(), tracing_id, &e);
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

/// Retrieve the per-request [`Auth`] from request extensions, falling back to
/// a default full-access [`Auth`] when no auth middleware is configured.
fn take_request_auth(req: &actix_web::HttpRequest) -> Auth {
    req.extensions_mut().remove::<Auth>().unwrap_or_else(|| {
        let remote = if audit_trust_forwarded_headers() {
            forwarded::forwarded_for_http(req)
        } else {
            None
        }
        .or_else(|| req.peer_addr().map(|a| a.ip().to_string()));
        let tracing_id = extract_tracing_id(|h| {
            req.headers()
                .get(h)
                .and_then(|val| val.to_str().ok())
                .map(str::to_string)
        });
        Auth::new(
            Access::full("All requests have full by default access when API key is not configured"),
            None,
            remote,
            AuthType::None,
            tracing_id,
        )
    })
}

/// Actix extractor that retrieves the per-request [`Auth`] context from
/// request extensions.  When no authentication middleware is configured,
/// a default [`Auth`] with full access is created.
pub struct ActixAuth(pub Auth);

impl FromRequest for ActixAuth {
    type Error = Infallible;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(
        req: &actix_web::HttpRequest,
        _payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        ready(Ok(ActixAuth(take_request_auth(req))))
    }
}

/// Actix extractor that enforces global `manage` access *before* any other
/// extractors run.
///
/// Place this as the **first** parameter in a handler signature, ahead of
/// body-consuming extractors such as `MultipartForm`. Actix-web polls tuple
/// extractors in declaration order and short-circuits on the first
/// `Ready(Err)` (see actix-web `extract.rs` `tuple_from_req!`). Because
/// [`Self::from_request`] returns a synchronously-ready result, an
/// unauthorized request is rejected with `403 Forbidden` before any
/// subsequent extractor's future is polled — meaning the request body is
/// never read from the socket and no temp file is created.
///
/// This is the fix for the read-only-key snapshot-upload disk-exhaustion
/// issue described in GHSA-3v92-w72v-j994.
pub struct ActixAccessManage {
    pub auth: Auth,
    pub multipass: CollectionMultipass,
}

impl FromRequest for ActixAccessManage {
    type Error = HttpError;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(
        req: &actix_web::HttpRequest,
        _payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        let auth = take_request_auth(req);
        // Use the matched route pattern (e.g.
        // `/collections/{collection_name}/snapshots/upload`) as the audit
        // action name, falling back to the raw path for unmatched requests.
        let action = req
            .match_pattern()
            .unwrap_or_else(|| req.path().to_string());
        match auth.check_global_access(AccessRequirements::new().manage(), &action) {
            Ok(multipass) => ready(Ok(Self { auth, multipass })),
            Err(err) => ready(Err(HttpError::from(err))),
        }
    }
}
