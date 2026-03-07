use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::future::{Ready, ready};
use std::sync::Arc;

use actix_web::body::EitherBody;
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform, forward_ready};
use actix_web::http::Method;
use actix_web::{Error, FromRequest, HttpMessage, HttpResponse, ResponseError};
use futures_util::future::LocalBoxFuture;
use storage::audit::audit_trust_forwarded_headers;
use storage::rbac::Access;

use super::forwarded;
use super::helpers::HttpError;
use crate::common::auth::{Auth, AuthError, AuthKeys, AuthType};
use crate::settings::BlacklistConfig;

/// Actix middleware factory that validates API keys / JWTs and inserts an
/// [`Auth`] object into request extensions.
///
/// Renamed from the previous `Auth` to avoid confusion with the new
/// per-request [`Auth`] context type.
pub struct AuthTransform {
    auth_keys: AuthKeys,
    whitelist: Vec<WhitelistItem>,
    blacklist: Blacklist,
}

impl AuthTransform {
    pub fn new(auth_keys: AuthKeys, whitelist: Vec<WhitelistItem>, blacklist: Blacklist) -> Self {
        Self {
            auth_keys,
            whitelist,
            blacklist,
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
            blacklist: self.blacklist.clone(),
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

#[derive(Clone, Debug)]
pub struct Blacklist(HashMap<Method, HashSet<String>>);

impl Blacklist {
    pub fn matches(&self, method: &Method, path: &str) -> bool {
        let Some(paths) = self.0.get(method) else {
            return false;
        };

        paths.iter().any(|path_str| {
            let mut blacklist_iter = path_str.split('/');
            let mut passed_iter = path.split('/');

            loop {
                match blacklist_iter.next() {
                    Some(blacklist_part) => match passed_iter.next() {
                        Some(passed_part) => {
                            if blacklist_part != passed_part && blacklist_part != "*" {
                                return false;
                            }
                        }
                        None => return false,
                    },
                    None => match passed_iter.next() {
                        Some(_passed_part) => return false,
                        None => return true,
                    },
                };
            }
        })
    }

    #[cfg(test)]
    fn into_inner(self) -> HashMap<Method, HashSet<String>> {
        self.0
    }
}

impl TryFrom<HashMap<String, HashSet<String>>> for Blacklist {
    type Error = std::io::Error;

    fn try_from(val: HashMap<String, HashSet<String>>) -> Result<Self, Self::Error> {
        let mut blacklist = HashMap::new();

        for (method_str, paths) in val {
            let method =
                Method::from_bytes(method_str.to_uppercase().as_bytes()).map_err(|_| {
                    Self::Error::other(format!(
                        "Provided invalid method for a blacklist: {method_str}"
                    ))
                })?;

            blacklist.insert(method, paths);
        }

        Ok(Self(blacklist))
    }
}

impl TryFrom<&str> for Blacklist {
    type Error = std::io::Error;

    fn try_from(blacklist_str: &str) -> Result<Self, Self::Error> {
        if let Ok(json) = serde_json::from_str::<HashMap<String, HashSet<String>>>(blacklist_str) {
            return Self::try_from(json);
        }

        let mut blacklist = HashMap::new();

        if !blacklist_str.is_empty() {
            for pair in blacklist_str.trim().split(',') {
                let mut pair_iter = pair.trim().split(' ');

                let Some(method_str) = pair_iter.next() else {
                    return Err(Self::Error::other(
                        "No method provided for a blacklist item",
                    ));
                };
                let method = Method::from_bytes(method_str.trim().to_uppercase().as_bytes())
                    .map_err(|_| {
                        Self::Error::other(format!(
                            "Provided invalid method for a blacklist: {method_str}"
                        ))
                    })?;

                let Some(path_str) = pair_iter.next() else {
                    return Err(Self::Error::other("No path provided for a blacklist item"));
                };
                if pair_iter.next().is_some() {
                    return Err(Self::Error::other(
                        "Provided extra parts for a blacklist item",
                    ));
                }

                match blacklist.get_mut(&method) {
                    None => {
                        let paths = HashSet::from([path_str.trim().to_string()]);
                        blacklist.insert(method, paths);
                    }
                    Some(paths) => {
                        paths.insert(path_str.trim().to_string());
                    }
                };
            }
        };

        Ok(Self(blacklist))
    }
}

impl TryFrom<Option<&BlacklistConfig>> for Blacklist {
    type Error = std::io::Error;

    fn try_from(config: Option<&BlacklistConfig>) -> Result<Self, Self::Error> {
        let Some(config) = config else {
            return Ok(Self(Default::default()));
        };

        match config {
            BlacklistConfig::Raw(s) => Self::try_from(s.as_str()),
            BlacklistConfig::Parsed(val) => Self::try_from(val.clone()),
        }
    }
}

pub struct AuthMiddleware<S> {
    auth_keys: Arc<AuthKeys>,
    /// List of items whitelisted from authentication.
    whitelist: Vec<WhitelistItem>,
    /// List of items blackisted for JWT authentication by configuration.
    blacklist: Blacklist,
    service: Arc<S>,
}

impl<S> AuthMiddleware<S> {
    pub fn is_path_whitelisted(&self, path: &str) -> bool {
        self.whitelist.iter().any(|item| item.matches(path))
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
        let path = req.path();
        if self.is_path_whitelisted(path) {
            return Box::pin(self.service.call(req));
        }

        let auth_keys = self.auth_keys.clone();
        let service = self.service.clone();
        let blacklist_matches = self.blacklist.matches(req.method(), path);

        Box::pin(async move {
            match auth_keys
                .validate_request(
                    |key| req.headers().get(key).and_then(|val| val.to_str().ok()),
                    blacklist_matches,
                )
                .await
            {
                Ok((access, inference_token, auth_type, subject)) => {
                    let remote = if audit_trust_forwarded_headers() {
                        forwarded::forwarded_for(&req)
                    } else {
                        None
                    }
                    .or_else(|| req.peer_addr().map(|a| a.ip().to_string()));
                    let auth = Auth::new(access, subject, remote, auth_type);
                    let previous = req.extensions_mut().insert(auth);
                    req.extensions_mut().insert(inference_token);
                    debug_assert!(
                        previous.is_none(),
                        "Previous auth object should not exist in the request"
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
        let auth = req.extensions_mut().remove::<Auth>().unwrap_or_else(|| {
            let remote = if audit_trust_forwarded_headers() {
                forwarded::forwarded_for_http(req)
            } else {
                None
            }
            .or_else(|| req.peer_addr().map(|a| a.ip().to_string()));
            Auth::new(
                Access::full(
                    "All requests have full by default access when API key is not configured",
                ),
                None,
                remote,
                AuthType::None,
            )
        });
        ready(Ok(ActixAuth(auth)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blacklist_parsing() {
        let blacklist = Blacklist::try_from("").unwrap().into_inner();
        assert!(blacklist.is_empty());

        let blacklist = Blacklist::try_from(
            "GET /debugger, put /cluster/metadata/keys/*, PUT /collections/*/snapshots/recover",
        )
        .unwrap();
        assert!(blacklist.matches(&Method::GET, "/debugger"));
        assert!(!blacklist.matches(&Method::GET, "/debuggerr"));
        assert!(!blacklist.matches(&Method::POST, "/debugger"));
        assert!(blacklist.matches(&Method::PUT, "/collections/c1/snapshots/recover"));
        assert!(!blacklist.matches(&Method::PUT, "/collections/c1/snapshots/recover/1"));
        assert!(!blacklist.matches(&Method::PUT, "/collections/c1/snapshots"));

        let blacklist = Blacklist::try_from(r#"{"get":["/debugger"],"PUT":["/cluster/metadata/keys/*","/collections/*/snapshots/recover"]}"#).unwrap();
        assert!(blacklist.matches(&Method::GET, "/debugger"));
        assert!(!blacklist.matches(&Method::GET, "/debuggerr"));
        assert!(!blacklist.matches(&Method::POST, "/debugger"));
        assert!(blacklist.matches(&Method::PUT, "/collections/c1/snapshots/recover"));
        assert!(!blacklist.matches(&Method::PUT, "/collections/c1/snapshots/recover/1"));
        assert!(!blacklist.matches(&Method::PUT, "/collections/c1/snapshots"));

        let config = BlacklistConfig::Raw(
            "GET /debugger, put /cluster/metadata/keys/*, PUT /collections/*/snapshots/recover"
                .to_string(),
        );
        let blacklist = Blacklist::try_from(Some(&config)).unwrap();
        assert!(blacklist.matches(&Method::GET, "/debugger"));
        assert!(!blacklist.matches(&Method::GET, "/debuggerr"));
        assert!(!blacklist.matches(&Method::POST, "/debugger"));
        assert!(blacklist.matches(&Method::PUT, "/collections/c1/snapshots/recover"));
        assert!(!blacklist.matches(&Method::PUT, "/collections/c1/snapshots/recover/1"));
        assert!(!blacklist.matches(&Method::PUT, "/collections/c1/snapshots"));

        let config = BlacklistConfig::Raw(r#"{"get":["/debugger"],"PUT":["/cluster/metadata/keys/*","/collections/*/snapshots/recover"]}"#.to_string());
        let blacklist = Blacklist::try_from(Some(&config)).unwrap();
        assert!(blacklist.matches(&Method::GET, "/debugger"));
        assert!(!blacklist.matches(&Method::GET, "/debuggerr"));
        assert!(!blacklist.matches(&Method::POST, "/debugger"));
        assert!(blacklist.matches(&Method::PUT, "/collections/c1/snapshots/recover"));
        assert!(!blacklist.matches(&Method::PUT, "/collections/c1/snapshots/recover/1"));
        assert!(!blacklist.matches(&Method::PUT, "/collections/c1/snapshots"));

        let map = serde_json::from_str::<HashMap<String, HashSet<String>>>(r#"{"get":["/debugger"],"PUT":["/cluster/metadata/keys/*","/collections/*/snapshots/recover"]}"#).unwrap();
        let config = BlacklistConfig::Parsed(map);
        let blacklist = Blacklist::try_from(Some(&config)).unwrap();
        assert!(blacklist.matches(&Method::GET, "/debugger"));
        assert!(!blacklist.matches(&Method::GET, "/debuggerr"));
        assert!(!blacklist.matches(&Method::POST, "/debugger"));
        assert!(blacklist.matches(&Method::PUT, "/collections/c1/snapshots/recover"));
        assert!(!blacklist.matches(&Method::PUT, "/collections/c1/snapshots/recover/1"));
        assert!(!blacklist.matches(&Method::PUT, "/collections/c1/snapshots"));

        let config = serde_json::from_str::<BlacklistConfig>(r#"{"get":["/debugger"],"PUT":["/cluster/metadata/keys/*","/collections/*/snapshots/recover"]}"#).unwrap();
        let blacklist = Blacklist::try_from(Some(&config)).unwrap();
        assert!(blacklist.matches(&Method::GET, "/debugger"));
        assert!(!blacklist.matches(&Method::GET, "/debuggerr"));
        assert!(!blacklist.matches(&Method::POST, "/debugger"));
        assert!(blacklist.matches(&Method::PUT, "/collections/c1/snapshots/recover"));
        assert!(!blacklist.matches(&Method::PUT, "/collections/c1/snapshots/recover/1"));
        assert!(!blacklist.matches(&Method::PUT, "/collections/c1/snapshots"));
    }
}
