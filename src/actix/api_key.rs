use std::future::{ready, Ready};

use actix_web::body::{BoxBody, EitherBody};
use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::http::Method;
use actix_web::{Error, HttpResponse};
use futures_util::future::LocalBoxFuture;

use crate::common::auth::AuthScheme;
use crate::common::strings::ct_eq;

pub struct ApiKey {
    auth_scheme: Option<AuthScheme>,
    skip_prefixes: Vec<String>,
}

impl ApiKey {
    pub fn new(auth_scheme: Option<AuthScheme>, skip_prefixes: Vec<String>) -> Self {
        Self {
            auth_scheme,
            skip_prefixes,
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
            skip_prefixes: self.skip_prefixes.clone(),
            auth_scheme: self.auth_scheme.clone(),
            service,
        }))
    }
}

pub struct ApiKeyMiddleware<S> {
    skip_prefixes: Vec<String>,
    auth_scheme: Option<AuthScheme>,
    service: S,
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
        if self
            .skip_prefixes
            .iter()
            .any(|prefix| req.path().starts_with(prefix))
        {
            return Box::pin(self.service.call(req));
        }

        if let Some(key) = req.headers().get("api-key") {
            if let Ok(key) = key.to_str() {
                let is_allowed = match self.auth_scheme {
                    Some(AuthScheme::SeparateReadAndReadWrite {
                        read_write: ref rw_key,
                        read_only: ref ro_key,
                    }) => ct_eq(rw_key, key) || (is_read_only(&req) && ct_eq(ro_key, key)),
                    Some(AuthScheme::ReadWrite {
                        read_write: ref rw_key,
                    }) => ct_eq(rw_key, key),
                    Some(AuthScheme::ReadOnly {
                        read_only: ref ro_key,
                    }) => is_read_only(&req) && ct_eq(ro_key, key),
                    None => {
                        // This code path should not be reached
                        log::warn!(
                            "Auth for REST API is set up incorrectly. Denying access by default."
                        );
                        false
                    }
                };
                if is_allowed {
                    return Box::pin(self.service.call(req));
                }
            }
        }

        Box::pin(async {
            Ok(req
                .into_response(HttpResponse::Forbidden().body("Invalid api-key"))
                .map_into_right_body())
        })
    }
}

fn is_read_only(req: &ServiceRequest) -> bool {
    static READ_ONLY_POST_PATTERNS: [&str; 8] = [
        "/collections/{name}/points",
        "/collections/{name}/points/count",
        "/collections/{name}/points/search",
        "/collections/{name}/points/search/batch",
        "/collections/{name}/points/recommend",
        "/collections/{name}/points/recommend/groups",
        "/collections/{name}/points/recommend/batch",
        "/collections/{name}/points/scroll",
    ];

    match *req.method() {
        Method::GET => true,
        Method::POST => req
            .match_pattern()
            .map(|pattern| {
                READ_ONLY_POST_PATTERNS
                    .iter()
                    .any(|pat| ct_eq(&pattern, pat))
            })
            .unwrap_or_default(),
        _ => false,
    }
}
