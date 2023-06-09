use std::future::{ready, Ready};
use std::sync::OnceLock;

use actix_web::body::{BoxBody, EitherBody};
use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::http::Method;
use actix_web::{Error, HttpResponse};
use futures_util::future::LocalBoxFuture;
use regex::Regex;

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
    static READ_ONLY_PATHS: OnceLock<Vec<Regex>> = OnceLock::new();

    let read_only_paths = READ_ONLY_PATHS.get_or_init(|| {
        vec![
            Regex::new(r"^/collections/[a-z0-9-_&;]+/points/count/?(\?.*)?$").expect("bad regex"),
            Regex::new(r"^/collections/[a-z0-9-_&;]+/points/search/?(\?.*)?$").expect("bad regex"),
            Regex::new(r"^/collections/[a-z0-9-_&;]+/points/search/batch/?(\?.*)?$")
                .expect("bad regex"),
            Regex::new(r"^/collections/[a-z0-9-_&;]+/points/recommend/?(\?.*)?$")
                .expect("bad regex"),
            Regex::new(r"^/collections/[a-z0-9-_&;]+/points/recommend/groups/?(\?.*)?$")
                .expect("bad regex"),
            Regex::new(r"^/collections/[a-z0-9-_&;]+/points/recommend/batch/?(\?.*)?$")
                .expect("bad regex"),
            Regex::new(r"^/collections/[a-z0-9-_&;]+/points/?(\?.*)?$").expect("bad regex"),
            Regex::new(r"^/collections/[a-z0-9-_&;]+/points/scroll/?(\?.*)?$").expect("bad regex"),
        ]
    });

    req.method() == Method::GET
        || (req.method() == Method::POST
            && read_only_paths.iter().any(|re| re.is_match(req.path())))
}
