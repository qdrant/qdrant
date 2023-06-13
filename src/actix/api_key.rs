use std::future::{ready, Ready};

use actix_web::body::{BoxBody, EitherBody};
use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::{Error, HttpResponse};
use constant_time_eq::constant_time_eq;
use futures_util::future::LocalBoxFuture;

pub struct ApiKey {
    api_key: String,
    skip_prefixes: Vec<String>,
}

impl ApiKey {
    pub fn new(api_key: &str, skip_prefixes: Vec<String>) -> Self {
        Self {
            api_key: api_key.to_string(),
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
            api_key: self.api_key.clone(),
            skip_prefixes: self.skip_prefixes.clone(),
            service,
        }))
    }
}

pub struct ApiKeyMiddleware<S> {
    api_key: String,
    skip_prefixes: Vec<String>,
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
                if constant_time_eq(self.api_key.as_bytes(), key.as_bytes()) {
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
