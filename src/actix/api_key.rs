use std::future::{ready, Ready};

use actix_web::body::{BoxBody, EitherBody};
use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::{Error, HttpResponse};
use constant_time_eq::constant_time_eq;
use futures_util::future::LocalBoxFuture;

pub struct ApiKey {
    api_key: String,
}

impl ApiKey {
    pub fn new(api_key: &str) -> Self {
        Self {
            api_key: api_key.to_string(),
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
            service,
        }))
    }
}

pub struct ApiKeyMiddleware<S> {
    api_key: String,
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
        // Grab API key from request
        let key =
            // Request header
            req.headers().get("api-key").map(|key| key.as_bytes())
            // Fall back to query parameter
            .or_else(|| req.query_string()
                .split('&')
                .filter_map(|option| option.split_once('='))
                .find(|(key, _)| key.eq_ignore_ascii_case("api-key"))
                .map(|(_, value)| value.as_bytes())
            );

        // If we have an API key, compare in constant time
        if let Some(key) = key {
            if constant_time_eq(self.api_key.as_bytes(), key) {
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
