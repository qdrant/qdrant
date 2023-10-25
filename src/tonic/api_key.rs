use std::task::{Context, Poll};

use actix_web_httpauth::headers::authorization::{Bearer, Scheme};
use constant_time_eq::constant_time_eq;
use futures_util::future::BoxFuture;
use reqwest::header::HeaderValue;
use reqwest::StatusCode;
use tonic::body::BoxBody;
use tonic::Code;
use tower::Service;
use tower_layer::Layer;

#[derive(Clone)]
pub struct ApiKeyMiddleware<T> {
    service: T,
    api_key: String,
}

#[derive(Clone)]
pub struct ApiKeyMiddlewareLayer {
    api_key: String,
}

impl<S> Service<tonic::codegen::http::Request<tonic::transport::Body>> for ApiKeyMiddleware<S>
where
    S: Service<
        tonic::codegen::http::Request<tonic::transport::Body>,
        Response = tonic::codegen::http::Response<tonic::body::BoxBody>,
    >,
    S::Future: Send + 'static,
{
    type Response = tonic::codegen::http::Response<tonic::body::BoxBody>;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, S::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(
        &mut self,
        request: tonic::codegen::http::Request<tonic::transport::Body>,
    ) -> Self::Future {
        // Grab API key from request
        let key =
            // Request header
            request.headers().get("api-key").and_then(|key| key.to_str().ok()).map(|key| key.to_string())
            // Fall back to authentication header with bearer token
            .or_else(|| {
                request.headers().get("authorization")
                    .and_then(|auth| {
                        Bearer::parse(auth).ok().map(|bearer| bearer.token().into())
                    })
            });

        // If we have an API key, compare in constant time
        if let Some(key) = key {
            if constant_time_eq(self.api_key.as_bytes(), key.as_bytes()) {
                let future = self.service.call(request);

                return Box::pin(async move {
                    let response = future.await?;
                    Ok(response)
                });
            }
        }

        let mut response = Self::Response::new(BoxBody::default());
        *response.status_mut() = StatusCode::FORBIDDEN;
        response.headers_mut().append(
            "grpc-status",
            HeaderValue::from(Code::PermissionDenied as i32),
        );
        response
            .headers_mut()
            .append("grpc-message", HeaderValue::from_static("Invalid api-key"));

        Box::pin(async move { Ok(response) })
    }
}

impl ApiKeyMiddlewareLayer {
    pub fn new(api_key: String) -> Self {
        Self { api_key }
    }
}

impl<S> Layer<S> for ApiKeyMiddlewareLayer {
    type Service = ApiKeyMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        ApiKeyMiddleware {
            service,
            api_key: self.api_key.clone(),
        }
    }
}
