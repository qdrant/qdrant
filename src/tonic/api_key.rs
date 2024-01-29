use std::task::{Context, Poll};

use actix_web_httpauth::headers::authorization::{Bearer, Scheme};
use futures_util::future::BoxFuture;
use reqwest::header::HeaderValue;
use reqwest::StatusCode;
use tonic::body::BoxBody;
use tonic::Code;
use tower::Service;
use tower_layer::Layer;

use crate::common::auth::AuthKeys;
use crate::common::strings::ct_eq;

const READ_ONLY_RPC_PATHS: [&str; 14] = [
    "/qdrant.Collections/CollectionExists",
    "/qdrant.Collections/List",
    "/qdrant.Collections/Get",
    "/qdrant.Points/Scroll",
    "/qdrant.Points/Get",
    "/qdrant.Points/Count",
    "/qdrant.Points/Search",
    "/qdrant.Points/SearchGroups",
    "/qdrant.Points/SearchBatch",
    "/qdrant.Points/Recommend",
    "/qdrant.Points/RecommendGroups",
    "/qdrant.Points/RecommendBatch",
    "/qdrant.Points/Discover",
    "/qdrant.Points/DiscoverBatch",
];

#[derive(Clone)]
pub struct ApiKeyMiddleware<T> {
    service: T,
    auth_keys: AuthKeys,
}

#[derive(Clone)]
pub struct ApiKeyMiddlewareLayer {
    auth_keys: AuthKeys,
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

        if let Some(key) = key {
            let is_allowed = self.auth_keys.can_write(&key)
                || (is_read_only(&request) && self.auth_keys.can_read(&key));
            if is_allowed {
                return Box::pin(self.service.call(request));
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
    pub fn new(auth_keys: AuthKeys) -> Self {
        Self { auth_keys }
    }
}

impl<S> Layer<S> for ApiKeyMiddlewareLayer {
    type Service = ApiKeyMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        ApiKeyMiddleware {
            service,
            auth_keys: self.auth_keys.clone(),
        }
    }
}

fn is_read_only<R>(req: &tonic::codegen::http::Request<R>) -> bool {
    let uri_path = req.uri().path();
    READ_ONLY_RPC_PATHS
        .iter()
        .any(|ro_uri_path| ct_eq(uri_path, ro_uri_path))
}
