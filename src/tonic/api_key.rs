use std::task::{Context, Poll};

use futures_util::future::BoxFuture;
use reqwest::header::HeaderValue;
use reqwest::StatusCode;
use tonic::body::BoxBody;
use tonic::Code;
use tower::Service;
use tower_layer::Layer;

use crate::common::auth::AuthScheme;
use crate::common::strings::ct_eq;

#[derive(Clone)]
pub struct ApiKeyMiddleware<T> {
    service: T,
    auth_scheme: AuthScheme,
}

#[derive(Clone)]
pub struct ApiKeyMiddlewareLayer {
    auth_scheme: AuthScheme,
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
        if let Some(key) = request.headers().get("api-key") {
            if let Ok(key) = key.to_str() {
                let is_allowed = match self.auth_scheme {
                    AuthScheme::SeparateReadAndReadWrite {
                        read_write: ref rw_key,
                        read_only: ref ro_key,
                    } => ct_eq(rw_key, key) || (is_read_only(&request) && ct_eq(ro_key, key)),
                    AuthScheme::ReadWrite {
                        read_write: ref rw_key,
                    } => ct_eq(rw_key, key),
                    AuthScheme::ReadOnly {
                        read_only: ref ro_key,
                    } => is_read_only(&request) && ct_eq(ro_key, key),
                };
                if is_allowed {
                    return Box::pin(self.service.call(request));
                }
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
    pub fn new(auth_scheme: AuthScheme) -> Self {
        Self { auth_scheme }
    }
}

impl<S> Layer<S> for ApiKeyMiddlewareLayer {
    type Service = ApiKeyMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        ApiKeyMiddleware {
            service,
            auth_scheme: self.auth_scheme.clone(),
        }
    }
}

fn is_read_only<R>(req: &tonic::codegen::http::Request<R>) -> bool {
    static READ_ONLY_RPC_PATHS: [&str; 6] = [
        "/qdrant.Collections/List",
        "/qdrant.Collections/Get",
        "/qdrant.Points/Search",
        "/qdrant.Points/Scroll",
        "/qdrant.Points/Get",
        "/qdrant.Points/Recommend",
    ];
    let uri_path = req.uri().path();
    READ_ONLY_RPC_PATHS
        .iter()
        .any(|ro_uri_path| ct_eq(uri_path, ro_uri_path))
}
