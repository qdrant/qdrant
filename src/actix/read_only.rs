use std::future::{Ready, ready};
use std::pin::Pin;
use std::sync::Arc;

use actix_web::body::EitherBody;
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, forward_ready};
use actix_web::{Error, HttpResponse};

/// Actix middleware factory that rejects write HTTP methods (PUT, POST, DELETE, PATCH)
/// with 403 Forbidden when read-only mode is enabled.
///
/// Read-only methods (GET, HEAD, OPTIONS) pass through normally.
pub struct ReadOnlyTransform {
    read_only: bool,
}

impl ReadOnlyTransform {
    /// Create a new read-only transform.
    ///
    /// # Arguments
    ///
    /// * `read_only` - If `true`, write HTTP methods will be rejected with 403 Forbidden.
    ///                 If `false`, all requests pass through unchanged.
    pub fn new(read_only: bool) -> Self {
        Self { read_only }
    }
}

impl<S, B> actix_web::dev::Transform<S, ServiceRequest> for ReadOnlyTransform
where
    S: Service<ServiceRequest, Response = ServiceResponse<EitherBody<B>>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = Error;
    type InitError = ();
    type Transform = ReadOnlyMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    /// Wrap the given service in a [`ReadOnlyMiddleware`].
    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(ReadOnlyMiddleware {
            read_only: self.read_only,
            service: Arc::new(service),
        }))
    }
}

/// Actix-web middleware that rejects write HTTP methods when read-only mode is active.
///
/// When `read_only` is `true`, requests using PUT, POST, DELETE, or PATCH are
/// immediately answered with `403 Forbidden`. All other requests are forwarded
/// to the inner service unchanged.
pub struct ReadOnlyMiddleware<S> {
    read_only: bool,
    service: Arc<S>,
}

impl<S, B> Service<ServiceRequest> for ReadOnlyMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<EitherBody<B>>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = Error;
    type Future = Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>>>>;

    forward_ready!(service);

    /// Handle an incoming request.
    ///
    /// If read-only mode is enabled and the request uses a write HTTP method,
    /// responds with `403 Forbidden`. Otherwise delegates to the wrapped service.
    fn call(&self, req: ServiceRequest) -> Self::Future {
        if self.read_only && is_write_method(req.method()) {
            let response = HttpResponse::Forbidden().json(serde_json::json!({
                "status": {
                    "error": "Read-only mode: write operations are not allowed. \
                              The server is running with --read-only flag.",
                },
                "time": 0.0,
            }));
            return Box::pin(async move { Ok(req.into_response(response).map_into_right_body()) });
        }

        let fut = self.service.call(req);
        Box::pin(async move { fut.await })
    }
}

/// Returns true for HTTP methods that modify data.
fn is_write_method(method: &actix_web::http::Method) -> bool {
    matches!(
        method,
        &actix_web::http::Method::PUT
            | &actix_web::http::Method::POST
            | &actix_web::http::Method::DELETE
            | &actix_web::http::Method::PATCH
    )
}
