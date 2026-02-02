use std::task::{Context, Poll};

use futures_util::future::BoxFuture;
use tonic::Code;
use tonic::body::BoxBody;
use tonic::codegen::http::header::FORWARDED;
use tonic::codegen::http::{HeaderName, Response};
use tower::Service;
use tower_layer::Layer;

static X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");

#[derive(Clone)]
pub struct LoggingMiddleware<T> {
    inner: T,
    trust_forwarded_headers: bool,
}

#[derive(Clone)]
pub struct LoggingMiddlewareLayer {
    trust_forwarded_headers: bool,
}

impl LoggingMiddlewareLayer {
    pub fn new(trust_forwarded_headers: bool) -> Self {
        Self {
            trust_forwarded_headers,
        }
    }
}

/// Extracts the client IP from forwarded headers.
/// Supports both the standard Forwarded header (RFC 7239) and legacy X-Forwarded-For header.
/// The standard Forwarded header takes precedence.
fn extract_forwarded_ip(
    request: &tonic::codegen::http::Request<tonic::transport::Body>,
) -> Option<String> {
    let headers = request.headers();

    // Try standard Forwarded header first (RFC 7239)
    // Format: Forwarded: for=192.0.2.43;proto=https
    if let Some(forwarded) = headers.get(FORWARDED).and_then(|v| v.to_str().ok()) {
        // Parse the first entry (original client) from comma-separated list
        if let Some(first_entry) = forwarded.split(',').next() {
            for part in first_entry.split(';') {
                let part = part.trim();
                if let Some(value) = part.strip_prefix("for=") {
                    // Remove quotes and brackets if present (IPv6 addresses)
                    let ip = value
                        .trim_matches('"')
                        .trim_start_matches('[')
                        .trim_end_matches(']');
                    return Some(ip.to_string());
                }
            }
        }
    }

    // Fallback to legacy X-Forwarded-For header
    headers
        .get(&X_FORWARDED_FOR)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(|ip| ip.trim().to_string())
}

impl<S> Service<tonic::codegen::http::Request<tonic::transport::Body>> for LoggingMiddleware<S>
where
    S: Service<tonic::codegen::http::Request<tonic::transport::Body>, Response = Response<BoxBody>>
        + Clone,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<S::Response, S::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(
        &mut self,
        request: tonic::codegen::http::Request<tonic::transport::Body>,
    ) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let method_name = request.uri().path().to_string();
        let log_prefix = self
            .trust_forwarded_headers
            .then(|| extract_forwarded_ip(&request))
            .flatten()
            .map(|ip| format!("{ip} "))
            .unwrap_or_default();
        let instant = std::time::Instant::now();
        let future = inner.call(request);
        Box::pin(async move {
            let response = future.await;
            let elapsed_sec = instant.elapsed().as_secs_f32();
            match response {
                Err(error) => {
                    log::error!("{log_prefix}gRPC request error {method_name}");
                    Err(error)
                }
                Ok(response_tonic) => {
                    let grpc_status = tonic::Status::from_header_map(response_tonic.headers());
                    if let Some(grpc_status) = grpc_status {
                        match grpc_status.code() {
                            Code::Ok => {
                                log::trace!("{log_prefix}gRPC {method_name} Ok {elapsed_sec:.6}");
                            }
                            Code::Cancelled => {
                                // cluster mode generates a large amount of `stream error received: stream no longer needed`
                                log::trace!(
                                    "{log_prefix}gRPC cancelled {method_name} {elapsed_sec:.6}"
                                );
                            }
                            Code::DeadlineExceeded
                            | Code::Aborted
                            | Code::OutOfRange
                            | Code::ResourceExhausted
                            | Code::NotFound
                            | Code::InvalidArgument
                            | Code::AlreadyExists
                            | Code::FailedPrecondition
                            | Code::PermissionDenied
                            | Code::Unauthenticated => {
                                log::info!(
                                    "{log_prefix}gRPC {} failed with {} {:?} {:.6}",
                                    method_name,
                                    grpc_status.code(),
                                    grpc_status.message(),
                                    elapsed_sec,
                                );
                            }
                            Code::Internal
                            | Code::Unimplemented
                            | Code::Unavailable
                            | Code::DataLoss
                            | Code::Unknown => log::error!(
                                "{log_prefix}gRPC {} unexpectedly failed with {} {:?} {:.6}",
                                method_name,
                                grpc_status.code(),
                                grpc_status.message(),
                                elapsed_sec,
                            ),
                        };
                    } else {
                        // Fallback to response's `status_code` if no `grpc-status` header found.
                        match response_tonic.status().as_u16() {
                            100..=199 => {
                                log::trace!(
                                    "{log_prefix}gRPC information {} {} {:.6}",
                                    method_name,
                                    response_tonic.status(),
                                    elapsed_sec,
                                );
                            }
                            200..=299 => {
                                log::trace!(
                                    "{log_prefix}gRPC success {} {} {:.6}",
                                    method_name,
                                    response_tonic.status(),
                                    elapsed_sec,
                                );
                            }
                            300..=399 => {
                                log::debug!(
                                    "{log_prefix}gRPC redirection {} {} {:.6}",
                                    method_name,
                                    response_tonic.status(),
                                    elapsed_sec,
                                );
                            }
                            400..=499 => {
                                log::info!(
                                    "{log_prefix}gRPC client error {} {} {:.6}",
                                    method_name,
                                    response_tonic.status(),
                                    elapsed_sec,
                                );
                            }
                            500..=599 => {
                                log::error!(
                                    "{log_prefix}gRPC server error {} {} {:.6}",
                                    method_name,
                                    response_tonic.status(),
                                    elapsed_sec,
                                );
                            }
                            _ => {
                                log::warn!(
                                    "{log_prefix}gRPC {} unknown status code {} {:.6}",
                                    method_name,
                                    response_tonic.status(),
                                    elapsed_sec,
                                );
                            }
                        };
                    }
                    Ok(response_tonic)
                }
            }
        })
    }
}

impl<S> Layer<S> for LoggingMiddlewareLayer {
    type Service = LoggingMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        LoggingMiddleware {
            inner: service,
            trust_forwarded_headers: self.trust_forwarded_headers,
        }
    }
}
