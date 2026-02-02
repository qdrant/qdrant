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
    if let Some(forwarded) = headers.get(FORWARDED).and_then(|v| v.to_str().ok())
        && let Some(ip) = parse_forwarded_header(forwarded)
    {
        return Some(ip);
    }

    // Fallback to legacy X-Forwarded-For header
    headers
        .get(&X_FORWARDED_FOR)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_x_forwarded_for)
}

/// Parses the standard Forwarded header (RFC 7239) and extracts the client IP.
/// Format: Forwarded: for=192.0.2.43;proto=https
/// IPv6 format: Forwarded: for="[2001:db8::1]:4711"
fn parse_forwarded_header(header: &str) -> Option<String> {
    // Parse the first entry (original client) from comma-separated list
    let first_entry = header.split(',').next()?;
    for part in first_entry.split(';') {
        let part = part.trim();
        if let Some(value) = part.strip_prefix("for=") {
            // Remove quotes if present (required for IPv6 addresses per RFC 7239)
            let value = value.trim_matches('"');
            let ip = if let Some(stripped) = value.strip_prefix('[') {
                // IPv6 address in brackets, possibly with port after ']'
                stripped
                    .split_once(']')
                    .map(|(ip, _)| ip)
                    .unwrap_or(stripped)
            } else {
                // IPv4 or unbracketed value, strip port if present
                value.split(':').next().unwrap_or(value)
            };
            return Some(ip.to_string());
        }
    }
    None
}

/// Parses the legacy X-Forwarded-For header and extracts the client IP.
/// Format: X-Forwarded-For: client, proxy1, proxy2
fn parse_x_forwarded_for(header: &str) -> Option<String> {
    header.split(',').next().map(|ip| ip.trim().to_string())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_forwarded_header_ipv4() {
        assert_eq!(
            parse_forwarded_header("for=192.0.2.43"),
            Some("192.0.2.43".to_string())
        );
    }

    #[test]
    fn test_parse_forwarded_header_ipv4_with_port() {
        assert_eq!(
            parse_forwarded_header("for=\"192.0.2.43:8080\""),
            Some("192.0.2.43".to_string())
        );
    }

    #[test]
    fn test_parse_forwarded_header_ipv4_with_proto() {
        assert_eq!(
            parse_forwarded_header("for=192.0.2.43;proto=https"),
            Some("192.0.2.43".to_string())
        );
    }

    #[test]
    fn test_parse_forwarded_header_ipv6() {
        assert_eq!(
            parse_forwarded_header("for=\"[2001:db8::1]\""),
            Some("2001:db8::1".to_string())
        );
    }

    #[test]
    fn test_parse_forwarded_header_ipv6_with_port() {
        assert_eq!(
            parse_forwarded_header("for=\"[2001:db8:cafe::17]:4711\""),
            Some("2001:db8:cafe::17".to_string())
        );
    }

    #[test]
    fn test_parse_forwarded_header_multiple_proxies() {
        assert_eq!(
            parse_forwarded_header("for=203.0.113.50, for=198.51.100.178"),
            Some("203.0.113.50".to_string())
        );
    }

    #[test]
    fn test_parse_forwarded_header_complex() {
        assert_eq!(
            parse_forwarded_header("for=192.0.2.60;proto=http;by=203.0.113.43"),
            Some("192.0.2.60".to_string())
        );
    }

    #[test]
    fn test_parse_forwarded_header_case_sensitivity() {
        // RFC 7239: parameter names are case-insensitive, but we only check lowercase
        assert_eq!(parse_forwarded_header("FOR=192.0.2.43"), None);
    }

    #[test]
    fn test_parse_forwarded_header_no_for() {
        assert_eq!(parse_forwarded_header("proto=https;by=203.0.113.43"), None);
    }

    #[test]
    fn test_parse_x_forwarded_for_single() {
        assert_eq!(
            parse_x_forwarded_for("203.0.113.50"),
            Some("203.0.113.50".to_string())
        );
    }

    #[test]
    fn test_parse_x_forwarded_for_multiple() {
        assert_eq!(
            parse_x_forwarded_for("203.0.113.50, 198.51.100.178, 192.0.2.1"),
            Some("203.0.113.50".to_string())
        );
    }

    #[test]
    fn test_parse_x_forwarded_for_with_spaces() {
        assert_eq!(
            parse_x_forwarded_for("  203.0.113.50  ,  198.51.100.178  "),
            Some("203.0.113.50".to_string())
        );
    }

    #[test]
    fn test_parse_x_forwarded_for_ipv6() {
        assert_eq!(
            parse_x_forwarded_for("2001:db8::1"),
            Some("2001:db8::1".to_string())
        );
    }
}
