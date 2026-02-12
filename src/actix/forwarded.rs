use actix_web::HttpRequest;
use actix_web::dev::ServiceRequest;
use actix_web::http::header::{FORWARDED, X_FORWARDED_FOR};

/// Extract the raw `X-Forwarded-For` header value from an actix
/// [`ServiceRequest`].
///
/// The value is returned **as-is** – it may contain a comma-separated list of
/// addresses (e.g. `"client, proxy1, proxy2"`).  No parsing or validation is
/// performed; downstream log analysis tools are expected to handle the format.
///
/// Returns `None` when the header is absent or not valid UTF-8.
pub fn forwarded_for(req: &ServiceRequest) -> Option<String> {
    forwarded_for_headers(req.headers())
}

/// Same as [`forwarded_for`] but accepts an [`HttpRequest`] (used in the
/// extractor path where no `ServiceRequest` is available).
pub fn forwarded_for_http(req: &HttpRequest) -> Option<String> {
    forwarded_for_headers(req.headers())
}

fn forwarded_for_headers(headers: &actix_web::http::header::HeaderMap) -> Option<String> {
    if let Some(forwarded) = headers
        .get(FORWARDED)
        .and_then(|header| header.to_str().ok())
    {
        return Some(forwarded.to_string());
    }

    headers
        .get(X_FORWARDED_FOR)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}
