use tonic::codegen::http::HeaderName;
use tonic::codegen::http::header::FORWARDED;

static X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");

/// Extract the raw `X-Forwarded-For` header value from a tonic/http request.
///
/// The value is returned **as-is** – it may contain a comma-separated list of
/// addresses (e.g. `"client, proxy1, proxy2"`).  No parsing or validation is
/// performed; downstream log analysis tools are expected to handle the format.
///
/// Returns `None` when the header is absent or not valid UTF-8.
pub fn forwarded_for(
    req: &tonic::codegen::http::Request<tonic::transport::Body>,
) -> Option<String> {
    let headers = req.headers();

    // Try standard Forwarded header first (RFC 7239)
    if let Some(forwarded) = headers.get(FORWARDED).and_then(|v| v.to_str().ok()) {
        return Some(forwarded.to_string());
    }

    // Fallback to legacy X-Forwarded-For header
    headers
        .get(&X_FORWARDED_FOR)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}
