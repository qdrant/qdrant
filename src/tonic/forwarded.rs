use tonic::codegen::http::HeaderName;
use tonic::codegen::http::header::FORWARDED;

static X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");

/// Extract the raw forwarded-for header value from a tonic/http request.
///
/// Checks the standard RFC 7239 `Forwarded` header first, then falls back to
/// the legacy `X-Forwarded-For` header.  The value is returned **as-is** – no
/// parsing or validation is performed.
///
/// Returns `None` when neither header is present or the value is not valid UTF-8.
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
