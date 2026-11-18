/// Request headers (REST) or metadata (gRPC) ending in this suffix carry the caller's own
/// external-provider keys (e.g. `openai-api-key`), which are forwarded to the inference service.
pub const PROVIDER_API_KEY_HEADER_SUFFIX: &str = "-api-key";

/// Whether a request header should be forwarded to the inference service as a provider key.
pub fn is_provider_api_key(header_name: &str) -> bool {
    header_name.ends_with(PROVIDER_API_KEY_HEADER_SUFFIX)
}
