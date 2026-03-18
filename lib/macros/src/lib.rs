use proc_macro::TokenStream;

mod anonymize;
mod telemetry_wrapper;

/// Grep for `trait Anonymize` for doc.
#[proc_macro_derive(Anonymize, attributes(anonymize))]
pub fn derive_anonymize(input: TokenStream) -> TokenStream {
    match anonymize::derive_anonymize(input.into()) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Generates a wrapper struct that implements the same trait by delegating every
/// method to the inner service, automatically extracting `collection_name` from
/// every request and attaching `CollectionName` to every response.
///
/// Usage:
/// ```ignore
/// #[macros::attach_collection_name(PointsTelemetryWrapper)]
/// #[tonic::async_trait]
/// impl Points for PointsService {
///     async fn upsert(&self, request: Request<UpsertPoints>) -> Result<Response<...>, Status> {
///         // handler code — no need to manually attach collection name
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn attach_collection_name(attr: TokenStream, item: TokenStream) -> TokenStream {
    match telemetry_wrapper::attach_collection_name(attr.into(), item.into()) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}
