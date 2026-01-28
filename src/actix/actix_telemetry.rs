use std::borrow::Cow;
use std::future::{Ready, ready};
use std::sync::Arc;

use actix_web::Error;
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use futures_util::future::LocalBoxFuture;
use parking_lot::Mutex;
use urlencoding::decode;

use crate::common::telemetry_ops::requests_telemetry::{
    ActixTelemetryCollector, ActixWorkerTelemetryCollector,
};

/// Extracts collection name from request path.
/// Path pattern: `/collections/{name}/...`
/// Returns None if path doesn't match or collection name is missing.
///
/// Returns `Cow::Borrowed` when no URL decoding is needed (zero allocation),
/// or `Cow::Owned` when the collection name contains percent-encoded characters.
fn extract_collection(path: &str) -> Option<Cow<'_, str>> {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    // Find "collections" segment and get the next one
    let pos = segments.iter().position(|&s| s == "collections")?;
    let collection = *segments.get(pos + 1)?;

    // URL decode if needed - decode returns Cow, so we can avoid allocation
    // when the string doesn't contain any percent-encoded characters
    decode(collection).ok()
}

/// Actix middleware service that collects telemetry for each request.
pub struct ActixTelemetryService<S> {
    service: S,
    telemetry_data: Arc<Mutex<ActixWorkerTelemetryCollector>>,
}

/// Transform factory for creating [`ActixTelemetryService`] instances.
pub struct ActixTelemetryTransform {
    telemetry_collector: Arc<Mutex<ActixTelemetryCollector>>,
}

/// Actix telemetry service. It hooks every request and looks into response status code.
///
/// More about actix service with similar example
/// <https://actix.rs/docs/middleware/>
impl<S, B> Service<ServiceRequest> for ActixTelemetryService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    actix_web::dev::forward_ready!(service);

    fn call(&self, request: ServiceRequest) -> Self::Future {
        let match_pattern = request
            .match_pattern()
            .unwrap_or_else(|| "unknown".to_owned());
        let request_key = format!("{} {}", request.method(), match_pattern);
        // Convert to owned String immediately to avoid borrowing `request`
        // (which gets moved into self.service.call below)
        let collection: Option<String> = extract_collection(request.path()).map(Cow::into_owned);
        let future = self.service.call(request);
        let telemetry_data = self.telemetry_data.clone();
        Box::pin(async move {
            let instant = std::time::Instant::now();
            let response = future.await?;
            let status = response.response().status().as_u16();
            telemetry_data.lock().add_response_with_collection(
                &request_key,
                instant,
                status,
                collection.as_deref(),
            );
            Ok(response)
        })
    }
}

impl ActixTelemetryTransform {
    /// Creates a new transform with the given telemetry collector.
    pub fn new(telemetry_collector: Arc<Mutex<ActixTelemetryCollector>>) -> Self {
        Self {
            telemetry_collector,
        }
    }
}

/// Actix telemetry transform. It's a builder for an actix service
///
/// More about actix transform with similar example
/// <https://actix.rs/docs/middleware/>
impl<S, B> Transform<S, ServiceRequest> for ActixTelemetryTransform
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Transform = ActixTelemetryService<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(ActixTelemetryService {
            service,
            telemetry_data: self
                .telemetry_collector
                .lock()
                .create_web_worker_telemetry(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_collection_with_subpath() {
        let result = extract_collection("/collections/my_collection/points");
        assert_eq!(result.as_deref(), Some("my_collection"));
        // Should be borrowed (no allocation) when no decoding needed
        assert!(matches!(result, Some(Cow::Borrowed(_))));
    }

    #[test]
    fn test_extract_collection_minimal() {
        let result = extract_collection("/collections/test");
        assert_eq!(result.as_deref(), Some("test"));
        assert!(matches!(result, Some(Cow::Borrowed(_))));
    }

    #[test]
    fn test_extract_collection_no_name() {
        assert_eq!(extract_collection("/collections"), None);
        assert_eq!(extract_collection("/collections/"), None);
    }

    #[test]
    fn test_extract_collection_no_collections_segment() {
        assert_eq!(extract_collection("/health"), None);
        assert_eq!(extract_collection("/cluster/status"), None);
    }

    #[test]
    fn test_extract_collection_url_encoded() {
        // Collection name with special chars (URL encoded)
        let result = extract_collection("/collections/my%20collection/points");
        assert_eq!(result.as_deref(), Some("my collection"));
        // Should be owned (allocation required) when decoding needed
        assert!(matches!(result, Some(Cow::Owned(_))));
    }

    #[test]
    fn test_extract_collection_with_pipe_in_name() {
        // Collection name containing pipe character (previously problematic with separator)
        let result = extract_collection("/collections/my%7Ccollection/points");
        assert_eq!(result.as_deref(), Some("my|collection"));
        assert!(matches!(result, Some(Cow::Owned(_))));
    }
}
