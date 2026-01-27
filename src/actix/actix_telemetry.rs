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
fn extract_collection(path: &str) -> Option<String> {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    // Find "collections" segment and get the next one
    let pos = segments.iter().position(|&s| s == "collections")?;
    let collection = segments.get(pos + 1)?;

    // URL decode if needed
    decode(collection).ok().map(|s| s.into_owned())
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
        let collection = extract_collection(request.path());
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
        assert_eq!(
            extract_collection("/collections/my_collection/points"),
            Some("my_collection".to_string())
        );
    }

    #[test]
    fn test_extract_collection_minimal() {
        assert_eq!(
            extract_collection("/collections/test"),
            Some("test".to_string())
        );
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
        assert_eq!(
            extract_collection("/collections/my%20collection/points"),
            Some("my collection".to_string())
        );
    }
}
