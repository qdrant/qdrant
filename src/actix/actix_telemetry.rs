use std::future::{Ready, ready};
use std::sync::Arc;

use actix_web::Error;
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use futures_util::future::LocalBoxFuture;
use parking_lot::Mutex;

use crate::common::telemetry_ops::requests_telemetry::{
    ActixTelemetryCollector, ActixWorkerTelemetryCollector,
};

/// Extract collection name from request path
///
/// Examples:
/// - "/collections/my_collection/points/search" -> Some("my_collection")
/// - "/collections/my-collection/points" -> Some("my-collection")
/// - "/cluster/status" -> None
fn extract_collection_name(path: &str) -> Option<String> {
    // Only extract collection name for paths that start with /collections/
    let prefix = "/collections/";
    if let Some(start) = path.find(prefix) {
        let after_prefix = &path[start + prefix.len()..];
        // Get the collection name (until next / or end)
        let end = after_prefix.find('/').unwrap_or(after_prefix.len());
        if end > 0 {
            return Some(after_prefix[..end].to_string());
        }
    }
    None
}

pub struct ActixTelemetryService<S> {
    service: S,
    telemetry_data: Arc<Mutex<ActixWorkerTelemetryCollector>>,
}

pub struct ActixTelemetryTransform {
    telemetry_collector: Arc<Mutex<ActixTelemetryCollector>>,
    enable_per_collection: bool,
    max_collections: usize,
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

        // Extract collection name from request path
        let path = request.path().to_string();
        let collection = extract_collection_name(&path);

        let future = self.service.call(request);
        let telemetry_data = self.telemetry_data.clone();
        Box::pin(async move {
            let instant = std::time::Instant::now();
            let response = future.await?;
            let status = response.response().status().as_u16();
            telemetry_data
                .lock()
                .add_response(request_key, status, instant, collection);
            Ok(response)
        })
    }
}

impl ActixTelemetryTransform {
    pub fn new(telemetry_collector: Arc<Mutex<ActixTelemetryCollector>>) -> Self {
        Self {
            telemetry_collector,
            enable_per_collection: true, // Default for backward compatibility
            max_collections: 1000, // Default limit
        }
    }

    pub fn with_config(
        telemetry_collector: Arc<Mutex<ActixTelemetryCollector>>,
        enable_per_collection: bool,
        max_collections: usize,
    ) -> Self {
        Self {
            telemetry_collector,
            enable_per_collection,
            max_collections,
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
                .create_web_worker_telemetry_with_config(
                    self.enable_per_collection,
                    self.max_collections,
                ),
        }))
    }
}
