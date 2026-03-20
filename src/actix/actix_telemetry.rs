use std::future::{Ready, ready};
use std::sync::Arc;

use actix_web::Error;
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use futures_util::future::LocalBoxFuture;
use parking_lot::Mutex;

use crate::common::telemetry_ops::requests_telemetry::{
    ActixTelemetryCollector, ActixWorkerTelemetryCollector,
};

pub struct ActixTelemetryService<S> {
    service: S,
    telemetry_data: Arc<Mutex<ActixWorkerTelemetryCollector>>,
}

pub struct ActixTelemetryTransform {
    telemetry_collector: Arc<Mutex<ActixTelemetryCollector>>,
}

/// Extract collection name from the request path.
///
/// Returns `Some(collection_name)` if the path matches `/collections/{collection_name}/...`,
/// otherwise returns `None`.
fn extract_collection_from_path(path: &str) -> Option<&str> {
    let mut it = path.split('/');
    match (it.next(), it.next(), it.next()) {
        (Some(""), Some("collections"), Some(collection)) if !collection.is_empty() => {
            Some(collection)
        }
        _ => None,
    }
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
        // Extract collection BEFORE moving request into service.call(request)
        let collection = extract_collection_from_path(request.path()).map(|s| s.to_string());

        // Prefer Actix match pattern; if missing, fall back to normalized path
        let match_pattern = request.match_pattern();
        let endpoint = match match_pattern {
            Some(p) if p != "unknown" => p.clone(),
            _ => {
                let path = request.path();

                // Normalize collection name to avoid high-cardinality metrics
                if extract_collection_from_path(path).is_some() {
                    // Convert "/collections/<real>/..." -> "/collections/{name}/..."
                    // Keep the rest of the path intact.
                    let mut it = path.splitn(4, '/'); // "", "collections", "<name>", "<rest...>"
                    match (it.next(), it.next(), it.next(), it.next()) {
                        (Some(""), Some("collections"), Some(_), Some(rest)) => {
                            format!("/collections/{{name}}/{rest}")
                        }
                        (Some(""), Some("collections"), Some(_), None) => {
                            "/collections/{name}".to_string()
                        }
                        _ => path.to_string(),
                    }
                } else {
                    path.to_string()
                }
            }
        };

        let request_key = format!("{} {}", request.method(), endpoint);

        let future = self.service.call(request);
        let telemetry_data = self.telemetry_data.clone();

        Box::pin(async move {
            let instant = std::time::Instant::now();
            let response = future.await?;
            let status = response.response().status().as_u16();

            {
                let mut guard = telemetry_data.lock();
                guard.add_response(request_key.clone(), status, instant);

                if let Some(collection) = collection {
                    guard.add_response_by_collection(request_key, collection, status, instant);
                }
            }

            Ok(response)
        })
    }
}

impl ActixTelemetryTransform {
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
