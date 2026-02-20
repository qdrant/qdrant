use std::future::{Ready, ready};
use std::sync::Arc;

use actix_web::Error;
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use futures_util::future::LocalBoxFuture;
use parking_lot::Mutex;

use crate::common::telemetry_ops::requests_telemetry::{
    ActixTelemetryCollector, ActixWorkerTelemetryCollector,
};

/// Actix middleware service that records per-request telemetry (status codes, durations, collection names).
pub struct ActixTelemetryService<S> {
    service: S,
    telemetry_data: Arc<Mutex<ActixWorkerTelemetryCollector>>,
}

/// Extract and percent-decode the collection name from a REST URL path.
///
/// Expects paths like `/collections/{name}/points/search`. The `{name}` segment
/// may be percent-encoded (e.g. `my%20collection`), so this function decodes it
/// before returning.
///
/// Returns `None` if the path does not start with `/collections/` or the name
/// segment is empty.
fn extract_collection_name(path: &str) -> Option<String> {
    let path = path.strip_prefix("/collections/")?;
    let name = path.split('/').next()?;
    if name.is_empty() {
        return None;
    }
    Some(
        urlencoding::decode(name)
            .unwrap_or(std::borrow::Cow::Borrowed(name))
            .into_owned(),
    )
}

/// Actix middleware transform that creates [`ActixTelemetryService`] instances for each worker.
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

        // Extract collection name from URL path for per-collection metrics.
        // URL pattern: /collections/{name}/...
        let collection_name = extract_collection_name(request.path());

        let future = self.service.call(request);
        let telemetry_data = self.telemetry_data.clone();
        Box::pin(async move {
            let instant = std::time::Instant::now();
            let response = future.await?;
            let status = response.response().status().as_u16();
            telemetry_data
                .lock()
                .add_response(request_key, status, instant, collection_name);
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
