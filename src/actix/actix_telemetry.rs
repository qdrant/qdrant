use std::future::{ready, Ready};
use std::sync::Arc;

use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::Error;
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

// The uri will be `/collections/collection_name...`
fn get_collection_from_uri(uri: &str) -> Option<String> {
    let mut uri_segments = dbg!(uri).split('/');
    let seg0 = uri_segments.next();
    if let Some(seg0) = seg0 {
        if seg0 != "" {
            return None;
        }
    } else {
        return None;
    }
    let seg1 = uri_segments.next();
    if let Some(seg1) = seg1 {
        if seg1 != "collections" {
            return None;
        }
    } else {
        return None;
    }
    let seg2 = uri_segments.next();
    if let Some(seg2) = seg2 {
        Some(seg2.to_owned())
    } else {
        None
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
        let collection_name = get_collection_from_uri(request.uri().path());
        let match_pattern = request
            .match_pattern()
            .unwrap_or_else(|| "unknown".to_owned());
        let request_key = format!("{} {}", request.method(), match_pattern);
        let future = self.service.call(request);
        let telemetry_data = self.telemetry_data.clone();
        Box::pin(async move {
            let instant = std::time::Instant::now();
            let response = future.await?;
            let status = response.response().status().as_u16();
            telemetry_data
                .lock()
                .add_response(collection_name, request_key, status, instant);
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
