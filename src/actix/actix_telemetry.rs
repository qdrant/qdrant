use std::sync::Arc;

use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::Error;
use futures_util::future::LocalBoxFuture;
use tokio::sync::Mutex;

use crate::common::telemetry::{TelemetryCollector, WebApiTelemetry};

pub struct ActixTelemetryService<S> {
    service: S,
    telemetry_data: Arc<Mutex<WebApiTelemetry>>,
}

pub struct ActixTelemetryTransform {
    telemetry_collector: Arc<Mutex<TelemetryCollector>>,
}

/// Actix telemetry service. It hooks every request and looks into response status code.
///
/// More about actix service with similar example
/// https://actix.rs/docs/middleware/
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
        let future = self.service.call(request);
        let telemetry_data = self.telemetry_data.clone();
        Box::pin(async move {
            let response = future.await?;
            let status = response.response().status().as_u16();
            telemetry_data.lock().await.add_response(status);
            Ok(response)
        })
    }
}

impl ActixTelemetryTransform {
    pub fn new(telemetry_collector: Arc<Mutex<TelemetryCollector>>) -> Self {
        Self {
            telemetry_collector,
        }
    }
}

/// Actix telemetry transform. It's a builder for an actix service
///
/// More about actix transform with similar example
/// https://actix.rs/docs/middleware/
impl<S, B> Transform<S, ServiceRequest> for ActixTelemetryTransform
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = ActixTelemetryService<S>;
    type Future = LocalBoxFuture<'static, Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        let telemetry_collector = self.telemetry_collector.clone();
        Box::pin(async move {
            let mut telemetry_collector = telemetry_collector.lock().await;
            Ok(ActixTelemetryService {
                service,
                telemetry_data: telemetry_collector.create_web_worker_telemetry(),
            })
        })
    }
}
