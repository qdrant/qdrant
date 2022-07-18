use std::sync::{Arc, Mutex};

use actix_utils::future::{ready, Ready};
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::Error;
use futures_util::future::LocalBoxFuture;

use crate::common::user_telemetry::{UserTelemetryCollector, UserTelemetryWebData};

pub struct ActixTelemetryService<S> {
    service: S,
    telemetry_data: Arc<Mutex<UserTelemetryWebData>>,
}

pub struct ActixTelemetryTransform {
    telemetry_collector: Arc<Mutex<UserTelemetryCollector>>,
}

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
            let status = response.response().status().as_u16() as usize;
            telemetry_data.lock().unwrap().add_response(status);
            Ok(response)
        })
    }
}

impl ActixTelemetryTransform {
    pub fn new(telemetry_collector: Arc<Mutex<UserTelemetryCollector>>) -> Self {
        Self {
            telemetry_collector,
        }
    }
}

impl<S, B> Transform<S, ServiceRequest> for ActixTelemetryTransform
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = ActixTelemetryService<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(ActixTelemetryService {
            service,
            telemetry_data: self
                .telemetry_collector
                .lock()
                .unwrap()
                .create_web_worker_telemetry(),
        }))
    }
}
