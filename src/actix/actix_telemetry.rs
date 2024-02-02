use std::future::{ready, Ready};
use std::sync::Arc;

use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::{web, Error};
use futures_util::future::LocalBoxFuture;
use parking_lot::Mutex;

use crate::actix::api::CollectionPath;
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
        let future = self.service.call(request);
        let telemetry_data = self.telemetry_data.clone();

        Box::pin(async move {
            let instant = std::time::Instant::now();
            let response = future.await?;
            let request = response.request();

            // first try to get the collection name by the path,
            // if not found, try to get it from the query params
            let collection = {
                let collection_name = request.match_info().get("name").map(|v| v.to_owned());
                collection_name.unwrap_or_else(|| {
                    let params = web::Query::<CollectionPath>::from_query(request.query_string());
                    if let Ok(params) = params {
                        params.name.clone()
                    } else {
                        "unknown".to_string()
                    }
                })
            };

            println!("collection: {collection:?}");

            let status = response.response().status().as_u16();
            telemetry_data
                .lock()
                .add_response(collection, request_key, status, instant);
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
