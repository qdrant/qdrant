use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::future::BoxFuture;
use tower::Service;
use tower_layer::Layer;

use crate::common::telemetry_ops::requests_telemetry::{
    TonicTelemetryCollector, TonicWorkerTelemetryCollector,
};

#[derive(Clone)]
pub struct TonicTelemetryService<T> {
    service: T,
    telemetry_data: Arc<parking_lot::Mutex<TonicWorkerTelemetryCollector>>,
    enable_per_collection: bool,
}

#[derive(Clone)]
pub struct TonicTelemetryLayer {
    telemetry_collector: Arc<parking_lot::Mutex<TonicTelemetryCollector>>,
    enable_per_collection: bool,
}

impl<S, B> Service<tonic::codegen::http::Request<tonic::transport::Body>>
    for TonicTelemetryService<S>
where
    S: Service<
            tonic::codegen::http::Request<tonic::transport::Body>,
            Response = tonic::codegen::http::Response<B>,
        >,
    S::Future: Send + 'static,
    B: 'static,
{
    type Response = tonic::codegen::http::Response<B>;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(
        &mut self,
        request: tonic::codegen::http::Request<tonic::transport::Body>,
    ) -> Self::Future {
        let method_name = request.uri().path().to_string();
        let future = self.service.call(request);
        let enable_per_collection = self.enable_per_collection;
        let telemetry_data = self.telemetry_data.clone();

        Box::pin(async move {
            let instant = std::time::Instant::now();
            let response = future.await?;
            let collection_name = if enable_per_collection {
                response
                    .extensions()
                    .get::<crate::common::telemetry_ops::telemetry_context::CollectionName>()
                    .map(|c| c.0.clone())
            } else {
                None
            };
            telemetry_data
                .lock()
                .add_response(method_name, instant, collection_name);
            Ok(response)
        })
    }
}

impl TonicTelemetryLayer {
    pub fn new(
        telemetry_collector: Arc<parking_lot::Mutex<TonicTelemetryCollector>>,
        enable_per_collection: bool,
    ) -> TonicTelemetryLayer {
        Self {
            telemetry_collector,
            enable_per_collection,
        }
    }
}

impl<S> Layer<S> for TonicTelemetryLayer {
    type Service = TonicTelemetryService<S>;

    fn layer(&self, service: S) -> Self::Service {
        TonicTelemetryService {
            service,
            telemetry_data: self
                .telemetry_collector
                .lock()
                .create_grpc_telemetry_collector(),
            enable_per_collection: self.enable_per_collection,
        }
    }
}
