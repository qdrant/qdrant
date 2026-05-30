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
}

#[derive(Clone)]
pub struct TonicTelemetryLayer {
    telemetry_collector: Arc<parking_lot::Mutex<TonicTelemetryCollector>>,
}

impl<S> Service<tonic::codegen::http::Request<tonic::transport::Body>> for TonicTelemetryService<S>
where
    S: Service<tonic::codegen::http::Request<tonic::transport::Body>>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<S::Response, S::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(
        &mut self,
        request: tonic::codegen::http::Request<tonic::transport::Body>,
    ) -> Self::Future {
        let method_name = request.uri().path().to_string();
        let future = self.service.call(request);
        let telemetry_data = self.telemetry_data.clone();
        Box::pin(async move {
            let instant = std::time::Instant::now();
            let response = future.await?;
            telemetry_data.lock().add_response(method_name, instant);
            Ok(response)
        })
    }
}

impl TonicTelemetryLayer {
    pub fn new(
        telemetry_collector: Arc<parking_lot::Mutex<TonicTelemetryCollector>>,
    ) -> TonicTelemetryLayer {
        Self {
            telemetry_collector,
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
        }
    }
}
