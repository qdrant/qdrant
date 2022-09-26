use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::future::BoxFuture;
use segment::telemetry::{TelemetryOperationAggregator, TelemetryOperationTimer};
use tower::Service;
use tower_layer::Layer;

use crate::common::telemetry::TonicTelemetryCollector;

#[derive(Clone)]
pub struct TonicTelemetryService<T> {
    service: T,
    calls_aggregator: Arc<parking_lot::Mutex<TelemetryOperationAggregator>>,
}

#[derive(Clone)]
pub struct TonicTelemetryLayer {
    telemetry_collector: Arc<parking_lot::Mutex<TonicTelemetryCollector>>,
}

impl<S, Request> Service<Request> for TonicTelemetryService<S>
where
    S: Service<Request>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<S::Response, S::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let future = self.service.call(request);
        let calls_aggregator = self.calls_aggregator.clone();
        Box::pin(async move {
            let instant = std::time::Instant::now();
            let _timer = TelemetryOperationTimer::new(&calls_aggregator);
            let response = future.await?;
            segment::GRPC_WRITE.update(instant.elapsed());
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
            calls_aggregator: self
                .telemetry_collector
                .lock()
                .create_grpc_telemetry_collector(),
        }
    }
}
