use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::future::BoxFuture;
use tonic::body::BoxBody;
use tower::Service;
use tower_layer::Layer;

use crate::common::telemetry_ops::requests_telemetry::{
    TonicTelemetryCollector, TonicWorkerTelemetryCollector,
};

/// Shared slot for communicating the target collection name from gRPC handlers
/// to the telemetry middleware. The middleware inserts a default (empty) slot
/// into the request extensions; the handler fills it; the middleware reads it
/// after the response resolves.
#[derive(Clone, Default)]
pub struct GrpcCollectionSlot(Arc<parking_lot::Mutex<Option<String>>>);

impl GrpcCollectionSlot {
    pub fn set(&self, collection: String) {
        let mut guard = self.0.lock();
        *guard = Some(collection);
    }

    pub fn take(&self) -> Option<String> {
        let mut guard = self.0.lock();
        guard.take()
    }
}

/// Based on https://grpc.io/docs/guides/status-codes/
/// Default gRPC status code for all responses (0 = OK)
const DEFAULT_SUCCESS_GRPC_STATUS_CODE: i32 = 0;

/// Based on https://grpc.io/docs/guides/status-codes/
/// Default gRPC status code for errors (2 = UNKNOWN)
const DEFAULT_FAILURE_GRPC_STATUS_CODE: i32 = 2;

const GRPC_STATUS_HEADER: &str = "grpc-status";

type Request = tonic::codegen::http::Request<tonic::transport::Body>;
type Response = tonic::codegen::http::Response<BoxBody>;

#[derive(Clone)]
pub struct TonicTelemetryService<T> {
    service: T,
    telemetry_data: Arc<parking_lot::Mutex<TonicWorkerTelemetryCollector>>,
}

#[derive(Clone)]
pub struct TonicTelemetryLayer {
    telemetry_collector: Arc<parking_lot::Mutex<TonicTelemetryCollector>>,
}

impl<S> Service<Request> for TonicTelemetryService<S>
where
    S: Service<Request, Response = Response>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<S::Response, S::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request) -> Self::Future {
        let method_name = request.uri().path().to_string();

        // Insert a shared slot; handlers write their collection name into it.
        let slot = GrpcCollectionSlot::default();
        let slot_reader = slot.clone();
        request.extensions_mut().insert(slot);

        let future = self.service.call(request);
        let telemetry_data = self.telemetry_data.clone();
        Box::pin(async move {
            let instant = std::time::Instant::now();
            let response = future.await?;

            // For gRPC, HTTP status is usually 200, check grpc-status header instead
            // grpc-status: 0 = OK, non-zero = error
            let status_code = response
                .headers()
                .get(GRPC_STATUS_HEADER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<i32>().ok())
                .unwrap_or_else(|| {
                    if response.status().is_success() {
                        DEFAULT_SUCCESS_GRPC_STATUS_CODE
                    } else {
                        DEFAULT_FAILURE_GRPC_STATUS_CODE
                    }
                });

            // slot_reader is captured by this async block so it's dropped here,
            // not at the end of call() — avoids rustc 1.93.1 ICE in check_mod_deathness.
            let collection = slot_reader.take();

            // Inline per-collection tracking to avoid rustc 1.93.1 ICE.
            // The ICE is triggered when calling a method in requests_telemetry that takes
            // Option<String> from this async block, even without .as_deref().
            // NOTE: crate MSRV is 1.92, but the ICE was observed on rustc 1.93.1
            // in check_mod_deathness. Keep this structure until upstream is fixed.
            let mut telemetry = telemetry_data.lock();
            if let Some(c) = collection {
                telemetry.add_response_with_collection(method_name, status_code, instant, Some(&c));
            } else {
                telemetry.add_response(method_name, status_code, instant);
            }

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
