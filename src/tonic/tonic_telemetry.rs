use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::future::BoxFuture;
use tonic::body::BoxBody;
use tower::Service;
use tower_layer::Layer;

use crate::common::telemetry_ops::requests_telemetry::{
    TonicTelemetryCollector, TonicWorkerTelemetryCollector,
};

/// Based on https://grpc.io/docs/guides/status-codes/
/// Default gRPC status code for all responses (0 = OK)
const DEFAULT_SUCCESS_GRPC_STATUS_CODE: i32 = 0;

/// Based on https://grpc.io/docs/guides/status-codes/
/// Default gRPC status code for errors (2 = UNKNOWN)
const DEFAULT_FAILURE_GRPC_STATUS_CODE: i32 = 2;

const GRPC_STATUS_HEADER: &str = "grpc-status";

/// Extract collection name from gRPC request path
///
/// Examples:
/// - "/qdrant.Collections/GetCollectionInfo" -> None (no collection name)
/// - "/qdrant.Points/Search" -> None (need to extract from request body, not implemented)
/// - For now, gRPC collection extraction is more complex and would require
///   inspecting request bodies. This is left as future enhancement.
fn extract_collection_name_grpc(_path: &str) -> Option<String> {
    // gRPC collection extraction is complex and would require parsing request bodies.
    // For initial implementation, we skip per-collection metrics for gRPC.
    // This can be enhanced in the future by adding collection name extraction
    // from the request context or metadata.
    None
}

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
    enable_per_collection: bool,
    max_collections: usize,
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

    fn call(&mut self, request: Request) -> Self::Future {
        let method_name = request.uri().path().to_string();
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

            // Extract collection name from gRPC request (currently not implemented)
            let collection = extract_collection_name_grpc(&method_name);

            telemetry_data
                .lock()
                .add_response(method_name, instant, status_code, collection);
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
            enable_per_collection: true, // Default for backward compatibility
            max_collections: 1000, // Default limit
        }
    }

    pub fn with_config(
        telemetry_collector: Arc<parking_lot::Mutex<TonicTelemetryCollector>>,
        enable_per_collection: bool,
        max_collections: usize,
    ) -> TonicTelemetryLayer {
        Self {
            telemetry_collector,
            enable_per_collection,
            max_collections,
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
                .create_grpc_telemetry_collector_with_config(
                    self.enable_per_collection,
                    self.max_collections,
                ),
        }
    }
}
