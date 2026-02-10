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

/// Extract collection name from gRPC request metadata
/// 
/// gRPC requests typically include collection name in the metadata headers
/// or as part of the request body. For now, we extract it from the URI path
/// if it follows the pattern with collection_name parameter.
fn extract_collection_from_metadata(
    request: &tonic::codegen::http::Request<tonic::transport::Body>,
) -> Option<String> {
    // Try to extract from headers first (if collection_name header is set)
    if let Some(collection_header) = request.headers().get("collection-name") {
        if let Ok(collection_str) = collection_header.to_str() {
            return Some(collection_str.to_string());
        }
    }
    
    // Note: For gRPC, the collection name is typically in the request body,
    // not in the URI or headers. Since we can't easily parse the body here
    // without consuming it, we'll need to rely on the application layer
    // to set the collection-name header if per-collection metrics are desired.
    // 
    // Alternative: The gRPC handlers could be modified to set this header
    // before the request reaches this middleware.
    
    None
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
        let collection_name = extract_collection_from_metadata(&request);
        let future = self.service.call(request);
        let telemetry_data = self.telemetry_data.clone();
        Box::pin(async move {
            let instant = std::time::Instant::now();
            let response = future.await?;
            telemetry_data.lock().add_response(method_name, collection_name, instant);
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
