// `c:\Users\Admin\Desktop\ag\budget\qdrant\src\tonic\interceptor.rs`
use std::task::{Context, Poll};
use std::time::Instant;
use std::sync::Arc;
use std::future::Future;
use std::pin::Pin;

use http::{Request, Response};
use tonic::body::BoxBody;
use bytes::Bytes;
use prost::Message;

// Uses hyper/http_body implementations typical for a Tonic standard
use http_body_util::{BodyExt, Full}; 

use crate::telemetry::GrpcTelemetry;

// Assuming max limit before falling back to global is 64 KB based on common constraints
const MAX_BUFFER_SIZE: usize = 64 * 1024;

/// Minimal Protobuf structure matching Qdrant schema where collection_name 
/// is almost universally field #1 across operations like Search/Upsert/Recommend.
#[derive(Clone, PartialEq, Message)]
pub struct ExtractCollectionName {
    #[prost(string, tag = "1")]
    pub collection_name: String,
}

#[derive(Clone)]
pub struct GrpcTelemetryLayer {
    telemetry: Arc<GrpcTelemetry>,
}

impl GrpcTelemetryLayer {
    pub fn new(telemetry: Arc<GrpcTelemetry>) -> Self {
        Self { telemetry }
    }
}

impl<S> tower::Layer<S> for GrpcTelemetryLayer {
    type Service = GrpcTelemetryService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcTelemetryService {
            inner,
            telemetry: self.telemetry.clone(),
        }
    }
}

#[derive(Clone)]
pub struct GrpcTelemetryService<S> {
    inner: S,
    telemetry: Arc<GrpcTelemetry>,
}

impl<S, ResBody> tower::Service<Request<BoxBody>> for GrpcTelemetryService<S>
where
    S: tower::Service<Request<BoxBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    // Uses BoxFuture for avoiding manual pin projections around dynamic HTTP bodies
    type Future = futures_util::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
        let mut inner_service = self.inner.clone();
        let telemetry = self.telemetry.clone();
        
        // Extract URI Endpoint before consumption
        let uri_path = req.uri().path().to_string(); 

        Box::pin(async move {
            let start = Instant::now();
            
            // Only inspect endpoints known to target a specific collection
            let targets_collection = uri_path.contains("/Search") 
                || uri_path.contains("/Upsert") 
                || uri_path.contains("/Recommend");
            
            let mut extracted_collection = None;
            let mut forwarded_req = req;

            if targets_collection {
                let (parts, body) = forwarded_req.into_parts();
                
                // Attempt to buffer and extract the first gRPC frame
                let collected = match body.collect().await {
                    Ok(c) => c.to_bytes(),
                    Err(_) => Bytes::new(), // If stream drops, fall back safely
                };
                
                let mut is_too_large = false;

                if collected.len() > MAX_BUFFER_SIZE {
                    // Payload too large to safely intercept without dragging IO
                    is_too_large = true;
                } else if collected.len() > 5 {
                    // gRPC Frame header: 1 byte compressed flag, 4 bytes length
                    let grpc_msg_bytes = &collected[5..];
                    
                    if let Ok(msg) = ExtractCollectionName::decode(grpc_msg_bytes) {
                        if !msg.collection_name.is_empty() {
                            extracted_collection = Some(msg.collection_name);
                        }
                    }
                }
                
                // Reconstruct the Http Request Body for the downstream Tonic endpoint handler
                let new_body = tonic::body::boxed(Full::new(collected));
                forwarded_req = Request::from_parts(parts, new_body);
                
                if is_too_large {
                    // Overflows fallback to the "unknown" metric route
                    extracted_collection = Some("unknown".to_string());
                }
            } else {
                extracted_collection = Some("unknown".to_string());
            }

            let response = inner_service.call(forwarded_req).await?;
            let duration = start.elapsed();
            
            let collection_label = extracted_collection.unwrap_or_else(|| "unknown".to_string());
            
            telemetry.record(&uri_path, &collection_label, duration);
            
            Ok(response)
        })
    }
}
