use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use std::future::poll_fn;
use std::pin::Pin;

use prost::Message;
use sha2::{Digest, Sha256};
use bytes::Bytes;
use http_body::Body;
use tonic::codegen::http::{Request, Response};

type GrpcRequest = Request<tonic::transport::Body>;
type GrpcResponse = Response<tonic::body::BoxBody>;

const MAX_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Clone, PartialEq, Message)]
pub struct ExtractCollectionName {
    #[prost(string, tag = "1")]
    pub collection_name: String,
}

#[derive(Debug, Default)]
pub struct OperationDurationsAggregator {
    count: AtomicU64,
    total_duration_micros: AtomicU64,
}

impl OperationDurationsAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline(always)]
    pub fn add(&self, duration: Duration) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_duration_micros
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }
}

pub struct GrpcTelemetry {
    aggregators: Mutex<HashMap<String, Arc<OperationDurationsAggregator>>>,
    max_collections: usize,
    anonymize: bool,
    global_aggregator: Arc<OperationDurationsAggregator>,
}

impl GrpcTelemetry {
    pub fn new(max_collections: usize, anonymize: bool) -> Self {
        Self {
            aggregators: Mutex::new(HashMap::new()),
            max_collections,
            anonymize,
            global_aggregator: Arc::new(OperationDurationsAggregator::new()),
        }
    }

    pub fn get_or_create_aggregator(
        &self,
        endpoint: &str,
        collection_name: &str,
    ) -> Arc<OperationDurationsAggregator> {
        let mut actual_name = collection_name;
        if actual_name.is_empty() {
            actual_name = "unknown";
        }

        let final_name = if self.anonymize && actual_name != "unknown" {
            Self::hash_collection_name(actual_name)
        } else {
            actual_name.to_string()
        };

        let key = format!("{}|{}", endpoint, final_name);

        let mut map = self.aggregators.lock().unwrap();

        if let Some(agg) = map.get(&key) {
            return agg.clone();
        }

        if map.len() >= self.max_collections && actual_name != "unknown" {
            return self.global_aggregator.clone();
        }

        let new_agg = Arc::new(OperationDurationsAggregator::new());
        map.insert(key, new_agg.clone());
        new_agg
    }

    pub fn record(&self, endpoint: &str, collection_name: &str, duration: Duration) {
        let agg = self.get_or_create_aggregator(endpoint, collection_name);
        agg.add(duration);
    }

    fn hash_collection_name(name: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(name.as_bytes());
        format!("{:x}", hasher.finalize())
    }
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

impl<S> tower::Service<GrpcRequest> for GrpcTelemetryService<S>
where
    S: tower::Service<GrpcRequest, Response = GrpcResponse> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures_util::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: GrpcRequest) -> Self::Future {
        let mut inner_service = self.inner.clone();
        let telemetry = self.telemetry.clone();
        let uri_path = req.uri().path().to_string();

        Box::pin(async move {
            let start = Instant::now();
            let targets_collection = uri_path.contains("/Search")
                || uri_path.contains("/Upsert")
                || uri_path.contains("/Recommend");

            let mut extracted_collection = None;
            let mut forwarded_req = req;

            if targets_collection {
                let (parts, mut body) = forwarded_req.into_parts();
                
                let mut collected_bytes = Vec::new();
                let mut is_too_large = false;

                use std::task::Poll;
                while let Some(chunk_res) = poll_fn(|cx| -> Poll<Option<Result<bytes::Bytes, <tonic::transport::Body as http_body::Body>::Error>>> {
                    Pin::new(&mut body).poll_data(cx)
                }).await {
                    match chunk_res {
                        Ok(chunk) => {
                            // Assume chunk derefs to [u8] like Bytes does
                            let data: &[u8] = chunk.as_ref();
                            collected_bytes.extend_from_slice(data);
                            if collected_bytes.len() > MAX_BUFFER_SIZE {
                                is_too_large = true;
                                break;
                            }
                        }
                        Err(_) => {
                            break;
                        }
                    }
                }

                if collected_bytes.len() > 5 {
                    let grpc_msg_bytes = &collected_bytes[5..];
                    if let Ok(msg) = ExtractCollectionName::decode(grpc_msg_bytes) {
                        if !msg.collection_name.is_empty() {
                            extracted_collection = Some(msg.collection_name);
                        }
                    }
                }

                let new_body = tonic::transport::Body::from(Bytes::from(collected_bytes));
                forwarded_req = Request::from_parts(parts, new_body);

                if is_too_large {
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

fn escape_prometheus_label(val: &str) -> String {
    val.replace('\\', "\\\\")
       .replace('\n', "\\n")
       .replace('"', "\\\"")
}

pub fn render_prometheus_grpc(telemetry: &GrpcTelemetry) -> String {
    let mut output = String::new();
    let iter = telemetry.aggregators.lock().unwrap();

    for (k, agg) in iter.iter() {
        let count = agg.count.load(Ordering::Relaxed);
        let duration = agg.total_duration_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        
        let mut parts = k.split('|');
        let endpoint = parts.next().unwrap_or("unknown");
        let collection_raw = parts.next().unwrap_or("unknown");
        let collection = escape_prometheus_label(collection_raw);
        
        output.push_str(&format!(
            "qdrant_grpc_requests_total{{endpoint=\"{}\", collection=\"{}\"}} {}\n",
            endpoint, collection, count
        ));
        output.push_str(&format!(
            "qdrant_grpc_responses_duration_seconds{{endpoint=\"{}\", collection=\"{}\"}} {}\n",
            endpoint, collection, duration
        ));
    }
    output
}
