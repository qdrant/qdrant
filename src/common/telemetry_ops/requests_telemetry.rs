use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use common::types::TelemetryDetail;
use lru::LruCache;
use parking_lot::Mutex;
use schemars::JsonSchema;
use segment::common::anonymize::{Anonymize, anonymize_collection_values};
use segment::common::operation_time_statistics::{
    OperationDurationStatistics, OperationDurationsAggregator, ScopeDurationMeasurer,
};
use serde::Serialize;
use storage::rbac::{AccessRequirements, Auth};

pub type HttpStatusCode = u16;

pub type GrpcStatusCode = i32;

/// Key for (collection, endpoint) tracking in LRU cache.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CollectionEndpointKey {
    pub collection: String,
    pub endpoint: String,
}

/// Get LRU capacity from ENV or default to 1000.
/// This limits retained `(collection, endpoint)` pairs.
///
/// **Operational note**: When a `(collection, endpoint)` pair is evicted from the
/// LRU cache and later re-observed, its per-collection counter resets to zero.
/// In Prometheus this can appear as a counter decrease for high-churn workloads
/// (many distinct collection names). Increase `QDRANT_TELEMETRY_COLLECTION_LIMIT`
/// if you need stable counters across a large number of collections.
fn get_per_collection_lru_capacity() -> NonZeroUsize {
    match std::env::var("QDRANT_TELEMETRY_COLLECTION_LIMIT") {
        Ok(val) => match val.parse::<usize>().ok().and_then(NonZeroUsize::new) {
            Some(limit) => limit,
            None => {
                log::warn!(
                    "Invalid QDRANT_TELEMETRY_COLLECTION_LIMIT value '{val}', using default 1000"
                );
                NonZeroUsize::new(1000).unwrap()
            }
        },
        Err(_) => NonZeroUsize::new(1000).unwrap(),
    }
}

#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct WebApiTelemetry {
    pub responses: HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[schemars(skip)]
    // intentionally hidden from OpenAPI schema to avoid schema churn while feature stabilizes
    pub responses_per_collection:
        HashMap<String, HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>>,
}

#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct GrpcTelemetry {
    pub responses: HashMap<String, HashMap<GrpcStatusCode, OperationDurationStatistics>>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[schemars(skip)]
    // intentionally hidden from OpenAPI schema to avoid schema churn while feature stabilizes
    pub responses_per_collection:
        HashMap<String, HashMap<String, HashMap<GrpcStatusCode, OperationDurationStatistics>>>,
}

pub struct ActixTelemetryCollector {
    pub workers: Vec<Arc<Mutex<ActixWorkerTelemetryCollector>>>,
}

pub struct ActixWorkerTelemetryCollector {
    methods: HashMap<String, HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
    /// Bounded LRU tracking per-collection request statistics.
    /// LRU eviction resets counters for evicted `(collection, endpoint)` pairs;
    /// see [`get_per_collection_lru_capacity`] for details.
    per_collection_data: LruCache<
        CollectionEndpointKey,
        HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>,
    >,
}

impl Default for ActixWorkerTelemetryCollector {
    fn default() -> Self {
        Self {
            methods: HashMap::new(),
            per_collection_data: LruCache::new(get_per_collection_lru_capacity()),
        }
    }
}

pub struct TonicTelemetryCollector {
    pub workers: Vec<Arc<Mutex<TonicWorkerTelemetryCollector>>>,
}

pub struct TonicWorkerTelemetryCollector {
    methods: HashMap<String, HashMap<GrpcStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
    /// Bounded LRU tracking per-collection request statistics.
    /// LRU eviction resets counters for evicted `(collection, endpoint)` pairs;
    /// see [`get_per_collection_lru_capacity`] for details.
    per_collection_data: LruCache<
        CollectionEndpointKey,
        HashMap<GrpcStatusCode, Arc<Mutex<OperationDurationsAggregator>>>,
    >,
}

impl Default for TonicWorkerTelemetryCollector {
    fn default() -> Self {
        Self {
            methods: HashMap::new(),
            per_collection_data: LruCache::new(get_per_collection_lru_capacity()),
        }
    }
}

impl ActixTelemetryCollector {
    pub fn create_web_worker_telemetry(&mut self) -> Arc<Mutex<ActixWorkerTelemetryCollector>> {
        let worker: Arc<Mutex<_>> = Default::default();
        self.workers.push(worker.clone());
        worker
    }

    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> WebApiTelemetry {
        let mut result = WebApiTelemetry::default();
        for web_data in &self.workers {
            let lock = web_data.lock().get_telemetry_data(detail);
            result.merge(&lock);
        }
        result
    }
}

impl TonicTelemetryCollector {
    pub fn create_grpc_telemetry_collector(&mut self) -> Arc<Mutex<TonicWorkerTelemetryCollector>> {
        let worker: Arc<Mutex<_>> = Default::default();
        self.workers.push(worker.clone());
        worker
    }

    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> GrpcTelemetry {
        let mut result = GrpcTelemetry::default();
        for grpc_data in &self.workers {
            let lock = grpc_data.lock().get_telemetry_data(detail);
            result.merge(&lock);
        }
        result
    }
}

impl TonicWorkerTelemetryCollector {
    pub fn add_response(
        &mut self,
        method: String,
        status_code: GrpcStatusCode,
        instant: std::time::Instant,
    ) {
        self.add_response_with_collection(method, status_code, instant, None);
    }

    pub fn add_response_with_collection(
        &mut self,
        method: String,
        status_code: GrpcStatusCode,
        instant: std::time::Instant,
        collection: Option<&str>,
    ) {
        if let Some(collection_name) = collection {
            // Clone needed: method goes to both global map key and per-collection key
            let aggregator = self
                .methods
                .entry(method.clone())
                .or_default()
                .entry(status_code)
                .or_insert_with(OperationDurationsAggregator::new);
            ScopeDurationMeasurer::new_with_instant(aggregator, instant);

            let key = CollectionEndpointKey {
                collection: collection_name.to_string(),
                endpoint: method,
            };
            let status_map = self
                .per_collection_data
                .get_or_insert_mut(key, HashMap::new);
            let aggregator = status_map
                .entry(status_code)
                .or_insert_with(OperationDurationsAggregator::new);
            ScopeDurationMeasurer::new_with_instant(aggregator, instant);
        } else {
            // No per-collection tracking: move method without cloning
            let aggregator = self
                .methods
                .entry(method)
                .or_default()
                .entry(status_code)
                .or_insert_with(OperationDurationsAggregator::new);
            ScopeDurationMeasurer::new_with_instant(aggregator, instant);
        }
    }

    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> GrpcTelemetry {
        let mut responses = HashMap::new();
        for (method, status_codes) in &self.methods {
            let mut status_codes_map = HashMap::new();
            for (status_code, aggregator) in status_codes {
                status_codes_map.insert(*status_code, aggregator.lock().get_statistics(detail));
            }
            responses.insert(method.clone(), status_codes_map);
        }

        let mut responses_per_collection = HashMap::new();
        for (key, status_codes) in self.per_collection_data.iter() {
            let collection_map = responses_per_collection
                .entry(key.collection.clone())
                .or_insert_with(HashMap::new);

            let status_map = collection_map
                .entry(key.endpoint.clone())
                .or_insert_with(HashMap::new);
            for (status_code, aggregator) in status_codes {
                status_map.insert(*status_code, aggregator.lock().get_statistics(detail));
            }
        }

        GrpcTelemetry {
            responses,
            responses_per_collection,
        }
    }
}

impl ActixWorkerTelemetryCollector {
    pub fn add_response(
        &mut self,
        endpoint: &str,
        status: HttpStatusCode,
        instant: std::time::Instant,
    ) {
        self.add_response_with_collection(endpoint, status, instant, None);
    }

    pub fn add_response_with_collection(
        &mut self,
        endpoint: &str,
        status: HttpStatusCode,
        instant: std::time::Instant,
        collection: Option<&str>,
    ) {
        // Global tracking
        let aggregator = self
            .methods
            .entry(endpoint.to_string())
            .or_default()
            .entry(status)
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);

        // Per-collection tracking
        if let Some(collection_name) = collection {
            let key = CollectionEndpointKey {
                collection: collection_name.to_string(),
                endpoint: endpoint.to_string(),
            };

            let status_map = self
                .per_collection_data
                .get_or_insert_mut(key, HashMap::new);
            let aggregator = status_map
                .entry(status)
                .or_insert_with(OperationDurationsAggregator::new);
            ScopeDurationMeasurer::new_with_instant(aggregator, instant);
        }
    }

    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> WebApiTelemetry {
        let mut responses = HashMap::new();
        for (endpoint, status_codes) in &self.methods {
            let mut status_codes_map = HashMap::new();
            for (status_code, aggregator) in status_codes {
                status_codes_map.insert(*status_code, aggregator.lock().get_statistics(detail));
            }
            responses.insert(endpoint.clone(), status_codes_map);
        }

        let mut responses_per_collection = HashMap::new();
        for (key, status_codes) in self.per_collection_data.iter() {
            let collection_map = responses_per_collection
                .entry(key.collection.clone())
                .or_insert_with(HashMap::new);

            let endpoint_map = collection_map
                .entry(key.endpoint.clone())
                .or_insert_with(HashMap::new);

            for (status_code, aggregator) in status_codes {
                endpoint_map.insert(*status_code, aggregator.lock().get_statistics(detail));
            }
        }

        WebApiTelemetry {
            responses,
            responses_per_collection,
        }
    }
}

impl GrpcTelemetry {
    pub fn merge(&mut self, other: &GrpcTelemetry) {
        for (method, success_map) in &other.responses {
            let entry = self.responses.entry(method.clone()).or_default();
            for (is_success, statistics) in success_map {
                let status_entry = entry.entry(*is_success).or_default();
                *status_entry = status_entry.clone() + statistics.clone();
            }
        }

        for (collection, endpoints) in &other.responses_per_collection {
            let self_collection = self
                .responses_per_collection
                .entry(collection.clone())
                .or_default();
            for (endpoint, status_codes) in endpoints {
                let self_endpoint = self_collection.entry(endpoint.clone()).or_default();
                for (status_code, statistics) in status_codes {
                    let entry = self_endpoint.entry(*status_code).or_default();
                    *entry = entry.clone() + statistics.clone();
                }
            }
        }
    }
}

impl WebApiTelemetry {
    pub fn merge(&mut self, other: &WebApiTelemetry) {
        for (method, status_codes) in &other.responses {
            let status_codes_map = self.responses.entry(method.clone()).or_default();
            for (status_code, statistics) in status_codes {
                let entry = status_codes_map.entry(*status_code).or_default();
                *entry = entry.clone() + statistics.clone();
            }
        }

        for (collection, endpoints) in &other.responses_per_collection {
            let self_collection = self
                .responses_per_collection
                .entry(collection.clone())
                .or_default();
            for (endpoint, status_codes) in endpoints {
                let self_endpoint = self_collection.entry(endpoint.clone()).or_default();
                for (status_code, statistics) in status_codes {
                    let entry = self_endpoint.entry(*status_code).or_default();
                    *entry = entry.clone() + statistics.clone();
                }
            }
        }
    }
}

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct RequestsTelemetry {
    pub rest: WebApiTelemetry,
    pub grpc: GrpcTelemetry,
}

impl RequestsTelemetry {
    pub fn collect(
        auth: &Auth,
        actix_collector: &ActixTelemetryCollector,
        tonic_collector: &TonicTelemetryCollector,
        detail: TelemetryDetail,
    ) -> Option<Self> {
        let global_access = AccessRequirements::new();
        if auth
            .check_global_access(global_access, "telemetry_requests")
            .is_ok()
        {
            let rest = actix_collector.get_telemetry_data(detail);
            let grpc = tonic_collector.get_telemetry_data(detail);
            Some(Self { rest, grpc })
        } else {
            None
        }
    }
}

impl Anonymize for WebApiTelemetry {
    fn anonymize(&self) -> Self {
        let responses = self
            .responses
            .iter()
            .map(|(key, value)| (key.clone(), anonymize_collection_values(value)))
            .collect();

        let responses_per_collection = self
            .responses_per_collection
            .iter()
            .map(|(collection, endpoints)| {
                let collection_name = collection.clone();
                let anonymized_endpoints = endpoints
                    .iter()
                    .map(|(endpoint, statuses)| {
                        (endpoint.clone(), anonymize_collection_values(statuses))
                    })
                    .collect();
                (collection_name, anonymized_endpoints)
            })
            .collect();

        WebApiTelemetry {
            responses,
            responses_per_collection,
        }
    }
}

impl Anonymize for GrpcTelemetry {
    fn anonymize(&self) -> Self {
        let responses = self
            .responses
            .iter()
            .map(|(key, value)| (key.clone(), anonymize_collection_values(value)))
            .collect();

        let responses_per_collection = self
            .responses_per_collection
            .iter()
            .map(|(collection, endpoints)| {
                let collection_name = collection.clone();
                let anonymized_endpoints = endpoints
                    .iter()
                    .map(|(endpoint, statuses)| {
                        (endpoint.clone(), anonymize_collection_values(statuses))
                    })
                    .collect();
                (collection_name, anonymized_endpoints)
            })
            .collect();

        GrpcTelemetry {
            responses,
            responses_per_collection,
        }
    }
}
