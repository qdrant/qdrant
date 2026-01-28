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
use storage::rbac::{Access, AccessRequirements};

pub type HttpStatusCode = u16;

/// Key for per-collection telemetry data, combining collection name and endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CollectionEndpointKey {
    /// Name of the collection.
    pub collection: String,
    /// API endpoint path pattern.
    pub endpoint: String,
}

/// Telemetry data for REST API requests.
#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct WebApiTelemetry {
    /// Global response statistics grouped by endpoint and HTTP status code.
    pub responses: HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>,
    /// Per-collection response statistics: collection -> endpoint -> status -> stats.
    #[serde(rename = "responses_per_collection")]
    pub responses_per_collection:
        HashMap<String, HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>>,
}

/// Telemetry data for gRPC requests.
#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct GrpcTelemetry {
    /// Global response statistics grouped by gRPC method name.
    pub responses: HashMap<String, OperationDurationStatistics>,
    /// Per-collection response statistics: collection -> endpoint -> stats.
    #[serde(rename = "responses_per_collection")]
    pub responses_per_collection: HashMap<String, HashMap<String, OperationDurationStatistics>>,
}

pub struct ActixTelemetryCollector {
    pub workers: Vec<Arc<Mutex<ActixWorkerTelemetryCollector>>>,
}

/// Maximum number of unique (collection, endpoint) pairs to track per worker.
///
/// This LRU limit acts as a guardrail against unbounded memory growth in multi-tenant
/// or adversarial scenarios where collection names are user-controlled. Without this
/// limit, an attacker could create arbitrarily many collections to exhaust memory.
const PER_COLLECTION_TELEMETRY_LRU_CAPACITY: usize = 1000;

/// Per-worker telemetry collector for Actix (REST API) requests.
///
/// Uses an LRU cache for per-collection metrics to prevent unbounded memory growth.
/// This is a guardrail for multi-tenant deployments where collection names are
/// user-controlled and could be used to exhaust memory via cardinality explosion.
pub struct ActixWorkerTelemetryCollector {
    methods: HashMap<String, HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
    per_collection_data: LruCache<
        CollectionEndpointKey,
        HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>,
    >,
}

impl ActixWorkerTelemetryCollector {
    /// Creates a new collector with LRU cache capacity set to prevent memory blow-up.
    pub fn new() -> Self {
        Self {
            methods: HashMap::new(),
            per_collection_data: LruCache::new(
                NonZeroUsize::new(PER_COLLECTION_TELEMETRY_LRU_CAPACITY).unwrap(),
            ),
        }
    }
}

pub struct TonicTelemetryCollector {
    pub workers: Vec<Arc<Mutex<TonicWorkerTelemetryCollector>>>,
}

/// Per-worker telemetry collector for Tonic (gRPC) requests.
///
/// Uses an LRU cache for per-collection metrics to prevent unbounded memory growth.
/// This is a guardrail for multi-tenant deployments where collection names are
/// user-controlled and could be used to exhaust memory via cardinality explosion.
pub struct TonicWorkerTelemetryCollector {
    methods: HashMap<String, Arc<Mutex<OperationDurationsAggregator>>>,
    per_collection_data: LruCache<CollectionEndpointKey, Arc<Mutex<OperationDurationsAggregator>>>,
}

impl TonicWorkerTelemetryCollector {
    /// Creates a new collector with LRU cache capacity set to prevent memory blow-up.
    pub fn new() -> Self {
        Self {
            methods: HashMap::new(),
            per_collection_data: LruCache::new(
                NonZeroUsize::new(PER_COLLECTION_TELEMETRY_LRU_CAPACITY).unwrap(),
            ),
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
    /// Records a gRPC response without collection context.
    pub fn add_response(&mut self, method: String, instant: std::time::Instant) {
        self.add_response_with_collection(method, instant, None);
    }

    /// Records a gRPC response with optional collection context.
    ///
    /// Updates both global metrics and per-collection metrics (if collection is provided).
    pub fn add_response_with_collection(
        &mut self,
        method: String,
        instant: std::time::Instant,
        collection: Option<&str>,
    ) {
        // Always update global metrics
        let aggregator = self
            .methods
            .entry(method.clone())
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);

        // If collection is provided, update per-collection metrics
        if let Some(collection_name) = collection {
            let key = CollectionEndpointKey {
                collection: collection_name.to_string(),
                endpoint: method,
            };
            let aggregator = self
                .per_collection_data
                .get_or_insert_mut(key, OperationDurationsAggregator::new);
            ScopeDurationMeasurer::new_with_instant(aggregator, instant);
        }
    }

    /// Returns aggregated gRPC telemetry data.
    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> GrpcTelemetry {
        let mut responses = HashMap::new();
        for (method, aggregator) in self.methods.iter() {
            responses.insert(method.clone(), aggregator.lock().get_statistics(detail));
        }

        // Build nested structure: collection -> endpoint -> stats
        let mut responses_per_collection: HashMap<
            String,
            HashMap<String, OperationDurationStatistics>,
        > = HashMap::new();
        for (key, aggregator) in self.per_collection_data.iter() {
            responses_per_collection
                .entry(key.collection.clone())
                .or_default()
                .insert(
                    key.endpoint.clone(),
                    aggregator.lock().get_statistics(detail),
                );
        }

        GrpcTelemetry {
            responses,
            responses_per_collection,
        }
    }
}

impl ActixWorkerTelemetryCollector {
    /// Records a REST API response with optional collection context.
    ///
    /// Updates both global metrics and per-collection metrics (if collection is provided).
    pub fn add_response_with_collection(
        &mut self,
        method: &str,
        instant: std::time::Instant,
        status: HttpStatusCode,
        collection: Option<&str>,
    ) {
        // Always update global metrics
        let aggregator = self
            .methods
            .entry(method.to_string())
            .or_default()
            .entry(status)
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);

        // If collection is provided, update per-collection metrics
        if let Some(collection_name) = collection {
            let key = CollectionEndpointKey {
                collection: collection_name.to_string(),
                endpoint: method.to_string(),
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

    /// Returns aggregated REST API telemetry data.
    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> WebApiTelemetry {
        let mut responses = HashMap::new();
        for (method, status_codes) in &self.methods {
            let mut status_codes_map = HashMap::new();
            for (status_code, aggregator) in status_codes {
                status_codes_map.insert(*status_code, aggregator.lock().get_statistics(detail));
            }
            responses.insert(method.clone(), status_codes_map);
        }

        // Build nested structure: collection -> endpoint -> status -> stats
        let mut responses_per_collection: HashMap<
            String,
            HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>,
        > = HashMap::new();
        for (key, status_codes) in self.per_collection_data.iter() {
            let mut status_codes_map = HashMap::new();
            for (status_code, aggregator) in status_codes {
                status_codes_map.insert(*status_code, aggregator.lock().get_statistics(detail));
            }
            responses_per_collection
                .entry(key.collection.clone())
                .or_default()
                .insert(key.endpoint.clone(), status_codes_map);
        }

        WebApiTelemetry {
            responses,
            responses_per_collection,
        }
    }
}

impl GrpcTelemetry {
    /// Merges telemetry data from another collector into this one.
    pub fn merge(&mut self, other: &GrpcTelemetry) {
        for (method, other_statistics) in &other.responses {
            let entry = self.responses.entry(method.clone()).or_default();
            *entry = entry.clone() + other_statistics.clone();
        }
        for (collection, endpoints) in &other.responses_per_collection {
            let collection_entry = self
                .responses_per_collection
                .entry(collection.clone())
                .or_default();
            for (endpoint, other_statistics) in endpoints {
                let entry = collection_entry.entry(endpoint.clone()).or_default();
                *entry = entry.clone() + other_statistics.clone();
            }
        }
    }
}

impl WebApiTelemetry {
    /// Merges telemetry data from another collector into this one.
    pub fn merge(&mut self, other: &WebApiTelemetry) {
        for (method, status_codes) in &other.responses {
            let status_codes_map = self.responses.entry(method.clone()).or_default();
            for (status_code, statistics) in status_codes {
                let entry = status_codes_map.entry(*status_code).or_default();
                *entry = entry.clone() + statistics.clone();
            }
        }
        for (collection, endpoints) in &other.responses_per_collection {
            let collection_entry = self
                .responses_per_collection
                .entry(collection.clone())
                .or_default();
            for (endpoint, status_codes) in endpoints {
                let endpoint_entry = collection_entry.entry(endpoint.clone()).or_default();
                for (status_code, statistics) in status_codes {
                    let entry = endpoint_entry.entry(*status_code).or_default();
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
        access: &Access,
        actix_collector: &ActixTelemetryCollector,
        tonic_collector: &TonicTelemetryCollector,
        detail: TelemetryDetail,
    ) -> Option<Self> {
        let global_access = AccessRequirements::new();
        if access.check_global_access(global_access).is_ok() {
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
                let anonymized_collection = collection.clone().anonymize();
                let anonymized_endpoints = endpoints
                    .iter()
                    .map(|(endpoint, status_map)| {
                        (endpoint.clone(), anonymize_collection_values(status_map))
                    })
                    .collect();
                (anonymized_collection, anonymized_endpoints)
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
        let responses = anonymize_collection_values(&self.responses);

        let responses_per_collection = self
            .responses_per_collection
            .iter()
            .map(|(collection, endpoints)| {
                let anonymized_collection = collection.clone().anonymize();
                let anonymized_endpoints = endpoints
                    .iter()
                    .map(|(endpoint, stats)| (endpoint.clone(), stats.anonymize()))
                    .collect();
                (anonymized_collection, anonymized_endpoints)
            })
            .collect();

        GrpcTelemetry {
            responses,
            responses_per_collection,
        }
    }
}

impl Default for ActixWorkerTelemetryCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for TonicWorkerTelemetryCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_actix_lru_eviction() {
        let mut collector = ActixWorkerTelemetryCollector::new();
        let instant = std::time::Instant::now();

        // Insert more than LRU capacity entries
        for i in 0..PER_COLLECTION_TELEMETRY_LRU_CAPACITY + 10 {
            let collection_name = format!("collection_{i}");
            collector.add_response_with_collection(
                "GET /collections/{name}",
                instant,
                200,
                Some(&collection_name),
            );
        }

        // Verify LRU cache size is bounded
        assert_eq!(
            collector.per_collection_data.len(),
            PER_COLLECTION_TELEMETRY_LRU_CAPACITY
        );

        // Oldest entries should be evicted
        let oldest_key = CollectionEndpointKey {
            collection: "collection_0".to_string(),
            endpoint: "GET /collections/{name}".to_string(),
        };
        assert!(!collector.per_collection_data.contains(&oldest_key));

        // Recent entries should still exist
        let recent_key = CollectionEndpointKey {
            collection: format!("collection_{}", PER_COLLECTION_TELEMETRY_LRU_CAPACITY + 9),
            endpoint: "GET /collections/{name}".to_string(),
        };
        assert!(collector.per_collection_data.contains(&recent_key));
    }

    #[test]
    fn test_tonic_lru_eviction() {
        let mut collector = TonicWorkerTelemetryCollector::new();
        let instant = std::time::Instant::now();

        // Insert more than LRU capacity entries
        for i in 0..PER_COLLECTION_TELEMETRY_LRU_CAPACITY + 10 {
            let collection_name = format!("collection_{i}");
            collector.add_response_with_collection(
                "Qdrant/Search".to_string(),
                instant,
                Some(&collection_name),
            );
        }

        // Verify LRU cache size is bounded
        assert_eq!(
            collector.per_collection_data.len(),
            PER_COLLECTION_TELEMETRY_LRU_CAPACITY
        );

        // Oldest entries should be evicted
        let oldest_key = CollectionEndpointKey {
            collection: "collection_0".to_string(),
            endpoint: "Qdrant/Search".to_string(),
        };
        assert!(!collector.per_collection_data.contains(&oldest_key));

        // Recent entries should still exist
        let recent_key = CollectionEndpointKey {
            collection: format!("collection_{}", PER_COLLECTION_TELEMETRY_LRU_CAPACITY + 9),
            endpoint: "Qdrant/Search".to_string(),
        };
        assert!(collector.per_collection_data.contains(&recent_key));
    }

    #[test]
    fn test_backward_compatibility() {
        let mut collector = ActixWorkerTelemetryCollector::new();
        let instant = std::time::Instant::now();

        collector.add_response_with_collection("GET /health", instant, 200, None);

        let telemetry = collector.get_telemetry_data(TelemetryDetail::default());
        assert!(telemetry.responses.contains_key("GET /health"));
    }

    #[test]
    fn test_per_collection_telemetry() {
        let mut collector = ActixWorkerTelemetryCollector::new();
        let instant = std::time::Instant::now();

        collector.add_response_with_collection(
            "GET /collections/{name}",
            instant,
            200,
            Some("c1"),
        );

        let telemetry = collector.get_telemetry_data(TelemetryDetail::default());

        // Check nested structure: collection -> endpoint -> status -> stats
        assert!(telemetry.responses_per_collection.contains_key("c1"));
        let c1_endpoints = telemetry.responses_per_collection.get("c1").unwrap();
        assert!(c1_endpoints.contains_key("GET /collections/{name}"));

        // Global responses should also be present
        assert!(telemetry.responses.contains_key("GET /collections/{name}"));
    }

    #[test]
    fn test_nested_structure_multiple_collections() {
        let mut collector = ActixWorkerTelemetryCollector::new();
        let instant = std::time::Instant::now();

        // Add requests for multiple collections and endpoints
        collector.add_response_with_collection(
            "GET /collections/{name}/points",
            instant,
            200,
            Some("collection_a"),
        );
        collector.add_response_with_collection(
            "POST /collections/{name}/points/search",
            instant,
            200,
            Some("collection_a"),
        );
        collector.add_response_with_collection(
            "GET /collections/{name}/points",
            instant,
            200,
            Some("collection_b"),
        );

        let telemetry = collector.get_telemetry_data(TelemetryDetail::default());

        // Verify structure
        assert_eq!(telemetry.responses_per_collection.len(), 2);

        let collection_a = telemetry
            .responses_per_collection
            .get("collection_a")
            .unwrap();
        assert_eq!(collection_a.len(), 2);
        assert!(collection_a.contains_key("GET /collections/{name}/points"));
        assert!(collection_a.contains_key("POST /collections/{name}/points/search"));

        let collection_b = telemetry
            .responses_per_collection
            .get("collection_b")
            .unwrap();
        assert_eq!(collection_b.len(), 1);
        assert!(collection_b.contains_key("GET /collections/{name}/points"));
    }
}
