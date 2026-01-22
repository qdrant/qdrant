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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CollectionEndpointKey {
    pub collection: String,
    pub endpoint: String,
}

#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct WebApiTelemetry {
    pub responses: HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>,
}

#[derive(Serialize, Clone, Default, Debug, JsonSchema, Anonymize)]
pub struct GrpcTelemetry {
    #[anonymize(with = anonymize_collection_values)]
    pub responses: HashMap<String, OperationDurationStatistics>,
}

pub struct ActixTelemetryCollector {
    pub workers: Vec<Arc<Mutex<ActixWorkerTelemetryCollector>>>,
}

pub struct ActixWorkerTelemetryCollector {
    methods: HashMap<String, HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
    per_collection_data: LruCache<
        CollectionEndpointKey,
        HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>,
    >,
}

impl ActixWorkerTelemetryCollector {
    const DEFAULT_MAX_COLLECTIONS: usize = 1000;

    pub fn new() -> Self {
        Self {
            methods: HashMap::new(),
            per_collection_data: LruCache::new(
                NonZeroUsize::new(Self::DEFAULT_MAX_COLLECTIONS).unwrap(),
            ),
        }
    }
}

pub struct TonicTelemetryCollector {
    pub workers: Vec<Arc<Mutex<TonicWorkerTelemetryCollector>>>,
}

pub struct TonicWorkerTelemetryCollector {
    methods: HashMap<String, Arc<Mutex<OperationDurationsAggregator>>>,
    per_collection_data: LruCache<CollectionEndpointKey, Arc<Mutex<OperationDurationsAggregator>>>,
}

impl TonicWorkerTelemetryCollector {
    const DEFAULT_MAX_COLLECTIONS: usize = 1000;

    pub fn new() -> Self {
        Self {
            methods: HashMap::new(),
            per_collection_data: LruCache::new(
                NonZeroUsize::new(Self::DEFAULT_MAX_COLLECTIONS).unwrap(),
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
    pub fn add_response(&mut self, method: String, instant: std::time::Instant) {
        self.add_response_with_collection(method, instant, None);
    }

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
                .get_or_insert_mut(key, || OperationDurationsAggregator::new());
            ScopeDurationMeasurer::new_with_instant(aggregator, instant);
        }
    }

    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> GrpcTelemetry {
        let mut responses = HashMap::new();
        for (method, aggregator) in self.methods.iter() {
            responses.insert(method.clone(), aggregator.lock().get_statistics(detail));
        }
        GrpcTelemetry { responses }
    }
}

impl ActixWorkerTelemetryCollector {
    pub fn add_response(
        &mut self,
        method: String,
        status_code: HttpStatusCode,
        instant: std::time::Instant,
    ) {
        self.add_response_with_collection(&method, instant, status_code, None);
    }

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

    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> WebApiTelemetry {
        let mut responses = HashMap::new();
        for (method, status_codes) in &self.methods {
            let mut status_codes_map = HashMap::new();
            for (status_code, aggregator) in status_codes {
                status_codes_map.insert(*status_code, aggregator.lock().get_statistics(detail));
            }
            responses.insert(method.clone(), status_codes_map);
        }
        WebApiTelemetry { responses }
    }
}

impl GrpcTelemetry {
    pub fn merge(&mut self, other: &GrpcTelemetry) {
        for (method, other_statistics) in &other.responses {
            let entry = self.responses.entry(method.clone()).or_default();
            *entry = entry.clone() + other_statistics.clone();
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

        WebApiTelemetry { responses }
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

        // Insert more than DEFAULT_MAX_COLLECTIONS entries
        let max_collections = ActixWorkerTelemetryCollector::DEFAULT_MAX_COLLECTIONS;
        for i in 0..max_collections + 10 {
            let collection_name = format!("collection_{}", i);
            collector.add_response_with_collection(
                "GET /collections/{name}",
                instant,
                200,
                Some(&collection_name),
            );
        }

        // Verify LRU cache size is bounded
        assert_eq!(collector.per_collection_data.len(), max_collections);

        // Oldest entries should be evicted
        let oldest_key = CollectionEndpointKey {
            collection: "collection_0".to_string(),
            endpoint: "GET /collections/{name}".to_string(),
        };
        assert!(!collector.per_collection_data.contains(&oldest_key));

        // Recent entries should still exist
        let recent_key = CollectionEndpointKey {
            collection: format!("collection_{}", max_collections + 9),
            endpoint: "GET /collections/{name}".to_string(),
        };
        assert!(collector.per_collection_data.contains(&recent_key));
    }

    #[test]
    fn test_tonic_lru_eviction() {
        let mut collector = TonicWorkerTelemetryCollector::new();
        let instant = std::time::Instant::now();

        // Insert more than DEFAULT_MAX_COLLECTIONS entries
        let max_collections = TonicWorkerTelemetryCollector::DEFAULT_MAX_COLLECTIONS;
        for i in 0..max_collections + 10 {
            let collection_name = format!("collection_{}", i);
            collector.add_response_with_collection(
                "Qdrant/Search".to_string(),
                instant,
                Some(&collection_name),
            );
        }

        // Verify LRU cache size is bounded
        assert_eq!(collector.per_collection_data.len(), max_collections);

        // Oldest entries should be evicted
        let oldest_key = CollectionEndpointKey {
            collection: "collection_0".to_string(),
            endpoint: "Qdrant/Search".to_string(),
        };
        assert!(!collector.per_collection_data.contains(&oldest_key));

        // Recent entries should still exist
        let recent_key = CollectionEndpointKey {
            collection: format!("collection_{}", max_collections + 9),
            endpoint: "Qdrant/Search".to_string(),
        };
        assert!(collector.per_collection_data.contains(&recent_key));
    }

    #[test]
    fn test_backward_compatibility() {
        let mut collector = ActixWorkerTelemetryCollector::new();
        let instant = std::time::Instant::now();

        // Test that existing add_response method still works (delegates to new method with None)
        collector.add_response("GET /health".to_string(), 200, instant);

        // Verify global metrics are tracked
        let telemetry = collector.get_telemetry_data(TelemetryDetail::default());
        assert!(telemetry.responses.contains_key("GET /health"));
    }
}
