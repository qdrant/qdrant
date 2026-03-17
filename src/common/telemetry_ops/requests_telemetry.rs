use std::collections::HashMap;
use std::sync::Arc;

use common::types::TelemetryDetail;
use parking_lot::Mutex;
use schemars::JsonSchema;
use segment::common::anonymize::{Anonymize, anonymize_collection_values};
use segment::common::operation_time_statistics::{
    OperationDurationStatistics, OperationDurationsAggregator, ScopeDurationMeasurer,
};
use serde::Serialize;
use storage::rbac::{AccessRequirements, Auth};

/// Wrapper for passing collection name through gRPC response extensions.
#[derive(Clone, Debug)]
pub struct CollectionName(pub String);

pub type HttpStatusCode = u16;

pub type GrpcStatusCode = i32;

/// Per-collection key: collection_name -> (method -> status_code -> stats)
pub type PerCollectionResponses<StatusCode> =
    HashMap<String, HashMap<String, HashMap<StatusCode, OperationDurationStatistics>>>;

type PerCollectionAggregators<StatusCode> =
    HashMap<String, HashMap<String, HashMap<StatusCode, Arc<Mutex<OperationDurationsAggregator>>>>>;

#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct WebApiTelemetry {
    pub responses: HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub per_collection_responses: PerCollectionResponses<HttpStatusCode>,
}

#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct GrpcTelemetry {
    pub responses: HashMap<String, HashMap<GrpcStatusCode, OperationDurationStatistics>>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub per_collection_responses: PerCollectionResponses<GrpcStatusCode>,
}

pub struct ActixTelemetryCollector {
    pub workers: Vec<Arc<Mutex<ActixWorkerTelemetryCollector>>>,
}

#[derive(Default)]
pub struct ActixWorkerTelemetryCollector {
    methods: HashMap<String, HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
    /// collection_name -> method -> status_code -> aggregator
    per_collection_methods: PerCollectionAggregators<HttpStatusCode>,
}

pub struct TonicTelemetryCollector {
    pub workers: Vec<Arc<Mutex<TonicWorkerTelemetryCollector>>>,
}

#[derive(Default)]
pub struct TonicWorkerTelemetryCollector {
    methods: HashMap<String, HashMap<GrpcStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
    /// collection_name -> method -> status_code -> aggregator
    per_collection_methods: PerCollectionAggregators<GrpcStatusCode>,
}

impl ActixTelemetryCollector {
    pub fn create_web_worker_telemetry(&mut self) -> Arc<Mutex<ActixWorkerTelemetryCollector>> {
        let worker = Arc::new(Mutex::new(ActixWorkerTelemetryCollector::default()));
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
        let worker = Arc::new(Mutex::new(TonicWorkerTelemetryCollector::default()));
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
        instant: std::time::Instant,
        status_code: GrpcStatusCode,
        collection_name: Option<String>,
    ) {
        let aggregator = self
            .methods
            .entry(method.clone())
            .or_default()
            .entry(status_code)
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);

        if let Some(collection) = collection_name {
            let aggregator = self
                .per_collection_methods
                .entry(collection)
                .or_default()
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

        let per_collection_responses = if detail.per_collection {
            let mut per_collection_responses = HashMap::new();
            for (collection, methods) in &self.per_collection_methods {
                let mut methods_map = HashMap::new();
                for (method, status_codes) in methods {
                    let mut status_codes_map = HashMap::new();
                    for (status_code, aggregator) in status_codes {
                        status_codes_map
                            .insert(*status_code, aggregator.lock().get_statistics(detail));
                    }
                    methods_map.insert(method.clone(), status_codes_map);
                }
                per_collection_responses.insert(collection.clone(), methods_map);
            }
            per_collection_responses
        } else {
            HashMap::new()
        };

        GrpcTelemetry {
            responses,
            per_collection_responses,
        }
    }
}

impl ActixWorkerTelemetryCollector {
    pub fn add_response(
        &mut self,
        method: String,
        status_code: HttpStatusCode,
        instant: std::time::Instant,
        collection_name: Option<String>,
    ) {
        let aggregator = self
            .methods
            .entry(method.clone())
            .or_default()
            .entry(status_code)
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);

        if let Some(collection) = collection_name {
            let aggregator = self
                .per_collection_methods
                .entry(collection)
                .or_default()
                .entry(method)
                .or_default()
                .entry(status_code)
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

        let per_collection_responses = if detail.per_collection {
            let mut per_collection_responses = HashMap::new();
            for (collection, methods) in &self.per_collection_methods {
                let mut methods_map = HashMap::new();
                for (method, status_codes) in methods {
                    let mut status_codes_map = HashMap::new();
                    for (status_code, aggregator) in status_codes {
                        status_codes_map
                            .insert(*status_code, aggregator.lock().get_statistics(detail));
                    }
                    methods_map.insert(method.clone(), status_codes_map);
                }
                per_collection_responses.insert(collection.clone(), methods_map);
            }
            per_collection_responses
        } else {
            HashMap::new()
        };

        WebApiTelemetry {
            responses,
            per_collection_responses,
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
        for (collection, methods) in &other.per_collection_responses {
            let col_entry = self
                .per_collection_responses
                .entry(collection.clone())
                .or_default();
            for (method, status_codes) in methods {
                let method_entry = col_entry.entry(method.clone()).or_default();
                for (status_code, statistics) in status_codes {
                    let entry = method_entry.entry(*status_code).or_default();
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
        for (collection, methods) in &other.per_collection_responses {
            let col_entry = self
                .per_collection_responses
                .entry(collection.clone())
                .or_default();
            for (method, status_codes) in methods {
                let method_entry = col_entry.entry(method.clone()).or_default();
                for (status_code, statistics) in status_codes {
                    let entry = method_entry.entry(*status_code).or_default();
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

        // Anonymize per-collection: keep structure but anonymize the stats, not collection names
        // (collection names are user data, so strip them on anonymize)
        WebApiTelemetry {
            responses,
            per_collection_responses: HashMap::new(),
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

        GrpcTelemetry {
            responses,
            per_collection_responses: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use common::types::{DetailsLevel, TelemetryDetail};
    use segment::common::anonymize::Anonymize;

    use super::*;

    fn detail_with_per_collection() -> TelemetryDetail {
        TelemetryDetail {
            level: DetailsLevel::Level0,
            histograms: false,
            per_collection: true,
        }
    }

    // Helper: actix add_response signature is (method, status_code, instant, collection)
    fn actix_add(
        worker: &mut ActixWorkerTelemetryCollector,
        method: &str,
        status: u16,
        collection: Option<&str>,
    ) {
        worker.add_response(
            method.into(),
            status,
            std::time::Instant::now(),
            collection.map(String::from),
        );
    }

    #[test]
    fn test_actix_add_response_without_collection() {
        let mut worker = ActixWorkerTelemetryCollector::default();
        actix_add(&mut worker, "GET /collections/{name}/points", 200, None);

        let telemetry = worker.get_telemetry_data(detail_with_per_collection());
        assert_eq!(telemetry.responses.len(), 1);
        assert!(telemetry.per_collection_responses.is_empty());
    }

    #[test]
    fn test_actix_add_response_with_collection() {
        let mut worker = ActixWorkerTelemetryCollector::default();
        actix_add(
            &mut worker,
            "POST /collections/{name}/points/search",
            200,
            Some("my_collection"),
        );

        let telemetry = worker.get_telemetry_data(detail_with_per_collection());

        // Global should still be populated
        assert_eq!(telemetry.responses.len(), 1);
        assert!(
            telemetry
                .responses
                .contains_key("POST /collections/{name}/points/search")
        );

        // Per-collection should also be populated
        assert_eq!(telemetry.per_collection_responses.len(), 1);
        let collection_methods = &telemetry.per_collection_responses["my_collection"];
        assert!(collection_methods.contains_key("POST /collections/{name}/points/search"));
    }

    #[test]
    fn test_actix_per_collection_hidden_by_default() {
        let mut worker = ActixWorkerTelemetryCollector::default();
        actix_add(
            &mut worker,
            "POST /collections/{name}/points/search",
            200,
            Some("my_collection"),
        );

        // Default detail has per_collection=false, so per_collection_responses should be empty
        let telemetry = worker.get_telemetry_data(TelemetryDetail::default());
        assert_eq!(telemetry.responses.len(), 1);
        assert!(telemetry.per_collection_responses.is_empty());
    }

    #[test]
    fn test_actix_multiple_collections() {
        let mut worker = ActixWorkerTelemetryCollector::default();
        let method = "POST /collections/{name}/points/search";

        actix_add(&mut worker, method, 200, Some("col_a"));
        actix_add(&mut worker, method, 200, Some("col_b"));
        actix_add(&mut worker, method, 200, Some("col_a"));

        let telemetry = worker.get_telemetry_data(detail_with_per_collection());

        // Global: 3 total requests
        let global_stats = &telemetry.responses[method][&200];
        assert_eq!(global_stats.count, 3);

        // Per-collection: col_a=2, col_b=1
        assert_eq!(telemetry.per_collection_responses.len(), 2);
        assert_eq!(
            telemetry.per_collection_responses["col_a"][method][&200].count,
            2
        );
        assert_eq!(
            telemetry.per_collection_responses["col_b"][method][&200].count,
            1
        );
    }

    #[test]
    fn test_tonic_add_response_with_collection() {
        let mut worker = TonicWorkerTelemetryCollector::default();
        worker.add_response(
            "/qdrant.Points/Search".into(),
            std::time::Instant::now(),
            0,
            Some("my_collection".into()),
        );

        let telemetry = worker.get_telemetry_data(detail_with_per_collection());

        assert_eq!(telemetry.responses.len(), 1);
        assert_eq!(telemetry.per_collection_responses.len(), 1);
        assert!(
            telemetry
                .per_collection_responses
                .contains_key("my_collection")
        );
    }

    #[test]
    fn test_merge_per_collection_responses() {
        let mut worker1 = ActixWorkerTelemetryCollector::default();
        let mut worker2 = ActixWorkerTelemetryCollector::default();
        let method = "POST /collections/{name}/points/search";

        actix_add(&mut worker1, method, 200, Some("col_a"));
        actix_add(&mut worker2, method, 200, Some("col_a"));
        actix_add(&mut worker2, method, 200, Some("col_b"));

        let detail = detail_with_per_collection();
        let t1 = worker1.get_telemetry_data(detail);
        let t2 = worker2.get_telemetry_data(detail);

        let mut merged = WebApiTelemetry::default();
        merged.merge(&t1);
        merged.merge(&t2);

        // Global: 3 total
        assert_eq!(merged.responses[method][&200].count, 3);

        // Per-collection: col_a=2 (merged from two workers), col_b=1
        assert_eq!(
            merged.per_collection_responses["col_a"][method][&200].count,
            2
        );
        assert_eq!(
            merged.per_collection_responses["col_b"][method][&200].count,
            1
        );
    }

    #[test]
    fn test_collector_merges_workers() {
        let mut collector = ActixTelemetryCollector { workers: vec![] };
        let w1 = collector.create_web_worker_telemetry();
        let w2 = collector.create_web_worker_telemetry();
        let method = "POST /collections/{name}/points/search";

        w1.lock().add_response(
            method.into(),
            200,
            std::time::Instant::now(),
            Some("col_a".into()),
        );
        w2.lock().add_response(
            method.into(),
            200,
            std::time::Instant::now(),
            Some("col_a".into()),
        );

        let telemetry = collector.get_telemetry_data(detail_with_per_collection());
        assert_eq!(telemetry.responses[method][&200].count, 2);
        assert_eq!(
            telemetry.per_collection_responses["col_a"][method][&200].count,
            2
        );
    }

    #[test]
    fn test_anonymize_strips_per_collection() {
        let mut worker = ActixWorkerTelemetryCollector::default();
        actix_add(
            &mut worker,
            "POST /collections/{name}/points/search",
            200,
            Some("secret_collection"),
        );

        let telemetry = worker.get_telemetry_data(detail_with_per_collection());
        assert!(!telemetry.per_collection_responses.is_empty());

        let anonymized = telemetry.anonymize();
        assert!(anonymized.per_collection_responses.is_empty());
        // Global responses should still be present
        assert!(!anonymized.responses.is_empty());
    }

    #[test]
    fn test_all_collections_tracked() {
        let mut worker = ActixWorkerTelemetryCollector::default();
        let method = "POST /collections/{name}/points/search";

        for i in 0..500 {
            actix_add(&mut worker, method, 200, Some(&format!("col_{i}")));
        }

        let telemetry = worker.get_telemetry_data(detail_with_per_collection());
        assert_eq!(telemetry.per_collection_responses.len(), 500);
    }
}
