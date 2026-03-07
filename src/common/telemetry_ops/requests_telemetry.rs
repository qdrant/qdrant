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

pub type HttpStatusCode = u16;

pub type GrpcStatusCode = i32;

/// Per-collection response statistics for REST API
#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct WebApiTelemetry {
    /// Global responses aggregated across all collections
    pub responses: HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>,
    /// Per-collection responses (only for whitelisted endpoints)
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub responses_per_collection: HashMap<String, HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>>,
}

/// Per-collection response statistics for gRPC
#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct GrpcTelemetry {
    /// Global responses aggregated across all collections
    pub responses: HashMap<String, HashMap<GrpcStatusCode, OperationDurationStatistics>>,
    /// Per-collection responses (only for whitelisted endpoints)
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub responses_per_collection: HashMap<String, HashMap<String, HashMap<GrpcStatusCode, OperationDurationStatistics>>>,
}

pub struct ActixTelemetryCollector {
    pub workers: Vec<Arc<Mutex<ActixWorkerTelemetryCollector>>>,
}

pub struct ActixWorkerTelemetryCollector {
    /// Global method statistics
    methods: HashMap<String, HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
    /// Per-collection method statistics
    methods_per_collection: HashMap<String, HashMap<String, HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>>,
    /// Configuration for per-collection metrics
    enable_per_collection: bool,
    max_collections: usize,
}

impl Default for ActixWorkerTelemetryCollector {
    fn default() -> Self {
        Self {
            methods: Default::default(),
            methods_per_collection: Default::default(),
            enable_per_collection: true, // Default to enabled for backwards compatibility
            max_collections: 1000, // Default limit
        }
    }
}

pub struct TonicTelemetryCollector {
    pub workers: Vec<Arc<Mutex<TonicWorkerTelemetryCollector>>>,
}

pub struct TonicWorkerTelemetryCollector {
    /// Global method statistics
    methods: HashMap<String, HashMap<GrpcStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
    /// Per-collection method statistics
    methods_per_collection: HashMap<String, HashMap<String, HashMap<GrpcStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>>,
    /// Configuration for per-collection metrics
    enable_per_collection: bool,
    max_collections: usize,
}

impl Default for TonicWorkerTelemetryCollector {
    fn default() -> Self {
        Self {
            methods: Default::default(),
            methods_per_collection: Default::default(),
            enable_per_collection: true, // Default to enabled for backwards compatibility
            max_collections: 1000, // Default limit
        }
    }
}

impl ActixTelemetryCollector {
    pub fn create_web_worker_telemetry(&mut self) -> Arc<Mutex<ActixWorkerTelemetryCollector>> {
        let worker: Arc<Mutex<_>> = Default::default();
        self.workers.push(worker.clone());
        worker
    }

    /// Creates a new worker telemetry collector for web API requests with configuration
    ///
    /// Returns an Arc<Mutex> wrapper around the worker collector that can be
    /// shared across threads and used to track request statistics.
    pub fn create_web_worker_telemetry_with_config(
        &mut self,
        enable_per_collection: bool,
        max_collections: usize,
    ) -> Arc<Mutex<ActixWorkerTelemetryCollector>> {
        let worker = ActixWorkerTelemetryCollector {
            methods: Default::default(),
            methods_per_collection: Default::default(),
            enable_per_collection,
            max_collections,
        };
        let worker = Arc::new(Mutex::new(worker));
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

    /// Creates a new worker telemetry collector for gRPC requests with configuration
    ///
    /// Returns an Arc<Mutex> wrapper around the worker collector that can be
    /// shared across threads and used to track gRPC request statistics.
    pub fn create_grpc_telemetry_collector_with_config(
        &mut self,
        enable_per_collection: bool,
        max_collections: usize,
    ) -> Arc<Mutex<TonicWorkerTelemetryCollector>> {
        let worker = TonicWorkerTelemetryCollector {
            methods: Default::default(),
            methods_per_collection: Default::default(),
            enable_per_collection,
            max_collections,
        };
        let worker = Arc::new(Mutex::new(worker));
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
        collection: Option<String>,
    ) {
        // Add to global statistics
        let aggregator = self
            .methods
            .entry(method.clone())
            .or_default()
            .entry(status_code)
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);

        // Add to per-collection statistics if collection is provided and per-collection tracking is enabled
        if let Some(collection) = collection
            && self.enable_per_collection
            && self.methods_per_collection.len() < self.max_collections
        {
            let collection_aggregator = self
                .methods_per_collection
                .entry(method)
                .or_default()
                .entry(collection)
                .or_default()
                .entry(status_code)
                .or_insert_with(OperationDurationsAggregator::new);
            ScopeDurationMeasurer::new_with_instant(collection_aggregator, instant);
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
        for (method, collections) in &self.methods_per_collection {
            let mut collections_map = HashMap::new();
            for (collection, status_codes) in collections {
                let mut status_codes_map = HashMap::new();
                for (status_code, aggregator) in status_codes {
                    status_codes_map.insert(*status_code, aggregator.lock().get_statistics(detail));
                }
                collections_map.insert(collection.clone(), status_codes_map);
            }
            responses_per_collection.insert(method.clone(), collections_map);
        }

        GrpcTelemetry { responses, responses_per_collection }
    }
}

impl ActixWorkerTelemetryCollector {
    pub fn add_response(
        &mut self,
        method: String,
        status_code: HttpStatusCode,
        instant: std::time::Instant,
        collection: Option<String>,
    ) {
        // Add to global statistics
        let aggregator = self
            .methods
            .entry(method.clone())
            .or_default()
            .entry(status_code)
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);

        // Add to per-collection statistics if collection is provided and per-collection tracking is enabled
        if let Some(collection) = collection
            && self.enable_per_collection
            && self.methods_per_collection.len() < self.max_collections
        {
            let collection_aggregator = self
                .methods_per_collection
                .entry(method)
                .or_default()
                .entry(collection)
                .or_default()
                .entry(status_code)
                .or_insert_with(OperationDurationsAggregator::new);
            ScopeDurationMeasurer::new_with_instant(collection_aggregator, instant);
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

        let mut responses_per_collection = HashMap::new();
        for (method, collections) in &self.methods_per_collection {
            let mut collections_map = HashMap::new();
            for (collection, status_codes) in collections {
                let mut status_codes_map = HashMap::new();
                for (status_code, aggregator) in status_codes {
                    status_codes_map.insert(*status_code, aggregator.lock().get_statistics(detail));
                }
                collections_map.insert(collection.clone(), status_codes_map);
            }
            responses_per_collection.insert(method.clone(), collections_map);
        }

        WebApiTelemetry { responses, responses_per_collection }
    }
}

impl GrpcTelemetry {
    pub fn merge(&mut self, other: &GrpcTelemetry) {
        // Merge global responses
        for (method, success_map) in &other.responses {
            let entry = self.responses.entry(method.clone()).or_default();
            for (status_code, statistics) in success_map {
                let status_entry = entry.entry(*status_code).or_default();
                *status_entry = status_entry.clone() + statistics.clone();
            }
        }

        // Merge per-collection responses
        for (method, collections) in &other.responses_per_collection {
            let entry = self.responses_per_collection.entry(method.clone()).or_default();
            for (collection, status_codes) in collections {
                let collection_entry = entry.entry(collection.clone()).or_default();
                for (status_code, statistics) in status_codes {
                    let status_entry = collection_entry.entry(*status_code).or_default();
                    *status_entry = status_entry.clone() + statistics.clone();
                }
            }
        }
    }
}

impl WebApiTelemetry {
    pub fn merge(&mut self, other: &WebApiTelemetry) {
        // Merge global responses
        for (method, status_codes) in &other.responses {
            let status_codes_map = self.responses.entry(method.clone()).or_default();
            for (status_code, statistics) in status_codes {
                let entry = status_codes_map.entry(*status_code).or_default();
                *entry = entry.clone() + statistics.clone();
            }
        }

        // Merge per-collection responses
        for (method, collections) in &other.responses_per_collection {
            let collections_map = self.responses_per_collection.entry(method.clone()).or_default();
            for (collection, status_codes) in collections {
                let status_codes_map = collections_map.entry(collection.clone()).or_default();
                for (status_code, statistics) in status_codes {
                    let entry = status_codes_map.entry(*status_code).or_default();
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
        // Global responses - keep as is
        let responses = self
            .responses
            .iter()
            .map(|(key, value)| (key.clone(), anonymize_collection_values(value)))
            .collect();

        // Per-collection responses - aggregate across all collections for anonymization
        let mut aggregated_per_collection: HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>> = HashMap::new();
        for (method, collections) in &self.responses_per_collection {
            let method_entry = aggregated_per_collection.entry(method.clone()).or_default();
            for (_collection, status_codes) in collections {
                for (status_code, statistics) in status_codes {
                    let entry = method_entry.entry(*status_code).or_default();
                    *entry = entry.clone() + statistics.clone();
                }
            }
        }

        // For anonymized telemetry, don't expose per-collection breakdown
        WebApiTelemetry { responses, responses_per_collection: HashMap::new() }
    }
}

impl Anonymize for GrpcTelemetry {
    fn anonymize(&self) -> Self {
        // Global responses - keep as is
        let responses = self
            .responses
            .iter()
            .map(|(key, value)| (key.clone(), anonymize_collection_values(value)))
            .collect();

        // Per-collection responses - aggregate across all collections for anonymization
        let mut aggregated_per_collection: HashMap<String, HashMap<GrpcStatusCode, OperationDurationStatistics>> = HashMap::new();
        for (method, collections) in &self.responses_per_collection {
            let method_entry = aggregated_per_collection.entry(method.clone()).or_default();
            for (_collection, status_codes) in collections {
                for (status_code, statistics) in status_codes {
                    let entry = method_entry.entry(*status_code).or_default();
                    *entry = entry.clone() + statistics.clone();
                }
            }
        }

        // For anonymized telemetry, don't expose per-collection breakdown
        GrpcTelemetry { responses, responses_per_collection: HashMap::new() }
    }
}
