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

/// HTTP status code type alias used for REST telemetry tracking.
pub type HttpStatusCode = u16;

/// gRPC status code type alias used for gRPC telemetry tracking.
pub type GrpcStatusCode = i32;

/// Aggregated telemetry data for REST API responses.
///
/// Tracks per-endpoint and per-collection request duration statistics,
/// keyed by HTTP method + endpoint pattern and HTTP status code.
#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct WebApiTelemetry {
    /// Per-endpoint response statistics: `"METHOD /path"` → status code → stats.
    pub responses: HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>,
    /// Per-collection request statistics: endpoint → collection name → status code → stats.
    ///
    /// Only populated for whitelisted collection endpoints.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub collection_responses:
        HashMap<String, HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>>,
}

/// Aggregated telemetry data for gRPC responses.
///
/// Tracks per-endpoint request duration statistics keyed by gRPC method
/// path and gRPC status code.
#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct GrpcTelemetry {
    /// Per-endpoint response statistics: gRPC method path → status code → stats.
    pub responses: HashMap<String, HashMap<GrpcStatusCode, OperationDurationStatistics>>,
}

/// Collects REST API telemetry across all Actix web workers.
///
/// Each Actix worker thread gets its own [`ActixWorkerTelemetryCollector`];
/// this struct aggregates data from all of them when telemetry is requested.
pub struct ActixTelemetryCollector {
    /// Per-worker telemetry collectors, one for each Actix worker thread.
    pub workers: Vec<Arc<Mutex<ActixWorkerTelemetryCollector>>>,
}

/// Per-worker telemetry collector for Actix REST API requests.
///
/// Records request durations and status codes for each endpoint,
/// both globally and broken down by collection name.
#[derive(Default)]
pub struct ActixWorkerTelemetryCollector {
    methods: HashMap<String, HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
    /// Per-collection request stats: endpoint → collection name → status code → aggregator.
    collection_methods: HashMap<
        String,
        HashMap<String, HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
    >,
}

/// Collects gRPC telemetry across all Tonic worker threads.
///
/// Each gRPC worker gets its own [`TonicWorkerTelemetryCollector`];
/// this struct aggregates data from all of them when telemetry is requested.
pub struct TonicTelemetryCollector {
    /// Per-worker telemetry collectors, one for each Tonic worker thread.
    pub workers: Vec<Arc<Mutex<TonicWorkerTelemetryCollector>>>,
}

/// Per-worker telemetry collector for Tonic gRPC requests.
///
/// Records request durations and status codes for each gRPC endpoint.
#[derive(Default)]
pub struct TonicWorkerTelemetryCollector {
    methods: HashMap<String, HashMap<GrpcStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
}

impl ActixTelemetryCollector {
    /// Creates a new per-worker telemetry collector and registers it with this aggregator.
    pub fn create_web_worker_telemetry(&mut self) -> Arc<Mutex<ActixWorkerTelemetryCollector>> {
        let worker: Arc<Mutex<_>> = Default::default();
        self.workers.push(worker.clone());
        worker
    }

    /// Aggregates telemetry data from all workers and returns the combined result.
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
    /// Creates a new per-worker gRPC telemetry collector and registers it with this aggregator.
    pub fn create_grpc_telemetry_collector(&mut self) -> Arc<Mutex<TonicWorkerTelemetryCollector>> {
        let worker: Arc<Mutex<_>> = Default::default();
        self.workers.push(worker.clone());
        worker
    }

    /// Aggregates gRPC telemetry data from all workers and returns the combined result.
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
    /// Records a gRPC response with its method, timing, and status code.
    pub fn add_response(
        &mut self,
        method: String,
        instant: std::time::Instant,
        status_code: GrpcStatusCode,
    ) {
        let aggregator = self
            .methods
            .entry(method)
            .or_default()
            .entry(status_code)
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);
    }

    /// Returns the aggregated gRPC telemetry data for this worker.
    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> GrpcTelemetry {
        let mut responses = HashMap::new();
        for (method, status_codes) in &self.methods {
            let mut status_codes_map = HashMap::new();
            for (status_code, aggregator) in status_codes {
                status_codes_map.insert(*status_code, aggregator.lock().get_statistics(detail));
            }
            responses.insert(method.clone(), status_codes_map);
        }
        GrpcTelemetry { responses }
    }
}

impl ActixWorkerTelemetryCollector {
    /// Records a REST API response with its endpoint, status code, timing, and optional collection name.
    ///
    /// If a `collection` name is provided, the response is also tracked in
    /// per-collection metrics for finer-grained monitoring.
    pub fn add_response(
        &mut self,
        method: String,
        status_code: HttpStatusCode,
        instant: std::time::Instant,
        collection: Option<String>,
    ) {
        let aggregator = self
            .methods
            .entry(method.clone())
            .or_default()
            .entry(status_code)
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);

        // Also record per-collection if a collection name was extracted
        if let Some(collection_name) = collection {
            let aggregator = self
                .collection_methods
                .entry(method)
                .or_default()
                .entry(collection_name)
                .or_default()
                .entry(status_code)
                .or_insert_with(OperationDurationsAggregator::new);
            ScopeDurationMeasurer::new_with_instant(aggregator, instant);
        }
    }

    /// Returns the aggregated REST telemetry data for this worker, including per-collection stats.
    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> WebApiTelemetry {
        let mut responses = HashMap::new();
        for (method, status_codes) in &self.methods {
            let mut status_codes_map = HashMap::new();
            for (status_code, aggregator) in status_codes {
                status_codes_map.insert(*status_code, aggregator.lock().get_statistics(detail));
            }
            responses.insert(method.clone(), status_codes_map);
        }

        let mut collection_responses = HashMap::new();
        for (method, collections) in &self.collection_methods {
            let mut collections_map = HashMap::new();
            for (collection, status_codes) in collections {
                let mut status_codes_map = HashMap::new();
                for (status_code, aggregator) in status_codes {
                    status_codes_map.insert(*status_code, aggregator.lock().get_statistics(detail));
                }
                collections_map.insert(collection.clone(), status_codes_map);
            }
            collection_responses.insert(method.clone(), collections_map);
        }

        WebApiTelemetry {
            responses,
            collection_responses,
        }
    }
}

impl GrpcTelemetry {
    /// Merges another `GrpcTelemetry` into this one, combining statistics for matching endpoints.
    pub fn merge(&mut self, other: &GrpcTelemetry) {
        for (method, success_map) in &other.responses {
            let entry = self.responses.entry(method.clone()).or_default();
            for (is_success, statistics) in success_map {
                let status_entry = entry.entry(*is_success).or_default();
                *status_entry = status_entry.clone() + statistics.clone();
            }
        }
    }
}

impl WebApiTelemetry {
    /// Merges another `WebApiTelemetry` into this one, combining both global and per-collection statistics.
    pub fn merge(&mut self, other: &WebApiTelemetry) {
        for (method, status_codes) in &other.responses {
            let status_codes_map = self.responses.entry(method.clone()).or_default();
            for (status_code, statistics) in status_codes {
                let entry = status_codes_map.entry(*status_code).or_default();
                *entry = entry.clone() + statistics.clone();
            }
        }
        for (method, collections) in &other.collection_responses {
            let collections_map = self.collection_responses.entry(method.clone()).or_default();
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

/// Combined REST and gRPC request telemetry.
///
/// Provides a unified view of all API request statistics, collected from
/// both the Actix (REST) and Tonic (gRPC) telemetry pipelines.
#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct RequestsTelemetry {
    /// REST API telemetry data.
    pub rest: WebApiTelemetry,
    /// gRPC API telemetry data.
    pub grpc: GrpcTelemetry,
}

impl RequestsTelemetry {
    /// Collects request telemetry from both REST and gRPC collectors, if authorized.
    ///
    /// Returns `None` if the caller lacks the required global access permissions.
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

        // Don't include per-collection data in anonymized telemetry
        WebApiTelemetry {
            responses,
            collection_responses: HashMap::new(),
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

        GrpcTelemetry { responses }
    }
}
