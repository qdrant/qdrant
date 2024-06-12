use std::collections::HashMap;
use std::sync::Arc;

use common::types::TelemetryDetail;
use parking_lot::Mutex;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::common::operation_time_statistics::{
    OperationDurationStatistics, OperationDurationsAggregator, ScopeDurationMeasurer,
};
use serde::Serialize;

/// What Actix routing matches against when selecting the handler.
/// Something like `/collections/{name}/points/search`.
type Route = String;

/// Collection that was targeted by a request measured.
type CollectionName = String;

/// Breakdown of the time spent in the handler for a specific route,
/// under an Arc and a Mutex.
type SharedAgg = Arc<Mutex<OperationDurationsAggregator>>;

pub type HttpStatusCode = u16;

#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct WebApiTelemetry {
    /// See [ActixWorkerTelemetryCollector::aggregator_of].
    pub responses: HashMap<
        Route,
        HashMap<CollectionName, HashMap<HttpStatusCode, OperationDurationStatistics>>,
    >,
}

#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct GrpcTelemetry {
    pub responses: HashMap<String, OperationDurationStatistics>,
}

pub struct ActixTelemetryCollector {
    pub workers: Vec<Arc<Mutex<ActixWorkerTelemetryCollector>>>,
}

#[derive(Default)]
pub struct ActixWorkerTelemetryCollector {
    /// Mapping of `(Route, CollectionName, StatusCode)` to its timing histogram.
    aggregator_of: HashMap<Route, HashMap<CollectionName, HashMap<HttpStatusCode, SharedAgg>>>,
}

pub struct TonicTelemetryCollector {
    pub workers: Vec<Arc<Mutex<TonicWorkerTelemetryCollector>>>,
}

#[derive(Default)]
pub struct TonicWorkerTelemetryCollector {
    methods: HashMap<String, Arc<Mutex<OperationDurationsAggregator>>>,
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub fn add_response(&mut self, method: String, instant: std::time::Instant) {
        let aggregator = self
            .methods
            .entry(method)
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);
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
        target_collection: Option<&str>,
        status_code: HttpStatusCode,
        instant: std::time::Instant,
    ) {
        let aggregator = self
            .aggregator_of
            .entry(method)
            .or_default()
            .entry(target_collection.unwrap_or_default().to_string())
            .or_default()
            .entry(status_code)
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);
    }

    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> WebApiTelemetry {
        // Route -> CollectionName -> StatusCode -> Statistics
        let mut responses = HashMap::new();
        for (method, collections) in &self.aggregator_of {
            // CollectionName -> StatusCode -> Statistics
            let mut collections_map = HashMap::new();
            for (collection, status_codes) in collections {
                // StatusCode -> Statistics
                let mut status_codes_map = HashMap::new();
                for (status_code, aggregator) in status_codes {
                    status_codes_map.insert(*status_code, aggregator.lock().get_statistics(detail));
                }
                collections_map.insert(collection.clone(), status_codes_map);
            }
            responses.insert(method.clone(), collections_map);
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
        for (method, collections) in &other.responses {
            let collections_map = self.responses.entry(method.clone()).or_default();
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

#[derive(Serialize, Clone, Debug, JsonSchema)]
pub struct RequestsTelemetry {
    pub rest: WebApiTelemetry,
    pub grpc: GrpcTelemetry,
}

impl RequestsTelemetry {
    pub fn collect(
        actix_collector: &ActixTelemetryCollector,
        tonic_collector: &TonicTelemetryCollector,
        detail: TelemetryDetail,
    ) -> Self {
        let rest = actix_collector.get_telemetry_data(detail);
        let grpc = tonic_collector.get_telemetry_data(detail);
        Self { rest, grpc }
    }
}

impl Anonymize for RequestsTelemetry {
    fn anonymize(&self) -> Self {
        let rest = self.rest.anonymize();
        let grpc = self.grpc.anonymize();
        Self { rest, grpc }
    }
}

impl Anonymize for WebApiTelemetry {
    fn anonymize(&self) -> Self {
        let responses = self
            .responses
            .iter()
            .map(|(key, value)| {
                let value: HashMap<_, _> = value
                    .iter()
                    .map(|(key, value)| {
                        let value: HashMap<_, _> = value
                            .iter()
                            .map(|(key, value)| (*key, value.anonymize()))
                            .collect();
                        (key.clone(), value)
                    })
                    .collect();
                (key.clone(), value)
            })
            .collect();

        WebApiTelemetry { responses }
    }
}

impl Anonymize for GrpcTelemetry {
    fn anonymize(&self) -> Self {
        let responses = self
            .responses
            .iter()
            .map(|(key, value)| (key.clone(), value.anonymize()))
            .collect();

        GrpcTelemetry { responses }
    }
}
