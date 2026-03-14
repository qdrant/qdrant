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

/// Telemetry data for the Web API (REST).
#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct WebApiTelemetry {
    pub responses: HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>,

    pub responses_by_collection:
        HashMap<String, HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>>,
}

#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct GrpcTelemetry {
    pub responses: HashMap<String, HashMap<GrpcStatusCode, OperationDurationStatistics>>,
}

/// Collector for Actix telemetry data across multiple workers.
pub struct ActixTelemetryCollector {
    pub workers: Vec<Arc<Mutex<ActixWorkerTelemetryCollector>>>,
}

/// Collector for Actix telemetry data for a single worker.
#[derive(Default)]
pub struct ActixWorkerTelemetryCollector {
    methods: HashMap<String, HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,

    methods_by_collection: HashMap<
        String,
        HashMap<String, HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
    >,
}

pub struct TonicTelemetryCollector {
    pub workers: Vec<Arc<Mutex<TonicWorkerTelemetryCollector>>>,
}

#[derive(Default)]
pub struct TonicWorkerTelemetryCollector {
    methods: HashMap<String, HashMap<GrpcStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
}

impl ActixTelemetryCollector {
    pub fn create_web_worker_telemetry(&mut self) -> Arc<Mutex<ActixWorkerTelemetryCollector>> {
        let worker: Arc<Mutex<_>> = Default::default();
        self.workers.push(worker.clone());
        worker
    }

    /// Get the aggregated telemetry data for all Actix workers.
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
    /// Record a response for a method.
    pub fn add_response(
        &mut self,
        method: String,
        status_code: HttpStatusCode,
        instant: std::time::Instant,
    ) {
        let aggregator = self
            .methods
            .entry(method)
            .or_default()
            .entry(status_code)
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);
    }

    /// Record a response for a specific collection.
    pub fn add_response_by_collection(
        &mut self,
        method: String,
        collection: String,
        status_code: HttpStatusCode,
        instant: std::time::Instant,
    ) {
        let aggregator = self
            .methods_by_collection
            .entry(method)
            .or_default()
            .entry(collection)
            .or_default()
            .entry(status_code)
            .or_insert_with(OperationDurationsAggregator::new);

        ScopeDurationMeasurer::new_with_instant(aggregator, instant);
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
        let mut responses_by_collection = HashMap::new();
        for (method, collections_map) in &self.methods_by_collection {
            let mut by_collection = HashMap::new();
            for (collection, status_codes) in collections_map {
                let mut status_codes_map = HashMap::new();
                for (status_code, aggregator) in status_codes {
                    status_codes_map.insert(*status_code, aggregator.lock().get_statistics(detail));
                }
                by_collection.insert(collection.clone(), status_codes_map);
            }
            responses_by_collection.insert(method.clone(), by_collection);
        }
        WebApiTelemetry {
            responses,
            responses_by_collection,
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

        for (method, collections_map) in &other.responses_by_collection {
            let my_collections = self
                .responses_by_collection
                .entry(method.clone())
                .or_default();
            for (collection, status_codes) in collections_map {
                let my_statuses = my_collections.entry(collection.clone()).or_default();
                for (status_code, statistics) in status_codes {
                    let entry = my_statuses.entry(*status_code).or_default();
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

        let responses_by_collection = self
            .responses_by_collection
            .iter()
            .map(|(method_key, collections_map)| {
                let mut collection_names: Vec<_> = collections_map.keys().collect();
                collection_names.sort();

                let anon_collections_map = collection_names
                    .into_iter()
                    .enumerate()
                    .map(|(idx, collection_name)| {
                        (
                            format!("collection_{idx}"),
                            collections_map.get(collection_name).unwrap().clone(),
                        )
                    })
                    .collect();

                (method_key.clone(), anon_collections_map)
            })
            .collect();

        WebApiTelemetry {
            responses,
            responses_by_collection,
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
