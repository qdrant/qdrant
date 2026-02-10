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

#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct WebApiTelemetry {
    pub responses: HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub responses_by_collection: HashMap<String, HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>>,
}

#[derive(Serialize, Clone, Default, Debug, JsonSchema, Anonymize)]
pub struct GrpcTelemetry {
    #[anonymize(with = anonymize_collection_values)]
    pub responses: HashMap<String, OperationDurationStatistics>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[anonymize(with = anonymize_collection_values)]
    pub responses_by_collection: HashMap<String, HashMap<String, OperationDurationStatistics>>,
}

pub struct ActixTelemetryCollector {
    pub workers: Vec<Arc<Mutex<ActixWorkerTelemetryCollector>>>,
}

#[derive(Default)]
pub struct ActixWorkerTelemetryCollector {
    methods: HashMap<String, HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
    methods_by_collection: HashMap<String, HashMap<String, HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>>,
}

pub struct TonicTelemetryCollector {
    pub workers: Vec<Arc<Mutex<TonicWorkerTelemetryCollector>>>,
}

#[derive(Default)]
pub struct TonicWorkerTelemetryCollector {
    methods: HashMap<String, Arc<Mutex<OperationDurationsAggregator>>>,
    methods_by_collection: HashMap<String, HashMap<String, Arc<Mutex<OperationDurationsAggregator>>>>,
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
    pub fn add_response(&mut self, method: String, collection_name: Option<String>, instant: std::time::Instant) {
        // Add to global metrics
        let aggregator = self
            .methods
            .entry(method.clone())
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);

        // Add to per-collection metrics if collection name is provided
        if let Some(collection) = collection_name {
            let collection_aggregator = self
                .methods_by_collection
                .entry(collection)
                .or_default()
                .entry(method)
                .or_insert_with(OperationDurationsAggregator::new);
            ScopeDurationMeasurer::new_with_instant(collection_aggregator, instant);
        }
    }

    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> GrpcTelemetry {
        let mut responses = HashMap::new();
        for (method, aggregator) in self.methods.iter() {
            responses.insert(method.clone(), aggregator.lock().get_statistics(detail));
        }

        let mut responses_by_collection = HashMap::new();
        for (collection, methods) in self.methods_by_collection.iter() {
            let mut collection_responses = HashMap::new();
            for (method, aggregator) in methods.iter() {
                collection_responses.insert(method.clone(), aggregator.lock().get_statistics(detail));
            }
            responses_by_collection.insert(collection.clone(), collection_responses);
        }

        GrpcTelemetry { 
            responses,
            responses_by_collection,
        }
    }
}

impl ActixWorkerTelemetryCollector {
    pub fn add_response(
        &mut self,
        method: String,
        status_code: HttpStatusCode,
        collection_name: Option<String>,
        instant: std::time::Instant,
    ) {
        // Add to global metrics
        let aggregator = self
            .methods
            .entry(method.clone())
            .or_default()
            .entry(status_code)
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);

        // Add to per-collection metrics if collection name is provided
        if let Some(collection) = collection_name {
            let collection_aggregator = self
                .methods_by_collection
                .entry(collection)
                .or_default()
                .entry(method)
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

        let mut responses_by_collection = HashMap::new();
        for (collection, methods) in &self.methods_by_collection {
            let mut collection_responses = HashMap::new();
            for (method, status_codes) in methods {
                let mut status_codes_map = HashMap::new();
                for (status_code, aggregator) in status_codes {
                    status_codes_map.insert(*status_code, aggregator.lock().get_statistics(detail));
                }
                collection_responses.insert(method.clone(), status_codes_map);
            }
            responses_by_collection.insert(collection.clone(), collection_responses);
        }

        WebApiTelemetry { 
            responses,
            responses_by_collection,
        }
    }
}

impl GrpcTelemetry {
    pub fn merge(&mut self, other: &GrpcTelemetry) {
        for (method, other_statistics) in &other.responses {
            let entry = self.responses.entry(method.clone()).or_default();
            *entry = entry.clone() + other_statistics.clone();
        }

        for (collection, methods) in &other.responses_by_collection {
            let collection_entry = self.responses_by_collection.entry(collection.clone()).or_default();
            for (method, other_statistics) in methods {
                let entry = collection_entry.entry(method.clone()).or_default();
                *entry = entry.clone() + other_statistics.clone();
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

        for (collection, methods) in &other.responses_by_collection {
            let collection_entry = self.responses_by_collection.entry(collection.clone()).or_default();
            for (method, status_codes) in methods {
                let method_entry = collection_entry.entry(method.clone()).or_default();
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

        let responses_by_collection = self
            .responses_by_collection
            .iter()
            .map(|(collection, methods)| {
                let anonymized_methods = methods
                    .iter()
                    .map(|(method, status_codes)| {
                        (method.clone(), anonymize_collection_values(status_codes))
                    })
                    .collect();
                (collection.clone(), anonymized_methods)
            })
            .collect();

        WebApiTelemetry { 
            responses,
            responses_by_collection,
        }
    }
}
