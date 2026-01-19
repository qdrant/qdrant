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
use storage::rbac::{Access, AccessRequirements};

pub type HttpStatusCode = u16;

#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct WebApiTelemetry {
    pub responses: HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub per_collection_responses:
        HashMap<String, HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>>,
}

#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct GrpcTelemetry {
    pub responses: HashMap<String, OperationDurationStatistics>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub per_collection_responses: HashMap<String, HashMap<String, OperationDurationStatistics>>,
}

pub struct ActixTelemetryCollector {
    pub workers: Vec<Arc<Mutex<ActixWorkerTelemetryCollector>>>,
}

#[derive(Default)]
pub struct ActixWorkerTelemetryCollector {
    methods: HashMap<String, HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
    per_collection_methods: HashMap<
        String,
        HashMap<String, HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
    >,
}

pub struct TonicTelemetryCollector {
    pub workers: Vec<Arc<Mutex<TonicWorkerTelemetryCollector>>>,
}

#[derive(Default)]
pub struct TonicWorkerTelemetryCollector {
    methods: HashMap<String, Arc<Mutex<OperationDurationsAggregator>>>,
    per_collection_methods:
        HashMap<String, HashMap<String, Arc<Mutex<OperationDurationsAggregator>>>>,
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
        instant: std::time::Instant,
        collection_name: Option<String>,
    ) {
        let aggregator = self
            .methods
            .entry(method.clone())
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);

        if let Some(collection_name) = collection_name {
            let aggregator = self
                .per_collection_methods
                .entry(collection_name)
                .or_default()
                .entry(method)
                .or_insert_with(OperationDurationsAggregator::new);
            ScopeDurationMeasurer::new_with_instant(aggregator, instant);
        }
    }

    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> GrpcTelemetry {
        let mut responses = HashMap::new();
        for (method, aggregator) in self.methods.iter() {
            responses.insert(method.clone(), aggregator.lock().get_statistics(detail));
        }

        let mut per_collection_responses = HashMap::new();
        for (collection, methods) in &self.per_collection_methods {
            let mut methods_map = HashMap::new();
            for (method, aggregator) in methods {
                methods_map.insert(method.clone(), aggregator.lock().get_statistics(detail));
            }
            per_collection_responses.insert(collection.clone(), methods_map);
        }

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

        if let Some(collection_name) = collection_name {
            let aggregator = self
                .per_collection_methods
                .entry(collection_name)
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

        let mut per_collection_responses = HashMap::new();
        for (collection, methods) in &self.per_collection_methods {
            let mut methods_map = HashMap::new();
            for (method, status_codes) in methods {
                let mut status_codes_map = HashMap::new();
                for (status_code, aggregator) in status_codes {
                    status_codes_map.insert(*status_code, aggregator.lock().get_statistics(detail));
                }
                methods_map.insert(method.clone(), status_codes_map);
            }
            per_collection_responses.insert(collection.clone(), methods_map);
        }

        WebApiTelemetry {
            responses,
            per_collection_responses,
        }
    }
}

impl GrpcTelemetry {
    pub fn merge(&mut self, other: &GrpcTelemetry) {
        for (method, other_statistics) in &other.responses {
            let entry = self.responses.entry(method.clone()).or_default();
            *entry = entry.clone() + other_statistics.clone();
        }

        for (collection, methods) in &other.per_collection_responses {
            let collection_entry = self
                .per_collection_responses
                .entry(collection.clone())
                .or_default();
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

        for (collection, methods) in &other.per_collection_responses {
            let collection_entry = self
                .per_collection_responses
                .entry(collection.clone())
                .or_default();
            for (method, status_codes) in methods {
                let status_codes_map = collection_entry.entry(method.clone()).or_default();
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
        let responses = self.responses.clone();

        let per_collection_responses = self
            .per_collection_responses
            .iter()
            .map(|(key, value)| (key.anonymize(), value.clone()))
            .collect();

        WebApiTelemetry {
            responses,
            per_collection_responses,
        }
    }
}

impl Anonymize for GrpcTelemetry {
    fn anonymize(&self) -> Self {
        let responses = self.responses.clone();

        let per_collection_responses = self
            .per_collection_responses
            .iter()
            .map(|(key, value)| (key.anonymize(), value.clone()))
            .collect();

        GrpcTelemetry {
            responses,
            per_collection_responses,
        }
    }
}
