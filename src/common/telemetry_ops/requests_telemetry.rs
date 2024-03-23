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

pub type HttpStatusCode = u16;

#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct WebApiTelemetry {
    pub responses: HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>,
    pub collection_responses:
        HashMap<(String, String), HashMap<HttpStatusCode, OperationDurationStatistics>>,
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
    methods: HashMap<String, HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
    // (Collection, Method) -> {Status code -> Aggregator}
    collections: HashMap<
        (String, String),
        HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>,
    >,
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
        collection_name: Option<String>,
        method: String,
        status_code: HttpStatusCode,
        instant: std::time::Instant,
    ) {
        if let Some(collection_name) = collection_name {
            let aggregator = self
                .collections
                .entry((collection_name, method.clone()))
                .or_default()
                .entry(status_code)
                .or_insert_with(OperationDurationsAggregator::new);
            ScopeDurationMeasurer::new_with_instant(aggregator, instant);
        }
        let aggregator = self
            .methods
            .entry(method)
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
        let mut collection_responses = HashMap::new();
        for ((collection, method), status_codes) in &self.collections {
            let mut status_codes_map = HashMap::new();
            for (status_code, aggregator) in status_codes {
                status_codes_map.insert(*status_code, aggregator.lock().get_statistics(detail));
            }
            collection_responses.insert((collection.clone(), method.clone()), status_codes_map);
        }
        WebApiTelemetry {
            responses,
            collection_responses,
        }
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
        for ((collection, method), status_codes) in &other.collection_responses {
            let status_codes_map = self
                .collection_responses
                .entry((collection.clone(), method.clone()))
                .or_default();
            for (status_code, statistics) in status_codes {
                let entry = status_codes_map.entry(*status_code).or_default();
                *entry = entry.clone() + statistics.clone();
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
                    .map(|(key, value)| (*key, value.anonymize()))
                    .collect();
                (key.clone(), value)
            })
            .collect();

        let collection_responses = self
            .collection_responses
            .iter()
            .map(|((collection, method), value)| {
                let value: HashMap<_, _> = value
                    .iter()
                    .map(|(key, value)| (*key, value.anonymize()))
                    .collect();
                ((collection.to_owned(), method.clone()), value)
            })
            .collect();

        WebApiTelemetry {
            responses,
            collection_responses,
        }
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
