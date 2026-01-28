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

#[derive(Hash, Eq, PartialEq, Clone, Debug, Serialize, JsonSchema, Anonymize)]
pub struct CollectionEndpointKey {
    pub collection: String,
    pub endpoint: String,
}

#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct WebApiTelemetry {
    pub responses: HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>,
    #[serde(skip)]
    pub responses_by_collection: HashMap<CollectionEndpointKey, HashMap<HttpStatusCode, OperationDurationStatistics>>,
}

#[derive(Serialize, Clone, Default, Debug, JsonSchema, Anonymize)]
pub struct GrpcTelemetry {
    #[anonymize(with = anonymize_collection_values)]
    pub responses: HashMap<String, OperationDurationStatistics>,
    #[serde(skip)]
    pub responses_by_collection: HashMap<CollectionEndpointKey, OperationDurationStatistics>,
}

pub struct ActixTelemetryCollector {
    pub workers: Vec<Arc<Mutex<ActixWorkerTelemetryCollector>>>,
    pub per_collection_metrics: bool,
    pub max_collections: usize,
}


pub struct ActixWorkerTelemetryCollector {
    methods: HashMap<String, HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
    pub methods_by_collection: LruCache<CollectionEndpointKey, HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
    pub per_collection_metrics: bool,
}

pub struct TonicTelemetryCollector {
    pub workers: Vec<Arc<Mutex<TonicWorkerTelemetryCollector>>>,
    pub per_collection_metrics: bool,
    pub max_collections: usize,
}


pub struct TonicWorkerTelemetryCollector {
    methods: HashMap<String, Arc<Mutex<OperationDurationsAggregator>>>,
    pub methods_by_collection: LruCache<CollectionEndpointKey, Arc<Mutex<OperationDurationsAggregator>>>,
    pub per_collection_metrics: bool,
}

impl Default for ActixWorkerTelemetryCollector {
    fn default() -> Self {
        Self {
            methods: Default::default(),
            methods_by_collection: LruCache::new(NonZeroUsize::new(1000).unwrap()),
            per_collection_metrics: true,
        }
    }
}

impl Default for TonicWorkerTelemetryCollector {
    fn default() -> Self {
        Self {
            methods: Default::default(),
            methods_by_collection: LruCache::new(NonZeroUsize::new(1000).unwrap()),
            per_collection_metrics: true,
        }
    }
}

impl ActixTelemetryCollector {
    pub fn create_web_worker_telemetry(&mut self) -> Arc<Mutex<ActixWorkerTelemetryCollector>> {
        let worker: Arc<Mutex<_>> = Arc::new(Mutex::new(ActixWorkerTelemetryCollector {
             methods: Default::default(),
             methods_by_collection: LruCache::new(NonZeroUsize::new(self.max_collections).unwrap()),
             per_collection_metrics: self.per_collection_metrics,
        }));
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
        let worker: Arc<Mutex<_>> = Arc::new(Mutex::new(TonicWorkerTelemetryCollector {
            methods: Default::default(),
            methods_by_collection: LruCache::new(NonZeroUsize::new(self.max_collections).unwrap()),
            per_collection_metrics: self.per_collection_metrics,
        }));
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

        if self.per_collection_metrics {
            if let Some(collection_name) = collection_name {
                let key = CollectionEndpointKey {
                    collection: collection_name,
                    endpoint: method,
                };
                let aggregator = self
                    .methods_by_collection
                    .get_or_insert_mut(key, || {
                        // LruCache value initialization
                        OperationDurationsAggregator::new()
                    });
                ScopeDurationMeasurer::new_with_instant(aggregator, instant);
            }
        }
    }

    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> GrpcTelemetry {
        let mut responses = HashMap::new();
        for (method, aggregator) in self.methods.iter() {
            responses.insert(method.clone(), aggregator.lock().get_statistics(detail));
        }

        let mut responses_by_collection = HashMap::new();
        for (key, aggregator) in self.methods_by_collection.iter() {
            responses_by_collection.insert(key.clone(), aggregator.lock().get_statistics(detail));
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

        if self.per_collection_metrics {
            if let Some(collection_name) = collection_name {
                let key = CollectionEndpointKey {
                    collection: collection_name,
                    endpoint: method,
                };
                let aggregator = self
                    .methods_by_collection
                    .get_or_insert_mut(key, || {
                        // LruCache value initialization
                        HashMap::new()
                    })
                    .entry(status_code)
                    .or_insert_with(OperationDurationsAggregator::new);
                ScopeDurationMeasurer::new_with_instant(aggregator, instant);
            }
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
        for (key, status_codes) in self.methods_by_collection.iter() {
            let mut status_codes_map = HashMap::new();
            for (status_code, aggregator) in status_codes {
                status_codes_map.insert(*status_code, aggregator.lock().get_statistics(detail));
            }
            responses_by_collection.insert(key.clone(), status_codes_map);
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
        for (key, other_statistics) in &other.responses_by_collection {
            let entry = self
                .responses_by_collection
                .entry(key.clone())
                .or_default();
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
        for (key, status_codes) in &other.responses_by_collection {
            let status_codes_map = self
                .responses_by_collection
                .entry(key.clone())
                .or_default();
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

        WebApiTelemetry {
            responses,
            responses_by_collection: Default::default(),
        }
    }
}
