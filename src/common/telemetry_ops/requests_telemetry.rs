use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::common::operation_time_statistics::{
    OperationDurationStatistics, OperationDurationsAggregator, ScopeDurationMeasurer,
};
use serde::{Deserialize, Serialize};

pub type HttpStatusCode = u16;

pub type StatusCodeToAggregator = HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>;
pub type StatusCodeToStatistics = HashMap<HttpStatusCode, OperationDurationStatistics>;
pub type MethodsToStatusCode = HashMap<String, StatusCodeToStatistics>;
pub type CollectionToMethods = HashMap<String, MethodsToStatusCode>;

#[derive(Serialize, Deserialize, Clone, Default, Debug, JsonSchema, PartialEq)]
pub struct WebApiTelemetry {
    pub responses: CollectionToMethods,
}

#[derive(Serialize, Deserialize, Clone, Default, Debug, JsonSchema)]
pub struct GrpcTelemetry {
    pub responses: HashMap<String, OperationDurationStatistics>,
}

pub struct ActixTelemetryCollector {
    pub workers: Vec<Arc<Mutex<ActixWorkerTelemetryCollector>>>,
}

#[derive(Default)]
pub struct ActixWorkerTelemetryCollector {
    // collections: CollectionToMethods,
    collections: HashMap<
        // collection_name
        String,
        // k: method_name
        HashMap<String, StatusCodeToAggregator>,
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

    pub fn get_telemetry_data(&self, collections: &Vec<String>) -> WebApiTelemetry {
        let mut result = WebApiTelemetry::default();
        for web_data in &self.workers {
            let lock = web_data.lock().get_telemetry_data_for(collections);
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

    pub fn get_telemetry_data(&self) -> GrpcTelemetry {
        let mut result = GrpcTelemetry::default();
        for grpc_data in &self.workers {
            let lock = grpc_data.lock().get_telemetry_data();
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

    pub fn get_telemetry_data(&self) -> GrpcTelemetry {
        let mut responses = HashMap::new();
        for (method, aggregator) in self.methods.iter() {
            responses.insert(method.clone(), aggregator.lock().get_statistics());
        }
        GrpcTelemetry { responses }
    }
}

impl ActixWorkerTelemetryCollector {
    pub fn add_response(
        &mut self,
        collection: String,
        method: String,
        status_code: HttpStatusCode,
        instant: std::time::Instant,
    ) {
        let aggregator = self
            .collections
            .entry(collection)
            .or_default()
            .entry(method)
            .or_default()
            .entry(status_code)
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);
    }

    pub fn get_telemetry_data(&self) -> WebApiTelemetry {
        let mut responses: CollectionToMethods = HashMap::new();

        for (collection, methods) in &self.collections {
            let mut method_status_codes_map: MethodsToStatusCode = HashMap::new();

            for (method_name, status_map) in methods {
                for (status_code, aggregator) in status_map {
                    let mut status_statistics = HashMap::new();
                    status_statistics.insert(*status_code, aggregator.lock().get_statistics());
                    method_status_codes_map.insert(method_name.clone(), status_statistics);
                }
            }
            responses.insert(collection.clone(), method_status_codes_map);
        }
        WebApiTelemetry { responses }
    }

    /// If the given collection are empty, will return data for all collections
    pub fn get_telemetry_data_for(&self, collections: &Vec<String>) -> WebApiTelemetry {
        let mut responses: CollectionToMethods = HashMap::new();

        if collections.is_empty() {
            return self.get_telemetry_data();
        }

        for collection in collections {
            if let Some(methods) = self.collections.get(collection) {
                let mut method_status_codes_map: MethodsToStatusCode = HashMap::new();

                for (method_name, status_map) in methods {
                    for (status_code, aggregator) in status_map {
                        let mut status_statistics = HashMap::new();
                        status_statistics.insert(*status_code, aggregator.lock().get_statistics());
                        method_status_codes_map.insert(method_name.clone(), status_statistics);
                    }
                }
                responses.insert(collection.clone(), method_status_codes_map);
            }
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
        for (other_collection_name, other_method) in &other.responses {
            let my_method_to_stats = self
                .responses
                .entry(other_collection_name.clone())
                .or_default();

            for (other_method_name, other_status_codes_to_statistics) in other_method {
                let my_code_to_statistics = my_method_to_stats
                    .entry(other_method_name.clone())
                    .or_default();

                for (other_status_code, other_statistics) in other_status_codes_to_statistics {
                    let my_statistics =
                        my_code_to_statistics.entry(*other_status_code).or_default();
                    *my_statistics = my_statistics.clone() + other_statistics.clone();
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct RequestsTelemetry {
    pub rest: WebApiTelemetry,
    pub grpc: GrpcTelemetry,
}

impl RequestsTelemetry {
    pub fn collect(
        actix_collector: &ActixTelemetryCollector,
        tonic_collector: &TonicTelemetryCollector,
        collections: &Vec<String>,
    ) -> Self {
        let rest = actix_collector.get_telemetry_data(collections);
        let grpc = tonic_collector.get_telemetry_data();
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
                        let new_value: HashMap<u16, OperationDurationStatistics> =
                            value.iter().map(|(k, v)| (*k, v.anonymize())).collect();
                        (key.clone(), new_value)
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use segment::common::operation_time_statistics::OperationDurationStatistics;

    use super::WebApiTelemetry;
    use crate::common::telemetry_ops::requests_telemetry::MethodsToStatusCode;

    #[test]
    fn can_web_api_telemetry_merge() {
        let collection_name = "test_collection".to_string();
        let method_name = "search".to_string();

        // we will merge "other_stats" into "my_stats"
        let my_stats = OperationDurationStatistics {
            count: 3,
            ..Default::default()
        };
        let status_code_to_statistics = HashMap::from([(200, my_stats)]);
        let method_to_stats: MethodsToStatusCode =
            HashMap::from([(method_name.clone(), status_code_to_statistics)]);
        let my_responses = HashMap::from([(collection_name.clone(), method_to_stats)]);
        let mut my_web_api_telemetry = WebApiTelemetry {
            responses: my_responses,
        };

        let other_stats = OperationDurationStatistics {
            count: 2,
            fail_count: 5,
            ..Default::default()
        };
        let other_status_code_to_statistics = HashMap::from([(200, other_stats)]);
        let other_method_to_stats: MethodsToStatusCode =
            HashMap::from([(method_name.clone(), other_status_code_to_statistics)]);
        let other_responses = HashMap::from([(collection_name.clone(), other_method_to_stats)]);
        let other_web_api_telemetry = WebApiTelemetry {
            responses: other_responses,
        };

        my_web_api_telemetry.merge(&other_web_api_telemetry);

        assert_eq!(
            my_web_api_telemetry,
            WebApiTelemetry {
                responses: HashMap::from([(
                    collection_name,
                    HashMap::from([(
                        method_name,
                        HashMap::from([(
                            200,
                            OperationDurationStatistics {
                                count: 5,
                                fail_count: 5,
                                avg_duration_micros: None,
                                min_duration_micros: None,
                                max_duration_micros: None,
                                last_responded: None,
                            },
                        )]),
                    )]),
                )]),
            }
        );
    }
}
