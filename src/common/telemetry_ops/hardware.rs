use std::collections::HashMap;

use api::rest::models::HardwareUsage;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::Serialize;
use storage::dispatcher::Dispatcher;

#[derive(Serialize, Clone, Debug, JsonSchema)]
pub struct HardwareTelemetry {
    pub(crate) collection_data: HashMap<String, HardwareUsage>,
}

impl HardwareTelemetry {
    pub(crate) fn new(dispatcher: &Dispatcher) -> Self {
        Self {
            collection_data: dispatcher.all_hw_metrics(),
        }
    }
}

impl Anonymize for HardwareTelemetry {
    fn anonymize(&self) -> Self {
        let collection_data = self
            .collection_data
            .iter()
            .map(|i| (i.0.anonymize(), i.1.clone()))
            .collect();
        Self { collection_data }
    }
}
