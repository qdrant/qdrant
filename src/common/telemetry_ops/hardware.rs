use std::collections::HashMap;

use api::rest::models::HardwareUsage;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::Serialize;
use storage::dispatcher::Dispatcher;
use storage::rbac::{Access, AccessRequirements};

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct HardwareTelemetry {
    pub(crate) collection_data: HashMap<String, HardwareUsage>,
}

impl HardwareTelemetry {
    pub(crate) fn new(dispatcher: &Dispatcher, access: &Access) -> Self {
        let mut all_hw_metrics = dispatcher.all_hw_metrics();

        let collection_data = match access {
            Access::Global(_) => all_hw_metrics,
            Access::Collection(collection_access_list) => {
                let required_access = AccessRequirements::new().whole();
                let allowed_collections =
                    collection_access_list.meeting_requirements(required_access);
                let mut resolved_collection_data = HashMap::new();
                for collection in allowed_collections {
                    if let Some(hw_metrics) = all_hw_metrics.remove(collection) {
                        resolved_collection_data.insert(collection.clone(), hw_metrics);
                    }
                }
                resolved_collection_data
            }
        };

        Self { collection_data }
    }
}
