use std::sync::Arc;

use common::counter::hardware_accumulator::HwMeasurementAcc;

use super::TableOfContent;

impl TableOfContent {
    pub fn get_collection_hw_metrics(&self, collection_id: &str) -> Arc<HwMeasurementAcc> {
        self.collection_hw_metrics
            .entry(collection_id.to_string())
            .or_default()
            .clone()
    }
}

pub struct RequestHwCounter(HwMeasurementAcc);

impl RequestHwCounter {
    /// Manually creates a new `RequestHwConuter`, without applying the values to the corresponding collection.
    /// This should only be used in scenarios, where the hardware utilization gets returned in the API but
    /// not applied to a collection eg. in the internal API.
    ///
    /// Otherwise `report_hw_measurements()` of `TableOfContent` must be used!
    pub fn new_discard_collection(hardware: HwMeasurementAcc) -> Self {
        Self(hardware)
    }

    pub fn discard(&self) {
        self.0.discard()
    }
}

impl From<RequestHwCounter> for api::grpc::models::HardwareUsage {
    fn from(value: RequestHwCounter) -> api::grpc::models::HardwareUsage {
        let cpu = value.0.get_cpu();
        let api_response = api::grpc::models::HardwareUsage { cpu };
        value.0.discard();
        api_response
    }
}

impl From<RequestHwCounter> for api::grpc::qdrant::HardwareUsage {
    fn from(value: RequestHwCounter) -> api::grpc::qdrant::HardwareUsage {
        let cpu = value.0.get_cpu() as u64;
        let api_response = api::grpc::qdrant::HardwareUsage { cpu };
        value.0.discard();
        api_response
    }
}
