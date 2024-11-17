use std::sync::Arc;

use common::counter::hardware_accumulator::HwMeasurementAcc;

use super::TableOfContent;

impl TableOfContent {
    pub fn get_collection_hw_metrics(&self, collection_id: String) -> Arc<HwMeasurementAcc> {
        self.collection_hw_metrics
            .entry(collection_id)
            .or_default()
            .clone()
    }
}

pub struct RequestHwCounter {
    counter: HwMeasurementAcc,
    /// If this flag is set, RequestHwCounter will be converted into non-None API representation.
    /// Otherwise, it will be ignored.
    report_to_api: bool,
    /// This flag defines if the request is internal (goes between nodes) or external (comes from the user).
    /// If request is external, we always want to accumulate hardware counters.
    /// But if it's internal, we can't accumulate them in all cases, as it may lead to double counting.
    /// Instead, we accumulate it only if the RequestHwCounter was not converted to API representation.
    /// In other words, we clear the counter if it was converted to API representation.
    internal: bool,
}

impl RequestHwCounter {
    pub fn new(counter: HwMeasurementAcc, report_to_api: bool, internal: bool) -> Self {
        Self {
            counter,
            report_to_api,
            internal,
        }
    }

    pub fn get_counter(&self) -> &HwMeasurementAcc {
        &self.counter
    }

    pub fn to_rest_api(self) -> Option<api::rest::models::HardwareUsage> {
        if self.report_to_api {
            let res = Some(api::rest::models::HardwareUsage {
                cpu: self.counter.get_cpu(),
            });
            if self.internal {
                // Do not accumulate the counter if it's internal,
                // as it will be accumulated on the coordinator node.
                self.counter.discard();
            }
            res
        } else {
            None
        }
    }

    pub fn to_grpc_api(self) -> Option<api::grpc::qdrant::HardwareUsage> {
        if self.report_to_api {
            let res = Some(api::grpc::qdrant::HardwareUsage {
                cpu: self.counter.get_cpu() as u64,
            });
            if self.internal {
                // Do not accumulate the counter if it's internal,
                // as it will be accumulated on the coordinator node.
                self.counter.discard();
            }
            res
        } else {
            None
        }
    }
}
