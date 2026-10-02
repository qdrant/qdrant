use std::sync::Arc;

use common::counter::hw::HwMetric;
use common::counter::{AmbientContext, HwSharedDrain};

use super::TableOfContent;

impl TableOfContent {
    pub fn get_collection_hw_metrics(&self, collection_id: String) -> Arc<HwSharedDrain> {
        self.collection_hw_metrics
            .entry(collection_id)
            .or_default()
            .clone()
    }
}

#[derive(Clone)]
pub struct RequestHwCounter {
    counter: AmbientContext,
    /// If this flag is set, RequestHwCounter will be converted into non-None API representation.
    /// Otherwise, it will be ignored.
    report_to_api: bool,
}

impl RequestHwCounter {
    pub fn new(counter: AmbientContext, report_to_api: bool) -> Self {
        Self {
            counter,
            report_to_api,
        }
    }

    pub fn get_counter(&self) -> AmbientContext {
        AmbientContext::clone(&self.counter)
    }

    pub fn to_rest_api(self) -> Option<api::rest::models::HardwareUsage> {
        if self.report_to_api {
            let data = self.counter.hw_data();
            let m = |metric: HwMetric| data[metric];
            Some(api::rest::models::HardwareUsage {
                cpu: m(HwMetric::Cpu),
                payload_io_read: m(HwMetric::PayloadIoRead),
                payload_io_write: m(HwMetric::PayloadIoWrite),
                payload_index_io_read: m(HwMetric::PayloadIndexIoRead),
                payload_index_io_write: m(HwMetric::PayloadIndexIoWrite),
                vector_io_read: m(HwMetric::VectorIoRead),
                vector_io_write: m(HwMetric::VectorIoWrite),
            })
        } else {
            None
        }
    }

    pub fn to_grpc_api(self) -> Option<api::grpc::qdrant::HardwareUsage> {
        self.report_to_api
            .then(|| api::grpc::qdrant::HardwareUsage::from(self.counter))
    }
}
