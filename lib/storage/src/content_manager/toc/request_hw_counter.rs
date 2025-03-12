use common::counter::hardware_accumulator::{HwMeasurementAcc, HwSharedDrain};

use super::TableOfContent;

impl TableOfContent {
    pub fn get_collection_hw_metrics(&self, collection_id: String) -> HwSharedDrain {
        self.collection_hw_metrics
            .entry(collection_id)
            .or_default()
            .clone()
    }
}

#[derive(Clone)]
pub struct RequestHwCounter {
    counter: HwMeasurementAcc,
    /// If this flag is set, RequestHwCounter will be converted into non-None API representation.
    /// Otherwise, it will be ignored.
    report_to_api: bool,
}

impl RequestHwCounter {
    pub fn new(counter: HwMeasurementAcc, report_to_api: bool) -> Self {
        Self {
            counter,
            report_to_api,
        }
    }

    pub fn get_counter(&self) -> HwMeasurementAcc {
        self.counter.clone()
    }

    pub fn to_rest_api(self) -> Option<api::rest::models::HardwareUsage> {
        if self.report_to_api {
            Some(api::rest::models::HardwareUsage {
                cpu: self.counter.get_cpu(),
                payload_io_read: self.counter.get_payload_io_read(),
                payload_io_write: self.counter.get_payload_io_write(),
                payload_index_io_read: self.counter.get_payload_index_io_read(),
                payload_index_io_write: self.counter.get_payload_index_io_write(),
                vector_io_read: self.counter.get_vector_io_read(),
                vector_io_write: self.counter.get_vector_io_write(),
            })
        } else {
            None
        }
    }

    pub fn to_grpc_api(self) -> Option<api::grpc::qdrant::HardwareUsage> {
        if self.report_to_api {
            Some(api::grpc::qdrant::HardwareUsage {
                cpu: self.counter.get_cpu() as u64,
                payload_io_read: self.counter.get_payload_io_read() as u64,
                payload_io_write: self.counter.get_payload_io_write() as u64,
                payload_index_io_read: self.counter.get_payload_index_io_read() as u64,
                payload_index_io_write: self.counter.get_payload_index_io_write() as u64,
                vector_io_read: self.counter.get_vector_io_read() as u64,
                vector_io_write: self.counter.get_vector_io_write() as u64,
            })
        } else {
            None
        }
    }
}
