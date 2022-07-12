use crate::config::CollectionConfig;
use crate::operations::types::{CollectionStatus, OptimizersStatus};
use serde::Serialize;

#[derive(Serialize, Clone)]
pub struct CollectionTelemetry {
    pub id: String,
    pub config: CollectionConfig,
    pub init_time: std::time::Duration,
    pub status: CollectionStatus,
    pub optimizer_status: OptimizersStatus,
    pub vectors_count: usize,
    pub segments_count: usize,
    pub disk_data_size: usize,
    pub ram_data_size: usize,
}

impl CollectionTelemetry {
    pub fn new(id: String, config: CollectionConfig, init_time: std::time::Duration) -> Self {
        Self {
            id,
            config,
            init_time,
            status: CollectionStatus::Green,
            optimizer_status: OptimizersStatus::Ok,
            vectors_count: 0,
            segments_count: 0,
            disk_data_size: 0,
            ram_data_size: 0,
        }
    }
}
