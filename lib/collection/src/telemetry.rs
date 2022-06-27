use crate::config::CollectionConfig;
use crate::operations::types::{CollectionStatus, OptimizersStatus};
use parking_lot::Mutex;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::mpsc::Sender;
use std::sync::Arc;

pub type CollectionTelemetrySender = Arc<Mutex<Option<Sender<CollectionTelemetryMessage>>>>;

#[derive(Clone)]
pub enum CollectionTelemetryMessage {
    LoadSegment {
        id: String,
        config: CollectionConfig,
        load_time: std::time::Duration,
    },
    NewSegment {
        id: String,
        config: CollectionConfig,
        creation_time: std::time::Duration,
    },
    Info {
        id: String,
        status: CollectionStatus,
        optimizer_status: OptimizersStatus,
        vectors_count: usize,
        segments_count: usize,
        disk_data_size: usize,
        ram_data_size: usize,
    },
}

pub fn send_telemetry_message(
    sender: &CollectionTelemetrySender,
    message: CollectionTelemetryMessage,
) {
    let sender = sender.lock();
    if let Some(sender) = &*sender {
        if let Err(send_error) = sender.send(message) {
            log::error!("Cannot send telemetry message: {}", send_error);
        }
    }
}

pub fn get_empty_telemetry_sender() -> CollectionTelemetrySender {
    Arc::new(Mutex::new(None))
}

pub fn telemetry_hash(s: &str) -> String {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish().to_string()
}

pub fn telemetry_round(cnt: usize) -> usize {
    let leading_zeros = cnt.leading_zeros();
    let skip_bytes_count = if leading_zeros > 4 {
        leading_zeros - 4
    } else {
        0
    };
    (cnt >> skip_bytes_count) << skip_bytes_count
}
