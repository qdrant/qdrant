use crate::config::CollectionConfig;
use std::sync::Arc;
use std::sync::mpsc::Sender;
use parking_lot::Mutex;

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
}

pub fn send_telemetry_message(sender: &mut CollectionTelemetrySender, message: CollectionTelemetryMessage) {
    let mut sender = sender.lock();
    if let Some(sender) = &mut *sender {
        if let Err(send_error) = sender.send(message) {
            log::error!("Cannot send telemetry message: {}", send_error);
        }
    }
}
