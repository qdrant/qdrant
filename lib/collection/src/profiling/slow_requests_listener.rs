use std::sync::Arc;

use tokio::sync::RwLock;

use crate::operations::generalizer::GeneralizationLevel;
use crate::profiling::slow_requests_log::{Generalizer, SlowRequestsLog};

struct SlowRequestMessage {
    request: Arc<dyn Generalizer>,
    duration: std::time::Duration,
    collection_name: String,
    request_name: String,
}

/// This structure is responsible for listening to slow requests and logging them, if needed.
/// It is supposed to be a singleton in the application and run in a separate future.
pub struct SlowRequestsListener {
    log: RwLock<SlowRequestsLog>,
    sender: tokio::sync::mpsc::Sender<SlowRequestMessage>,
    receiver: tokio::sync::mpsc::Receiver<SlowRequestMessage>,
}

const MAX_REQUESTS_LOGGED: usize = 100;
const QUEUE_CAPACITY: usize = 64;

impl SlowRequestsListener {
    pub fn new() -> Self {
        let log = SlowRequestsLog::new(MAX_REQUESTS_LOGGED, GeneralizationLevel::VectorAndValues);
        let (sender, receiver) = tokio::sync::mpsc::channel(QUEUE_CAPACITY);

        SlowRequestsListener {
            log: RwLock::new(log),
            sender,
            receiver,
        }
    }
}
