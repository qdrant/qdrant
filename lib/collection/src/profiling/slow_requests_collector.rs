use std::sync::Arc;

use tokio::sync::RwLock;

use crate::operations::generalizer::Loggable;
use crate::profiling::slow_requests_log::SlowRequestsLog;

/// Logger should ignore everything below this threshold
pub const MIN_SLOW_REQUEST_DURATION: std::time::Duration = std::time::Duration::from_millis(50);

/// Message, used to communicate between main application and profile listener.
/// This is not supposed to be exposed to the users directly, use helper functions instead.
pub struct RequestProfileMessage {
    request: Box<dyn Loggable + Send + Sync>,
    duration: std::time::Duration,
    collection_name: String,
}

impl RequestProfileMessage {
    pub fn new(
        request: Box<dyn Loggable + Send + Sync>,
        duration: std::time::Duration,
        collection_name: String,
    ) -> Self {
        RequestProfileMessage {
            request,
            duration,
            collection_name,
        }
    }
}

/// This structure is responsible for listening to slow requests and logging them, if needed.
/// It is supposed to be a singleton in the application and run in a separate future.
pub struct RequestsCollector {
    log: Arc<RwLock<SlowRequestsLog>>,
    sender: tokio::sync::mpsc::Sender<RequestProfileMessage>,
}

const MAX_REQUESTS_LOGGED: usize = 100;
const QUEUE_CAPACITY: usize = 64;

impl RequestsCollector {
    pub fn new() -> (Self, tokio::sync::mpsc::Receiver<RequestProfileMessage>) {
        let log = SlowRequestsLog::new(MAX_REQUESTS_LOGGED);
        let (sender, receiver) = tokio::sync::mpsc::channel(QUEUE_CAPACITY);

        (
            RequestsCollector {
                log: Arc::new(RwLock::new(log)),
                sender,
            },
            receiver,
        )
    }

    pub fn get_log(&self) -> Arc<RwLock<SlowRequestsLog>> {
        self.log.clone()
    }

    pub fn send_if_available(&self, message: RequestProfileMessage) {
        self.sender.try_send(message).unwrap_or_else(|err| {
            log::error!("Failed to send message: {err}");
        })
    }

    pub async fn run(
        log: Arc<RwLock<SlowRequestsLog>>,
        receiver: tokio::sync::mpsc::Receiver<RequestProfileMessage>,
    ) {
        let mut receiver = receiver;
        while let Some(message) = receiver.recv().await {
            let RequestProfileMessage {
                request,
                duration,
                collection_name,
            } = message;

            log.write()
                .await
                .log_request(&collection_name, duration, request.as_ref());
        }
    }
}
