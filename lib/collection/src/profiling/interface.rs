use tokio::runtime::Handle;
use tokio::sync::OnceCell;

use crate::operations::generalizer::Loggable;
use crate::profiling::slow_requests_collector::{MIN_SLOW_REQUEST_DURATION, RequestProfileMessage};
use crate::profiling::slow_requests_log::LogEntry;

static REQUESTS_COLLECTOR: OnceCell<crate::profiling::slow_requests_collector::RequestsCollector> =
    OnceCell::const_new();

/// This function should be used to log request profiles into the shared log structure.
/// This structure is later can be read via API.
pub fn log_request_to_collector(
    collection_name: impl Into<String>,
    request: impl Loggable + Send + Sync + 'static,
    duration: std::time::Duration,
) {
    if duration < MIN_SLOW_REQUEST_DURATION {
        return;
    }

    if let Some(listener) = REQUESTS_COLLECTOR.get() {
        let message =
            RequestProfileMessage::new(Box::new(request), duration, collection_name.into());
        listener.send_if_available(message);
    } else {
        log::warn!("SlowRequestsListener is not initialized");
    }
}

/// This function initializes a global listener for slow requests channel
///
/// It should be called once during the application startup with a valid Tokio runtime handle
/// to spawn the listener task.
pub fn init_requests_profile_collector(runtime: Handle) {
    runtime.spawn(async move {
        REQUESTS_COLLECTOR
            .get_or_init(async || {
                let (listener, receiver) =
                    crate::profiling::slow_requests_collector::RequestsCollector::new();
                let log = listener.get_log();
                tokio::spawn(
                    crate::profiling::slow_requests_collector::RequestsCollector::run(
                        log, receiver,
                    ),
                );
                listener
            })
            .await;
    });
}

/// Read current log of slow requests with associated data.
pub async fn get_requests_profile_log(limit: usize) -> Vec<LogEntry> {
    let listener = REQUESTS_COLLECTOR.get();
    if let Some(listener) = listener {
        listener.get_log().read().await.get_log_entries(limit)
    } else {
        log::warn!("RequestsCollector is not initialized");
        vec![]
    }
}
