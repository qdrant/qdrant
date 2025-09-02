use std::time::Duration;

use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use itertools::Itertools;
use schemars::JsonSchema;
use serde::Serialize;

use crate::operations::generalizer::Loggable;

#[derive(Serialize, PartialEq, Eq, Clone, JsonSchema)]
pub struct LogEntry {
    collection_name: String,
    #[serde(serialize_with = "duration_as_seconds")]
    duration: Duration,
    request_name: String,
    request_body: serde_json::Value,
}

fn duration_as_seconds<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_f64(duration.as_millis() as f64 / 1000.0)
}

impl PartialOrd for LogEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LogEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.duration.cmp(&other.duration)
    }
}

pub struct SlowRequestsLog {
    log_priority_queue: FixedLengthPriorityQueue<LogEntry>,
}

impl SlowRequestsLog {
    pub fn new(max_entries: usize) -> Self {
        SlowRequestsLog {
            log_priority_queue: FixedLengthPriorityQueue::new(max_entries),
        }
    }

    /// Try to log a request if the log.
    /// If proposed log is slower than the fastest logged request, it will be kept in the log.
    /// Otherwise, it will be ignored.
    ///
    /// Returns the log entry that was removed from the log, if any.
    pub fn log_request(
        &mut self,
        collection_name: &str,
        duration: Duration,
        request: &dyn Loggable,
    ) -> Option<LogEntry> {
        if !self.log_priority_queue.is_full() {
            let entry = LogEntry {
                collection_name: collection_name.to_string(),
                duration,
                request_name: request.request_name().to_string(),
                request_body: request.to_log_value(),
            };
            return self.log_priority_queue.push(entry);
        }

        // Check if we can insert into the queue before actually serializing the request
        // Safety: unwrap is safe because we checked that the queue is full
        let fastest_logged = self.log_priority_queue.top().unwrap();

        if duration <= fastest_logged.duration {
            // Our queue is already slower than this request
            return None;
        }

        let entry = LogEntry {
            collection_name: collection_name.to_string(),
            duration,
            request_name: request.request_name().to_string(),
            request_body: request.to_log_value(),
        };

        self.log_priority_queue.push(entry)
    }

    pub fn get_log_entries(&self, limit: usize) -> Vec<LogEntry> {
        self.log_priority_queue
            .iter_unsorted()
            .sorted_by(|a, b| b.cmp(a))
            .take(limit)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::{Value, json};

    use super::*;

    struct DummyLoggable;
    impl Loggable for DummyLoggable {
        fn to_log_value(&self) -> Value {
            json!({"dummy": true})
        }

        fn request_name(&self) -> &'static str {
            "dummy"
        }
    }

    #[test]
    fn test_get_slow_requests_returns_all_logged() {
        let mut log = SlowRequestsLog::new(3);
        let request = DummyLoggable;
        log.log_request("col1", Duration::from_secs(1), &request);
        log.log_request("col2", Duration::from_secs(2), &request);
        log.log_request("col3", Duration::from_secs(3), &request);
        let entries = log.get_log_entries(10);
        assert_eq!(entries.len(), 3);

        let evicted = log.log_request("col4", Duration::from_secs(4), &request);
        assert!(evicted.is_some());
        let evicted = evicted.unwrap();
        assert_eq!(evicted.collection_name, "col1");

        let entries = log.get_log_entries(10);
        assert_eq!(entries.len(), 3);

        let evicted = log.log_request("col5", Duration::from_secs(1), &request);
        assert!(evicted.is_none());
        let entries = log.get_log_entries(10);
        assert_eq!(entries.len(), 3);
    }
}
