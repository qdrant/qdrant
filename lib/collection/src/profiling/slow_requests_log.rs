use std::time::Duration;

use common::fixed_length_priority_queue::FixedLengthPriorityQueue;

pub(crate) use crate::operations::generalizer::{GeneralizationLevel, Generalizer};

#[derive(serde::Serialize, PartialEq, Eq, Clone)]
pub struct LogEntry {
    collection_name: String,
    duration: Duration,
    request_name: String,
    request_body: serde_json::Value,
}

impl PartialOrd for LogEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.duration.cmp(&other.duration))
    }
}

impl Ord for LogEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.duration.cmp(&other.duration)
    }
}

pub struct SlowRequestsLog {
    log_priority_queue: FixedLengthPriorityQueue<LogEntry>,
    generalization_level: GeneralizationLevel,
}

impl SlowRequestsLog {
    pub fn new(max_entries: usize, generalization_level: GeneralizationLevel) -> Self {
        SlowRequestsLog {
            log_priority_queue: FixedLengthPriorityQueue::new(max_entries),
            generalization_level,
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
        request_name: &str,
        request: &dyn Generalizer,
    ) -> Option<LogEntry> {
        if !self.log_priority_queue.is_full() {
            let entry = LogEntry {
                collection_name: collection_name.to_string(),
                duration,
                request_name: request_name.to_string(),
                request_body: request.generalize(self.generalization_level),
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
            request_name: request_name.to_string(),
            request_body: request.generalize(self.generalization_level),
        };

        self.log_priority_queue.push(entry)
    }

    pub fn get_slow_requests(&self) -> Vec<LogEntry> {
        self.log_priority_queue.iter_unsorted().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;

    use super::*;

    struct DummyGeneralizer;
    impl Generalizer for DummyGeneralizer {
        fn generalize(&self, _level: GeneralizationLevel) -> serde_json::Value {
            json!({"dummy": true})
        }
    }

    #[test]
    fn test_get_slow_requests_returns_all_logged() {
        let mut log = SlowRequestsLog::new(3, GeneralizationLevel::OnlyVector);
        let request = DummyGeneralizer;
        log.log_request("col", Duration::from_secs(1), "req1", &request);
        log.log_request("col", Duration::from_secs(2), "req2", &request);
        log.log_request("col", Duration::from_secs(3), "req3", &request);
        let entries = log.get_slow_requests();
        assert_eq!(entries.len(), 3);

        let evicted = log.log_request("col", Duration::from_secs(4), "req4", &request);
        assert!(evicted.is_some());
        let evicted = evicted.unwrap();
        assert_eq!(evicted.request_name, "req1");

        let entries = log.get_slow_requests();
        assert_eq!(entries.len(), 3);

        let evicted = log.log_request("col", Duration::from_secs(1), "req5", &request);
        assert!(evicted.is_none());
        let entries = log.get_slow_requests();
        assert_eq!(entries.len(), 3);
    }
}
