use std::hash::{Hash, Hasher};
use std::time::Duration;

use ahash::AHashMap;
use chrono::{DateTime, Utc};
use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use count_min_sketch::CountMinSketch64;
use itertools::Itertools;
use schemars::JsonSchema;
use serde::Serialize;

use crate::operations::loggable::Loggable;

#[derive(Serialize, PartialEq, Eq, Clone, JsonSchema)]
pub struct LogEntry {
    collection_name: String,
    #[serde(serialize_with = "duration_as_seconds")]
    duration: Duration,
    datetime: DateTime<Utc>,
    request_name: &'static str,
    approx_count: usize,
    request_body: serde_json::Value,
    /// Used for fast comparison and lookup
    #[serde(skip)]
    content_hash: u64,
}

impl LogEntry {
    pub fn new(
        collection_name: String,
        duration: Duration,
        datetime: DateTime<Utc>,
        request_name: &'static str,
        request_body: serde_json::Value,
        content_hash: u64, // Pre-computed content hash
    ) -> Self {
        LogEntry {
            collection_name,
            duration,
            datetime,
            request_name,
            approx_count: 1,
            request_body,
            content_hash,
        }
    }

    pub fn upd_counter(&mut self, count: usize) {
        self.approx_count = count;
    }
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
    log_priority_queue: AHashMap<&'static str, FixedLengthPriorityQueue<LogEntry>>,
    counters: CountMinSketch64<u64>,
    max_entries: usize,
}

impl SlowRequestsLog {
    pub fn new(max_entries: usize) -> Self {
        SlowRequestsLog {
            log_priority_queue: Default::default(),
            counters: CountMinSketch64::new(1024, 0.95, 0.1).unwrap(), // 95% probability, 10% tolerance
            max_entries,
        }
    }

    /// Try insert an entry into the log in the way, that it is not duplicated by content.
    fn try_insert_dedup(&mut self, entry: LogEntry) -> Option<LogEntry> {
        let queue = self
            .log_priority_queue
            .entry(entry.request_name)
            .or_insert_with(|| FixedLengthPriorityQueue::new(self.max_entries));

        let duplicate = queue.iter_unsorted().find(|e| {
            e.content_hash == entry.content_hash // Fast check
        });

        if let Some(duplicate) = duplicate {
            if duplicate.duration >= entry.duration {
                // Existing record took longer, keep it
                None
            } else {
                // New record took longer, replace existing record
                queue.retain(|e| e.content_hash != entry.content_hash);
                queue.push(entry)
            }
        } else {
            // just insert
            queue.push(entry)
        }
    }

    fn inc_counter(&mut self, content_hash: u64) {
        self.counters.increment(&content_hash);
    }

    fn content_hash(request_hash: u64, collection_name: &str) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        request_hash.hash(&mut hasher);
        collection_name.hash(&mut hasher);
        hasher.finish()
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
        datetime: DateTime<Utc>,
        request: &dyn Loggable,
    ) -> Option<LogEntry> {
        let content_hash = Self::content_hash(request.request_hash(), collection_name);

        self.inc_counter(content_hash);

        let queue = self
            .log_priority_queue
            .entry(request.request_name())
            .or_insert_with(|| FixedLengthPriorityQueue::new(self.max_entries));

        if !queue.is_full() {
            let entry = LogEntry::new(
                collection_name.to_string(),
                duration,
                datetime,
                request.request_name(),
                request.to_log_value(),
                content_hash,
            );
            return self.try_insert_dedup(entry);
        }

        // Check if we can insert into the queue before actually serializing the request
        // Safety: unwrap is safe because we checked that the queue is full
        let fastest_logged = queue.top().unwrap();

        if duration <= fastest_logged.duration {
            // Our queue is already slower than this request
            return None;
        }

        let entry = LogEntry::new(
            collection_name.to_string(),
            duration,
            datetime,
            request.request_name(),
            request.to_log_value(),
            content_hash,
        );

        self.try_insert_dedup(entry)
    }

    pub fn get_log_entries(&self, limit: usize, method_name_substr: Option<&str>) -> Vec<LogEntry> {
        self.log_priority_queue
            .iter()
            .filter(|(key, _value)| {
                if let Some(substr) = &method_name_substr {
                    key.contains(substr)
                } else {
                    true
                }
            })
            .flat_map(|(_key, queue)| queue.iter_unsorted())
            .sorted_by(|a, b| b.cmp(a))
            .take(limit)
            .cloned()
            .map(|mut entry| {
                let approx_count = self.counters.estimate(&entry.content_hash);
                entry.upd_counter(approx_count as usize);
                entry
            })
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

        fn request_hash(&self) -> u64 {
            42
        }
    }

    #[test]
    fn test_get_slow_requests_returns_all_logged() {
        let mut log = SlowRequestsLog::new(3);
        let request = DummyLoggable;
        log.log_request("col1", Duration::from_secs(1), Utc::now(), &request);
        log.log_request("col2", Duration::from_secs(2), Utc::now(), &request);
        log.log_request("col3", Duration::from_secs(3), Utc::now(), &request);
        let entries = log.get_log_entries(10, None);
        assert_eq!(entries.len(), 3);

        let evicted = log.log_request("col4", Duration::from_secs(4), Utc::now(), &request);
        assert!(evicted.is_some());
        let evicted = evicted.unwrap();
        assert_eq!(evicted.collection_name, "col1");

        let entries = log.get_log_entries(10, None);
        assert_eq!(entries.len(), 3);

        let evicted = log.log_request("col5", Duration::from_secs(1), Utc::now(), &request);
        assert!(evicted.is_none());
        let entries = log.get_log_entries(10, None);
        assert_eq!(entries.len(), 3);
    }
}
