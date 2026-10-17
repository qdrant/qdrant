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

#[derive(Serialize, Clone, JsonSchema)]
pub struct LogEntry {
    collection_name: String,
    #[serde(serialize_with = "duration_as_seconds")]
    duration: Duration,
    /// Time of the slowest request in this group
    datetime: DateTime<Utc>,
    /// Time of the oldest request in this group
    datetime_first: DateTime<Utc>,
    /// Time of the newest request in this group
    datetime_last: DateTime<Utc>,
    request_name: &'static str,
    approx_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    cpu_usage_ratio: Option<f32>,
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
        cpu_usage_ratio: Option<f32>,
    ) -> Self {
        LogEntry {
            collection_name,
            duration,
            datetime,
            datetime_first: datetime,
            datetime_last: datetime,
            request_name,
            approx_count: 1,
            cpu_usage_ratio,
            request_body,
            content_hash,
        }
    }

    pub fn upd_counter(&mut self, count: usize) {
        self.approx_count = count;
    }

    pub fn upd_datetime_range(&mut self, first: DateTime<Utc>, last: DateTime<Utc>) {
        self.datetime_first = first;
        self.datetime_last = last;
    }
}

impl PartialEq for LogEntry {
    fn eq(&self, other: &Self) -> bool {
        self.content_hash == other.content_hash
            && self.duration == other.duration
            && self.collection_name == other.collection_name
    }
}

impl Eq for LogEntry {}

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
    /// Oldest and newest request time of each group in the log, by content hash
    datetime_ranges: AHashMap<u64, (DateTime<Utc>, DateTime<Utc>)>,
    max_entries: usize,
}

impl SlowRequestsLog {
    pub fn new(max_entries: usize) -> Self {
        SlowRequestsLog {
            log_priority_queue: Default::default(),
            counters: CountMinSketch64::new(1024, 0.95, 0.1).unwrap(), // 95% probability, 10% tolerance
            datetime_ranges: Default::default(),
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
            self.datetime_ranges
                .insert(entry.content_hash, (entry.datetime, entry.datetime));
            let evicted = queue.push(entry);
            if let Some(evicted) = &evicted {
                self.datetime_ranges.remove(&evicted.content_hash);
            }
            evicted
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
        cpu_usage_ratio: Option<f32>,
    ) -> Option<LogEntry> {
        let content_hash = Self::content_hash(request.request_hash(), collection_name);

        self.inc_counter(content_hash);

        // Track oldest and newest request, also if this one is too fast to replace the logged one
        if let Some((first, last)) = self.datetime_ranges.get_mut(&content_hash) {
            *first = (*first).min(datetime);
            *last = (*last).max(datetime);
        }

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
                cpu_usage_ratio,
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
            cpu_usage_ratio,
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
                if let Some(&(first, last)) = self.datetime_ranges.get(&entry.content_hash) {
                    entry.upd_datetime_range(first, last);
                }
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
        log.log_request("col1", Duration::from_secs(1), Utc::now(), &request, None);
        log.log_request("col2", Duration::from_secs(2), Utc::now(), &request, None);
        log.log_request("col3", Duration::from_secs(3), Utc::now(), &request, None);
        let entries = log.get_log_entries(10, None);
        assert_eq!(entries.len(), 3);

        let evicted = log.log_request("col4", Duration::from_secs(4), Utc::now(), &request, None);
        assert!(evicted.is_some());
        let evicted = evicted.unwrap();
        assert_eq!(evicted.collection_name, "col1");

        let entries = log.get_log_entries(10, None);
        assert_eq!(entries.len(), 3);

        let evicted = log.log_request("col5", Duration::from_secs(1), Utc::now(), &request, None);
        assert!(evicted.is_none());
        let entries = log.get_log_entries(10, None);
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_get_slow_requests_datetime_range() {
        let mut log = SlowRequestsLog::new(2);
        let request = DummyLoggable;
        let t0 = Utc::now();
        let t = |secs| t0 + chrono::Duration::seconds(secs);

        log.log_request("col1", Duration::from_secs(2), t(1), &request, None);
        log.log_request("col2", Duration::from_secs(3), t(2), &request, None);
        // Too fast to be logged, but still a newer request in the col1 group
        log.log_request("col1", Duration::from_secs(1), t(3), &request, None);
        // Slower, replaces the logged col1 request
        log.log_request("col1", Duration::from_secs(5), t(4), &request, None);
        // Arrives out of order, an older request in the col1 group
        log.log_request("col1", Duration::from_secs(1), t(0), &request, None);

        let entries = log.get_log_entries(10, None);
        assert_eq!(entries.len(), 2);
        let col1 = entries
            .iter()
            .find(|e| e.collection_name == "col1")
            .unwrap();
        assert_eq!(col1.datetime, t(4));
        assert_eq!(col1.datetime_first, t(0));
        assert_eq!(col1.datetime_last, t(4));
        let col2 = entries
            .iter()
            .find(|e| e.collection_name == "col2")
            .unwrap();
        assert_eq!(col2.datetime, t(2));
        assert_eq!(col2.datetime_first, t(2));
        assert_eq!(col2.datetime_last, t(2));

        // Evicting col2 forgets its range, logging it again starts a new one
        let evicted = log.log_request("col3", Duration::from_secs(10), t(5), &request, None);
        assert_eq!(evicted.unwrap().collection_name, "col2");
        assert_eq!(log.datetime_ranges.len(), 2);
        log.log_request("col2", Duration::from_secs(20), t(6), &request, None);
        assert_eq!(log.datetime_ranges.len(), 2);

        let entries = log.get_log_entries(10, None);
        let col2 = entries
            .iter()
            .find(|e| e.collection_name == "col2")
            .unwrap();
        assert_eq!(col2.datetime_first, t(6));
        assert_eq!(col2.datetime_last, t(6));
    }
}
