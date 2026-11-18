//! Workload statistics: per-fingerprint counters of shard-level read operations,
//! comparable to `pg_stat_statements`.
//!
//! A fingerprint is the request with all literal values stripped (vectors, filter values,
//! ids, limits...), so requests of the same shape share one entry. Unlike the slow request
//! log, every request is counted, not only those over 50 ms.
//!
//! Only collected with the `query_profiling` beta feature flag: the fingerprint costs one
//! serialization of the request per shard operation.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::OnceLock;
use std::time::Duration;

use ahash::AHashMap;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;

use crate::operations::loggable::Loggable;

/// Version of the workload output schema.
pub const WORKLOAD_SCHEMA_VERSION: u32 = 1;

/// Fingerprints kept per node. When full, the entry with the least total time is evicted,
/// like `pg_stat_statements.max`.
const MAX_FINGERPRINTS: usize = 1024;

/// Normalized requests kept per fingerprint are cut to this many bytes of JSON, so the table
/// stays within `MAX_FINGERPRINTS * MAX_NORMALIZED_BYTES` (4 MiB) whatever the requests hold,
/// e.g. long payload paths in `with_payload`. The fingerprint hash covers the whole request.
const MAX_NORMALIZED_BYTES: usize = 4096;

/// Placeholder for a stripped literal.
const LITERAL: &str = "?";

/// Keys whose string values are literals (filter values, ids, documents, shard keys), not
/// structure. Numbers are always stripped; booleans and other strings (payload keys,
/// vector names, enum values like `fusion: rrf`) are kept, as they change the plan.
const LITERAL_KEYS: &[&str] = &[
    "value",
    "any",
    "except",
    "text",
    "text_any",
    "phrase",
    "gt",
    "gte",
    "lt",
    "lte",
    "has_id",
    "id",
    "ids",
    "offset",
    "start_from",
    "nearest",
    "positive",
    "negative",
    "target",
    "context",
    "image",
    "object",
    "shard_key",
    "shard_key_selector",
];

/// Strip literal values from a serialized request, in place.
///
/// Arrays are reduced to their distinct normalized elements, in first-seen order, so a batch
/// of 10 queries of the same shape has the fingerprint of one, and `should` over 3 values
/// of the same key has the fingerprint of one value (like PostgreSQL squashing `IN` lists).
pub fn normalize(value: &mut Value) {
    normalize_inner(value, false);
}

fn normalize_inner(value: &mut Value, literal: bool) {
    match value {
        Value::Null | Value::Bool(_) => {}
        Value::Number(_) => *value = Value::String(LITERAL.to_string()),
        Value::String(_) => {
            if literal {
                *value = Value::String(LITERAL.to_string());
            }
        }
        Value::Array(items) => {
            for item in items.iter_mut() {
                normalize_inner(item, literal);
            }
            let mut distinct: Vec<Value> = Vec::with_capacity(items.len().min(4));
            for item in items.drain(..) {
                if !distinct.contains(&item) {
                    distinct.push(item);
                }
            }
            *items = distinct;
        }
        Value::Object(map) => {
            for (key, item) in map.iter_mut() {
                normalize_inner(item, literal || LITERAL_KEYS.contains(&key.as_str()));
            }
        }
    }
}

/// Normalized shape of one request.
pub struct Fingerprint {
    pub hash: u64,
    pub request_name: &'static str,
    pub normalized: Value,
}

impl Fingerprint {
    pub fn of(collection_name: &str, request: &dyn Loggable) -> Self {
        let mut normalized = request.to_log_value();
        normalize(&mut normalized);
        let mut hasher = DefaultHasher::new();
        collection_name.hash(&mut hasher);
        request.request_name().hash(&mut hasher);
        // `Value` is not `Hash`; its canonical string is (object keys are sorted).
        let canonical = normalized.to_string();
        canonical.hash(&mut hasher);
        if canonical.len() > MAX_NORMALIZED_BYTES {
            let cut = canonical.floor_char_boundary(MAX_NORMALIZED_BYTES);
            normalized = Value::String(format!(
                "{}... ({} more bytes)",
                &canonical[..cut],
                canonical.len() - cut
            ));
        }
        Self {
            hash: hasher.finish(),
            request_name: request.request_name(),
            normalized,
        }
    }
}

/// Latency histogram with fixed log-scale buckets, so histograms of different nodes
/// can be merged exactly by adding bucket counts (for a cluster-wide view).
#[derive(Clone)]
struct Histogram {
    buckets: [u64; BUCKETS],
}

/// Upper bound of bucket `i` is `FIRST_BUCKET_MS * GROWTH^i`: 0.05 ms .. ~5 min, +-12.5%.
const BUCKETS: usize = 128;
const FIRST_BUCKET_MS: f64 = 0.05;
const GROWTH: f64 = 1.25;

impl Histogram {
    fn new() -> Self {
        Self {
            buckets: [0; BUCKETS],
        }
    }

    fn bucket(ms: f64) -> usize {
        if ms <= FIRST_BUCKET_MS {
            return 0;
        }
        let i = ((ms / FIRST_BUCKET_MS).ln() / GROWTH.ln()).ceil() as usize;
        i.min(BUCKETS - 1)
    }

    fn upper_bound_ms(i: usize) -> f64 {
        FIRST_BUCKET_MS * GROWTH.powi(i as i32)
    }

    fn record(&mut self, ms: f64) {
        self.buckets[Self::bucket(ms)] += 1;
    }

    /// Upper bound of the bucket holding quantile `q`, at most `max_ms`.
    fn quantile_ms(&self, q: f64, max_ms: f64) -> f64 {
        let total: u64 = self.buckets.iter().sum();
        if total == 0 {
            return 0.0;
        }
        let rank = ((total as f64) * q).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (i, count) in self.buckets.iter().enumerate() {
            seen += count;
            if seen >= rank {
                return Self::upper_bound_ms(i).min(max_ms);
            }
        }
        max_ms
    }
}

struct Entry {
    collection_name: String,
    request_name: &'static str,
    normalized: Value,
    calls: u64,
    errors: u64,
    total_ms: f64,
    min_ms: f64,
    max_ms: f64,
    histogram: Histogram,
    vector_scores: u64,
    quantized_vector_scores: u64,
    first_seen: DateTime<Utc>,
    last_seen: DateTime<Utc>,
}

/// Counters of one fingerprint on this node, as returned by `GET /profiler/workload`.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct WorkloadEntry {
    /// Hex hash of collection, request type and normalized request.
    pub fingerprint: String,
    pub collection_name: String,
    /// Shard-level operation: `query`, `scroll`, `count`, `facet`, `retrieve`.
    pub request_name: &'static str,
    /// The request with literal values replaced by `"?"`.
    pub normalized_request: Value,
    /// Shard operations counted. A request touching N local shards counts N times.
    pub calls: u64,
    pub errors: u64,
    pub total_time_ms: f64,
    pub mean_time_ms: f64,
    pub min_time_ms: f64,
    pub max_time_ms: f64,
    /// Upper bound of the histogram bucket (+-12.5%) holding the 50th percentile.
    pub p50_time_ms: f64,
    /// Upper bound of the histogram bucket (+-12.5%) holding the 95th percentile.
    pub p95_time_ms: f64,
    /// Points visited: distance computations against original vectors, in total.
    pub vector_scores: u64,
    /// Points visited: distance computations against quantized vectors, in total.
    pub quantized_vector_scores: u64,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}

/// Measurements of one shard operation.
pub struct WorkloadSample {
    pub duration: Duration,
    pub ok: bool,
    pub vector_scores: u64,
    pub quantized_vector_scores: u64,
}

#[derive(Default)]
pub struct WorkloadStats {
    entries: AHashMap<u64, Entry>,
    since: Option<DateTime<Utc>>,
    evicted: u64,
}

/// Sort order of [`WorkloadStats::entries`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadSort {
    #[default]
    TotalTime,
    MeanTime,
    P95Time,
    Calls,
    VectorScores,
}

impl WorkloadStats {
    pub fn record(
        &mut self,
        collection_name: &str,
        fingerprint: Fingerprint,
        sample: WorkloadSample,
    ) {
        let now = Utc::now();
        self.since.get_or_insert(now);
        let ms = sample.duration.as_secs_f64() * 1000.0;

        if !self.entries.contains_key(&fingerprint.hash) && self.entries.len() >= MAX_FINGERPRINTS {
            // Evict the entry with the least total time; it matters least for the workload.
            if let Some(victim) = self
                .entries
                .iter()
                .min_by(|a, b| a.1.total_ms.total_cmp(&b.1.total_ms))
                .map(|(hash, _)| *hash)
            {
                self.entries.remove(&victim);
                self.evicted += 1;
            }
        }

        let entry = self
            .entries
            .entry(fingerprint.hash)
            .or_insert_with(|| Entry {
                collection_name: collection_name.to_string(),
                request_name: fingerprint.request_name,
                normalized: fingerprint.normalized,
                calls: 0,
                errors: 0,
                total_ms: 0.0,
                min_ms: f64::INFINITY,
                max_ms: 0.0,
                histogram: Histogram::new(),
                vector_scores: 0,
                quantized_vector_scores: 0,
                first_seen: now,
                last_seen: now,
            });
        entry.calls += 1;
        entry.errors += u64::from(!sample.ok);
        entry.total_ms += ms;
        entry.min_ms = entry.min_ms.min(ms);
        entry.max_ms = entry.max_ms.max(ms);
        entry.histogram.record(ms);
        entry.vector_scores += sample.vector_scores;
        entry.quantized_vector_scores += sample.quantized_vector_scores;
        entry.last_seen = now;
    }

    pub fn entries(
        &self,
        limit: usize,
        collection_name: Option<&str>,
        sort: WorkloadSort,
    ) -> Vec<WorkloadEntry> {
        let key = |e: &Entry| match sort {
            WorkloadSort::TotalTime => e.total_ms,
            WorkloadSort::MeanTime => e.total_ms / e.calls.max(1) as f64,
            WorkloadSort::P95Time => e.histogram.quantile_ms(0.95, e.max_ms),
            WorkloadSort::Calls => e.calls as f64,
            WorkloadSort::VectorScores => (e.vector_scores + e.quantized_vector_scores) as f64,
        };
        // Select first, then clone only the selected entries
        let mut selected: Vec<(f64, u64, &Entry)> = self
            .entries
            .iter()
            .filter(|(_, e)| collection_name.is_none_or(|name| e.collection_name == name))
            .map(|(hash, e)| (key(e), *hash, e))
            .collect();
        selected.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
        selected.truncate(limit);
        selected
            .into_iter()
            .map(|(_, hash, e)| WorkloadEntry {
                fingerprint: format!("{hash:016x}"),
                collection_name: e.collection_name.clone(),
                request_name: e.request_name,
                normalized_request: e.normalized.clone(),
                calls: e.calls,
                errors: e.errors,
                total_time_ms: e.total_ms,
                mean_time_ms: e.total_ms / e.calls.max(1) as f64,
                min_time_ms: if e.calls == 0 { 0.0 } else { e.min_ms },
                max_time_ms: e.max_ms,
                p50_time_ms: e.histogram.quantile_ms(0.50, e.max_ms),
                p95_time_ms: e.histogram.quantile_ms(0.95, e.max_ms),
                vector_scores: e.vector_scores,
                quantized_vector_scores: e.quantized_vector_scores,
                first_seen: e.first_seen,
                last_seen: e.last_seen,
            })
            .collect()
    }

    pub fn since(&self) -> Option<DateTime<Utc>> {
        self.since
    }

    pub fn evicted(&self) -> u64 {
        self.evicted
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

static WORKLOAD: OnceLock<Mutex<WorkloadStats>> = OnceLock::new();

/// Whether workload statistics are collected on this node (`query_profiling` beta flag).
#[inline]
pub fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| common::flags::feature_flags().query_profiling)
}

pub fn global() -> &'static Mutex<WorkloadStats> {
    WORKLOAD.get_or_init(|| Mutex::new(WorkloadStats::default()))
}

/// Record one shard operation. Only call when [`enabled`].
pub fn record(collection_name: &str, fingerprint: Fingerprint, sample: WorkloadSample) {
    global().lock().record(collection_name, fingerprint, sample);
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_normalize_strips_literals_keeps_structure() {
        let mut a = json!({
            "filter": {"must": [
                {"key": "city", "match": {"value": "London"}},
                {"key": "price", "range": {"gte": 10, "lt": 20.5}},
                {"has_id": [1, 2, 3]},
            ]},
            "limit": 10,
            "exact": false,
            "using": "image",
            "fusion": "rrf",
        });
        let mut b = json!({
            "filter": {"must": [
                {"key": "city", "match": {"value": "Berlin"}},
                {"key": "price", "range": {"gte": 1, "lt": 2}},
                {"has_id": ["2d1c4e4e-0000-0000-0000-000000000000"]},
            ]},
            "limit": 100,
            "exact": false,
            "using": "image",
            "fusion": "rrf",
        });
        normalize(&mut a);
        normalize(&mut b);
        assert_eq!(a, b);
        assert_eq!(a["filter"]["must"][0]["key"], "city");
        assert_eq!(a["filter"]["must"][0]["match"]["value"], "?");
        assert_eq!(a["using"], "image");
        assert_eq!(a["fusion"], "rrf");

        // Different payload key, vector name or flag: different fingerprint
        let mut c = json!({"filter": {"must": [{"key": "country", "match": {"value": "UK"}}]}});
        let mut d = json!({"filter": {"must": [{"key": "city", "match": {"value": "UK"}}]}});
        normalize(&mut c);
        normalize(&mut d);
        assert_ne!(c, d);
    }

    #[test]
    fn test_normalize_squashes_arrays() {
        let mut a = json!({"should": [
            {"key": "tag", "match": {"value": "a"}},
            {"key": "tag", "match": {"value": "b"}},
            {"key": "tag", "match": {"value": "c"}},
        ], "vector": [0.1, 0.2, 0.3]});
        normalize(&mut a);
        assert_eq!(
            a,
            json!({"should": [{"key": "tag", "match": {"value": "?"}}], "vector": ["?"]})
        );
    }

    #[test]
    fn test_histogram_quantiles() {
        let mut h = Histogram::new();
        for ms in 1..=100 {
            h.record(f64::from(ms));
        }
        let p95 = h.quantile_ms(0.95, 100.0);
        assert!((95.0..=95.0 * GROWTH).contains(&p95), "p95 = {p95}");
        let p50 = h.quantile_ms(0.50, 100.0);
        assert!((50.0..=50.0 * GROWTH).contains(&p50), "p50 = {p50}");
        assert_eq!(h.quantile_ms(1.0, 100.0), 100.0);
    }

    struct Req(Value);
    impl Loggable for Req {
        fn to_log_value(&self) -> Value {
            self.0.clone()
        }
        fn request_name(&self) -> &'static str {
            "query"
        }
        fn request_hash(&self) -> u64 {
            0
        }
    }

    #[test]
    fn test_stats_aggregate_by_fingerprint_and_collection() {
        let mut stats = WorkloadStats::default();
        let sample = |ms, scores| WorkloadSample {
            duration: Duration::from_millis(ms),
            ok: true,
            vector_scores: scores,
            quantized_vector_scores: 0,
        };
        let q = |v: &str| Req(json!({"filter": {"key": "city", "match": {"value": v}}}));
        for (i, city) in ["a", "b", "c", "d"].iter().enumerate() {
            stats.record(
                "c1",
                Fingerprint::of("c1", &q(city)),
                sample(10 * (i as u64 + 1), 100),
            );
        }
        stats.record("c2", Fingerprint::of("c2", &q("a")), sample(5, 1));

        let entries = stats.entries(10, None, WorkloadSort::TotalTime);
        assert_eq!(entries.len(), 2);
        let c1 = &entries[0];
        assert_eq!(c1.collection_name, "c1");
        assert_eq!(c1.calls, 4);
        assert_eq!(c1.total_time_ms, 100.0);
        assert_eq!(c1.mean_time_ms, 25.0);
        assert_eq!(c1.min_time_ms, 10.0);
        assert_eq!(c1.max_time_ms, 40.0);
        assert_eq!(c1.vector_scores, 400);
        assert_eq!(c1.normalized_request["filter"]["match"]["value"], "?");

        let only_c2 = stats.entries(10, Some("c2"), WorkloadSort::Calls);
        assert_eq!(only_c2.len(), 1);
        assert_eq!(only_c2[0].calls, 1);
    }

    #[test]
    fn test_normalized_request_is_capped() {
        let long_path = "x".repeat(10 * MAX_NORMALIZED_BYTES);
        let a = Fingerprint::of(
            "c",
            &Req(json!({"with_payload": [long_path.clone() + "a"]})),
        );
        let b = Fingerprint::of("c", &Req(json!({"with_payload": [long_path + "b"]})));
        // Distinct requests keep distinct fingerprints, but store a bounded shape
        assert_ne!(a.hash, b.hash);
        let stored = a.normalized.as_str().unwrap();
        assert!(stored.len() < MAX_NORMALIZED_BYTES + 64);
        assert!(stored.ends_with("more bytes)"));
    }

    #[test]
    fn test_eviction_keeps_heaviest() {
        let mut stats = WorkloadStats::default();
        for i in 0..MAX_FINGERPRINTS + 1 {
            let req = Req(json!({"key": format!("k{i}")}));
            stats.record(
                "c",
                Fingerprint::of("c", &req),
                WorkloadSample {
                    // The first one is the lightest and gets evicted
                    duration: Duration::from_millis(i as u64 + 1),
                    ok: true,
                    vector_scores: 0,
                    quantized_vector_scores: 0,
                },
            );
        }
        assert_eq!(stats.entries.len(), MAX_FINGERPRINTS);
        assert_eq!(stats.evicted(), 1);
        let all = stats.entries(usize::MAX, None, WorkloadSort::TotalTime);
        assert!(all.iter().all(|e| e.normalized_request["key"] != "k0"));
    }
}
