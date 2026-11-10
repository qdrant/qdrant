//! Counters for remote indexing (§4.9), exposed by the server in `/metrics`.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct RemoteIndexingMetrics {
    /// Tasks registered, one per submission attempt.
    pub tasks_registered: AtomicU64,
    /// Submissions the entry point refused or could not be reached for.
    pub submit_failures: AtomicU64,
    /// Graphs built remotely and used.
    pub tasks_succeeded: AtomicU64,
    /// Failures reported by the indexer.
    pub tasks_failed: AtomicU64,
    /// Leases that expired without a heartbeat.
    pub heartbeats_missed: AtomicU64,
    /// Tasks the indexer did not fetch within the queue lease.
    pub queue_timeouts: AtomicU64,
    /// Heartbeats received.
    pub heartbeats: AtomicU64,
    /// Uploaded graphs rejected by validation.
    pub validation_failures: AtomicU64,
    /// Resubmissions with a new token.
    pub retries: AtomicU64,
    /// Builds that went back to the local builder.
    pub fallbacks: AtomicU64,
    /// Tasks cancelled because the optimization stopped.
    pub cancelled: AtomicU64,
    /// Large enough builds kept local because an old graph is healed instead (D12).
    pub local_for_healing: AtomicU64,
    /// Vector bytes served to indexers.
    pub bytes_served: AtomicU64,
    /// Graph bytes received from indexers.
    pub graph_bytes_received: AtomicU64,
    /// Time from submission to `GetTask`, summed over tasks.
    pub queued_time_ms: AtomicU64,
    /// Time from `GetTask` to the outcome, summed over tasks.
    pub running_time_ms: AtomicU64,
    /// Time spent turning uploaded graphs into local ones, summed over tasks.
    pub accept_time_ms: AtomicU64,
}

pub static REMOTE_INDEXING_METRICS: RemoteIndexingMetrics = RemoteIndexingMetrics {
    tasks_registered: AtomicU64::new(0),
    submit_failures: AtomicU64::new(0),
    tasks_succeeded: AtomicU64::new(0),
    tasks_failed: AtomicU64::new(0),
    heartbeats_missed: AtomicU64::new(0),
    queue_timeouts: AtomicU64::new(0),
    heartbeats: AtomicU64::new(0),
    validation_failures: AtomicU64::new(0),
    retries: AtomicU64::new(0),
    fallbacks: AtomicU64::new(0),
    cancelled: AtomicU64::new(0),
    local_for_healing: AtomicU64::new(0),
    bytes_served: AtomicU64::new(0),
    graph_bytes_received: AtomicU64::new(0),
    queued_time_ms: AtomicU64::new(0),
    running_time_ms: AtomicU64::new(0),
    accept_time_ms: AtomicU64::new(0),
};

impl RemoteIndexingMetrics {
    /// `(name, help, value)` of every counter, for exporters.
    pub fn snapshot(&self) -> Vec<(&'static str, &'static str, u64)> {
        let Self {
            tasks_registered,
            submit_failures,
            tasks_succeeded,
            tasks_failed,
            heartbeats_missed,
            queue_timeouts,
            heartbeats,
            validation_failures,
            retries,
            fallbacks,
            cancelled,
            local_for_healing,
            bytes_served,
            graph_bytes_received,
            queued_time_ms,
            running_time_ms,
            accept_time_ms,
        } = self;
        let get = |c: &AtomicU64| c.load(Ordering::Relaxed);
        vec![
            ("tasks_registered", "remote indexing tasks registered, one per attempt", get(tasks_registered)),
            ("submit_failures", "remote indexing task submissions that failed", get(submit_failures)),
            ("tasks_succeeded", "graphs built remotely and used", get(tasks_succeeded)),
            ("tasks_failed", "remote indexing failures reported by the indexer", get(tasks_failed)),
            ("heartbeats_missed", "remote indexing leases expired without heartbeat", get(heartbeats_missed)),
            ("queue_timeouts", "remote indexing tasks not fetched within the queue lease", get(queue_timeouts)),
            ("heartbeats", "remote indexing heartbeats received", get(heartbeats)),
            ("validation_failures", "uploaded graphs rejected by validation", get(validation_failures)),
            ("retries", "remote indexing resubmissions with a new token", get(retries)),
            ("fallbacks", "remote builds that fell back to the local builder", get(fallbacks)),
            ("cancelled", "remote indexing tasks cancelled by core", get(cancelled)),
            ("local_for_healing", "builds large enough for remote indexing, kept local to heal an old graph", get(local_for_healing)),
            ("bytes_served", "vector bytes served to indexers", get(bytes_served)),
            ("graph_bytes_received", "graph bytes received from indexers", get(graph_bytes_received)),
            ("queued_time_ms", "time from submission to GetTask, summed", get(queued_time_ms)),
            ("running_time_ms", "time from GetTask to outcome, summed", get(running_time_ms)),
            ("accept_time_ms", "time spent converting uploaded graphs, summed", get(accept_time_ms)),
        ]
    }
}
