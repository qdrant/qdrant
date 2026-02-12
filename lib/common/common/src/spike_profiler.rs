use std::cell::RefCell;
use std::time::Instant;

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::Serialize;

/// Maximum number of spike records to retain.
const MAX_SPIKE_RECORDS: usize = 100;

/// A spike record representing a profiled scope. The tree structure mirrors the
/// call stack: each node has a name, duration, and nested children.
///
/// Top-level records (from `start_spiked_scope`) also carry a `timestamp`.
#[derive(Serialize, Clone, Debug, JsonSchema)]
pub struct SpikeRecord {
    /// When the spike was recorded (only present on root nodes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<DateTime<Utc>>,
    /// Name of this scope
    pub name: &'static str,
    /// Duration this scope was active, in milliseconds
    pub duration_ms: f64,
    /// Nested child scopes
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<SpikeRecord>,
}

// ---------------------------------------------------------------------------
// Global storage
// ---------------------------------------------------------------------------

/// Global storage for recorded spikes, kept sorted by duration_ms descending.
/// When at capacity, the smallest spike is evicted to make room for a larger one.
static SPIKE_RECORDS: Mutex<Vec<SpikeRecord>> = Mutex::new(Vec::new());

/// Retrieve all recorded spikes (greatest duration first).
pub fn get_spike_records() -> Vec<SpikeRecord> {
    SPIKE_RECORDS.lock().clone()
}

/// Clear all recorded spikes.
pub fn clear_spike_records() {
    SPIKE_RECORDS.lock().clear();
}

fn sort_descending(records: &mut [SpikeRecord]) {
    records.sort_unstable_by(|a, b| {
        b.duration_ms
            .partial_cmp(&a.duration_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
}

/// Store a spike record. Keeps the N greatest spikes by duration.
/// If at capacity, the new record replaces the smallest stored spike
/// only if it is larger.
fn store_spike(record: SpikeRecord) {
    let mut records = SPIKE_RECORDS.lock();
    if records.len() < MAX_SPIKE_RECORDS {
        records.push(record);
        sort_descending(&mut records);
    } else {
        // The smallest spike is at the end (descending order).
        let min_duration = records.last().map(|r| r.duration_ms).unwrap_or(0.0);
        if record.duration_ms > min_duration {
            *records.last_mut().unwrap() = record;
            sort_descending(&mut records);
        }
    }
}

// ---------------------------------------------------------------------------
// Thread-local profiler state
// ---------------------------------------------------------------------------

/// A scope being built on the stack.
struct BuildingNode {
    name: &'static str,
    start: Instant,
    children: Vec<SpikeRecord>,
}

struct SpikeProfilerState {
    scope_name: &'static str,
    start: Instant,
    /// Stack of in-progress nested scopes.
    stack: Vec<BuildingNode>,
    /// Completed top-level children (depth-0 scopes that have finished).
    completed_children: Vec<SpikeRecord>,
}

thread_local! {
    static PROFILER_STATE: RefCell<Option<SpikeProfilerState>> = const { RefCell::new(None) };
}

// ---------------------------------------------------------------------------
// start_spiked_scope
// ---------------------------------------------------------------------------

/// RAII guard for a top-level spiked scope.
///
/// On drop, saves the collected flame graph tree. The global storage keeps
/// only the N greatest spikes by duration.
#[must_use]
pub struct SpikedScopeGuard {
    active: bool,
}

/// Start a top-level profiled scope. Call this inside `spawn_blocking` closures.
///
/// Returns a guard that, on drop, stores the collected flame graph tree into
/// global storage (which keeps only the N greatest spikes by duration).
///
/// If another `start_spiked_scope` is already active on this thread, the returned
/// guard is a no-op.
#[must_use]
pub fn start_spiked_scope(name: &'static str) -> SpikedScopeGuard {
    PROFILER_STATE.with(|state| {
        let mut state = state.borrow_mut();
        if state.is_some() {
            return SpikedScopeGuard { active: false };
        }
        *state = Some(SpikeProfilerState {
            scope_name: name,
            start: Instant::now(),
            stack: Vec::new(),
            completed_children: Vec::new(),
        });
        SpikedScopeGuard { active: true }
    })
}

impl Drop for SpikedScopeGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        PROFILER_STATE.with(|state| {
            let profiler = state.borrow_mut().take();
            if let Some(profiler) = profiler {
                let elapsed = profiler.start.elapsed();
                let record = SpikeRecord {
                    timestamp: Some(Utc::now()),
                    name: profiler.scope_name,
                    duration_ms: elapsed.as_secs_f64() * 1000.0,
                    children: profiler.completed_children,
                };
                store_spike(record);
            }
        });
    }
}

// ---------------------------------------------------------------------------
// spiked_scope
// ---------------------------------------------------------------------------

/// RAII guard for a nested spiked scope. No-op when profiling is not active.
#[must_use]
pub struct SpikedNestedGuard {
    active: bool,
}

/// Define a nested profiling scope. No-op if no `start_spiked_scope` is active
/// on this thread.
///
/// On drop, the scope's duration and any children it accumulated are added to
/// the parent scope (or to the root's child list).
#[must_use]
pub fn spiked_scope(name: &'static str) -> SpikedNestedGuard {
    let active = PROFILER_STATE.with(|state| {
        let mut state = state.borrow_mut();
        if let Some(ref mut profiler) = *state {
            profiler.stack.push(BuildingNode {
                name,
                start: Instant::now(),
                children: Vec::new(),
            });
            true
        } else {
            false
        }
    });

    SpikedNestedGuard { active }
}

impl Drop for SpikedNestedGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        PROFILER_STATE.with(|state| {
            let mut state = state.borrow_mut();
            if let Some(ref mut profiler) = *state {
                if let Some(node) = profiler.stack.pop() {
                    let record = SpikeRecord {
                        timestamp: None,
                        name: node.name,
                        duration_ms: node.start.elapsed().as_secs_f64() * 1000.0,
                        children: node.children,
                    };
                    if let Some(parent) = profiler.stack.last_mut() {
                        parent.children.push(record);
                    } else {
                        profiler.completed_children.push(record);
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    // Tests share global state (SPIKE_RECORDS), so they must be serialized.
    static TEST_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn test_records_scope() {
        let _lock = TEST_MUTEX.lock();
        clear_spike_records();
        {
            let _guard = start_spiked_scope("slow_op");
            let _scope = spiked_scope("inner");
            std::thread::sleep(Duration::from_millis(5));
        }
        let spikes = get_spike_records();
        assert_eq!(spikes.len(), 1);
        assert_eq!(spikes[0].name, "slow_op");
        assert_eq!(spikes[0].children.len(), 1);
        assert_eq!(spikes[0].children[0].name, "inner");
        assert!(spikes[0].children[0].children.is_empty());
    }

    #[test]
    fn test_nested_tree_structure() {
        let _lock = TEST_MUTEX.lock();
        clear_spike_records();
        {
            let _guard = start_spiked_scope("root");
            {
                let _a = spiked_scope("level_0");
                {
                    let _b = spiked_scope("level_1");
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        }
        let spikes = get_spike_records();
        assert!(!spikes.is_empty());

        let root = spikes.iter().find(|s| s.name == "root").unwrap();
        assert!(root.timestamp.is_some());
        assert_eq!(root.children.len(), 1);

        let level_0 = &root.children[0];
        assert_eq!(level_0.name, "level_0");
        assert!(level_0.timestamp.is_none());
        assert_eq!(level_0.children.len(), 1);

        let level_1 = &level_0.children[0];
        assert_eq!(level_1.name, "level_1");
        assert!(level_1.children.is_empty());
    }

    #[test]
    fn test_sibling_scopes() {
        let _lock = TEST_MUTEX.lock();
        clear_spike_records();
        {
            let _guard = start_spiked_scope("root");
            {
                let _a = spiked_scope("first");
                std::thread::sleep(Duration::from_millis(3));
            }
            {
                let _b = spiked_scope("second");
                std::thread::sleep(Duration::from_millis(3));
            }
        }
        let spikes = get_spike_records();
        let root = spikes.iter().find(|s| s.name == "root").unwrap();
        assert_eq!(root.children.len(), 2);
        assert_eq!(root.children[0].name, "first");
        assert_eq!(root.children[1].name, "second");
    }

    #[test]
    fn test_spiked_scope_noop_without_start() {
        let _scope = spiked_scope("orphan");
        // No panic, no side effects
    }

    #[test]
    fn test_keeps_greatest_spikes() {
        let _lock = TEST_MUTEX.lock();
        clear_spike_records();

        for i in 0..MAX_SPIKE_RECORDS + 10 {
            let _guard = start_spiked_scope("op");
            std::thread::sleep(Duration::from_micros(i as u64 * 100));
        }
        let spikes = get_spike_records();
        assert_eq!(spikes.len(), MAX_SPIKE_RECORDS);

        for window in spikes.windows(2) {
            assert!(window[0].duration_ms >= window[1].duration_ms);
        }
    }

    #[test]
    fn test_small_spike_not_stored_when_full() {
        let _lock = TEST_MUTEX.lock();
        clear_spike_records();

        for _ in 0..MAX_SPIKE_RECORDS {
            let _guard = start_spiked_scope("big");
            std::thread::sleep(Duration::from_millis(2));
        }
        let before = get_spike_records();
        assert_eq!(before.len(), MAX_SPIKE_RECORDS);
        let min_before = before.last().unwrap().duration_ms;

        {
            let _guard = start_spiked_scope("tiny");
        }
        let after = get_spike_records();
        assert_eq!(after.len(), MAX_SPIKE_RECORDS);
        assert!(after.last().unwrap().duration_ms >= min_before);
    }

    #[test]
    fn test_json_structure() {
        let record = SpikeRecord {
            timestamp: Some(Utc::now()),
            name: "search_in_segment",
            duration_ms: 523.4,
            children: vec![
                SpikeRecord {
                    timestamp: None,
                    name: "segment_lock",
                    duration_ms: 510.2,
                    children: vec![],
                },
                SpikeRecord {
                    timestamp: None,
                    name: "search_batch",
                    duration_ms: 12.1,
                    children: vec![],
                },
            ],
        };
        let json = serde_json::to_value(&record).unwrap();
        assert!(json.get("timestamp").is_some());
        let children = json.get("children").unwrap().as_array().unwrap();
        assert_eq!(children.len(), 2);
        assert_eq!(children[0]["name"], "segment_lock");
        assert!(children[0].get("children").is_none());
        assert!(children[0].get("timestamp").is_none());
    }
}
