use std::cell::RefCell;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::Serialize;

/// Maximum number of spike records to retain.
const MAX_SPIKE_RECORDS: usize = 100;

/// State of computation when the spike scope was active.
#[derive(Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SpikeState {
    /// Time spent waiting for async operations
    Awaiting,
    /// Time spent in synchronous computation
    Processing,
}

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
    /// State of computation (awaiting async vs synchronous processing)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<SpikeState>,
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
// Thread-local profiler state (used within sync sections)
// ---------------------------------------------------------------------------

/// A scope being built on the stack.
struct BuildingNode {
    name: &'static str,
    start: Instant,
    children: Vec<SpikeRecord>,
}

struct SpikeProfilerState {
    /// Stack of in-progress nested scopes.
    stack: Vec<BuildingNode>,
    /// Completed top-level children (depth-0 scopes that have finished).
    completed_children: Vec<SpikeRecord>,
}

thread_local! {
    static PROFILER_STATE: RefCell<Option<SpikeProfilerState>> = const { RefCell::new(None) };
}

// ---------------------------------------------------------------------------
// SpikeProfilerHandle — thread-safe handle passed across async boundaries
// ---------------------------------------------------------------------------

struct HandleInner {
    name: &'static str,
    start: Instant,
    children: Vec<SpikeRecord>,
}

/// Thread-safe handle for spike profiling across async boundaries.
///
/// Created by [`start_spiked_scope`]. Can be cloned and passed to other
/// threads/tasks. Child records from sync sections and await sections are
/// collected into this handle.
#[derive(Clone)]
pub struct SpikeProfilerHandle {
    inner: Arc<Mutex<HandleInner>>,
}

impl SpikeProfilerHandle {
    /// Start a synchronous processing section.
    ///
    /// Sets up thread-local profiling state so that [`spiked_scope`] calls
    /// within this section build a nested tree. On drop, the collected tree
    /// is added to this handle with `state = Processing`.
    #[must_use]
    pub fn sync_section(&self, name: &'static str) -> SyncSectionGuard {
        PROFILER_STATE.with(|state| {
            *state.borrow_mut() = Some(SpikeProfilerState {
                stack: Vec::new(),
                completed_children: Vec::new(),
            });
        });
        SyncSectionGuard {
            handle: self.clone(),
            name,
            start: Instant::now(),
        }
    }

    /// Start an awaiting section. On drop, records the elapsed time with
    /// `state = Awaiting`.
    #[must_use]
    pub fn await_section(&self, name: &'static str) -> AwaitSectionGuard {
        AwaitSectionGuard {
            handle: self.clone(),
            name,
            start: Instant::now(),
        }
    }

    /// Create a child scope that nests under this handle.
    ///
    /// Returns a guard and a new child handle. Pass the child handle to callees
    /// so their sections become nested children. When the guard drops, the
    /// child's record (with all its collected children) is appended to this
    /// handle's children list.
    #[must_use]
    pub fn child_scope(&self, name: &'static str) -> (ChildScopeGuard, SpikeProfilerHandle) {
        let child_handle = SpikeProfilerHandle {
            inner: Arc::new(Mutex::new(HandleInner {
                name,
                start: Instant::now(),
                children: Vec::new(),
            })),
        };
        let guard = ChildScopeGuard {
            parent: self.clone(),
            child_handle: Some(child_handle.clone()),
        };
        (guard, child_handle)
    }
}

/// Combined guard + handle for a child spike scope.
///
/// Acts as an RAII guard (records profiling data on drop) and provides
/// the child handle for passing to nested functions via [`handle()`](Self::handle).
///
/// Created by [`child_spike_scope`]. When profiling is inactive (`None` input),
/// this is a no-op wrapper.
#[must_use]
pub struct ChildSpikeScope {
    _guard: Option<ChildScopeGuard>,
    child_handle: Option<SpikeProfilerHandle>,
}

impl ChildSpikeScope {
    /// Get the child handle for passing to nested functions.
    ///
    /// Returns `None` when profiling is inactive.
    pub fn handle(&self) -> Option<SpikeProfilerHandle> {
        self.child_handle.clone()
    }
}

/// Create a child scope from an `Option<SpikeProfilerHandle>`.
///
/// Returns a [`ChildSpikeScope`] that acts as both a guard and a handle source.
/// When the input is `None`, this is a no-op.
pub fn child_spike_scope(
    handle: &Option<SpikeProfilerHandle>,
    name: &'static str,
) -> ChildSpikeScope {
    match handle {
        Some(h) => {
            let (guard, child) = h.child_scope(name);
            ChildSpikeScope {
                _guard: Some(guard),
                child_handle: Some(child),
            }
        }
        None => ChildSpikeScope {
            _guard: None,
            child_handle: None,
        },
    }
}

// ---------------------------------------------------------------------------
// start_spiked_scope — top-level entry point
// ---------------------------------------------------------------------------

/// RAII guard for a top-level spiked scope.
///
/// On drop, saves the collected flame graph tree. The global storage keeps
/// only the N greatest spikes by duration.
#[must_use]
pub struct SpikedScopeGuard {
    handle: Option<SpikeProfilerHandle>,
}

/// Start a top-level profiled scope. Returns a guard and a handle.
///
/// The guard stores the completed spike record on drop. The handle can be
/// cloned and passed across async boundaries to collect profiling data from
/// sync sections and await sections.
#[must_use]
pub fn start_spiked_scope(name: &'static str) -> (SpikedScopeGuard, SpikeProfilerHandle) {
    let handle = SpikeProfilerHandle {
        inner: Arc::new(Mutex::new(HandleInner {
            name,
            start: Instant::now(),
            children: Vec::new(),
        })),
    };
    let guard = SpikedScopeGuard {
        handle: Some(handle.clone()),
    };
    (guard, handle)
}

impl Drop for SpikedScopeGuard {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let mut inner = handle.inner.lock();
            let record = SpikeRecord {
                timestamp: Some(Utc::now()),
                name: inner.name,
                duration_ms: inner.start.elapsed().as_secs_f64() * 1000.0,
                state: None,
                children: std::mem::take(&mut inner.children),
            };
            drop(inner);
            store_spike(record);
        }
    }
}

// ---------------------------------------------------------------------------
// SyncSectionGuard — collects thread-local profiling tree
// ---------------------------------------------------------------------------

/// Guard for a synchronous processing section within a spike-profiled scope.
///
/// On drop, collects the thread-local profiling tree built by [`spiked_scope`]
/// and adds it to the parent handle as a child with `state = Processing`.
#[must_use]
pub struct SyncSectionGuard {
    handle: SpikeProfilerHandle,
    name: &'static str,
    start: Instant,
}

impl Drop for SyncSectionGuard {
    fn drop(&mut self) {
        let children = PROFILER_STATE.with(|state| {
            state
                .borrow_mut()
                .take()
                .map(|p| p.completed_children)
                .unwrap_or_default()
        });

        let duration_ms = self.start.elapsed().as_secs_f64() * 1000.0;
        let record = SpikeRecord {
            timestamp: None,
            name: self.name,
            duration_ms,
            state: Some(SpikeState::Processing),
            children,
        };
        self.handle.inner.lock().children.push(record);
    }
}

// ---------------------------------------------------------------------------
// AwaitSectionGuard — records awaiting time
// ---------------------------------------------------------------------------

/// Guard for an awaiting section. On drop, records elapsed time with
/// `state = Awaiting`.
#[must_use]
pub struct AwaitSectionGuard {
    handle: SpikeProfilerHandle,
    name: &'static str,
    start: Instant,
}

impl Drop for AwaitSectionGuard {
    fn drop(&mut self) {
        let duration_ms = self.start.elapsed().as_secs_f64() * 1000.0;
        let record = SpikeRecord {
            timestamp: None,
            name: self.name,
            duration_ms,
            state: Some(SpikeState::Awaiting),
            children: Vec::new(),
        };
        self.handle.inner.lock().children.push(record);
    }
}

// ---------------------------------------------------------------------------
// ChildScopeGuard — nests child handle's records under a parent
// ---------------------------------------------------------------------------

/// Guard for a child scope. On drop, collects the child handle's records and
/// appends them as a single nested child of the parent handle.
#[must_use]
pub struct ChildScopeGuard {
    parent: SpikeProfilerHandle,
    child_handle: Option<SpikeProfilerHandle>,
}

impl Drop for ChildScopeGuard {
    fn drop(&mut self) {
        if let Some(child) = self.child_handle.take() {
            let mut inner = child.inner.lock();
            let record = SpikeRecord {
                timestamp: None,
                name: inner.name,
                duration_ms: inner.start.elapsed().as_secs_f64() * 1000.0,
                state: None,
                children: std::mem::take(&mut inner.children),
            };
            drop(inner);
            self.parent.inner.lock().children.push(record);
        }
    }
}

// ---------------------------------------------------------------------------
// spiked_scope — nested scope within a sync section
// ---------------------------------------------------------------------------

/// RAII guard for a nested spiked scope. No-op when profiling is not active.
#[must_use]
pub struct SpikedNestedGuard {
    active: bool,
}

/// Define a nested profiling scope. No-op if no sync section is active
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
                        state: None,
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
            let (_guard, handle) = start_spiked_scope("slow_op");
            let _sync = handle.sync_section("inner_sync");
            let _scope = spiked_scope("inner");
            std::thread::sleep(Duration::from_millis(5));
        }
        let spikes = get_spike_records();
        assert_eq!(spikes.len(), 1);
        assert_eq!(spikes[0].name, "slow_op");
        assert_eq!(spikes[0].children.len(), 1);
        assert_eq!(spikes[0].children[0].name, "inner_sync");
        assert!(spikes[0].children[0].state.is_some());
        assert_eq!(spikes[0].children[0].children.len(), 1);
        assert_eq!(spikes[0].children[0].children[0].name, "inner");
    }

    #[test]
    fn test_nested_tree_structure() {
        let _lock = TEST_MUTEX.lock();
        clear_spike_records();
        {
            let (_guard, handle) = start_spiked_scope("root");
            {
                let _sync = handle.sync_section("sync_section");
                {
                    let _a = spiked_scope("level_0");
                    {
                        let _b = spiked_scope("level_1");
                        std::thread::sleep(Duration::from_millis(5));
                    }
                }
            }
        }
        let spikes = get_spike_records();
        assert!(!spikes.is_empty());

        let root = spikes.iter().find(|s| s.name == "root").unwrap();
        assert!(root.timestamp.is_some());
        assert_eq!(root.children.len(), 1);

        let sync_section = &root.children[0];
        assert_eq!(sync_section.name, "sync_section");
        assert!(matches!(sync_section.state, Some(SpikeState::Processing)));
        assert_eq!(sync_section.children.len(), 1);

        let level_0 = &sync_section.children[0];
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
            let (_guard, handle) = start_spiked_scope("root");
            {
                let _sync = handle.sync_section("sync");
                {
                    let _a = spiked_scope("first");
                    std::thread::sleep(Duration::from_millis(3));
                }
                {
                    let _b = spiked_scope("second");
                    std::thread::sleep(Duration::from_millis(3));
                }
            }
        }
        let spikes = get_spike_records();
        let root = spikes.iter().find(|s| s.name == "root").unwrap();
        let sync = &root.children[0];
        assert_eq!(sync.children.len(), 2);
        assert_eq!(sync.children[0].name, "first");
        assert_eq!(sync.children[1].name, "second");
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
            let (_guard, handle) = start_spiked_scope("op");
            let _sync = handle.sync_section("work");
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
            let (_guard, handle) = start_spiked_scope("big");
            let _sync = handle.sync_section("work");
            std::thread::sleep(Duration::from_millis(2));
        }
        let before = get_spike_records();
        assert_eq!(before.len(), MAX_SPIKE_RECORDS);
        let min_before = before.last().unwrap().duration_ms;

        {
            let (_guard, _handle) = start_spiked_scope("tiny");
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
            state: None,
            children: vec![
                SpikeRecord {
                    timestamp: None,
                    name: "segment_lock",
                    duration_ms: 510.2,
                    state: Some(SpikeState::Processing),
                    children: vec![],
                },
                SpikeRecord {
                    timestamp: None,
                    name: "search_batch",
                    duration_ms: 12.1,
                    state: Some(SpikeState::Awaiting),
                    children: vec![],
                },
            ],
        };
        let json = serde_json::to_value(&record).unwrap();
        assert!(json.get("timestamp").is_some());
        let children = json.get("children").unwrap().as_array().unwrap();
        assert_eq!(children.len(), 2);
        assert_eq!(children[0]["name"], "segment_lock");
        assert_eq!(children[0]["state"], "processing");
        assert_eq!(children[1]["state"], "awaiting");
        assert!(children[0].get("children").is_none());
        assert!(children[0].get("timestamp").is_none());
    }

    #[test]
    fn test_await_section() {
        let _lock = TEST_MUTEX.lock();
        clear_spike_records();
        {
            let (_guard, handle) = start_spiked_scope("root");
            {
                let _await = handle.await_section("waiting_for_shard");
                std::thread::sleep(Duration::from_millis(5));
            }
            {
                let _sync = handle.sync_section("processing");
                std::thread::sleep(Duration::from_millis(3));
            }
        }
        let spikes = get_spike_records();
        let root = spikes.iter().find(|s| s.name == "root").unwrap();
        assert_eq!(root.children.len(), 2);
        assert_eq!(root.children[0].name, "waiting_for_shard");
        assert!(matches!(root.children[0].state, Some(SpikeState::Awaiting)));
        assert_eq!(root.children[1].name, "processing");
        assert!(matches!(
            root.children[1].state,
            Some(SpikeState::Processing)
        ));
    }

    #[test]
    fn test_handle_across_threads() {
        let _lock = TEST_MUTEX.lock();
        clear_spike_records();
        {
            let (_guard, handle) = start_spiked_scope("root");
            let h1 = handle.clone();
            let h2 = handle.clone();
            let t1 = std::thread::spawn(move || {
                let _sync = h1.sync_section("thread_1");
                let _scope = spiked_scope("work_1");
                std::thread::sleep(Duration::from_millis(5));
            });
            let t2 = std::thread::spawn(move || {
                let _sync = h2.sync_section("thread_2");
                let _scope = spiked_scope("work_2");
                std::thread::sleep(Duration::from_millis(5));
            });
            t1.join().unwrap();
            t2.join().unwrap();
        }
        let spikes = get_spike_records();
        let root = spikes.iter().find(|s| s.name == "root").unwrap();
        assert_eq!(root.children.len(), 2);
        let names: Vec<_> = root.children.iter().map(|c| c.name).collect();
        assert!(names.contains(&"thread_1"));
        assert!(names.contains(&"thread_2"));
    }

    #[test]
    fn test_child_scope_nesting() {
        let _lock = TEST_MUTEX.lock();
        clear_spike_records();
        {
            let (_guard, handle) = start_spiked_scope("root");
            // Create a child scope (simulates passing handle to a callee)
            let (_child_guard, child_handle) = handle.child_scope("level_1");
            {
                let _await = child_handle.await_section("leaf_await");
                std::thread::sleep(Duration::from_millis(3));
            }
            // Nested child scope
            let (_grandchild_guard, grandchild_handle) = child_handle.child_scope("level_2");
            {
                let _await = grandchild_handle.await_section("deep_leaf");
                std::thread::sleep(Duration::from_millis(3));
            }
            drop(_grandchild_guard);
            drop(_child_guard);
        }
        let spikes = get_spike_records();
        let root = spikes.iter().find(|s| s.name == "root").unwrap();
        // root should have one child: level_1
        assert_eq!(root.children.len(), 1);
        let level_1 = &root.children[0];
        assert_eq!(level_1.name, "level_1");
        assert!(level_1.state.is_none()); // child scope, not await/sync
        // level_1 should have two children: leaf_await and level_2
        assert_eq!(level_1.children.len(), 2);
        assert_eq!(level_1.children[0].name, "leaf_await");
        assert!(matches!(
            level_1.children[0].state,
            Some(SpikeState::Awaiting)
        ));
        let level_2 = &level_1.children[1];
        assert_eq!(level_2.name, "level_2");
        assert_eq!(level_2.children.len(), 1);
        assert_eq!(level_2.children[0].name, "deep_leaf");
    }

    #[test]
    fn test_child_spike_scope_helper() {
        let _lock = TEST_MUTEX.lock();
        clear_spike_records();
        // With None handle
        let scope = child_spike_scope(&None, "should_not_exist");
        assert!(scope.handle().is_none());

        // With Some handle
        {
            let (_guard, handle) = start_spiked_scope("root");
            let spike_handle = Some(handle);
            let child_scope = child_spike_scope(&spike_handle, "child");
            let ch = child_scope.handle().unwrap();
            {
                let _a = ch.await_section("inner");
                std::thread::sleep(Duration::from_millis(3));
            }
            drop(child_scope);
        }
        let spikes = get_spike_records();
        let root = spikes.iter().find(|s| s.name == "root").unwrap();
        assert_eq!(root.children.len(), 1);
        assert_eq!(root.children[0].name, "child");
        assert_eq!(root.children[0].children.len(), 1);
        assert_eq!(root.children[0].children[0].name, "inner");
    }
}
