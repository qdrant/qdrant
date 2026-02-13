//! Hierarchical progress tracker.
//!
//! # Example
//!
//! ```text
//!                                                             now
//! ─────────────────────────── Time ────────────────────────────┴╶╶╶╶╶╶▶
//!
//! ├───────────────────── Segment Indexing ──────────────────────╶╶╶╶╶╶╶
//!  ├─────── Quantization ───────┤├──── HNSW Index Building ─────╶╶╶╶╶╶╶
//!   ├─ Vector A ─┤├─ Vector B ─┤  ├─ Vector A ─┤├─ Vector B ────╶╶╶╶╶╶╶
//! ```
//!
//! # Errors and Panic Safety
//!
//! Most of methods are infallible (not returning `Result`/`Option`).
//! On debug builds they might panic, on release builds they will fallback to
//! some placeholder behavior.
//!
//! Why? Because progress tracking is a non-critical feature, and we don't want
//! to abort segment building just because of a bug in progress tracking code.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Read-only view of a root progress node.
///
/// Keep it around to observe the progress from another thread.
#[derive(Clone, Debug)]
pub struct ProgressView {
    root: Arc<Mutex<ProgressNode>>,
    /// This field is redundant, but kept outside of the lock for faster access.
    started_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ProgressTree {
    /// Name of the operation.
    pub name: String,

    /// When the operation started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,

    /// When the operation finished.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,

    /// For finished operations, how long they took, in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_sec: Option<f64>,

    /// Number of completed units of work, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub done: Option<u64>,

    /// Total number of units of work, if applicable and known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,

    /// Child operations.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<ProgressTree>,
}

impl ProgressView {
    pub fn snapshot(&self, root: impl Into<String>) -> ProgressTree {
        self.root.lock().render(root.into())
    }

    /// The same as `self.snapshot("").started_at().unwrap()`.
    pub fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }
}

/// Write-only handle to report the progress of operation.
///
/// Might be root node or a sub-task.
pub struct ProgressTracker {
    root: Arc<Mutex<ProgressNode>>,
    path: Vec<usize>,
}

#[derive(Debug)]
pub struct ProgressNode {
    /// Sub-tasks of this task, with their names, in order of creation.
    children: Vec<(String, ProgressNode)>,
    progress: Option<NodeProgress>,
    state: ProgressState,
}

#[derive(Debug)]
struct NodeProgress {
    current: Arc<AtomicU64>,
    total: Option<u64>,
}

#[derive(Debug)]
enum ProgressState {
    Pending,
    InProgress {
        started_at: DateTime<Utc>,
        started_instant: Instant,
    },
    Finished {
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        duration: Duration,
    },
}

/// Create a new root progress tracker.
///
/// Returns a read-only [`ProgressView`] to observe the progress,
/// and a write-only [`ProgressTracker`] to signal progress updates.
pub fn new_progress_tracker() -> (ProgressView, ProgressTracker) {
    let started_at = Utc::now();
    let started_instant = Instant::now();
    let root = Arc::new(Mutex::new(ProgressNode {
        children: Vec::new(),
        progress: None,
        state: ProgressState::InProgress {
            started_at,
            started_instant,
        },
    }));
    (
        ProgressView {
            root: root.clone(),
            started_at,
        },
        ProgressTracker {
            root,
            path: Vec::new(),
        },
    )
}

impl ProgressTracker {
    #[cfg(any(test, feature = "testing"))]
    pub fn new_for_test() -> Self {
        new_progress_tracker().1
    }

    /// Create a pending subtask.
    pub fn subtask(&self, name: impl Into<String>) -> ProgressTracker {
        self.subtask_impl(name.into(), true)
    }

    /// Similar to creating a [`Self::subtask()`], then immediately calling
    /// [`Self::start()`] on it.
    pub fn running_subtask(&self, name: impl Into<String>) -> ProgressTracker {
        self.subtask_impl(name.into(), false)
    }

    fn subtask_impl(&self, name: String, pending: bool) -> ProgressTracker {
        let mut root = self.root.lock();
        if let Some(parent) = root.get_mut(&self.path) {
            let mut path = Vec::with_capacity(self.path.len() + 1);
            path.extend_from_slice(&self.path);
            path.push(parent.children.len());

            parent.children.push((
                name,
                ProgressNode {
                    children: Vec::new(),
                    progress: None,
                    state: if pending {
                        ProgressState::Pending
                    } else {
                        ProgressState::InProgress {
                            started_at: Utc::now(),
                            started_instant: Instant::now(),
                        }
                    },
                },
            ));

            ProgressTracker {
                root: self.root.clone(),
                path,
            }
        } else {
            // Should never happen. But if it does, return an obviously invalid
            // path to avoid a panic.
            debug_assert!(false, "bug: invalid path when creating subtask");
            ProgressTracker {
                root: self.root.clone(),
                path: vec![usize::MAX, usize::MAX],
            }
        }
    }

    /// Enable progress tracking for this task.
    ///
    /// Accepts the total number of units of work, if known.
    /// Returns a counter that the caller should increment to report progress.
    /// Before entering hot loops, don't forget to call `Arc::deref` on it.
    pub fn track_progress(&self, total: Option<u64>) -> Arc<AtomicU64> {
        let progress = Arc::new(AtomicU64::new(0));
        let mut root = self.root.lock();
        if let Some(node) = root.get_mut(&self.path) {
            debug_assert!(
                node.progress.is_none(),
                "usage error: track_progress called multiple times on the same node",
            );
            node.progress = Some(NodeProgress {
                current: progress.clone(),
                total,
            });
        } else {
            debug_assert!(
                false,
                "bug: invalid path when adding adding progress tracking",
            );
        }
        progress
    }

    /// For tasks created using [`Self::subtask`], mark them as in-progress.
    pub fn start(&self) {
        let mut root = self.root.lock();
        if let Some(node) = root.get_mut(&self.path) {
            match node.state {
                ProgressState::Pending => {
                    node.state = ProgressState::InProgress {
                        started_at: Utc::now(),
                        started_instant: Instant::now(),
                    };
                }
                ProgressState::InProgress { .. } | ProgressState::Finished { .. } => (),
            }
        } else {
            debug_assert!(false, "bug: invalid path when starting a task");
        }
    }
}

impl Drop for ProgressTracker {
    fn drop(&mut self) {
        ProgressNode::finish(&self.root, &self.path);
    }
}

impl ProgressNode {
    fn get_mut(&mut self, path: &[usize]) -> Option<&mut ProgressNode> {
        let mut current = &mut *self;
        for &idx in path {
            current = &mut current.children.get_mut(idx)?.1;
        }
        Some(current)
    }

    fn render(&self, name: String) -> ProgressTree {
        let Self {
            children,
            progress,
            state,
        } = self;
        let (done, total) = match progress {
            Some(NodeProgress { current, total }) => {
                (Some(current.load(Ordering::Relaxed)), *total)
            }
            None => (None, None),
        };
        let (started_at, finished_at, duration_sec) = match state {
            ProgressState::Pending => (None, None, None),
            ProgressState::InProgress { started_at, .. } => (Some(*started_at), None, None),
            ProgressState::Finished {
                started_at,
                finished_at,
                duration,
            } => (
                Some(*started_at),
                Some(*finished_at),
                Some(duration.as_secs_f64()),
            ),
        };
        ProgressTree {
            name,
            started_at,
            finished_at,
            duration_sec,
            done,
            total,
            children: children
                .iter()
                .map(|(child_name, child_node)| child_node.render(child_name.clone()))
                .collect(),
        }
    }

    fn finish(root: &Arc<Mutex<ProgressNode>>, path: &[usize]) {
        let mut root = root.lock();
        if let Some(node) = root.get_mut(path) {
            match &node.state {
                ProgressState::Pending => {
                    let now = Utc::now();
                    node.state = ProgressState::Finished {
                        started_at: now,
                        finished_at: now,
                        duration: Duration::from_secs(0),
                    };
                }
                ProgressState::InProgress {
                    started_at,
                    started_instant,
                } => {
                    let finished_instant = Instant::now();
                    node.state = ProgressState::Finished {
                        started_at: *started_at,
                        finished_at: Utc::now(),
                        duration: finished_instant.duration_since(*started_instant),
                    };
                }
                ProgressState::Finished { .. } => (),
            }
        } else {
            // Should never happen.
            debug_assert!(false, "bug: invalid path when finishing a task");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    #[expect(unused_variables, reason = "testing drop behavior")]
    fn test_progress_tracker() {
        let (view, p) = new_progress_tracker();

        let p_foo = p.subtask("foo");
        let p_bar = p.subtask("bar");
        let p_baz = p.subtask("baz");

        let p_foo_x = p_foo.subtask("x");
        let p_foo_y = p_foo.subtask("y");
        let p_foo_z = p_foo.subtask("z");

        p_foo.start();
        p_foo_x.start();
        {
            let p_foo_x_a = p_foo_x.subtask("a");
            let p_foo_x_b = p_foo_x.subtask("b");
            let p_foo_x_c = p_foo_x.subtask("c");

            p_foo_x_a.start();
            p_foo_x_b.start();
            // c is not started explicitly, so it becomes finished on drop

            p_foo_x_a
                .track_progress(Some(7))
                .store(5, Ordering::Relaxed);
        }
        drop(p_foo_x);

        p_foo_y.start();
        {
            let p_foo_y_a = p_foo_y.subtask("a");
            let p_foo_y_b = p_foo_y.subtask("b");
            let p_foo_y_c = p_foo_y.subtask("c");

            p_foo_y_a.start();

            p_foo_y_a.track_progress(None).store(3, Ordering::Relaxed);

            check_state(
                &view,
                "
                    :in-progress {
                        foo:in-progress {
                            x:finished {
                                a:finished[5/7] {}
                                b:finished {}
                                c:finished {}
                            }
                            y:in-progress {
                                a:in-progress[3/?] {}
                                b:pending {}
                                c:pending {}
                            }
                            z:pending {}
                        }
                        bar:pending {}
                        baz:pending {}
                    }
                ",
            );
        }
    }

    fn test_render(node: &ProgressNode, output: &mut String) {
        output.push(':');
        match &node.state {
            ProgressState::Pending => output.push_str("pending"),
            ProgressState::InProgress { .. } => output.push_str("in-progress"),
            ProgressState::Finished { .. } => output.push_str("finished"),
        }
        if let Some(progress) = &node.progress {
            write!(output, "[{}/", progress.current.load(Ordering::Relaxed)).unwrap();
            if let Some(total) = progress.total {
                write!(output, "{total}]").unwrap();
            } else {
                output.push_str("?]");
            }
        }
        output.push('{');
        for (child_name, child_node) in &node.children {
            output.push_str(child_name);
            test_render(child_node, output);
        }
        output.push('}');
    }

    fn check_state(view: &ProgressView, expected: &str) {
        let mut rendered = String::new();
        test_render(&view.root.lock(), &mut rendered);
        assert_eq!(rendered, expected.replace(&[' ', '\n'][..], ""));
    }
}
