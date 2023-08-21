use std::collections::VecDeque;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::Mutex;

use super::holders::segment_holder::SegmentId;

pub mod config_mismatch_optimizer;
pub mod indexing_optimizer;
pub mod merge_optimizer;
pub mod segment_optimizer;
pub mod vacuum_optimizer;

/// A log of optimizer trackers holding their status
#[derive(Default, Clone, Debug)]
pub struct TrackerLog {
    descriptions: VecDeque<Tracker>,
}

impl TrackerLog {
    /// Register a new optimizer tracker
    pub fn register(&mut self, description: Tracker) {
        self.descriptions.push_back(description);
    }
}

/// Tracks the state of an optimizer
#[derive(Clone, Debug)]
pub struct Tracker {
    /// Name of the optimizer
    pub name: String,
    /// Segment IDs being optimized
    pub segment_ids: Vec<SegmentId>,
    /// Start time of the optimizer
    pub start_at: DateTime<Utc>,
    /// Latest state of the optimizer
    pub state: Arc<Mutex<TrackerState>>,
}

impl Tracker {
    /// Start a new optimizer tracker
    pub fn start(name: impl Into<String>, segment_ids: Vec<SegmentId>) -> Self {
        Self {
            name: name.into(),
            segment_ids,
            state: Default::default(),
            start_at: Utc::now(),
        }
    }

    /// Get handle to this tracker, allows updating state
    pub fn handle(&self) -> TrackerHandle {
        self.state.clone().into()
    }
}

/// Handle to an optimizer tracker, allows updating its state
#[derive(Clone)]
pub struct TrackerHandle {
    handle: Arc<Mutex<TrackerState>>,
}

impl TrackerHandle {
    pub fn update(&self, status: TrackerStatus) {
        self.handle.lock().update(status);
    }
}

impl From<Arc<Mutex<TrackerState>>> for TrackerHandle {
    fn from(state: Arc<Mutex<TrackerState>>) -> Self {
        Self { handle: state }
    }
}

/// Mutable state of an optimizer tracker
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TrackerState {
    pub status: TrackerStatus,
    pub end_at: Option<DateTime<Utc>>,
}

impl TrackerState {
    /// Update the tracker state to the given `status`
    pub fn update(&mut self, status: TrackerStatus) {
        match status {
            TrackerStatus::Done | TrackerStatus::Cancelled(_) | TrackerStatus::Error(_) => {
                self.end_at.replace(Utc::now());
            }
            TrackerStatus::Optimizing => {
                self.end_at.take();
            }
        }
        self.status = status;
    }
}

/// Represents the current state of the optimizer being tracked
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default, Eq, PartialEq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum TrackerStatus {
    #[default]
    Optimizing,
    Done,
    Cancelled(String),
    Error(String),
}
