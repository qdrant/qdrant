use std::collections::VecDeque;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::holders::segment_holder::SegmentId;

pub mod config_mismatch_optimizer;
pub mod indexing_optimizer;
pub mod merge_optimizer;
pub mod segment_optimizer;
pub mod vacuum_optimizer;

/// Number of last trackers to keep in tracker log
///
/// Will never remove older trackers for failed or still ongoing optimizations.
const KEEP_LAST_TRACKERS: usize = 16;

/// A log of optimizer trackers holding their status
#[derive(Default, Clone, Debug)]
pub struct TrackerLog {
    descriptions: VecDeque<Tracker>,
}

impl TrackerLog {
    /// Register a new optimizer tracker
    pub fn register(&mut self, description: Tracker) {
        self.descriptions.push_back(description);
        self.truncate();
    }

    /// Truncate and forget old trackers for successful/cancelled optimizations
    ///
    /// Will never remove older trackers with failed or still ongoing optimizations.
    ///
    /// Always keeps the last `KEEP_TRACKERS` trackers.
    fn truncate(&mut self) {
        let truncate_range = self.descriptions.len().saturating_sub(KEEP_LAST_TRACKERS);

        // Find items to truncate, start removing from the back
        let truncate = self
            .descriptions
            .iter()
            .enumerate()
            .take(truncate_range)
            .filter(|(_, tracker)| match tracker.state.lock().status {
                TrackerStatus::Optimizing | TrackerStatus::Error(_) => false,
                TrackerStatus::Done | TrackerStatus::Cancelled(_) => true,
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        truncate.into_iter().rev().for_each(|index| {
            self.descriptions.remove(index);
        });
    }

    /// Convert log into list of objects usable in telemetry
    pub fn to_telemetry(&self) -> Vec<TrackerTelemetry> {
        self.descriptions
            .iter()
            // Show latest items first
            .rev()
            .map(Tracker::to_telemetry)
            .collect()
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

    /// Convert into object used in telemetry
    pub fn to_telemetry(&self) -> TrackerTelemetry {
        let state = self.state.lock();
        TrackerTelemetry {
            name: self.name.clone(),
            segment_ids: self.segment_ids.clone(),
            status: state.status.clone(),
            start_at: self.start_at,
            end_at: state.end_at,
        }
    }
}

/// Tracker object used in telemetry
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct TrackerTelemetry {
    /// Name of the optimizer
    pub name: String,
    /// Segment IDs being optimized
    pub segment_ids: Vec<SegmentId>,
    /// Latest status of the optimizer
    pub status: TrackerStatus,
    /// Start time of the optimizer
    pub start_at: DateTime<Utc>,
    /// End time of the optimizer
    pub end_at: Option<DateTime<Utc>>,
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
