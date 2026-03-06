use std::collections::VecDeque;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use common::progress_tracker::{ProgressTracker, ProgressView, new_progress_tracker};
use parking_lot::Mutex;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::{Deserialize, Serialize};
use shard::segment_holder::SegmentId;
use uuid::Uuid;

use crate::operations::types::{Optimization, OptimizationSegmentInfo};
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

    pub fn iter(&self) -> impl Iterator<Item = &Tracker> {
        self.descriptions.iter()
    }
}

/// Tracks the state of an optimizer
#[derive(Clone, Debug)]
pub struct Tracker {
    /// Name of the optimizer
    pub name: &'static str,
    /// UUID of the upcoming segment being created by the optimizer
    pub uuid: Uuid,
    /// Segments being optimized
    pub segments: Vec<TrackerSegmentInfo>,
    /// Start time of the optimizer
    pub state: Arc<Mutex<TrackerState>>,
    /// A read-only view to progress tracker
    pub progress_view: ProgressView,
}

#[derive(Copy, Clone, Debug)]
pub struct TrackerSegmentInfo {
    pub id: SegmentId,
    pub uuid: Uuid,
    pub points_count: usize,
}

impl From<&TrackerSegmentInfo> for OptimizationSegmentInfo {
    fn from(value: &TrackerSegmentInfo) -> Self {
        let TrackerSegmentInfo {
            id: _,
            uuid,
            points_count,
        } = *value;
        OptimizationSegmentInfo { uuid, points_count }
    }
}

impl Tracker {
    /// Start a new optimizer tracker.
    ///
    /// Returns self (read-write) and a progress tracker (write-only).
    pub fn start(
        name: &'static str,
        uuid: Uuid,
        segments: Vec<TrackerSegmentInfo>,
    ) -> (Tracker, ProgressTracker) {
        let (progress_view, progress_tracker) = new_progress_tracker();
        let tracker = Self {
            name,
            uuid,
            segments,
            state: Default::default(),
            progress_view,
        };
        (tracker, progress_tracker)
    }

    /// Get handle to this tracker, allows updating state
    pub fn handle(&self) -> TrackerHandle {
        self.state.clone().into()
    }

    /// Convert into object used in telemetry
    pub fn to_telemetry(&self) -> TrackerTelemetry {
        let state = self.state.lock();
        TrackerTelemetry {
            name: self.name,
            uuid: self.uuid,
            segment_ids: self.segments.iter().map(|s| s.id).collect(),
            segment_uuids: self.segments.iter().map(|s| s.uuid).collect(),
            status: state.status.clone(),
            start_at: self.progress_view.started_at(),
            end_at: state.end_at,
        }
    }

    pub fn to_optimization(&self) -> Optimization {
        Optimization {
            optimizer: self.name.to_string(),
            uuid: self.uuid,
            segments: self.segments.iter().map(|s| s.into()).collect(),
            status: self.state.lock().status.clone(),
            progress: self.progress_view.snapshot("Segment Optimizing"),
        }
    }
}

/// Tracker object used in telemetry
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct TrackerTelemetry {
    /// Name of the optimizer
    #[anonymize(false)]
    pub name: &'static str,
    /// UUID of the upcoming segment being created by the optimizer
    pub uuid: Uuid,
    /// Internal segment IDs being optimized.
    /// These are local and in-memory, meaning that they can refer to different
    /// segments after a service restart.
    pub segment_ids: Vec<SegmentId>,
    /// Segment UUIDs being optimized.
    /// Refers to same segments as in `segment_ids`, but trackable across
    /// restarts, and reflect their directory name.
    pub segment_uuids: Vec<Uuid>,
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
        self.end_at = if status.is_running() {
            None
        } else {
            Some(Utc::now())
        };
        self.status = status;
    }
}

/// Represents the current state of the optimizer being tracked
#[derive(
    Serialize, Deserialize, Clone, Debug, JsonSchema, Anonymize, Default, Eq, PartialEq, Hash,
)]
#[serde(rename_all = "lowercase")]
pub enum TrackerStatus {
    #[default]
    Optimizing,
    Done,
    #[anonymize(false)]
    Cancelled(String),
    #[anonymize(false)]
    Error(String),
}

impl TrackerStatus {
    pub fn is_running(&self) -> bool {
        match self {
            TrackerStatus::Optimizing => true,
            TrackerStatus::Done | TrackerStatus::Cancelled(_) | TrackerStatus::Error(_) => false,
        }
    }
}
