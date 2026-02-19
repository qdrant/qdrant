use common::progress_tracker::ProgressTree;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::tracker::TrackerStatus;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Optimization {
    /// Unique identifier of the optimization process.
    ///
    /// After the optimization is complete, a new segment will be created with
    /// this UUID.
    pub uuid: Uuid,
    /// Name of the optimizer that performed this optimization.
    pub optimizer: String,
    pub status: TrackerStatus,
    /// Segments being optimized.
    ///
    /// After the optimization is complete, these segments will be replaced
    /// by the new optimized segment.
    pub segments: Vec<OptimizationSegmentInfo>,
    pub progress: ProgressTree,
}

/// Optimizations progress for the collection
#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct OptimizationsResponse {
    pub summary: OptimizationsSummary,
    /// Currently running optimizations.
    pub running: Vec<Optimization>,
    /// An estimated queue of pending optimizations.
    /// Requires `?with=queued`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queued: Option<Vec<PendingOptimization>>,
    /// Completed optimizations.
    /// Requires `?with=completed`. Limited by `?completed_limit=N`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed: Option<Vec<Optimization>>,
    /// Segments that don't require optimization.
    /// Requires `?with=idle_segments`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idle_segments: Option<Vec<OptimizationSegmentInfo>>,
}

impl OptimizationsResponse {
    /// Merge another `OptimizationsResponse` into this one.
    pub fn merge(&mut self, other: OptimizationsResponse) {
        let OptimizationsResponse {
            summary:
                OptimizationsSummary {
                    queued_optimizations,
                    queued_segments,
                    queued_points,
                    idle_segments: idle_segments_count,
                },
            running,
            queued,
            completed,
            idle_segments,
        } = other;

        self.running.extend(running);
        self.summary.queued_optimizations += queued_optimizations;
        self.summary.queued_segments += queued_segments;
        self.summary.queued_points += queued_points;
        self.summary.idle_segments += idle_segments_count;
        merge_optional_vec(&mut self.completed, completed);
        merge_optional_vec(&mut self.queued, queued);
        merge_optional_vec(&mut self.idle_segments, idle_segments);
    }
}

/// Merge two `Option<Vec<T>>` values: if either side has data, the result has data.
fn merge_optional_vec<T>(target: &mut Option<Vec<T>>, source: Option<Vec<T>>) {
    match (target.as_mut(), source) {
        (Some(target), Some(source)) => target.extend(source),
        (None, source @ Some(_)) => *target = source,
        (Some(_) | None, None) => {}
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct OptimizationsSummary {
    /// Number of pending optimizations in the queue.
    /// Each optimization will take one or more unoptimized segments and produce
    /// one optimized segment.
    pub queued_optimizations: usize,
    /// Number of unoptimized segments in the queue.
    pub queued_segments: usize,
    /// Number of points in unoptimized segments in the queue.
    pub queued_points: usize,
    /// Number of segments that don't require optimization.
    pub idle_segments: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PendingOptimization {
    /// Name of the optimizer that scheduled this optimization.
    pub optimizer: String,
    /// Segments that will be optimized.
    pub segments: Vec<OptimizationSegmentInfo>,
}

// See also [`segment::types::SegmentInfo`] which is used in telemetry.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OptimizationSegmentInfo {
    /// Unique identifier of the segment.
    pub uuid: Uuid,
    /// Number of non-deleted points in the segment.
    pub points_count: usize,
}

#[derive(Debug, Copy, Clone)]
pub struct OptimizationsRequestOptions {
    /// `?with=queued`
    pub queued: bool,
    /// `?with=completed` and `?completed_limit=N`
    pub completed_limit: Option<usize>,
    /// `?with=idle_segments`
    pub idle_segments: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct OptimizerThresholds {
    pub max_segment_size_kb: usize,
    pub memmap_threshold_kb: usize,
    pub indexing_threshold_kb: usize,
}
