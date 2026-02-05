use common::progress_tracker::ProgressTree;
use schemars::JsonSchema;
use serde::Serialize;
use uuid::Uuid;

use crate::optimize::TrackerStatus;

#[derive(Debug, Serialize, JsonSchema)]
pub struct Optimization {
    /// Unique identifier of the optimization process.
    ///
    /// After the optimization is complete, a new segment will be created with
    /// this UUID.
    pub uuid: Uuid,
    /// Name of the optimizer that performed this optimization.
    pub optimizer: &'static str,
    pub status: TrackerStatus,
    /// Segments being optimized.
    ///
    /// After the optimization is complete, these segments will be replaced
    /// by the new optimized segment.
    pub segments: Vec<OptimizationSegmentInfo>,
    pub progress: ProgressTree,
}

/// Optimizations progress for the collection
#[derive(Debug, Serialize, JsonSchema)]
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

#[derive(Debug, Serialize, JsonSchema)]
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

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct PendingOptimization {
    /// Name of the optimizer that scheduled this optimization.
    pub optimizer: &'static str,
    /// Segments that will be optimized.
    pub segments: Vec<OptimizationSegmentInfo>,
}

// See also [`segment::types::SegmentInfo`] which is used in telemetry.
#[derive(Debug, Clone, Serialize, JsonSchema)]
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
