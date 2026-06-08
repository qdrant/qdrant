use std::cmp::Ordering;

use ordered_float::OrderedFloat;
use strum::EnumIter;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Type of vector matching score
pub type ScoreType = f32;
/// Type of point index inside a segment
pub type PointOffsetType = u32;

#[derive(Copy, Clone, PartialEq, Debug, Default, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct ScoredPointOffset {
    pub idx: PointOffsetType,
    pub score: ScoreType,
}

impl Eq for ScoredPointOffset {}

impl Ord for ScoredPointOffset {
    fn cmp(&self, other: &Self) -> Ordering {
        OrderedFloat(self.score).cmp(&OrderedFloat(other.score))
    }
}

impl PartialOrd for ScoredPointOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Copy, Clone, Debug)]
pub struct TelemetryDetail {
    pub level: DetailsLevel,
    pub histograms: bool,
    pub per_collection: bool,
}

impl TelemetryDetail {
    pub fn new(level: DetailsLevel, histograms: bool) -> Self {
        Self {
            level,
            histograms,
            per_collection: false,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, EnumIter)]
pub enum DetailsLevel {
    /// Minimal information level
    /// - app info
    /// - minimal telemetry by endpoint
    /// - cluster status
    Level0,
    /// Detailed common info level
    /// - app info details
    /// - system info
    ///   - hardware flags
    ///   - hardware usage per collection
    ///   - RAM usage
    /// - cluster basic details
    /// - collections basic info
    Level1,
    /// Detailed consensus info - peers info
    /// Collections:
    ///  - detailed config
    ///  - Shards - basic config
    Level2,
    /// Shards:
    ///  - detailed config
    ///  - Optimizers info
    Level3,
    /// Segment level telemetry
    Level4,
}

impl Default for TelemetryDetail {
    fn default() -> Self {
        TelemetryDetail {
            level: DetailsLevel::Level0,
            histograms: false,
            per_collection: false,
        }
    }
}

impl From<usize> for DetailsLevel {
    fn from(value: usize) -> Self {
        match value {
            0 => DetailsLevel::Level0,
            1 => DetailsLevel::Level1,
            2 => DetailsLevel::Level2,
            3 => DetailsLevel::Level3,
            4 => DetailsLevel::Level4,
            _ => DetailsLevel::Level4,
        }
    }
}

/// Selects which "version" of each external id a reader sees when an
/// appendable segment carries deferred-points machinery.
#[derive(Clone, Copy)]
pub enum DeferredBehavior {
    /// Yield only the visible (active) version of each external id.
    /// Deferred mutations and fresh-above-cutoff inserts are hidden.
    /// Default for query paths.
    VisibleOnly,

    /// Yield the deferred head when one exists, falling back to the
    /// active head otherwise. Each external id is yielded once, with
    /// its latest version. Used by the optimiser and transfer paths
    /// that need to observe all in-flight mutations.
    WithDeferred,
}

impl DeferredBehavior {
    /// Apply the behavior to a given `deferred_internal_id`.
    pub fn apply(&self, deferred_internal_id: Option<PointOffsetType>) -> Option<PointOffsetType> {
        match self {
            // Keep the cutoff so callers drop ids at or above it.
            DeferredBehavior::VisibleOnly => deferred_internal_id,

            // Drop the cutoff so deferred ids flow through; consumers
            // dedup by external id via the shadow bit.
            DeferredBehavior::WithDeferred => None,
        }
    }

    /// Returns `true` if deferred ids should flow through the filter
    /// (consumer dedups by external id via the shadow bit).
    pub fn with_deferred_points(&self) -> bool {
        matches!(self, DeferredBehavior::WithDeferred)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_deferred_overwrite() {
        assert_eq!(DeferredBehavior::VisibleOnly.apply(Some(32)), Some(32));
        assert_eq!(DeferredBehavior::VisibleOnly.apply(None), None);

        // `WithDeferred` always returns `None` because then the cutoff filter is disabled.
        assert_eq!(DeferredBehavior::WithDeferred.apply(Some(32)), None);
        assert_eq!(DeferredBehavior::WithDeferred.apply(None), None);
    }
}
