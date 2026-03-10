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
}

impl TelemetryDetail {
    pub fn new(level: DetailsLevel, histograms: bool) -> Self {
        Self { level, histograms }
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

/// Overwrite the filtering of deferred points.
/// Can be used in places where we sometimes must access all points, including deferred ones.
#[derive(Clone, Copy)]
pub enum OverwriteDeferredFiltering {
    /// Deferred points are not affected nor visible.
    None,

    /// Deferred points are affected and visible.
    IncludeAll,
}

impl OverwriteDeferredFiltering {
    /// Apply the overwrite to a given `deferred_internal_id`.
    pub fn apply(&self, deferred_internal_id: Option<PointOffsetType>) -> Option<PointOffsetType> {
        match self {
            // No overwrite so we just return the passed `deferred_internal_id`.
            OverwriteDeferredFiltering::None => deferred_internal_id,

            // Setting `deferred_internal_id` to `None` results in no points being left out
            // at deferred-point filtering.
            OverwriteDeferredFiltering::IncludeAll => None,
        }
    }

    /// Returns `true` if filtering deferred points should be disabled
    /// and *all* points should be included in the result.
    pub fn include_all_points(&self) -> bool {
        matches!(self, OverwriteDeferredFiltering::IncludeAll)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_deferred_overwrite() {
        assert_eq!(OverwriteDeferredFiltering::None.apply(Some(32)), Some(32));
        assert_eq!(OverwriteDeferredFiltering::None.apply(None), None);

        // `IncludeAll` always returns `None` because then the filtering of deferred points is disabled.
        assert_eq!(OverwriteDeferredFiltering::IncludeAll.apply(Some(32)), None);
        assert_eq!(OverwriteDeferredFiltering::IncludeAll.apply(None), None);
    }
}
