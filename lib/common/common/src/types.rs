use std::cmp::Ordering;

use ordered_float::OrderedFloat;
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

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
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
