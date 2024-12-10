use std::cmp::Ordering;

use ordered_float::OrderedFloat;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Type of vector matching score
pub type ScoreType = f32;
/// Type of point index inside a segment
pub type PointOffsetType = u32;

#[derive(Copy, Clone, PartialEq, Debug, Default)]
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
    Level0,
    Level1,
    Level2,
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
            _ => DetailsLevel::Level2,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Hash, Default, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MaxOptimizationThreadsSetting {
    #[default]
    Auto,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Hash, JsonSchema)]
#[serde(untagged)]
pub enum MaxOptimizationThreads {
    Setting(MaxOptimizationThreadsSetting),
    Threads(usize),
}

impl Default for MaxOptimizationThreads {
    fn default() -> Self {
        MaxOptimizationThreads::Setting(MaxOptimizationThreadsSetting::Auto)
    }
}

impl MaxOptimizationThreads {
    pub fn value(&self) -> Option<usize> {
        match self {
            MaxOptimizationThreads::Setting(MaxOptimizationThreadsSetting::Auto) => None,
            MaxOptimizationThreads::Threads(value) => Some(*value),
        }
    }
}
