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

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, JsonSchema)]
#[serde(untagged)]
pub enum MaxOptimizationThreads {
    Auto, // Represents the "null-like" or default state
    Threads(usize),
}

#[allow(clippy::derivable_impls)]
impl Default for MaxOptimizationThreads {
    fn default() -> Self {
        MaxOptimizationThreads::Auto
    }
}

impl MaxOptimizationThreads {
    pub fn value(&self) -> Option<usize> {
        match self {
            MaxOptimizationThreads::Auto => None,
            MaxOptimizationThreads::Threads(value) => Some(*value),
        }
    }
}

impl From<Option<u64>> for MaxOptimizationThreads {
    fn from(opt: Option<u64>) -> Self {
        match opt {
            None => MaxOptimizationThreads::Auto,
            Some(n) => MaxOptimizationThreads::Threads(n as usize),
        }
    }
}

impl From<MaxOptimizationThreads> for Option<u64> {
    fn from(m: MaxOptimizationThreads) -> Self {
        match m {
            MaxOptimizationThreads::Auto => Some(0),
            MaxOptimizationThreads::Threads(n) => Some(n as u64),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_method() {
        let auto = MaxOptimizationThreads::Auto;
        assert_eq!(auto.value(), None);

        let threads = MaxOptimizationThreads::Threads(4);
        assert_eq!(threads.value(), Some(4));
    }

    #[test]
    fn test_from_option() {
        let opt_none: Option<u64> = None;
        let max_threads = MaxOptimizationThreads::from(opt_none);
        assert_eq!(max_threads, MaxOptimizationThreads::Auto);

        let opt_some: Option<u64> = Some(4);
        let max_threads = MaxOptimizationThreads::from(opt_some);
        assert_eq!(max_threads, MaxOptimizationThreads::Threads(4));
    }

    #[test]
    fn test_from_max_optimization_threads_to_option() {
        let auto = MaxOptimizationThreads::Auto;
        let opt_u64: Option<u64> = Option::from(auto);
        assert_eq!(opt_u64, Some(0));

        let threads = MaxOptimizationThreads::Threads(4);
        let opt_u64: Option<u64> = Option::from(threads);
        assert_eq!(opt_u64, Some(4));
    }
}
