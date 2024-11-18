use std::cmp::Ordering;

use crate::types::ScoredPoint;

// Newtype to provide alternative comparator for ScoredPoint which breaks ties by id
pub struct ScoredPointTies<'a>(pub &'a ScoredPoint);

impl<'a> From<&'a ScoredPoint> for ScoredPointTies<'a> {
    fn from(scored_point: &'a ScoredPoint) -> Self {
        ScoredPointTies(scored_point)
    }
}

impl Ord for ScoredPointTies<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .cmp(other.0)
            // for identical scores, we fallback to sorting by ids to have a stable output
            .then_with(|| self.0.id.cmp(&other.0.id))
    }
}

impl PartialOrd for ScoredPointTies<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for ScoredPointTies<'_> {}

impl PartialEq for ScoredPointTies<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
