use std::cmp::Ordering;

use crate::types::ScoredPoint;

// Newtype to provide alternative comparator for ScoredPoint which breaks ties by id
pub struct ScoredPointTies<'a>(pub &'a ScoredPoint);

impl<'a> From<&'a ScoredPoint> for ScoredPointTies<'a> {
    fn from(scored_point: &'a ScoredPoint) -> Self {
        ScoredPointTies(scored_point)
    }
}

impl<'a> Ord for ScoredPointTies<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        let res = self.0.cmp(other.0);

        // for identical scores, we fallback to sorting by ids to have a stable output
        if res == Ordering::Equal {
            self.0.id.cmp(&other.0.id)
        } else {
            res
        }
    }
}

impl<'a> PartialOrd for ScoredPointTies<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Eq for ScoredPointTies<'a> {}

impl<'a> PartialEq for ScoredPointTies<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
