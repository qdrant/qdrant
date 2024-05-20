use std::cmp::Ordering;

use segment::types::ScoredPoint;

// Newtype to provide alternative comparator for ScoredPoint which breaks ties by id
pub struct ScoredPointTies {
    pub scored_point: ScoredPoint,
}

impl From<ScoredPoint> for ScoredPointTies {
    fn from(scored_point: ScoredPoint) -> Self {
        ScoredPointTies { scored_point }
    }
}

impl Ord for ScoredPointTies {
    fn cmp(&self, other: &Self) -> Ordering {
        let res = self.scored_point.score.cmp(&other.scored_point.score);
        // for identical scores, we fallback to sorting by ids to have a stable output
        if res == Ordering::Equal {
            self.scored_point.id.cmp(&other.scored_point.id)
        } else {
            res
        }
    }
}

impl PartialOrd for ScoredPointTies {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for ScoredPointTies {}

impl PartialEq for ScoredPointTies {
    fn eq(&self, other: &Self) -> bool {
        self.scored_point == other.scored_point
    }
}
