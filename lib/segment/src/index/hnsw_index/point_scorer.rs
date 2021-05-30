use crate::types::{PointOffsetType, Filter, ScoreType};
use crate::vector_storage::vector_storage::{ScoredPointOffset, RawScorer};
use crate::payload_storage::payload_storage::ConditionChecker;


pub struct FilteredScorer<'a> {
    pub raw_scorer: &'a dyn RawScorer,
    pub condition_checker: &'a dyn ConditionChecker,
    pub filter: Option<&'a Filter>,
}

impl FilteredScorer<'_> {
    pub fn check_point(&self, point_id: PointOffsetType) -> bool {
        match self.filter {
            None => self.raw_scorer.check_point(point_id),
            Some(f) => self.condition_checker.check(point_id, f) && self.raw_scorer.check_point(point_id)
        }
    }

    pub fn score_iterable_points<F>(&self, points_iterator: &mut dyn Iterator<Item=PointOffsetType>, limit: usize, action: F)
        where F: FnMut(ScoredPointOffset) {
        match self.filter {
            None => self.raw_scorer.score_points(points_iterator).take(limit).for_each(action),
            Some(f) => {
                let mut points_filtered_iterator = points_iterator
                    .filter(move |id| self.condition_checker.check(*id, f));
                self.raw_scorer.score_points(&mut points_filtered_iterator).take(limit).for_each(action)
            }
        };
    }

    pub fn score_points<F>(&self, ids: &[PointOffsetType], limit: usize, action: F)
        where F: FnMut(ScoredPointOffset) {
        let mut points_iterator = ids
            .iter()
            .cloned();

        self.score_iterable_points(&mut points_iterator, limit, action);
    }

    pub fn score_point(&self, point_id: PointOffsetType) -> ScoreType {
        self.raw_scorer.score_point(point_id)
    }

    pub fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.raw_scorer.score_internal(point_a, point_b)
    }
}