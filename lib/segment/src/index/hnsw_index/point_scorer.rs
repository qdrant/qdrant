use crate::payload_storage::ConditionChecker;
use crate::types::{Filter, PointOffsetType, ScoreType};
use crate::vector_storage::{RawScorer, ScoredPointOffset};

pub struct FilteredScorer<'a> {
    pub raw_scorer: &'a dyn RawScorer,
    pub condition_checker: &'a dyn ConditionChecker,
    pub filter: Option<&'a Filter>,
}

impl<'a> FilteredScorer<'a> {
    pub fn new(
        raw_scorer: &'a dyn RawScorer,
        condition_checker: &'a dyn ConditionChecker,
        filter: Option<&'a Filter>,
    ) -> Self {
        FilteredScorer {
            raw_scorer,
            condition_checker,
            filter,
        }
    }

    pub fn check_point(&self, point_id: PointOffsetType) -> bool {
        match self.filter {
            None => self.raw_scorer.check_point(point_id),
            Some(f) => {
                self.condition_checker.check(point_id, f) && self.raw_scorer.check_point(point_id)
            }
        }
    }

    pub fn score_iterable_points<F>(
        &self,
        point_ids: &[PointOffsetType],
        scored_points_buffer: &mut [ScoredPointOffset],
        action: F,
    ) where
        F: FnMut(ScoredPointOffset),
    {
        match self.filter {
            None => {
                let count = self
                    .raw_scorer
                    .score_points(point_ids, scored_points_buffer);
                scored_points_buffer
                    .iter()
                    .take(count)
                    .copied()
                    .for_each(action);
            }
            Some(f) => {
                let filtered_points: Vec<PointOffsetType> = point_ids
                    .iter()
                    .copied()
                    .filter(|id| self.condition_checker.check(*id, f))
                    .collect();
                let count = self
                    .raw_scorer
                    .score_points(&filtered_points, scored_points_buffer);
                scored_points_buffer
                    .iter()
                    .take(count)
                    .copied()
                    .for_each(action);
            }
        };
    }

    pub fn score_point(&self, point_id: PointOffsetType) -> ScoreType {
        self.raw_scorer.score_point(point_id)
    }

    pub fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.raw_scorer.score_internal(point_a, point_b)
    }
}
