use crate::payload_storage::FilterContext;
use crate::types::{PointOffsetType, ScoreType};
use crate::vector_storage::{RawScorer, ScoredPointOffset};

pub struct FilteredScorer<'a> {
    pub raw_scorer: &'a dyn RawScorer,
    pub filter_context: Option<&'a dyn FilterContext>,
}

impl<'a> FilteredScorer<'a> {
    pub fn new(
        raw_scorer: &'a dyn RawScorer,
        filter_context: Option<&'a dyn FilterContext>,
    ) -> Self {
        FilteredScorer {
            raw_scorer,
            filter_context,
        }
    }

    pub fn check_point(&self, point_id: PointOffsetType) -> bool {
        match self.filter_context {
            None => self.raw_scorer.check_point(point_id),
            Some(f) => f.check(point_id) && self.raw_scorer.check_point(point_id),
        }
    }

    pub fn score_points_to_buffer<F>(
        &self,
        point_ids: &mut [PointOffsetType],
        scored_points_buffer: &mut [ScoredPointOffset],
        action: F,
    ) where
        F: FnMut(ScoredPointOffset),
    {
        // apply filter and store filtered ids to source slice memory
        let filtered_point_ids = match self.filter_context {
            None => point_ids,
            Some(f) => {
                let len = point_ids.len();
                let mut filtered_len = 0;
                for i in 0..len {
                    let point_id = point_ids[i];
                    if f.check(point_id) {
                        point_ids[filtered_len] = point_id;
                        filtered_len += 1;
                    }
                }
                &point_ids[0..filtered_len]
            }
        };

        let count = self
            .raw_scorer
            .score_points(filtered_point_ids, scored_points_buffer);
        scored_points_buffer
            .iter()
            .take(count)
            .copied()
            .for_each(action);
    }

    pub fn score_point(&self, point_id: PointOffsetType) -> ScoreType {
        self.raw_scorer.score_point(point_id)
    }

    pub fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.raw_scorer.score_internal(point_a, point_b)
    }
}
