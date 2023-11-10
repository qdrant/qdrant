use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};

use crate::payload_storage::FilterContext;
use crate::vector_storage::RawScorer;

pub struct FilteredScorer<'a> {
    pub raw_scorer: &'a dyn RawScorer,
    pub filter_context: Option<&'a dyn FilterContext>,
    points_buffer: Vec<ScoredPointOffset>,
}

impl<'a> FilteredScorer<'a> {
    pub fn new(
        raw_scorer: &'a dyn RawScorer,
        filter_context: Option<&'a dyn FilterContext>,
    ) -> Self {
        FilteredScorer {
            raw_scorer,
            filter_context,
            points_buffer: Vec::new(),
        }
    }

    pub fn check_vector(&self, point_id: PointOffsetType) -> bool {
        match self.filter_context {
            None => self.raw_scorer.check_vector(point_id),
            Some(f) => f.check(point_id) && self.raw_scorer.check_vector(point_id),
        }
    }

    /// Method filters and calculates scores for the given slice of points IDs
    ///
    /// For performance reasons this function mutates input values.
    /// For result slice allocation this function mutates self.
    ///
    /// # Arguments
    ///
    /// * `point_ids` - list of points to score. *Warn*: This input will be wrecked during the execution.
    /// * `limit` - limits the number of points to process after filtering.
    ///
    pub fn score_points(
        &mut self,
        point_ids: &mut [PointOffsetType],
        limit: usize,
    ) -> &[ScoredPointOffset] {
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
        if limit == 0 {
            self.points_buffer
                .resize_with(filtered_point_ids.len(), ScoredPointOffset::default);
        } else {
            self.points_buffer
                .resize_with(limit, ScoredPointOffset::default);
        }
        let count = self
            .raw_scorer
            .score_points(filtered_point_ids, &mut self.points_buffer);
        &self.points_buffer[0..count]
    }

    pub fn score_point(&self, point_id: PointOffsetType) -> ScoreType {
        self.raw_scorer.score_point(point_id)
    }

    pub fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.raw_scorer.score_internal(point_a, point_b)
    }
}
