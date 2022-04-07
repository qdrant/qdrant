use crate::payload_storage::FilterContext;
use crate::types::{PointOffsetType, ScoreType};
use crate::vector_storage::{RawScorer, ScoredPointOffset};
use std::cell::RefCell;
use std::ops::DerefMut;

pub struct FilteredScorer<'a> {
    pub raw_scorer: &'a dyn RawScorer,
    pub filter_context: Option<&'a dyn FilterContext>,
    points_buffer: RefCell<Vec<ScoredPointOffset>>,
}

impl<'a> FilteredScorer<'a> {
    pub fn new(
        raw_scorer: &'a dyn RawScorer,
        filter_context: Option<&'a dyn FilterContext>,
    ) -> Self {
        FilteredScorer {
            raw_scorer,
            filter_context,
            points_buffer: RefCell::new(vec![]),
        }
    }

    pub fn check_point(&self, point_id: PointOffsetType) -> bool {
        match self.filter_context {
            None => self.raw_scorer.check_point(point_id),
            Some(f) => f.check(point_id) && self.raw_scorer.check_point(point_id),
        }
    }

    /// Method filters and calculates scores for the given slice of points
    /// and yields obtained scores into the callback.
    ///
    /// For performance reasons:
    /// - This function uses callback instead of iterator
    /// - This function mutates input values
    ///
    /// # Arguments
    ///
    /// * `point_ids` - list of points to score. *Warn*: This input will be wrecked during the execution.
    /// * `limit` - limits the number of points to process after filtering.
    /// * `action` - callback. This function is called for each scored point (not more than `limit` times)
    ///
    pub fn score_points<F>(&self, point_ids: &mut [PointOffsetType], limit: usize, action: F)
    where
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

        let mut scored_points_buffer = self.points_buffer.borrow_mut();
        scored_points_buffer.resize(limit, ScoredPointOffset::default());

        let count = self
            .raw_scorer
            .score_points(filtered_point_ids, scored_points_buffer.deref_mut());
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
