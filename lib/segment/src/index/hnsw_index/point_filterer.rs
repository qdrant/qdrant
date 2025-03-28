use bitvec::slice::BitSlice;
use common::types::{PointOffsetType, ScoredPointOffset};

use crate::id_tracker::IdTracker;
use crate::payload_storage::FilterContext;
use crate::vector_storage::{VectorStorage, check_deleted_condition};

#[derive(Clone, Copy)]
pub struct SimplePointsFilterer<'a> {
    pub vec_deleted: &'a BitSlice,
    pub point_deleted: &'a BitSlice,
}

impl<'a> SimplePointsFilterer<'a> {
    pub fn new(
        vector_storage: &'a (impl VectorStorage + ?Sized),
        id_tracker: &'a (impl IdTracker + ?Sized),
    ) -> Self {
        SimplePointsFilterer {
            vec_deleted: vector_storage.deleted_vector_bitslice(),
            point_deleted: id_tracker.deleted_point_bitslice(),
        }
    }

    pub fn with_context(self, filter_context: Option<&'a dyn FilterContext>) -> PointsFilterer<'a> {
        PointsFilterer {
            simple_filterer: self,
            filter_context,
        }
    }

    pub fn check_vector(&self, point_id: PointOffsetType) -> bool {
        check_deleted_condition(point_id, self.vec_deleted, self.point_deleted)
    }
}

pub struct PointsFilterer<'a> {
    simple_filterer: SimplePointsFilterer<'a>,
    filter_context: Option<&'a dyn FilterContext>,
}

impl<'a> PointsFilterer<'a> {
    pub fn new(
        vector_storage: &'a (impl VectorStorage + ?Sized),
        id_tracker: &'a (impl IdTracker + ?Sized),
        filter_context: Option<&'a dyn FilterContext>,
    ) -> Self {
        SimplePointsFilterer::new(vector_storage, id_tracker).with_context(filter_context)
    }

    pub fn check_vector(&self, point_id: PointOffsetType) -> bool {
        match self.filter_context {
            None => self.simple_filterer.check_vector(point_id),
            Some(f) => self.simple_filterer.check_vector(point_id) && f.check(point_id),
        }
    }

    /// Filter a vector of [`ScoredPointOffset`] by their [`idx`].
    /// The [`score`] field is ignored.
    ///
    /// This method operates on [`ScoredPointOffset`] for performance reasons.
    /// The intended usage is to prepare a vector of [`ScoredPointOffset`]
    /// with only the [`idx`] being set, filter it using this method, and then
    /// pass it to [`RawScorer::score_points`].
    ///
    /// [`RawScorer::score_points`]: crate::vector_storage::RawScorer::score_points
    /// [`score`]: ScoredPointOffset::score
    /// [`idx`]: ScoredPointOffset::idx
    pub fn filter_scores(&self, scores: &mut Vec<ScoredPointOffset>, limit: usize) {
        match self.filter_context {
            None => scores.retain(|score| self.simple_filterer.check_vector(score.idx)),
            Some(f) => scores
                .retain(|score| self.simple_filterer.check_vector(score.idx) && f.check(score.idx)),
        }
        if limit != 0 {
            scores.truncate(limit);
        }
    }
}
