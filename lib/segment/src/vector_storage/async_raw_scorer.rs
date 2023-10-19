use std::sync::atomic::{AtomicBool, Ordering};

use bitvec::prelude::BitSlice;
use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};

use super::query_scorer::custom_query_scorer::CustomQueryScorer;
use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::QueryVector;
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric};
use crate::types::Distance;
use crate::vector_storage::memmap_vector_storage::MemmapVectorStorage;
use crate::vector_storage::mmap_vectors::MmapVectors;
use crate::vector_storage::query_scorer::metric_query_scorer::MetricQueryScorer;
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::{RawScorer, VectorStorage as _, DEFAULT_STOPPED};

pub fn new<'a>(
    query: QueryVector,
    storage: &'a MemmapVectorStorage,
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    Ok(AsyncRawScorerBuilder::new(query, storage, point_deleted)?
        .with_is_stopped(is_stopped)
        .build())
}

pub struct AsyncRawScorerImpl<'a, TQueryScorer: QueryScorer> {
    points_count: PointOffsetType,
    query_scorer: TQueryScorer,
    storage: &'a MmapVectors,
    point_deleted: &'a BitSlice,
    vec_deleted: &'a BitSlice,
    /// This flag indicates that the search process is stopped externally,
    /// the search result is no longer needed and the search process should be stopped as soon as possible.
    pub is_stopped: &'a AtomicBool,
}

impl<'a, TQueryScorer> AsyncRawScorerImpl<'a, TQueryScorer>
where
    TQueryScorer: QueryScorer,
{
    fn new(
        points_count: PointOffsetType,
        query_scorer: TQueryScorer,
        storage: &'a MmapVectors,
        point_deleted: &'a BitSlice,
        vec_deleted: &'a BitSlice,
        is_stopped: &'a AtomicBool,
    ) -> Self {
        Self {
            points_count,
            query_scorer,
            storage,
            point_deleted,
            vec_deleted,
            is_stopped,
        }
    }
}

impl<'a, TQueryScorer> RawScorer for AsyncRawScorerImpl<'a, TQueryScorer>
where
    TQueryScorer: QueryScorer,
{
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoredPointOffset]) -> usize {
        if self.is_stopped.load(Ordering::Relaxed) {
            return 0;
        }
        let points_stream = points
            .iter()
            .copied()
            .filter(|point_id| self.check_vector(*point_id));

        let mut processed = 0;
        self.storage
            .read_vectors_async(points_stream, |idx, point_id, other_vector| {
                scores[idx] = ScoredPointOffset {
                    idx: point_id,
                    score: self.query_scorer.score(other_vector),
                };
                processed += 1;
            })
            .unwrap();

        // ToDo: io_uring is experimental, it can fail if it is not supported.
        // Instead of silently falling back to the sync implementation, we prefer to panic
        // and notify the user that they better use the default IO implementation.

        processed
    }

    fn score_points_unfiltered(
        &self,
        points: &mut dyn Iterator<Item = PointOffsetType>,
    ) -> Vec<ScoredPointOffset> {
        if self.is_stopped.load(Ordering::Relaxed) {
            return vec![];
        }
        let mut scores = vec![];

        self.storage
            .read_vectors_async(points, |_idx, point_id, other_vector| {
                scores.push(ScoredPointOffset {
                    idx: point_id,
                    score: self.query_scorer.score(other_vector),
                });
            })
            .unwrap();

        // ToDo: io_uring is experimental, it can fail if it is not supported.
        // Instead of silently falling back to the sync implementation, we prefer to panic
        // and notify the user that they better use the default IO implementation.

        scores
    }

    fn check_vector(&self, point: PointOffsetType) -> bool {
        point < self.points_count
            // Deleted points propagate to vectors; check vector deletion for possible early return
            && !self
                .vec_deleted
                .get(point as usize)
                .map(|x| *x)
                // Default to not deleted if our deleted flags failed grow
                .unwrap_or(false)
            // Additionally check point deletion for integrity if delete propagation to vector failed
            && !self
                .point_deleted
                .get(point as usize)
                .map(|x| *x)
                // Default to deleted if the point mapping was removed from the ID tracker
                .unwrap_or(true)
    }

    fn score_point(&self, point: PointOffsetType) -> ScoreType {
        self.query_scorer.score_stored(point)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.query_scorer.score_internal(point_a, point_b)
    }

    fn peek_top_iter(
        &self,
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset> {
        if top == 0 {
            return vec![];
        }

        let mut pq = FixedLengthPriorityQueue::new(top);
        let points_stream = points
            .take_while(|_| !self.is_stopped.load(Ordering::Relaxed))
            .filter(|point_id| self.check_vector(*point_id));

        self.storage
            .read_vectors_async(points_stream, |_, point_id, other_vector| {
                let scored_point_offset = ScoredPointOffset {
                    idx: point_id,
                    score: self.query_scorer.score(other_vector),
                };
                pq.push(scored_point_offset);
            })
            .unwrap();

        // ToDo: io_uring is experimental, it can fail if it is not supported.
        // Instead of silently falling back to the sync implementation, we prefer to panic
        // and notify the user that they better use the default IO implementation.

        pq.into_vec()
    }

    fn peek_top_all(&self, top: usize) -> Vec<ScoredPointOffset> {
        if top == 0 {
            return vec![];
        }

        let points_stream = (0..self.points_count)
            .take_while(|_| !self.is_stopped.load(Ordering::Relaxed))
            .filter(|point_id| self.check_vector(*point_id));

        let mut pq = FixedLengthPriorityQueue::new(top);
        self.storage
            .read_vectors_async(points_stream, |_, point_id, other_vector| {
                let scored_point_offset = ScoredPointOffset {
                    idx: point_id,
                    score: self.query_scorer.score(other_vector),
                };
                pq.push(scored_point_offset);
            })
            .unwrap();

        // ToDo: io_uring is experimental, it can fail if it is not supported.
        // Instead of silently falling back to the sync implementation, we prefer to panic
        // and notify the user that they better use the default IO implementation.

        pq.into_vec()
    }
}

struct AsyncRawScorerBuilder<'a> {
    points_count: PointOffsetType,
    query: QueryVector,
    storage: &'a MemmapVectorStorage,
    point_deleted: &'a BitSlice,
    vec_deleted: &'a BitSlice,
    distance: Distance,
    is_stopped: Option<&'a AtomicBool>,
}

impl<'a> AsyncRawScorerBuilder<'a> {
    pub fn new(
        query: QueryVector,
        storage: &'a MemmapVectorStorage,
        point_deleted: &'a BitSlice,
    ) -> OperationResult<Self> {
        let points_count = storage.total_vector_count() as _;
        let vec_deleted = storage.deleted_vector_bitslice();

        let distance = storage.distance();

        let builder = Self {
            points_count,
            query,
            point_deleted,
            vec_deleted,
            storage,
            distance,
            is_stopped: None,
        };

        Ok(builder)
    }

    pub fn build(self) -> Box<dyn RawScorer + 'a> {
        match self.distance {
            Distance::Cosine => self._build_with_metric::<CosineMetric>(),
            Distance::Euclid => self._build_with_metric::<EuclidMetric>(),
            Distance::Dot => self._build_with_metric::<DotProductMetric>(),
        }
    }

    pub fn with_is_stopped(mut self, is_stopped: &'a AtomicBool) -> Self {
        self.is_stopped = Some(is_stopped);
        self
    }

    fn _build_with_metric<TMetric: Metric + 'a>(self) -> Box<dyn RawScorer + 'a> {
        let Self {
            points_count,
            query,
            storage,
            point_deleted,
            vec_deleted,
            distance: _,
            is_stopped,
        } = self;

        match query {
            QueryVector::Nearest(vector) => {
                let query_scorer = MetricQueryScorer::<TMetric, _>::new(vector, storage);
                Box::new(AsyncRawScorerImpl::new(
                    points_count,
                    query_scorer,
                    storage.get_mmap_vectors(),
                    point_deleted,
                    vec_deleted,
                    is_stopped.unwrap_or(&DEFAULT_STOPPED),
                ))
            }
            QueryVector::Recommend(reco_query) => {
                let query_scorer = CustomQueryScorer::<TMetric, _, _>::new(reco_query, storage);
                Box::new(AsyncRawScorerImpl::new(
                    points_count,
                    query_scorer,
                    storage.get_mmap_vectors(),
                    point_deleted,
                    vec_deleted,
                    is_stopped.unwrap_or(&DEFAULT_STOPPED),
                ))
            }
            QueryVector::Discovery(discovery_query) => {
                let query_scorer =
                    CustomQueryScorer::<TMetric, _, _>::new(discovery_query, storage);
                Box::new(AsyncRawScorerImpl::new(
                    points_count,
                    query_scorer,
                    storage.get_mmap_vectors(),
                    point_deleted,
                    vec_deleted,
                    is_stopped.unwrap_or(&DEFAULT_STOPPED),
                ))
            }
            QueryVector::Context(context_query) => {
                let query_scorer = CustomQueryScorer::<TMetric, _, _>::new(context_query, storage);
                Box::new(AsyncRawScorerImpl::new(
                    points_count,
                    query_scorer,
                    storage.get_mmap_vectors(),
                    point_deleted,
                    vec_deleted,
                    is_stopped.unwrap_or(&DEFAULT_STOPPED),
                ))
            }
        }
    }
}
