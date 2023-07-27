use std::cmp::Reverse;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};

use bitvec::prelude::BitSlice;
use common::fixed_length_priority_queue::FixedLengthPriorityQueue;

use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric};
use crate::types::{Distance, PointOffsetType, ScoreType};
use crate::vector_storage::memmap_vector_storage::MemmapVectorStorage;
use crate::vector_storage::mmap_vectors::MmapVectors;
use crate::vector_storage::{RawScorer, ScoredPointOffset, VectorStorage as _, DEFAULT_STOPPED};

pub fn new<'a>(
    vector: Vec<VectorElementType>,
    storage: &'a MemmapVectorStorage,
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    Ok(AsyncRawScorerBuilder::new(vector, storage, point_deleted)?
        .with_is_stopped(is_stopped)
        .build())
}

pub struct AsyncRawScorerImpl<'a, TMetric: Metric> {
    points_count: PointOffsetType,
    query: Vec<VectorElementType>,
    storage: &'a MmapVectors,
    point_deleted: &'a BitSlice,
    vec_deleted: &'a BitSlice,
    metric: PhantomData<TMetric>,
    /// This flag indicates that the search process is stopped externally,
    /// the search result is no longer needed and the search process should be stopped as soon as possible.
    pub is_stopped: &'a AtomicBool,
}

impl<'a, TMetric> AsyncRawScorerImpl<'a, TMetric>
where
    TMetric: Metric,
{
    fn new(
        points_count: PointOffsetType,
        vector: Vec<VectorElementType>,
        storage: &'a MmapVectors,
        point_deleted: &'a BitSlice,
        vec_deleted: &'a BitSlice,
        is_stopped: &'a AtomicBool,
    ) -> Self {
        Self {
            points_count,
            query: TMetric::preprocess(&vector).unwrap_or(vector),
            storage,
            point_deleted,
            vec_deleted,
            metric: PhantomData,
            is_stopped,
        }
    }
}

impl<'a, TMetric> RawScorer for AsyncRawScorerImpl<'a, TMetric>
where
    TMetric: Metric,
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
                    score: TMetric::similarity(&self.query, other_vector),
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
                    score: TMetric::similarity(&self.query, other_vector),
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
        let other_vector = self.storage.get_vector(point);
        TMetric::similarity(&self.query, other_vector)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        let vector_a = self.storage.get_vector(point_a);
        let vector_b = self.storage.get_vector(point_b);
        TMetric::similarity(vector_a, vector_b)
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
                    score: TMetric::similarity(&self.query, other_vector),
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
                    score: TMetric::similarity(&self.query, other_vector),
                };
                pq.push(scored_point_offset);
            })
            .unwrap();

        // ToDo: io_uring is experimental, it can fail if it is not supported.
        // Instead of silently falling back to the sync implementation, we prefer to panic
        // and notify the user that they better use the default IO implementation.

        pq.into_vec()
    }

    fn peek_worse_iter(
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
                    score: TMetric::similarity(&self.query, other_vector),
                };
                pq.push(Reverse(scored_point_offset));
            })
            .unwrap();

        // ToDo: io_uring is experimental, it can fail if it is not supported.
        // Instead of silently falling back to the sync implementation, we prefer to panic
        // and notify the user that they better use the default IO implementation.

        pq.into_vec().iter().map(|x| x.0).collect()
    }

    fn peek_worse_all(&self, top: usize) -> Vec<ScoredPointOffset> {
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
                    score: TMetric::similarity(&self.query, other_vector),
                };
                pq.push(Reverse(scored_point_offset));
            })
            .unwrap();

        // ToDo: io_uring is experimental, it can fail if it is not supported.
        // Instead of silently falling back to the sync implementation, we prefer to panic
        // and notify the user that they better use the default IO implementation.

        pq.into_vec().iter().map(|x| x.0).collect()
    }
}

struct AsyncRawScorerBuilder<'a> {
    points_count: PointOffsetType,
    vector: Vec<VectorElementType>,
    storage: &'a MmapVectors,
    point_deleted: &'a BitSlice,
    vec_deleted: &'a BitSlice,
    distance: Distance,
    is_stopped: Option<&'a AtomicBool>,
}

impl<'a> AsyncRawScorerBuilder<'a> {
    pub fn new(
        vector: Vec<VectorElementType>,
        storage: &'a MemmapVectorStorage,
        point_deleted: &'a BitSlice,
    ) -> OperationResult<Self> {
        let points_count = storage.total_vector_count() as _;
        let vec_deleted = storage.deleted_vector_bitslice();

        let distance = storage.distance();
        let storage = storage.get_mmap_vectors();

        let builder = Self {
            points_count,
            vector,
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
            Distance::Cosine => Box::new(self.with_metric::<CosineMetric>()),
            Distance::Euclid => Box::new(self.with_metric::<EuclidMetric>()),
            Distance::Dot => Box::new(self.with_metric::<DotProductMetric>()),
        }
    }

    pub fn with_is_stopped(mut self, is_stopped: &'a AtomicBool) -> Self {
        self.is_stopped = Some(is_stopped);
        self
    }

    fn with_metric<TMetric: Metric>(self) -> AsyncRawScorerImpl<'a, TMetric> {
        AsyncRawScorerImpl::new(
            self.points_count,
            self.vector,
            self.storage,
            self.point_deleted,
            self.vec_deleted,
            self.is_stopped.unwrap_or(&DEFAULT_STOPPED),
        )
    }
}
