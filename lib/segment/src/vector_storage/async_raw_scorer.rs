use std::marker::PhantomData;

use bitvec::prelude::BitSlice;
use futures::StreamExt;
use tokio_uring::fs::File as UringFile;

use crate::common::mmap_ops::transmute_from_u8_to_slice;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric};
use crate::spaces::tools::FixedLengthPriorityQueue;
use crate::types::{Distance, PointOffsetType, ScoreType};
use crate::vector_storage::memmap_vector_storage::MemmapVectorStorage;
use crate::vector_storage::mmap_vectors::MmapVectors;
use crate::vector_storage::{RawScorer, ScoredPointOffset, VectorStorage as _};

const DISK_PARALLELISM: usize = 16; // TODO: benchmark it better, or make it configurable

pub fn new<'a>(
    vector: Vec<VectorElementType>,
    storage: &'a MemmapVectorStorage,
    point_deleted: &'a BitSlice,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    Ok(AsyncRawScorerBuilder::new(vector, storage, point_deleted)?.build())
}

pub struct AsyncRawScorerImpl<'a, TMetric: Metric> {
    points_count: PointOffsetType,
    query: Vec<VectorElementType>,
    storage: &'a MmapVectors,
    point_deleted: &'a BitSlice,
    vec_deleted: &'a BitSlice,
    uring_file: UringFile,
    metric: PhantomData<TMetric>,
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
        uring_file: UringFile,
    ) -> Self {
        Self {
            points_count,
            query: TMetric::preprocess(&vector).unwrap_or(vector),
            storage,
            point_deleted,
            vec_deleted,
            uring_file,
            metric: PhantomData,
        }
    }

    async fn score_point(&self, point_id: PointOffsetType) -> ScoredPointOffset {
        let offset = self.storage.data_offset(point_id).unwrap(); // this is fine
        let raw_size = self.storage.raw_size();

        let buff = vec![0u8; raw_size];

        let (res, buff) = self.uring_file.read_at(buff, offset as u64).await;

        match res {
            Ok(sz) => {
                debug_assert_eq!(sz, raw_size);
                // Convert bytes vector into VectorElementType vector
                let other_vector = transmute_from_u8_to_slice(&buff);
                ScoredPointOffset {
                    idx: point_id,
                    score: TMetric::similarity(&self.query, other_vector),
                }
            }
            Err(err) => {
                debug_assert!(false, "Failed to read with uring: {:?}", err);
                // Fall back to mmap, if for some reason we can't read with uring
                // It might happen if we allowed to use uring on the old kernels
                let other_vector = self.storage.get_vector(point_id);
                ScoredPointOffset {
                    idx: point_id,
                    score: TMetric::similarity(&self.query, other_vector),
                }
            }
        }
    }
}

impl<'a, TMetric> RawScorer for AsyncRawScorerImpl<'a, TMetric>
where
    TMetric: Metric,
{
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoredPointOffset]) -> usize {
        tokio_uring::start(async {
            let points_stream = points
                .iter()
                .copied()
                .filter(|point_id| self.check_vector(*point_id))
                .map(|point_id| self.score_point(point_id));

            let mut scored_stream = futures::stream::iter(points_stream).buffered(DISK_PARALLELISM);
            let mut n: usize = 0;
            while let Some(result) = scored_stream.next().await {
                scores[n] = result;
                n += 1;
            }
            n
        })
    }

    fn check_vector(&self, point: PointOffsetType) -> bool {
        point < self.points_count
            // Deleted points propagate to vectors; check vector deletion for possible early return
            && !self
            .vec_deleted
            .get(point as usize)
            .map(|x| *x)
            .unwrap_or(false)
            // Additionally check point deletion for integrity if delete propagation to vector failed
            && !self
            .point_deleted
            .get(point as usize)
            .map(|x| *x)
            .unwrap_or(false)
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

        tokio_uring::start(async {
            let points_stream = points
                .filter(|point_id| self.check_vector(*point_id))
                .map(|point_id| self.score_point(point_id));

            let mut scored_stream = futures::stream::iter(points_stream).buffered(DISK_PARALLELISM);

            // Implement peek_top_iter, but with async streams
            let mut pq = FixedLengthPriorityQueue::new(top);
            while let Some(scored_point_offset) = scored_stream.next().await {
                pq.push(scored_point_offset);
            }
            pq.into_vec()
        })
    }

    fn peek_top_all(&self, top: usize) -> Vec<ScoredPointOffset> {
        if top == 0 {
            return vec![];
        }

        tokio_uring::start(async {
            let points_stream = (0..self.points_count)
                .filter(|point_id| self.check_vector(*point_id))
                .map(|point_id| self.score_point(point_id));

            let mut scored_stream = futures::stream::iter(points_stream).buffered(DISK_PARALLELISM);

            // Implement peek_top_iter, but with async streams
            let mut pq = FixedLengthPriorityQueue::new(top);
            while let Some(scored_point_offset) = scored_stream.next().await {
                pq.push(scored_point_offset);
            }
            pq.into_vec()
        })
    }
}

struct AsyncRawScorerBuilder<'a> {
    points_count: PointOffsetType,
    vector: Vec<VectorElementType>,
    storage: &'a MmapVectors,
    point_deleted: &'a BitSlice,
    vec_deleted: &'a BitSlice,
    uring_file: UringFile,
    distance: Distance,
}

impl<'a> AsyncRawScorerBuilder<'a> {
    pub fn new(
        vector: Vec<VectorElementType>,
        storage: &'a MemmapVectorStorage,
        point_deleted: &'a BitSlice,
    ) -> OperationResult<Self> {
        let uring_file =
            tokio_uring::start(async { UringFile::open(storage.vector_path()).await })?;

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
            uring_file,
            distance,
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

    fn with_metric<TMetric: Metric>(self) -> AsyncRawScorerImpl<'a, TMetric> {
        AsyncRawScorerImpl::new(
            self.points_count,
            self.vector,
            self.storage,
            self.point_deleted,
            self.vec_deleted,
            self.uring_file,
        )
    }
}
