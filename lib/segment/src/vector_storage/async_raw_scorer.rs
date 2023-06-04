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
use crate::vector_storage::mmap_vectors::MmapVectors;
use crate::vector_storage::{RawScorer, ScoredPointOffset, VectorStorage, VectorStorageEnum};

const DISK_PARALLELISM: usize = 16; // TODO: benchmark it better, or make it configurable

pub struct AsyncRawScorerImpl<'a, TMetric: Metric> {
    points_count: PointOffsetType,
    query: Vec<VectorElementType>,
    point_deleted: &'a BitSlice,
    vec_deleted: &'a BitSlice,
    metric: PhantomData<TMetric>,
    uring_file: UringFile,
    storage: &'a MmapVectors,
}

impl<'a, TMetric> AsyncRawScorerImpl<'a, TMetric>
where
    TMetric: Metric,
{
    pub fn new_raw_scorer(
        vector: Vec<VectorElementType>,
        vector_storage: &'a VectorStorageEnum,
        point_deleted: &'a BitSlice,
    ) -> OperationResult<Box<dyn RawScorer + 'a>> {
        let points_count = vector_storage.total_vector_count() as PointOffsetType;
        let vec_deleted = vector_storage.deleted_vector_bitslice();

        match vector_storage {
            VectorStorageEnum::Simple(_vs) => {
                unreachable!("Simple vector storage is not supported for async raw scorer")
            }
            VectorStorageEnum::AppendableMemmap(_vs) => unreachable!(
                "Appendable memmap vector storage is not supported for async raw scorer"
            ),
            VectorStorageEnum::Memmap(vs) => {
                let uring_file =
                    tokio_uring::start(async { UringFile::open(vs.vector_path()).await })?;

                let storage = vs.get_mmap_vectors();

                match vs.distance() {
                    Distance::Cosine => Ok(Box::new(AsyncRawScorerImpl::<'a, CosineMetric> {
                        points_count,
                        query: CosineMetric::preprocess(&vector).unwrap_or(vector),
                        point_deleted,
                        vec_deleted,
                        metric: PhantomData,
                        uring_file,
                        storage,
                    })),
                    Distance::Euclid => Ok(Box::new(AsyncRawScorerImpl::<'a, EuclidMetric> {
                        points_count,
                        query: EuclidMetric::preprocess(&vector).unwrap_or(vector),
                        point_deleted,
                        vec_deleted,
                        metric: PhantomData,
                        uring_file,
                        storage,
                    })),
                    Distance::Dot => Ok(Box::new(AsyncRawScorerImpl::<'a, DotProductMetric> {
                        points_count,
                        query: DotProductMetric::preprocess(&vector).unwrap_or(vector),
                        point_deleted,
                        vec_deleted,
                        metric: PhantomData,
                        uring_file,
                        storage,
                    })),
                }
            }
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
