use std::sync::atomic::{AtomicBool, Ordering};

use bitvec::prelude::BitSlice;
use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};

use super::query_scorer::custom_query_scorer::CustomQueryScorer;
use super::{DenseVectorStorage, VectorStorageEnum};
use crate::data_types::vectors::QueryVector;
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric};
use crate::spaces::tools::peek_top_largest_iterable;
use crate::types::Distance;
use crate::vector_storage::query_scorer::metric_query_scorer::MetricQueryScorer;
use crate::vector_storage::query_scorer::QueryScorer;

/// RawScorer            QueryScorer        Metric
/// ┌────────────────┐   ┌──────────────┐   ┌───────────────────┐
/// │                │   │              │   │  - Cosine         │
/// │       ┌─────┐  │   │     ┌─────┐  │   │  - Dot            │
/// │       │     │◄─┼───┤     │     │◄─┼───┤  - Euclidean      │
/// │       └─────┘  │   │     └─────┘  │   │                   │
/// │                │   │              │   │                   │
/// └────────────────┘   └──────────────┘   └───────────────────┘
/// - Deletions          - Scoring logic    - Vector Distance
/// - Stopping           - Query holding
/// - Access patterns    - Complex Queries
///
/// Optimized scorer for multiple scoring requests comparing with a single query
/// Holds current query and params, receives only subset of points to score
pub trait RawScorer {
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoredPointOffset]) -> usize;

    /// Score points without excluding deleted and filtered points
    ///
    /// # Arguments
    ///
    /// * `points` - points to score
    ///
    /// # Returns
    ///
    /// Vector of scored points
    fn score_points_unfiltered(
        &self,
        points: &mut dyn Iterator<Item = PointOffsetType>,
    ) -> Vec<ScoredPointOffset>;

    /// Return true if vector satisfies current search context for given point (exists and not deleted)
    fn check_vector(&self, point: PointOffsetType) -> bool;

    /// Score stored vector with vector under the given index
    fn score_point(&self, point: PointOffsetType) -> ScoreType;

    /// Return distance between stored points selected by IDs
    ///
    /// # Panics
    ///
    /// Panics if any id is out of range
    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType;

    fn peek_top_iter(
        &self,
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset>;

    fn peek_top_all(&self, top: usize) -> Vec<ScoredPointOffset>;
}

pub struct RawScorerImpl<'a, TQueryScorer: QueryScorer> {
    pub query_scorer: TQueryScorer,
    /// Point deleted flags should be explicitly present as `false`
    /// for each existing point in the segment.
    /// If there are no flags for some points, they are considered deleted.
    /// [`BitSlice`] defining flags for deleted points (and thus these vectors).
    pub point_deleted: &'a BitSlice,
    /// [`BitSlice`] defining flags for deleted vectors in this segment.
    pub vec_deleted: &'a BitSlice,
    /// This flag indicates that the search process is stopped externally,
    /// the search result is no longer needed and the search process should be stopped as soon as possible.
    pub is_stopped: &'a AtomicBool,
}

pub fn new_stoppable_raw_scorer<'a>(
    query: QueryVector,
    vector_storage: &'a VectorStorageEnum,
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> Box<dyn RawScorer + 'a> {
    match vector_storage {
        VectorStorageEnum::Simple(vs) => raw_scorer_impl(query, vs, point_deleted, is_stopped),

        VectorStorageEnum::Memmap(vs) => {
            if vs.has_async_reader() {
                #[cfg(target_os = "linux")]
                {
                    let scorer_result =
                        super::async_raw_scorer::new(query.clone(), vs, point_deleted, is_stopped);
                    match scorer_result {
                        Ok(raw_scorer) => return raw_scorer,
                        Err(err) => log::error!("failed to initialize async raw scorer: {err}"),
                    };
                }

                #[cfg(not(target_os = "linux"))]
                log::warn!("async raw scorer is only supported on Linux");
            }

            raw_scorer_impl(query, vs.as_ref(), point_deleted, is_stopped)
        }

        VectorStorageEnum::AppendableMemmap(vs) => {
            raw_scorer_impl(query, vs.as_ref(), point_deleted, is_stopped)
        }
    }
}

pub static DEFAULT_STOPPED: AtomicBool = AtomicBool::new(false);

pub fn new_raw_scorer<'a>(
    vector: QueryVector,
    vector_storage: &'a VectorStorageEnum,
    point_deleted: &'a BitSlice,
) -> Box<dyn RawScorer + 'a> {
    new_stoppable_raw_scorer(vector, vector_storage, point_deleted, &DEFAULT_STOPPED)
}

pub fn raw_scorer_impl<'a, TVectorStorage: DenseVectorStorage>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> Box<dyn RawScorer + 'a> {
    match vector_storage.distance() {
        Distance::Cosine => new_scorer_with_metric::<CosineMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Euclid => new_scorer_with_metric::<EuclidMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Dot => new_scorer_with_metric::<DotProductMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
    }
}

fn new_scorer_with_metric<'a, TMetric: Metric + 'a, TVectorStorage: DenseVectorStorage>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> Box<dyn RawScorer + 'a> {
    let vec_deleted = vector_storage.deleted_vector_bitslice();
    match query {
        QueryVector::Nearest(vector) => raw_scorer_from_query_scorer(
            MetricQueryScorer::<TMetric, _>::new(vector, vector_storage),
            point_deleted,
            vec_deleted,
            is_stopped,
        ),
        QueryVector::Recommend(reco_query) => raw_scorer_from_query_scorer(
            CustomQueryScorer::<TMetric, _, _>::new(reco_query, vector_storage),
            point_deleted,
            vec_deleted,
            is_stopped,
        ),
        QueryVector::Discovery(discovery_query) => raw_scorer_from_query_scorer(
            CustomQueryScorer::<TMetric, _, _>::new(discovery_query, vector_storage),
            point_deleted,
            vec_deleted,
            is_stopped,
        ),
        QueryVector::Context(discovery_context_query) => raw_scorer_from_query_scorer(
            CustomQueryScorer::<TMetric, _, _>::new(discovery_context_query, vector_storage),
            point_deleted,
            vec_deleted,
            is_stopped,
        ),
    }
}

pub fn raw_scorer_from_query_scorer<'a, TQueryScorer: QueryScorer + 'a>(
    query_scorer: TQueryScorer,
    point_deleted: &'a BitSlice,
    vec_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> Box<dyn RawScorer + 'a> {
    Box::new(RawScorerImpl::<TQueryScorer> {
        query_scorer,
        point_deleted,
        vec_deleted,
        is_stopped,
    })
}

impl<'a, TQueryScorer> RawScorer for RawScorerImpl<'a, TQueryScorer>
where
    TQueryScorer: QueryScorer,
{
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoredPointOffset]) -> usize {
        if self.is_stopped.load(Ordering::Relaxed) {
            return 0;
        }
        let mut size: usize = 0;
        for point_id in points.iter().copied() {
            if !self.check_vector(point_id) {
                continue;
            }
            scores[size] = ScoredPointOffset {
                idx: point_id,
                score: self.query_scorer.score_stored(point_id),
            };

            size += 1;
            if size == scores.len() {
                return size;
            }
        }
        size
    }

    fn score_points_unfiltered(
        &self,
        points: &mut dyn Iterator<Item = PointOffsetType>,
    ) -> Vec<ScoredPointOffset> {
        if self.is_stopped.load(Ordering::Relaxed) {
            return vec![];
        }
        let mut scores = vec![];
        for point_id in points {
            scores.push(ScoredPointOffset {
                idx: point_id,
                score: self.query_scorer.score_stored(point_id),
            });
        }
        scores
    }

    fn check_vector(&self, point: PointOffsetType) -> bool {
        // Deleted points propagate to vectors; check vector deletion for possible early return
        !self
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
        let scores = points
            .take_while(|_| !self.is_stopped.load(Ordering::Relaxed))
            .filter(|point_id| self.check_vector(*point_id))
            .map(|point_id| ScoredPointOffset {
                idx: point_id,
                score: self.query_scorer.score_stored(point_id),
            });
        peek_top_largest_iterable(scores, top)
    }

    fn peek_top_all(&self, top: usize) -> Vec<ScoredPointOffset> {
        let scores = (0..self.point_deleted.len() as PointOffsetType)
            .take_while(|_| !self.is_stopped.load(Ordering::Relaxed))
            .filter(|point_id| self.check_vector(*point_id))
            .map(|point_id| {
                let point_id = point_id as PointOffsetType;
                ScoredPointOffset {
                    idx: point_id,
                    score: self.query_scorer.score_stored(point_id),
                }
            });
        peek_top_largest_iterable(scores, top)
    }
}
