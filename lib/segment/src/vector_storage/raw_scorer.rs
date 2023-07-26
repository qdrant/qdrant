use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};

use bitvec::prelude::BitSlice;

use super::{ScoredPointOffset, VectorStorage, VectorStorageEnum};
use crate::data_types::vectors::VectorElementType;
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric};
use crate::spaces::tools::{peek_top_largest_iterable, peek_worse_iterable};
use crate::types::{Distance, PointOffsetType, ScoreType};

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

    fn peek_worse_iter(
        &self,
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset>;

    fn peek_worse_all(&self, top: usize) -> Vec<ScoredPointOffset>;
}

pub struct RawScorerImpl<'a, TMetric: Metric, TVectorStorage: VectorStorage> {
    pub points_count: PointOffsetType,
    pub query: Vec<VectorElementType>,
    pub vector_storage: &'a TVectorStorage,
    /// [`BitSlice`] defining flags for deleted points (and thus these vectors).
    pub point_deleted: &'a BitSlice,
    /// [`BitSlice`] defining flags for deleted vectors in this segment.
    pub vec_deleted: &'a BitSlice,
    pub metric: PhantomData<TMetric>,
    /// This flag indicates that the search process is stopped externally,
    /// the search result is no longer needed and the search process should be stopped as soon as possible.
    pub is_stopped: &'a AtomicBool,
}

static ASYNC_SCORER: AtomicBool = AtomicBool::new(false);

pub fn set_async_scorer(async_scorer: bool) {
    ASYNC_SCORER.store(async_scorer, Ordering::Relaxed);
}

pub fn get_async_scorer() -> bool {
    ASYNC_SCORER.load(Ordering::Relaxed)
}

pub fn new_stoppable_raw_scorer<'a>(
    vector: Vec<VectorElementType>,
    vector_storage: &'a VectorStorageEnum,
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> Box<dyn RawScorer + 'a> {
    match vector_storage {
        VectorStorageEnum::Simple(vs) => raw_scorer_impl(vector, vs, point_deleted, is_stopped),

        VectorStorageEnum::Memmap(vs) => {
            if get_async_scorer() {
                #[cfg(target_os = "linux")]
                match super::async_raw_scorer::new(vector.clone(), vs, point_deleted, is_stopped) {
                    Ok(raw_scorer) => return raw_scorer,
                    Err(err) => log::error!("failed to initialize async raw scorer: {err}"),
                }

                #[cfg(not(target_os = "linux"))]
                log::warn!("async raw scorer is only supported on Linux");
            }

            raw_scorer_impl(vector, vs.as_ref(), point_deleted, is_stopped)
        }

        VectorStorageEnum::AppendableMemmap(vs) => {
            raw_scorer_impl(vector, vs.as_ref(), point_deleted, is_stopped)
        }
    }
}

pub static DEFAULT_STOPPED: AtomicBool = AtomicBool::new(false);

pub fn new_raw_scorer<'a>(
    vector: Vec<VectorElementType>,
    vector_storage: &'a VectorStorageEnum,
    point_deleted: &'a BitSlice,
) -> Box<dyn RawScorer + 'a> {
    new_stoppable_raw_scorer(vector, vector_storage, point_deleted, &DEFAULT_STOPPED)
}

pub fn raw_scorer_impl<'a, TVectorStorage: VectorStorage>(
    vector: Vec<VectorElementType>,
    vector_storage: &'a TVectorStorage,
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> Box<dyn RawScorer + 'a> {
    let points_count = vector_storage.total_vector_count() as PointOffsetType;
    let vec_deleted = vector_storage.deleted_vector_bitslice();
    match vector_storage.distance() {
        Distance::Cosine => Box::new(RawScorerImpl::<'a, CosineMetric, TVectorStorage> {
            points_count,
            query: CosineMetric::preprocess(&vector).unwrap_or(vector),
            vector_storage,
            point_deleted,
            vec_deleted,
            metric: PhantomData,
            is_stopped,
        }),
        Distance::Euclid => Box::new(RawScorerImpl::<'a, EuclidMetric, TVectorStorage> {
            points_count,
            query: EuclidMetric::preprocess(&vector).unwrap_or(vector),
            vector_storage,
            point_deleted,
            vec_deleted,
            metric: PhantomData,
            is_stopped,
        }),
        Distance::Dot => Box::new(RawScorerImpl::<'a, DotProductMetric, TVectorStorage> {
            points_count,
            query: DotProductMetric::preprocess(&vector).unwrap_or(vector),
            vector_storage,
            point_deleted,
            vec_deleted,
            metric: PhantomData,
            is_stopped,
        }),
    }
}

impl<'a, TMetric, TVectorStorage> RawScorer for RawScorerImpl<'a, TMetric, TVectorStorage>
where
    TMetric: Metric,
    TVectorStorage: VectorStorage,
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
            let other_vector = self.vector_storage.get_vector(point_id);
            scores[size] = ScoredPointOffset {
                idx: point_id,
                score: TMetric::similarity(&self.query, other_vector),
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
            let other_vector = self.vector_storage.get_vector(point_id);
            scores.push(ScoredPointOffset {
                idx: point_id,
                score: TMetric::similarity(&self.query, other_vector),
            });
        }
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
        let other_vector = self.vector_storage.get_vector(point);
        TMetric::similarity(&self.query, other_vector)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        let vector_a = self.vector_storage.get_vector(point_a);
        let vector_b = self.vector_storage.get_vector(point_b);
        TMetric::similarity(vector_a, vector_b)
    }

    fn peek_top_iter(
        &self,
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset> {
        let scores = points
            .take_while(|_| !self.is_stopped.load(Ordering::Relaxed))
            .filter(|point_id| self.check_vector(*point_id))
            .map(|point_id| {
                let other_vector = self.vector_storage.get_vector(point_id);
                ScoredPointOffset {
                    idx: point_id,
                    score: TMetric::similarity(&self.query, other_vector),
                }
            });
        peek_top_largest_iterable(scores, top)
    }

    fn peek_top_all(&self, top: usize) -> Vec<ScoredPointOffset> {
        let scores = (0..self.points_count)
            .take_while(|_| !self.is_stopped.load(Ordering::Relaxed))
            .filter(|point_id| self.check_vector(*point_id))
            .map(|point_id| {
                let point_id = point_id as PointOffsetType;
                let other_vector = &self.vector_storage.get_vector(point_id);
                ScoredPointOffset {
                    idx: point_id,
                    score: TMetric::similarity(&self.query, other_vector),
                }
            });
        peek_top_largest_iterable(scores, top)
    }

    fn peek_worse_iter(
        &self,
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset> {
        let scores = points
            .take_while(|_| !self.is_stopped.load(Ordering::Relaxed))
            .filter(|point_id| self.check_vector(*point_id))
            .map(|point_id| {
                let other_vector = self.vector_storage.get_vector(point_id);
                ScoredPointOffset {
                    idx: point_id,
                    score: TMetric::similarity(&self.query, other_vector),
                }
            });
        peek_worse_iterable(scores, top)
    }

    fn peek_worse_all(&self, top: usize) -> Vec<ScoredPointOffset> {
        let scores = (0..self.points_count)
            .take_while(|_| !self.is_stopped.load(Ordering::Relaxed))
            .filter(|point_id| self.check_vector(*point_id))
            .map(|point_id| {
                let point_id = point_id as PointOffsetType;
                let other_vector = &self.vector_storage.get_vector(point_id);
                ScoredPointOffset {
                    idx: point_id,
                    score: TMetric::similarity(&self.query, other_vector),
                }
            });
        peek_worse_iterable(scores, top)
    }
}
