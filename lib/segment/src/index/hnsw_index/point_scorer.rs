use std::sync::atomic::AtomicBool;

use common::bitmap_scan::BatchedBitmapScan;
use common::bitvec::BitSlice;
use common::condition_checker::{CheckItem, ConditionChecker, Rest, Select};
use common::generic_consts::Random;
use common::top_k::TopK;
use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};
use smallvec::SmallVec;

use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::vectors::QueryVector;
use crate::index::query_optimization::optimized_filter::OptimizedFilter;
use crate::vector_storage::common::VECTOR_READ_BATCH_SIZE;
use crate::vector_storage::quantized::quantized_query_scorer::InternalScorerUnsupported;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectorsRead;
use crate::vector_storage::query_scorer::QueryScorerBytes;
use crate::vector_storage::{NotDeletedChecker, RawScorer, RawScorerBuilder, VectorStorageRead};
#[cfg(feature = "testing")]
use crate::vector_storage::{VectorStorageEnum, new_raw_scorer};

/// Scorers composition:
///
/// ```plaintext
///                                                               Metric
///                                                              ┌─────────────┐
///                                                              │ - Cosine    │
///  FilteredScorer      RawScorer          QueryScorer          │ - Dot       │
/// ┌─────────────────┐ ┌───────────────┐   ┌────────────────┐ ┌─┤ - Euclidean │
/// │ RawScorer ◄─────┼─┤ QueryScorer ◄─┼───│ Metric ◄───────┼─┘ └─────────────┘
/// │                 │ └───────────────┘   │                │    - Vector Distance
/// │ ConditionChecker│  - Access patterns  │ Query  ◄───────┼─┐
/// │                 │                     │                │ │  Query
/// │ deleted_points  │                     │ TVectorStorage │ │ ┌──────────────────┐
/// │ deleted_vectors │                     └────────────────┘ └─┤ - RecoQuery      │
/// └─────────────────┘                                          │ - DiscoverQuery  │
///                                                              │ - ContextQuery   │
///                                                              └──────────────────┘
///                                                              - Scoring logic
///                                                              - Complex queries
/// ```
///
/// The `BatchFilteredSearcher` contains an array of `RawScorer`s, a common filter and certain parameters.
///
/// ```plaintext
/// BatchFilteredSearcher  RawScorer
///  ┌─────────────────┐  ┌───────────────┐
///  │ [RawScorer] ◄───┼──┤ QueryScorer ◄─┼── (ditto)
///  │                 │  └───────────────┘
///  │ ConditionChecker│
///  └─────────────────┘
/// ```
pub struct FilteredScorer<'a> {
    raw_scorer: Box<dyn RawScorer + 'a>,
    filters: ScorerFilters<'a>,
    /// Temporary buffer for scores.
    scores_buffer: Vec<ScoreType>,
}

pub struct ScorerFilters<'a> {
    filter_context: Option<OptimizedFilter<'a>>,
    deleted: NotDeletedChecker<'a>,
}

/// Scores points given by their position in `to_global` with a scorer over segment ids.
struct RemappedRawScorer<'a> {
    inner: Box<dyn RawScorer + 'a>,
    to_global: &'a [PointOffsetType],
}

impl RawScorer for RemappedRawScorer<'_> {
    fn score_points(
        &self,
        points: &[PointOffsetType],
        scores: &mut [ScoreType],
    ) -> OperationResult<()> {
        let mut global = [0; VECTOR_READ_BATCH_SIZE];
        for (points, scores) in points
            .chunks(VECTOR_READ_BATCH_SIZE)
            .zip(scores.chunks_mut(VECTOR_READ_BATCH_SIZE))
        {
            let global = &mut global[..points.len()];
            for (global, &local) in global.iter_mut().zip(points) {
                *global = self.to_global[local as usize];
            }
            self.inner.score_points(global, scores)?;
        }
        Ok(())
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.inner.score_internal(
            self.to_global[point_a as usize],
            self.to_global[point_b as usize],
        )
    }

    fn scorer_bytes(&self) -> Option<&dyn QueryScorerBytes> {
        // Graph building scores by id only.
        None
    }
}

impl<'a> ScorerFilters<'a> {
    pub fn new(
        filter_context: Option<OptimizedFilter<'a>>,
        deleted: NotDeletedChecker<'a>,
    ) -> Self {
        ScorerFilters {
            filter_context,
            deleted,
        }
    }

    /// Return true if vector satisfies current search context for given point:
    /// exists, not deleted, and satisfies filter context.
    pub fn check_vector(&self, point_id: PointOffsetType) -> bool {
        self.deleted.check_infallible(point_id)
            && self
                .filter_context
                .as_ref()
                .is_none_or(|f| f.check_infallible(point_id))
    }
}

impl ConditionChecker for ScorerFilters<'_> {
    type Error = OperationError;

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        Ok(self.deleted.check(point_id)?
            && match &self.filter_context {
                Some(f) => f.check(point_id)?,
                None => true,
            })
    }

    fn check_infallible(&self, point_id: PointOffsetType) -> bool {
        self.check_vector(point_id)
    }

    #[inline]
    fn check_batched<K: CheckItem>(
        &self,
        ids: &mut [K],
        select: Select,
        rest: Rest,
    ) -> OperationResult<usize> {
        let Self {
            filter_context,
            deleted,
        } = self;
        match select {
            Select::Matches => {
                let n = deleted.check_batched(ids, Select::Matches, rest)?;
                match filter_context {
                    Some(f) => f.check_batched(&mut ids[..n], Select::Matches, rest),
                    None => Ok(n),
                }
            }
            Select::NonMatches => {
                let deleted_rest = rest.keep_if(filter_context.is_some());
                let mut f = deleted.check_batched(ids, Select::NonMatches, deleted_rest)?;
                if let Some(filter) = filter_context {
                    f += filter.check_batched(&mut ids[f..], Select::NonMatches, rest)?;
                }
                Ok(f)
            }
        }
    }
}

pub struct FilteredBytesScorer<'a> {
    pub(super) scorer_bytes: &'a dyn QueryScorerBytes,
    pub(super) filters: &'a ScorerFilters<'a>,
}

impl<'a> FilteredBytesScorer<'a> {
    pub fn score_points(
        &self,
        points: &mut Vec<(PointOffsetType, &[u8])>,
        limit: usize,
    ) -> impl Iterator<Item = ScoredPointOffset> {
        points.retain(|(point_id, _)| self.filters.check_vector(*point_id));
        if limit != 0 {
            points.truncate(limit);
        }

        points.iter().map(|&(idx, bytes)| ScoredPointOffset {
            idx,
            score: self.scorer_bytes.score_bytes(bytes),
        })
    }
}

impl<'a> FilteredScorer<'a> {
    /// Create a new filtered scorer.
    ///
    /// If present, `quantized_vectors` will be used for scoring, otherwise `vectors` will be used.
    pub fn new<V, Q>(
        query: QueryVector,
        vectors: &'a V,
        quantized_vectors: Option<&'a Q>,
        filter_context: Option<OptimizedFilter<'a>>,
        point_deleted: &'a BitSlice,
    ) -> OperationResult<Self>
    where
        V: VectorStorageRead + RawScorerBuilder,
        Q: QuantizedVectorsRead,
    {
        let raw_scorer = match quantized_vectors {
            Some(quantized_vectors) => quantized_vectors.raw_scorer(query)?,
            None => vectors.build_raw_scorer(query)?,
        };
        Ok(FilteredScorer {
            raw_scorer,
            filters: ScorerFilters::new(filter_context, vectors.not_deleted_checker(point_deleted)),
            scores_buffer: Vec::new(),
        })
    }

    pub fn new_internal<V, Q>(
        point_id: PointOffsetType,
        vectors: &'a V,
        quantized_vectors: Option<&'a Q>,
        filter_context: Option<OptimizedFilter<'a>>,
        point_deleted: &'a BitSlice,
    ) -> OperationResult<Self>
    where
        V: VectorStorageRead + RawScorerBuilder,
        Q: QuantizedVectorsRead,
    {
        // This is a fallback function, which is used if quantized vector storage
        // is not capable of reconstructing the query vector.
        let original_query_fn = || {
            let query = vectors.get_vector::<Random>(point_id);
            let query: QueryVector = query.as_vec_ref().into();
            query
        };
        let raw_scorer = match quantized_vectors {
            Some(quantized_vectors) => quantized_vectors.raw_internal_scorer(point_id).or_else(
                |InternalScorerUnsupported| quantized_vectors.raw_scorer(original_query_fn()),
            )?,
            None => {
                let query = original_query_fn();
                vectors.build_raw_scorer(query)?
            }
        };
        Ok(FilteredScorer {
            raw_scorer,
            filters: ScorerFilters::new(filter_context, vectors.not_deleted_checker(point_deleted)),
            scores_buffer: Vec::new(),
        })
    }

    /// Scorer for a payload block built in its own id space: point ids are positions in
    /// `to_global`, which maps them to segment ids, and `block_deleted` holds one flag per block
    /// point. Scores are those of [`Self::new_internal`] for the same segment points.
    pub fn new_block_scorer<V, Q>(
        point_id: PointOffsetType,
        to_global: &'a [PointOffsetType],
        vectors: &'a V,
        quantized_vectors: Option<&'a Q>,
        block_deleted: &'a BitSlice,
    ) -> OperationResult<Self>
    where
        V: VectorStorageRead + RawScorerBuilder,
        Q: QuantizedVectorsRead,
    {
        let scorer = Self::new_internal(point_id, vectors, quantized_vectors, None, block_deleted)?;
        Ok(FilteredScorer {
            raw_scorer: Box::new(RemappedRawScorer {
                inner: scorer.raw_scorer,
                to_global,
            }),
            filters: ScorerFilters::new(
                None,
                NotDeletedChecker {
                    point_deleted: block_deleted,
                    vec_deleted: block_deleted,
                },
            ),
            scores_buffer: Vec::new(),
        })
    }

    /// Create a new filtered scorer for testing purposes.
    ///
    /// # Panics
    ///
    /// Panics if [`new_raw_scorer`] fails.
    #[cfg(feature = "testing")]
    pub fn new_for_test(
        vector: QueryVector,
        vector_storage: &'a VectorStorageEnum,
        point_deleted: &'a BitSlice,
    ) -> Self {
        FilteredScorer {
            raw_scorer: new_raw_scorer(vector, vector_storage).unwrap(),
            filters: ScorerFilters::new(None, vector_storage.not_deleted_checker(point_deleted)),
            scores_buffer: Vec::new(),
        }
    }

    pub fn raw_scorer(&self) -> &dyn RawScorer {
        self.raw_scorer.as_ref()
    }

    pub fn filters(&self) -> &ScorerFilters<'a> {
        &self.filters
    }

    /// Return [`FilteredBytesScorer`] if the underlying scorer supports it.
    pub fn scorer_bytes(&self) -> Option<FilteredBytesScorer<'_>> {
        Some(FilteredBytesScorer {
            scorer_bytes: self.raw_scorer.scorer_bytes()?,
            filters: &self.filters,
        })
    }

    /// Filters and calculates scores for the given slice of points IDs.
    ///
    /// For performance reasons this method mutates `point_ids`.
    ///
    /// # Arguments
    ///
    /// * `point_ids` - list of points to score.
    ///   **Warning**: This input will be wrecked during the execution.
    /// * `limit` - limits the number of points to process after filtering.
    ///   `0` means no limit.
    #[inline(always)]
    pub fn score_points(
        &mut self,
        point_ids: &mut Vec<PointOffsetType>,
        limit: usize,
    ) -> OperationResult<impl Iterator<Item = ScoredPointOffset>> {
        let mut n = self
            .filters
            .check_batched(point_ids, Select::Matches, Rest::Discard)
            .unwrap_or(0 /* TODO(uio): propagate error */);
        if limit != 0 {
            n = n.min(limit);
        }
        point_ids.truncate(n);

        self.score_points_unfiltered(point_ids)
    }

    pub fn score_points_unfiltered(
        &mut self,
        point_ids: &[PointOffsetType],
    ) -> OperationResult<impl Iterator<Item = ScoredPointOffset>> {
        if self.scores_buffer.len() < point_ids.len() {
            self.scores_buffer.resize(point_ids.len(), 0.0);
        }

        self.raw_scorer
            .score_points(point_ids, &mut self.scores_buffer[..point_ids.len()])?;

        Ok(std::iter::zip(point_ids, &self.scores_buffer)
            .map(|(&idx, &score)| ScoredPointOffset { idx, score }))
    }

    pub fn score_point(&self, point_id: PointOffsetType) -> OperationResult<ScoreType> {
        self.raw_scorer.score_point(point_id)
    }

    pub fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.raw_scorer.score_internal(point_a, point_b)
    }
}

// We keep each scorer with its queue to reduce allocations and improve data locality.
struct BatchSearch<'a> {
    raw_scorer: Box<dyn RawScorer + 'a>,
    top_k: TopK,
}

pub struct BatchFilteredSearcher<'a> {
    scorer_batch: SmallVec<[BatchSearch<'a>; 1]>,
    filters: ScorerFilters<'a>,
}

impl<'a> BatchFilteredSearcher<'a> {
    /// Create a new batch filtered searcher.
    ///
    /// If present, `quantized_vectors` will be used for scoring, otherwise `vectors` will be used.
    pub fn new<V, Q>(
        queries: &[&QueryVector],
        vectors: &'a V,
        quantized_vectors: Option<&'a Q>,
        filter_context: Option<OptimizedFilter<'a>>,
        top: usize,
        point_deleted: &'a BitSlice,
    ) -> OperationResult<Self>
    where
        V: VectorStorageRead + RawScorerBuilder,
        Q: QuantizedVectorsRead,
    {
        let scorer_batch = queries
            .iter()
            .map(|&query| {
                let query = query.to_owned();
                let raw_scorer = match quantized_vectors {
                    Some(quantized_vectors) => quantized_vectors.raw_scorer(query),
                    None => vectors.build_raw_scorer(query),
                };
                let top_k = TopK::new(top);
                raw_scorer.map(|raw_scorer| BatchSearch { raw_scorer, top_k })
            })
            .collect::<Result<_, _>>()?;
        let filters =
            ScorerFilters::new(filter_context, vectors.not_deleted_checker(point_deleted));
        Ok(Self {
            scorer_batch,
            filters,
        })
    }

    /// Create a new batched filtered searcher for testing purposes.
    ///
    /// # Panics
    ///
    /// Panics if [`new_raw_scorer`] fails.
    #[cfg(feature = "testing")]
    pub fn new_for_test(
        vectors: &[QueryVector],
        vector_storage: &'a VectorStorageEnum,
        point_deleted: &'a BitSlice,
        top: usize,
    ) -> Self {
        let scorer_batch = vectors
            .iter()
            .map(|vector| {
                let raw_scorer = new_raw_scorer(vector.to_owned(), vector_storage).unwrap();
                BatchSearch {
                    raw_scorer,
                    top_k: TopK::new(top),
                }
            })
            .collect();
        Self {
            scorer_batch,
            filters: ScorerFilters::new(None, vector_storage.not_deleted_checker(point_deleted)),
        }
    }

    /// Iterator over every internal point ID that isn't soft-deleted in this
    /// searcher's `point_deleted` bitslice.
    ///
    /// Does not apply deferred-point filtering — wrap with
    /// `PointMappingsRefEnum::filter_deferred_and_deleted` (or compose otherwise) before
    /// passing to [`Self::peek_top_iter`] when deferred awareness is needed.
    ///
    /// The returned iterator borrows the underlying bitslice (lifetime `'a`),
    /// independent of `&self`, so it can be composed and then passed into
    /// `peek_top_iter(self, ...)` which consumes the searcher.
    pub fn iter_not_deleted(&self) -> impl Iterator<Item = PointOffsetType> + 'a {
        self.filters
            .deleted
            .point_deleted
            .iter_zeros()
            .map(|p| p as PointOffsetType)
    }

    /// Score every non-deleted point without deferred filtering.
    ///
    /// Production paths compose `iter_not_deleted` with
    /// `PointMappingsRefEnum::filter_deferred_and_deleted` and call
    /// [`Self::peek_top_iter`] directly.
    #[cfg(feature = "testing")]
    pub fn peek_top_all(
        self,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        let iter = self.iter_not_deleted();
        self.peek_top_iter(iter, is_stopped)
    }

    /// Full-scan counterpart of [`Self::peek_top_iter`]: scores every point that
    /// is unflagged in the deletion bitmaps and in `mapping_deleted` / `shadowed`,
    /// and below `cutoff` (when `Some`). Callers obtain those three arguments from
    /// `PointMappingsRefEnum::visible_scan_masks`; the word-wise harvest via
    /// [`BatchedBitmapScan`] is what makes this beat the per-id iterator path on
    /// full scans over mostly-live segments.
    pub fn peek_top_visible(
        self,
        cutoff: Option<PointOffsetType>,
        mapping_deleted: &BitSlice,
        shadowed: &BitSlice,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        // A whole harvested 64-point block must fit into one scoring chunk.
        const { assert!(VECTOR_READ_BATCH_SIZE >= 64) };

        let Self {
            mut scorer_batch,
            filters,
        } = self;

        // Ignore points without an entry in `point_deleted` (absent entries count as
        // deleted, see `NotDeletedChecker`) and points at or above the deferred cutoff.
        let mut point_count = filters.deleted.point_deleted.len();
        if let Some(cutoff) = cutoff {
            point_count = point_count.min(cutoff as usize);
        }

        let mut scan = BatchedBitmapScan::new(
            point_count,
            [
                filters.deleted.point_deleted,
                filters.deleted.vec_deleted,
                mapping_deleted,
                shadowed,
            ],
        );

        let mut chunk = [0; VECTOR_READ_BATCH_SIZE];
        let mut scores_buffer = [0.0; VECTOR_READ_BATCH_SIZE];
        loop {
            let n = scan.next_chunk(&mut chunk);
            if n == 0 {
                break;
            }
            check_process_stopped(is_stopped)?;
            score_chunk(
                &filters,
                &mut scorer_batch,
                &mut chunk[..n],
                &mut scores_buffer,
            )?;
        }

        let results = scorer_batch
            .into_iter()
            .map(|BatchSearch { top_k, .. }| top_k.into_vec())
            .collect();
        Ok(results)
    }

    /// This function expects deferred points to be already filtered from the iterator.
    pub fn peek_top_iter(
        mut self,
        mut points: impl Iterator<Item = PointOffsetType>,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        // Reuse the same buffer for all chunks, to avoid reallocation
        let mut chunk = [0; VECTOR_READ_BATCH_SIZE];
        let mut scores_buffer = [0.0; VECTOR_READ_BATCH_SIZE];

        loop {
            check_process_stopped(is_stopped)?;

            let mut chunk_size = 0;
            for point_id in &mut points {
                check_process_stopped(is_stopped)?;

                if !self.filters.check_vector(point_id) {
                    continue;
                }
                chunk[chunk_size] = point_id;
                chunk_size += 1;
                if chunk_size == VECTOR_READ_BATCH_SIZE {
                    break;
                }
            }

            if chunk_size == 0 {
                break;
            }

            // Switching the loops improves batching performance, but slightly degrades single-query performance.
            for BatchSearch { raw_scorer, top_k } in &mut self.scorer_batch {
                raw_scorer.score_points(&chunk[..chunk_size], &mut scores_buffer[..chunk_size])?;

                top_k.push_batch(&chunk[..chunk_size], &scores_buffer[..chunk_size]);
            }
        }

        let results = self
            .scorer_batch
            .into_iter()
            .map(|BatchSearch { top_k, .. }| top_k.into_vec())
            .collect();
        Ok(results)
    }
}

/// Score one harvested chunk against every scorer and push into its queue.
/// Applies `filter_context` if present; the deletion bitmaps were already folded into the harvest masks by the caller.
fn score_chunk(
    filters: &ScorerFilters<'_>,
    scorer_batch: &mut [BatchSearch<'_>],
    chunk: &mut [PointOffsetType],
    scores_buffer: &mut [ScoreType; VECTOR_READ_BATCH_SIZE],
) -> OperationResult<()> {
    let n = match &filters.filter_context {
        Some(f) => f.check_batched(chunk, Select::Matches, Rest::Discard)?,
        None => chunk.len(),
    };
    let chunk = &chunk[..n];
    if chunk.is_empty() {
        return Ok(());
    }
    for BatchSearch { raw_scorer, top_k } in scorer_batch {
        raw_scorer.score_points(chunk, &mut scores_buffer[..chunk.len()])?;
        top_k.push_batch(chunk, &scores_buffer[..chunk.len()]);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use common::ambient;
    use common::bitvec::{BitSliceExt as _, BitVec};
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;
    use crate::index::condition_checker::{ConditionCheckerEnum, TestBitOfId};
    use crate::types::Distance;
    use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
    use crate::vector_storage::{DEFAULT_STOPPED, VectorStorage as _};

    fn random_mask(rng: &mut StdRng, len: usize, rate: f64) -> BitVec {
        let mut mask = BitVec::repeat(false, len);
        for i in 0..len {
            if rng.random_bool(rate) {
                mask.set(i, true);
            }
        }
        mask
    }

    /// Both scan paths must match scalar insertion in the scorer's input order,
    /// even when a payload filter rearranges a chunk before scoring it.
    #[test]
    fn batch_threshold_matches_scalar_insertion() {
        struct RecordingScorer<'a> {
            scores: &'a [ScoreType],
            visited: &'a RefCell<Vec<PointOffsetType>>,
        }

        impl RawScorer for RecordingScorer<'_> {
            fn score_points(
                &self,
                points: &[PointOffsetType],
                scores: &mut [ScoreType],
            ) -> OperationResult<()> {
                self.visited.borrow_mut().extend_from_slice(points);
                for (&point, score) in points.iter().zip(scores) {
                    *score = self.scores[point as usize];
                }
                Ok(())
            }

            fn score_internal(&self, _: PointOffsetType, _: PointOffsetType) -> ScoreType {
                unreachable!()
            }

            fn scorer_bytes(&self) -> Option<&dyn QueryScorerBytes> {
                None
            }
        }

        const TOTAL: usize = 259;
        const CUTOFF: PointOffsetType = 253;
        let mut rng = StdRng::seed_from_u64(42);
        let point_deleted = random_mask(&mut rng, TOTAL, 0.1);
        let vec_deleted = random_mask(&mut rng, TOTAL, 0.1);
        let mapping_deleted = random_mask(&mut rng, TOTAL, 0.1);
        let shadowed = random_mask(&mut rng, TOTAL, 0.1);
        let scores: Vec<Vec<ScoreType>> = (0..64)
            .map(|query| {
                (0..TOTAL)
                    .map(|point| match (point + query) % 41 {
                        0 => ScoreType::NAN,
                        1 => ScoreType::NEG_INFINITY,
                        2 => ScoreType::INFINITY,
                        3 => -0.0,
                        4 => 0.0,
                        _ => ((point * 13 + query * 7) % 19) as ScoreType - 9.0,
                    })
                    .collect()
            })
            .collect();
        let bits = |points: Vec<ScoredPointOffset>| {
            points
                .into_iter()
                .map(|point| (point.idx, point.score.to_bits()))
                .collect::<Vec<_>>()
        };

        for batch_size in [1, 64] {
            for k in [1, 10, 31, 32, 65, 400] {
                for filtered in [false, true] {
                    for visible in [false, true] {
                        let visited: Vec<_> =
                            (0..batch_size).map(|_| RefCell::new(Vec::new())).collect();
                        let searcher = BatchFilteredSearcher {
                            scorer_batch: scores
                                .iter()
                                .zip(&visited)
                                .map(|(scores, visited)| BatchSearch {
                                    raw_scorer: Box::new(RecordingScorer { scores, visited }),
                                    top_k: TopK::new(k),
                                })
                                .collect(),
                            filters: ScorerFilters::new(
                                filtered.then(|| {
                                    OptimizedFilter::from_checker(
                                        ConditionCheckerEnum::TestBitOfId(TestBitOfId(1)),
                                    )
                                }),
                                NotDeletedChecker {
                                    point_deleted: &point_deleted,
                                    vec_deleted: &vec_deleted,
                                },
                            ),
                        };
                        let extra_visible = |id: PointOffsetType| {
                            id < CUTOFF && !mapping_deleted[id as usize] && !shadowed[id as usize]
                        };
                        let results = if visible {
                            searcher.peek_top_visible(
                                Some(CUTOFF),
                                &mapping_deleted,
                                &shadowed,
                                &DEFAULT_STOPPED,
                            )
                        } else {
                            searcher.peek_top_iter(
                                (0..TOTAL as PointOffsetType).filter(|&id| extra_visible(id)),
                                &DEFAULT_STOPPED,
                            )
                        }
                        .unwrap();
                        let expected_ids: Vec<_> = (0..TOTAL as PointOffsetType)
                            .filter(|&id| {
                                extra_visible(id)
                                    && !point_deleted[id as usize]
                                    && !vec_deleted[id as usize]
                                    && (!filtered || id >> 1 & 1 == 1)
                            })
                            .collect();

                        for ((result, visited), scores) in
                            results.into_iter().zip(visited).zip(&scores)
                        {
                            let mut visited = visited.into_inner();
                            let mut scalar = TopK::new(k);
                            for &idx in &visited {
                                scalar.push(ScoredPointOffset {
                                    idx,
                                    score: scores[idx as usize],
                                });
                            }
                            assert_eq!(
                                bits(result),
                                bits(scalar.into_vec()),
                                "B={batch_size}, k={k}, filtered={filtered}, visible={visible}"
                            );
                            visited.sort_unstable();
                            assert_eq!(visited, expected_ids);
                        }
                    }
                }
            }
        }
    }

    /// [`BatchFilteredSearcher::peek_top_visible`] must reproduce the iterator
    /// path exactly for every combination of deletion bitmaps (including
    /// lengths that are not word multiples and differ from the point count),
    /// extra masks, and deferred cutoff.
    #[test]
    fn peek_top_visible_matches_iter_reference() {
        const TOTAL: usize = 300;
        const DIM: usize = 4;
        const TOP: usize = 20;

        let mut rng = StdRng::seed_from_u64(42);
        let _scope = ambient::test_guard();

        let mut storage = new_volatile_dense_vector_storage(DIM, Distance::Dot);
        for i in 0..TOTAL {
            let vector: Vec<f32> = (0..DIM).map(|_| rng.random_range(-1.0..1.0)).collect();
            storage
                .insert_vector(i as PointOffsetType, vector.as_slice().into())
                .unwrap();
        }
        for i in 0..TOTAL {
            if rng.random_bool(0.15) {
                storage.delete_vector(i as PointOffsetType).unwrap();
            }
        }

        let queries: Vec<QueryVector> = (0..2)
            .map(|_| {
                let v: Vec<f32> = (0..DIM).map(|_| rng.random_range(-1.0..1.0)).collect();
                v.as_slice().into()
            })
            .collect();

        let empty = BitVec::new();
        let all_live = BitVec::repeat(false, TOTAL);
        let mut dead_prefix = BitVec::repeat(false, TOTAL);
        for i in 0..200 {
            dead_prefix.set(i, true);
        }

        // (point_deleted, mapping_deleted, shadowed, cutoff)
        let cases: Vec<(BitVec, BitVec, BitVec, Option<PointOffsetType>)> = vec![
            // All alive, no extra bitmaps, no cutoff — the fully-live block
            // fast path.
            (all_live.clone(), empty.clone(), empty.clone(), None),
            // Empty point_deleted bitmap: everything counts as deleted.
            (BitVec::new(), empty.clone(), empty.clone(), None),
            // Random deletions everywhere; extra bitmaps shorter and longer
            // than the point count.
            (
                random_mask(&mut rng, TOTAL, 0.3),
                random_mask(&mut rng, 100, 0.5),
                random_mask(&mut rng, 400, 0.2),
                None,
            ),
            // point_deleted shorter than the storage: tail points excluded.
            (
                random_mask(&mut rng, 250, 0.1),
                empty.clone(),
                empty.clone(),
                None,
            ),
            // Deferred cutoff mid-word, at zero, and beyond the range.
            (
                random_mask(&mut rng, TOTAL, 0.2),
                random_mask(&mut rng, TOTAL, 0.1),
                empty.clone(),
                Some(150),
            ),
            (all_live.clone(), empty.clone(), empty.clone(), Some(0)),
            (all_live.clone(), empty.clone(), empty.clone(), Some(1000)),
            // Exactly one word.
            (
                random_mask(&mut rng, 64, 0.4),
                empty.clone(),
                empty.clone(),
                None,
            ),
            // Long fully-dead stretch before the live region.
            (dead_prefix, empty.clone(), empty.clone(), None),
        ];

        for (case_idx, (point_deleted, mapping_deleted, shadowed, cutoff)) in
            cases.iter().enumerate()
        {
            let visible =
                BatchFilteredSearcher::new_for_test(&queries, &storage, point_deleted, TOP)
                    .peek_top_visible(*cutoff, mapping_deleted, shadowed, &DEFAULT_STOPPED)
                    .unwrap();

            // Reference: the same visibility predicate applied id by id
            // through the iterator path (`peek_top_iter` re-checks the
            // deletion bitmaps itself via `check_vector`).
            let bound = cutoff.map_or(usize::MAX, |c| c as usize);
            let ids = (0..TOTAL as PointOffsetType).filter(|&id| {
                (id as usize) < bound
                    && !mapping_deleted.get_bit(id as usize).unwrap_or(false)
                    && !shadowed.get_bit(id as usize).unwrap_or(false)
            });
            let reference =
                BatchFilteredSearcher::new_for_test(&queries, &storage, point_deleted, TOP)
                    .peek_top_iter(ids, &DEFAULT_STOPPED)
                    .unwrap();

            assert_eq!(visible, reference, "case {case_idx}");
        }
    }
}
