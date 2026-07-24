use std::sync::atomic::AtomicBool;

use common::bitvec::BitSlice;
use common::condition_checker::{CheckItem, ConditionChecker, Rest, Select};
use common::counter::hardware_counter::HardwareCounterCell;
use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::generic_consts::Random;
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
    scorer_bytes: &'a dyn QueryScorerBytes,
    filters: &'a ScorerFilters<'a>,
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
        hardware_counter: HardwareCounterCell,
    ) -> OperationResult<Self>
    where
        V: VectorStorageRead + RawScorerBuilder,
        Q: QuantizedVectorsRead,
    {
        let raw_scorer = match quantized_vectors {
            Some(quantized_vectors) => quantized_vectors.raw_scorer(query, hardware_counter)?,
            None => vectors.build_raw_scorer(query, hardware_counter)?,
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
        hardware_counter: HardwareCounterCell,
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
            Some(quantized_vectors) => quantized_vectors
                .raw_internal_scorer(point_id, hardware_counter)
                .or_else(|InternalScorerUnsupported(hardware_counter)| {
                    quantized_vectors.raw_scorer(original_query_fn(), hardware_counter)
                })?,
            None => {
                let query = original_query_fn();
                vectors.build_raw_scorer(query, hardware_counter)?
            }
        };
        Ok(FilteredScorer {
            raw_scorer,
            filters: ScorerFilters::new(filter_context, vectors.not_deleted_checker(point_deleted)),
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
            raw_scorer: new_raw_scorer(vector, vector_storage, HardwareCounterCell::new()).unwrap(),
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
    ) -> impl Iterator<Item = ScoredPointOffset> {
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

    /// Best-effort prefetch of the stored vectors for `point_ids` (experimental).
    ///
    /// Used to overlap the scattered-load latency of the subsequent
    /// `score_points` pass with the rest of the neighbor collection.
    pub fn prefetch_points(&self, point_ids: &[PointOffsetType]) {
        self.raw_scorer.prefetch_points(point_ids);
    }

    pub fn score_points_unfiltered(
        &mut self,
        point_ids: &[PointOffsetType],
    ) -> impl Iterator<Item = ScoredPointOffset> {
        if self.scores_buffer.len() < point_ids.len() {
            self.scores_buffer.resize(point_ids.len(), 0.0);
        }

        self.raw_scorer
            .score_points(point_ids, &mut self.scores_buffer[..point_ids.len()]);

        std::iter::zip(point_ids, &self.scores_buffer)
            .map(|(&idx, &score)| ScoredPointOffset { idx, score })
    }

    pub fn score_point(&self, point_id: PointOffsetType) -> ScoreType {
        self.raw_scorer.score_point(point_id)
    }

    pub fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.raw_scorer.score_internal(point_a, point_b)
    }
}

// We keep each scorer with its queue to reduce allocations and improve data locality.
struct BatchSearch<'a> {
    raw_scorer: Box<dyn RawScorer + 'a>,
    pq: FixedLengthPriorityQueue<ScoredPointOffset>,
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
        hardware_counter: HardwareCounterCell,
    ) -> OperationResult<Self>
    where
        V: VectorStorageRead + RawScorerBuilder,
        Q: QuantizedVectorsRead,
    {
        let scorer_batch = queries
            .iter()
            .map(|&query| {
                let query = query.to_owned();
                let hardware_counter = hardware_counter.fork();
                let raw_scorer = match quantized_vectors {
                    Some(quantized_vectors) => {
                        quantized_vectors.raw_scorer(query, hardware_counter)
                    }
                    None => vectors.build_raw_scorer(query, hardware_counter),
                };
                let pq = FixedLengthPriorityQueue::new(top);
                raw_scorer.map(|raw_scorer| BatchSearch { raw_scorer, pq })
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
                let raw_scorer = new_raw_scorer(
                    vector.to_owned(),
                    vector_storage,
                    HardwareCounterCell::new(),
                )
                .unwrap();
                BatchSearch {
                    raw_scorer,
                    pq: FixedLengthPriorityQueue::new(top),
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
            for BatchSearch { raw_scorer, pq } in &mut self.scorer_batch {
                raw_scorer.score_points(&chunk[..chunk_size], &mut scores_buffer[..chunk_size]);

                for i in 0..chunk_size {
                    pq.push(ScoredPointOffset {
                        idx: chunk[i],
                        score: scores_buffer[i],
                    });
                }
            }
        }

        let results = self
            .scorer_batch
            .into_iter()
            .map(|BatchSearch { pq, .. }| pq.into_sorted_vec())
            .collect();
        Ok(results)
    }
}
