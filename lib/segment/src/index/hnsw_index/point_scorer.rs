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
#[cfg(feature = "testing")]
use crate::vector_storage::new_raw_scorer;
use crate::vector_storage::quantized::quantized_query_scorer::InternalScorerUnsupported;
use crate::vector_storage::quantized::quantized_vectors::{QuantizedVectors, QuantizedVectorsRead};
use crate::vector_storage::query_scorer::QueryScorerBytes;
use crate::vector_storage::{
    NotDeletedChecker, RawScorer, RawScorerBuilder, VectorStorageEnum, VectorStorageRead,
};

/// The encoded vectors of one payload block, copied into a single buffer, where
/// the copy holds the *exact* bytes the scorer would otherwise have read.
pub struct BlockVectors {
    /// One encoded vector per block point, in local id order, spaced
    /// `stride` bytes apart.
    data: Vec<u64>,
    stride: usize,
    len: usize,
}

/// Largest block buffer worth copying, when one block is built at a time.
pub const MAX_BLOCK_GATHER_BYTES: usize = 32 * 1024 * 1024;

/// Floor for the per-block cap once it has been divided by the number of blocks
/// in flight.
pub const MIN_BLOCK_GATHER_BYTES: usize = 8 * 1024 * 1024;

/// Alignment the gather buffer can guarantee, from its `u64` backing store.
const GATHER_ALIGN: usize = align_of::<u64>();

impl BlockVectors {
    /// Copy the encoded vectors of `points` into one contiguous buffer.
    ///
    /// Returns `None` - meaning "score against the global storage instead" -
    /// when this storage exposes no fixed-layout byte view (sparse and
    /// multivector storages), when the layout needs more alignment than the
    /// buffer provides or a stride that is not a whole number of alignment
    /// units, or when the copy would exceed `max_bytes`.
    pub fn try_gather(
        points: &[PointOffsetType],
        vectors: &VectorStorageEnum,
        quantized_vectors: Option<&QuantizedVectors>,
        max_bytes: usize,
    ) -> Option<Self> {
        let layout = match quantized_vectors {
            Some(quantized) => quantized.get_quantized_vector_layout().ok()?,
            None => vectors.get_vector_layout().ok()?,
        };
        // A stride that is not a multiple of the alignment would leave every
        // other vector in the buffer misaligned. No current layout has one -
        // encoders that read plain bytes declare alignment 1 - so this guards
        // future encoders, not a reachable case.
        let stride = layout.size();
        if stride == 0 || layout.align() > GATHER_ALIGN || !stride.is_multiple_of(layout.align()) {
            return None;
        }

        let total = stride.checked_mul(points.len())?;
        if total == 0 || total > max_bytes {
            return None;
        }

        let mut data = vec![0u64; total.div_ceil(size_of::<u64>())];
        let bytes: &mut [u8] = bytemuck::cast_slice_mut(&mut data);

        for (local, &global) in points.iter().enumerate() {
            let dst = &mut bytes[local * stride..local * stride + stride];
            let copied = match quantized_vectors {
                Some(quantized) => {
                    let src = quantized.get_quantized_vector(global);
                    (src.len() == stride).then(|| dst.copy_from_slice(&src))
                }
                None => vectors
                    .with_vector_bytes_opt::<Random, _>(global, |src| {
                        (src.len() == stride).then(|| dst.copy_from_slice(src))
                    })
                    .ok()
                    .flatten()
                    .flatten(),
            };
            // A short or missing vector means the layout does not describe this
            // storage after all. Score against the storage rather than guess.
            copied?;
        }

        Some(BlockVectors {
            data,
            stride,
            len: points.len(),
        })
    }

    pub fn gather_constraints(
        vectors: &VectorStorageEnum,
        quantized_vectors: Option<&QuantizedVectors>,
        max_bytes: usize,
    ) -> String {
        let layout = match quantized_vectors {
            Some(quantized) => quantized.get_quantized_vector_layout(),
            None => vectors.get_vector_layout(),
        };
        match layout {
            Err(err) => format!("storage exposes no fixed-layout byte view ({err})"),
            Ok(layout) if layout.align() > GATHER_ALIGN => format!(
                "vector alignment {} exceeds the buffer's {GATHER_ALIGN} bytes",
                layout.align(),
            ),
            Ok(layout) if !layout.size().is_multiple_of(layout.align().max(1)) => format!(
                "stride {} B is not a multiple of the vector alignment {} B",
                layout.size(),
                layout.align(),
            ),
            Ok(layout) => format!(
                "stride {} B against a {} MiB per-block cap, so blocks over ~{} points",
                layout.size(),
                max_bytes / (1024 * 1024),
                max_bytes / layout.size().max(1),
            ),
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    /// Encoded bytes of the block point with local id `local_id`.
    #[inline]
    fn get(&self, local_id: PointOffsetType) -> &[u8] {
        // The slice op alone would not always catch an out-of-range id: the
        // buffer rounds up to whole `u64`s, and an id just past the end can
        // land in that slack and read pad bytes instead of panicking.
        debug_assert!((local_id as usize) < self.len);
        let start = self.stride * local_id as usize;
        &bytemuck::cast_slice::<u64, u8>(&self.data)[start..start + self.stride]
    }
}

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
        let raw_scorer =
            Self::internal_raw_scorer(point_id, vectors, quantized_vectors, hardware_counter)?;
        Ok(FilteredScorer {
            raw_scorer,
            filters: ScorerFilters::new(filter_context, vectors.not_deleted_checker(point_deleted)),
            scores_buffer: Vec::new(),
        })
    }

    /// Score a payload-block subgraph whose points are numbered `0..to_global.len()`.
    /// `to_global` maps every block point back to its segment-wide id, and `block_deleted`
    /// holds one flag per block point.
    pub fn new_block_scorer(
        global_point_id: PointOffsetType,
        to_global: &'a [PointOffsetType],
        vectors: &'a VectorStorageEnum,
        quantized_vectors: Option<&'a QuantizedVectors>,
        block_vectors: Option<&'a BlockVectors>,
        block_deleted: &'a BitSlice,
        hardware_counter: HardwareCounterCell,
    ) -> OperationResult<Self> {
        // One entry per block point, in `to_global`'s local order - a shorter
        // slice would make the filters treat valid block points as deleted.
        debug_assert_eq!(block_deleted.len(), to_global.len());
        // A mismatched copy is a caller bug, not a decline: in release it
        // falls back to the remapped path below, silently but correctly.
        debug_assert!(
            block_vectors.is_none_or(|block| block.len() == to_global.len()),
            "gathered buffer does not match the block it is scored for",
        );

        let inner = Self::internal_raw_scorer(
            global_point_id,
            vectors,
            quantized_vectors,
            hardware_counter,
        )?;

        let usable = block_vectors
            .filter(|block| block.len() == to_global.len())
            .filter(|_| inner.scorer_bytes().is_some());

        let raw_scorer: Box<dyn RawScorer + 'a> = match usable {
            Some(block) => Box::new(GatherRawScorer {
                inner,
                to_global,
                block,
            }),
            None => Box::new(RemappedRawScorer { inner, to_global }),
        };

        Ok(FilteredScorer {
            raw_scorer,
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

    /// Raw scorer that scores against the vector stored under `point_id`.
    fn internal_raw_scorer<V, Q>(
        point_id: PointOffsetType,
        vectors: &'a V,
        quantized_vectors: Option<&'a Q>,
        hardware_counter: HardwareCounterCell,
    ) -> OperationResult<Box<dyn RawScorer + 'a>>
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
        match quantized_vectors {
            Some(quantized_vectors) => quantized_vectors
                .raw_internal_scorer(point_id, hardware_counter)
                .or_else(|InternalScorerUnsupported(hardware_counter)| {
                    quantized_vectors.raw_scorer(original_query_fn(), hardware_counter)
                }),
            None => {
                let query = original_query_fn();
                vectors.build_raw_scorer(query, hardware_counter)
            }
        }
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

/// Drives a scorer built over segment-wide ids with the local ids of a
/// payload-block subgraph.
struct RemappedRawScorer<'a> {
    inner: Box<dyn RawScorer + 'a>,
    to_global: &'a [PointOffsetType],
}

/// Score block points through a scorer built over segment-wide ids, translating
/// local ids to global ones as it goes.
fn remapped_score_points(
    inner: &dyn RawScorer,
    to_global: &[PointOffsetType],
    points: &[PointOffsetType],
    scores: &mut [ScoreType],
) {
    debug_assert_eq!(points.len(), scores.len());
    let mut translated = [0; VECTOR_READ_BATCH_SIZE];
    for (points, scores) in points
        .chunks(VECTOR_READ_BATCH_SIZE)
        .zip(scores.chunks_mut(VECTOR_READ_BATCH_SIZE))
    {
        let translated = &mut translated[..points.len()];
        for (global, &local) in translated.iter_mut().zip(points) {
            *global = to_global[local as usize];
        }
        inner.score_points(translated, scores);
    }
}

impl RawScorer for RemappedRawScorer<'_> {
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoreType]) {
        remapped_score_points(self.inner.as_ref(), self.to_global, points, scores);
    }

    fn score_point(&self, point: PointOffsetType) -> ScoreType {
        self.inner.score_point(self.to_global[point as usize])
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.inner.score_internal(
            self.to_global[point_a as usize],
            self.to_global[point_b as usize],
        )
    }

    fn scorer_bytes(&self) -> Option<&dyn QueryScorerBytes> {
        None
    }
}

/// Scores candidates against a copy of the block's vectors instead of the
/// segment-wide storage.
struct GatherRawScorer<'a> {
    inner: Box<dyn RawScorer + 'a>,
    to_global: &'a [PointOffsetType],
    block: &'a BlockVectors,
}

impl RawScorer for GatherRawScorer<'_> {
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoreType]) {
        debug_assert_eq!(points.len(), scores.len());
        let Some(bytes_scorer) = self.inner.scorer_bytes() else {
            // Ruled out when this scorer was built. Score against the storage
            // rather than report something else.
            debug_assert!(false, "gather scorer built over a scorer without bytes");
            return remapped_score_points(self.inner.as_ref(), self.to_global, points, scores);
        };
        for (&local, score) in points.iter().zip(scores.iter_mut()) {
            *score = bytes_scorer.score_bytes(self.block.get(local));
        }
    }

    fn score_point(&self, point: PointOffsetType) -> ScoreType {
        match self.inner.scorer_bytes() {
            Some(bytes_scorer) => bytes_scorer.score_bytes(self.block.get(point)),
            None => self.inner.score_point(self.to_global[point as usize]),
        }
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        if let Some(bytes_scorer) = self.inner.scorer_bytes()
            && let Some(score) =
                bytes_scorer.score_internal_bytes(self.block.get(point_a), self.block.get(point_b))
        {
            return score;
        }
        self.inner.score_internal(
            self.to_global[point_a as usize],
            self.to_global[point_b as usize],
        )
    }

    fn scorer_bytes(&self) -> Option<&dyn QueryScorerBytes> {
        None
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

#[cfg(test)]
mod tests {
    // Config structs keep their deprecated placement fields until 2.0; a
    // struct literal has to name them either way.
    #![allow(deprecated)]

    use common::bitvec::BitVec;
    use rand::SeedableRng as _;
    use rand::rngs::StdRng;

    use super::*;
    use crate::data_types::vectors::{VectorElementType, VectorRef};
    use crate::fixtures::index_fixtures::random_vector;
    use crate::types::{
        Distance, QuantizationConfig, TurboQuantBitSize, TurboQuantQuantizationConfig,
        TurboQuantization,
    };
    use crate::vector_storage::VectorStorage;
    use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
    use crate::vector_storage::quantized::quantized_vectors::QuantizedVectorsStorageType;

    /// A dense storage with TurboQuant 4-bit quantization over it. At `dim`
    /// values not covered by [`TurboQuantizer::padded_dim`]'s packing, the
    /// quantized stride is not a whole number of alignment units, which is
    /// exactly the case the gather buffer's padded stride exists for.
    fn tq_fixture(
        dim: usize,
        num_vectors: usize,
    ) -> (VectorStorageEnum, QuantizedVectors, tempfile::TempDir) {
        let mut rng = StdRng::seed_from_u64(42);
        let mut storage = new_volatile_dense_vector_storage(dim, Distance::Dot);
        let hw_counter = HardwareCounterCell::new();
        for offset in 0..num_vectors as PointOffsetType {
            let vector =
                Distance::Dot.preprocess_vector::<VectorElementType>(random_vector(&mut rng, dim));
            storage
                .insert_vector(offset, VectorRef::from(&vector), &hw_counter)
                .unwrap();
        }

        let config = QuantizationConfig::Turbo(TurboQuantization {
            turbo: TurboQuantQuantizationConfig {
                always_ram: Some(true),
                memory: None,
                bits: Some(TurboQuantBitSize::Bits4),
            },
        });
        let dir = tempfile::tempdir().unwrap();
        let quantized = QuantizedVectors::create(
            &storage,
            &config,
            QuantizedVectorsStorageType::Immutable,
            dir.path(),
            1,
            &AtomicBool::new(false),
        )
        .unwrap();
        (storage, quantized, dir)
    }

    #[test]
    fn gather_packs_odd_stride_tightly() {
        // 10 dims at 4 bits pack into 5 bytes of codes plus a 4-byte extras
        // trailer: a 9-byte stride. TurboQuant declares byte alignment, so
        // the buffer packs vectors back to back at exactly that stride.
        let (storage, quantized, _dir) = tq_fixture(10, 64);
        let layout = quantized.get_quantized_vector_layout().unwrap();
        assert_ne!(
            layout.size() % 8,
            0,
            "fixture no longer produces an odd stride; pick a dim that does",
        );

        // Every other point, so local and global ids differ.
        let points: Vec<PointOffsetType> = (0..64).step_by(2).collect();
        let block =
            BlockVectors::try_gather(&points, &storage, Some(&quantized), MAX_BLOCK_GATHER_BYTES)
                .expect("a byte-aligned odd stride must gather");

        assert_eq!(block.stride, layout.size());
        for (local, &global) in points.iter().enumerate() {
            assert_eq!(
                block.get(local as PointOffsetType),
                &*quantized.get_quantized_vector(global),
                "gathered bytes of local {local} differ from storage",
            );
        }
    }

    #[test]
    fn gather_scores_match_storage() {
        let (storage, quantized, _dir) = tq_fixture(10, 64);

        // The gather only engages when the inner scorer scores raw bytes; if
        // TurboQuant ever stops offering that, the equality below would just
        // compare the fallback path against itself. Same for the symmetric
        // kernel and the internal-scoring comparison.
        let inner = FilteredScorer::internal_raw_scorer(
            0,
            &storage,
            Some(&quantized),
            HardwareCounterCell::new(),
        )
        .unwrap();
        let inner_bytes = inner
            .scorer_bytes()
            .expect("TQ scorer lost its byte entry point; this test no longer covers the gather");

        let points: Vec<PointOffsetType> = (1..64).step_by(3).collect();
        let block =
            BlockVectors::try_gather(&points, &storage, Some(&quantized), MAX_BLOCK_GATHER_BYTES)
                .unwrap();

        assert!(
            inner_bytes
                .score_internal_bytes(block.get(0), block.get(1))
                .is_some(),
            "TQ scorer lost its symmetric byte kernel; the internal-scoring \
             comparison below no longer covers the gathered path",
        );

        let block_deleted = BitVec::repeat(false, points.len());
        let scorer = |block_vectors| {
            FilteredScorer::new_block_scorer(
                points[0],
                &points,
                &storage,
                Some(&quantized),
                block_vectors,
                &block_deleted,
                HardwareCounterCell::new(),
            )
            .unwrap()
        };
        let gathered = scorer(Some(&block));
        let ungathered = scorer(None);

        for local in 0..points.len() as PointOffsetType {
            assert_eq!(
                gathered.score_point(local).to_bits(),
                ungathered.score_point(local).to_bits(),
                "scores diverge at local {local}",
            );
        }

        for a in 0..points.len() as PointOffsetType {
            for b in 0..points.len() as PointOffsetType {
                assert_eq!(
                    gathered.score_internal(a, b).to_bits(),
                    ungathered.score_internal(a, b).to_bits(),
                    "internal scores diverge from storage at locals {a}, {b}",
                );
            }
        }
    }

    #[test]
    fn gather_dense_bytes_match_storage() {
        // f32 dense: stride dim * 4 with 4-byte alignment.
        let mut rng = StdRng::seed_from_u64(42);
        let dim = 8;
        let mut storage = new_volatile_dense_vector_storage(dim, Distance::Dot);
        let hw_counter = HardwareCounterCell::new();
        for offset in 0..32 {
            let vector = random_vector(&mut rng, dim);
            storage
                .insert_vector(offset, VectorRef::from(&vector), &hw_counter)
                .unwrap();
        }

        let points: Vec<PointOffsetType> = (0..32).step_by(2).collect();
        let block = BlockVectors::try_gather(&points, &storage, None, MAX_BLOCK_GATHER_BYTES)
            .expect("aligned dense storage must gather");

        assert_eq!(block.stride, dim * size_of::<f32>());
        for (local, &global) in points.iter().enumerate() {
            let matches = storage
                .with_vector_bytes_opt::<Random, _>(global, |src| {
                    src == block.get(local as PointOffsetType)
                })
                .unwrap()
                .unwrap();
            assert!(
                matches,
                "gathered bytes of local {local} differ from storage"
            );
        }
    }
}
