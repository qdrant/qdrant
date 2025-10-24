use std::sync::atomic::AtomicBool;

use bitvec::slice::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::cow::BoxCow;
use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};

use crate::common::operation_error::{CancellableResult, OperationResult, check_process_stopped};
use crate::data_types::vectors::QueryVector;
use crate::payload_storage::FilterContext;
use crate::vector_storage::common::VECTOR_READ_BATCH_SIZE;
use crate::vector_storage::quantized::quantized_query_scorer::InternalScorerUnsupported;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::query_scorer::QueryScorerBytes;
use crate::vector_storage::{
    Random, RawScorer, VectorStorage, VectorStorageEnum, check_deleted_condition, new_raw_scorer,
};

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
/// │ FilterContext   │  - Access patterns  │ Query  ◄───────┼─┐
/// │                 │                     │                │ │  Query
/// │ deleted_points  │                     │ TVectorStorage │ │ ┌──────────────────┐
/// │ deleted_vectors │                     └────────────────┘ └─┤ - RecoQuery      │
/// └─────────────────┘                                          │ - DiscoveryQuery │
///                                                              │ - ContextQuery   │
///                                                              └──────────────────┘
///                                                              - Scoring logic
///                                                              - Complex queries
/// ```
pub struct FilteredScorer<'a> {
    raw_scorer: Box<dyn RawScorer + 'a>,
    filters: ScorerFilters<'a>,
    /// Temporary buffer for scores.
    scores_buffer: Vec<ScoreType>,
}

pub struct ScorerFilters<'a> {
    filter_context: Option<BoxCow<'a, dyn FilterContext + 'a>>,
    /// Point deleted flags should be explicitly present as `false`
    /// for each existing point in the segment.
    /// If there are no flags for some points, they are considered deleted.
    /// [`BitSlice`] defining flags for deleted points (and thus these vectors).
    point_deleted: &'a BitSlice,
    /// [`BitSlice`] defining flags for deleted vectors in this segment.
    vec_deleted: &'a BitSlice,
}

impl<'a> ScorerFilters<'a> {
    /// Return true if vector satisfies current search context for given point:
    /// exists, not deleted, and satisfies filter context.
    pub fn check_vector(&self, point_id: PointOffsetType) -> bool {
        check_deleted_condition(point_id, self.vec_deleted, self.point_deleted)
            && self
                .filter_context
                .as_ref()
                .is_none_or(|f| f.check(point_id))
    }

    fn as_borrowed(&'a self) -> Self {
        ScorerFilters {
            filter_context: self.filter_context.as_ref().map(BoxCow::as_borrowed),
            point_deleted: self.point_deleted,
            vec_deleted: self.vec_deleted,
        }
    }
}

pub struct FilteredBytesScorer<'a> {
    scorer_bytes: &'a dyn QueryScorerBytes,
    filters: ScorerFilters<'a>,
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
    pub fn new(
        query: QueryVector,
        vectors: &'a VectorStorageEnum,
        quantized_vectors: Option<&'a QuantizedVectors>,
        filter_context: Option<BoxCow<'a, dyn FilterContext + 'a>>,
        point_deleted: &'a BitSlice,
        hardware_counter: HardwareCounterCell,
    ) -> OperationResult<Self> {
        let raw_scorer = match quantized_vectors {
            Some(quantized_vectors) => quantized_vectors.raw_scorer(query, hardware_counter)?,
            None => new_raw_scorer(query, vectors, hardware_counter)?,
        };
        Ok(FilteredScorer {
            raw_scorer,
            filters: ScorerFilters {
                filter_context,
                point_deleted,
                vec_deleted: vectors.deleted_vector_bitslice(),
            },
            scores_buffer: Vec::new(),
        })
    }

    pub fn new_internal(
        point_id: PointOffsetType,
        vectors: &'a VectorStorageEnum,
        quantized_vectors: Option<&'a QuantizedVectors>,
        filter_context: Option<BoxCow<'a, dyn FilterContext + 'a>>,
        point_deleted: &'a BitSlice,
        hardware_counter: HardwareCounterCell,
    ) -> OperationResult<Self> {
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
                new_raw_scorer(query, vectors, hardware_counter)?
            }
        };
        Ok(FilteredScorer {
            raw_scorer,
            filters: ScorerFilters {
                filter_context,
                point_deleted,
                vec_deleted: vectors.deleted_vector_bitslice(),
            },
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
            filters: ScorerFilters {
                filter_context: None,
                point_deleted,
                vec_deleted: vector_storage.deleted_vector_bitslice(),
            },
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
            filters: self.filters.as_borrowed(),
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
    pub fn score_points(
        &mut self,
        point_ids: &mut Vec<PointOffsetType>,
        limit: usize,
    ) -> impl Iterator<Item = ScoredPointOffset> {
        point_ids.retain(|point_id| self.filters.check_vector(*point_id));
        if limit != 0 {
            point_ids.truncate(limit);
        }

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

    pub fn peek_top_all(
        &self,
        top: usize,
        is_stopped: &AtomicBool,
    ) -> CancellableResult<Vec<ScoredPointOffset>> {
        let iter = self
            .filters
            .point_deleted
            .iter_zeros()
            .map(|p| p as PointOffsetType);
        self.peek_top_iter(iter, top, is_stopped)
    }

    pub fn peek_top_iter(
        &self,
        mut points: impl Iterator<Item = PointOffsetType>,
        top: usize,
        is_stopped: &AtomicBool,
    ) -> CancellableResult<Vec<ScoredPointOffset>> {
        if top == 0 {
            return Ok(vec![]);
        }

        let mut pq = FixedLengthPriorityQueue::new(top);

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

            self.raw_scorer
                .score_points(&chunk[..chunk_size], &mut scores_buffer[..chunk_size]);

            for i in 0..chunk_size {
                pq.push(ScoredPointOffset {
                    idx: chunk[i],
                    score: scores_buffer[i],
                });
            }
        }

        Ok(pq.into_sorted_vec())
    }
}
