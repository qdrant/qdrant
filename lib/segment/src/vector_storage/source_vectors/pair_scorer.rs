//! Scores of pairs of stored points of [`SourceVectors`].

use common::generic_consts::Random;
use common::types::{PointOffsetType, ScoreType};

use super::SourceVectors;
use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::QueryVector;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectorStorage;
use crate::vector_storage::{RawScorer, RawScorerBuilder as _, VectorStorageRead as _};

/// Scores pairs of stored points as `score_internal` of HNSW insertion's scorer
/// (`FilteredScorer::new_internal`) does. Only pairs: no query, and no filters, so deleted points
/// are the caller's to skip.
pub enum PairScorer<'a> {
    /// From the codes alone. A scorer would also make a query, which TurboQuant makes from the
    /// original vector of the point it's made for.
    Quantized(&'a QuantizedVectorStorage),
    /// A raw scorer, as `FilteredScorer::new_internal` makes one without quantized vectors, for
    /// some point: `score_internal(a, b)` scores the stored vectors of `a` and `b`, whatever point
    /// the scorer was made for. Making one copies that point's vector into a query, so callers
    /// scoring many pairs make one per batch of points. Raw scorers aren't `Send`, so threads
    /// can't share them through a pool.
    Original(Box<dyn RawScorer + 'a>),
}

impl<'a> SourceVectors<'a> {
    /// A [`PairScorer`] over these vectors. Over the originals, made for `point`, whose vector it
    /// reads; over quantized vectors, it reads nothing. Fails if a raw scorer over the originals
    /// can't be made.
    pub fn pair_scorer(self, point: PointOffsetType) -> OperationResult<PairScorer<'a>> {
        Ok(match self {
            Self::Quantized(quantized) => PairScorer::Quantized(quantized.get_storage()),
            Self::Original(storage) => {
                let query = storage.get_vector::<Random>(point);
                let query = QueryVector::from(query.as_vec_ref());
                PairScorer::Original(storage.build_raw_scorer(query)?)
            }
        })
    }
}

impl PairScorer<'_> {
    pub fn score(&self, a: PointOffsetType, b: PointOffsetType) -> ScoreType {
        match self {
            Self::Quantized(storage) => storage.score_internal(a, b),
            Self::Original(scorer) => scorer.score_internal(a, b),
        }
    }
}
