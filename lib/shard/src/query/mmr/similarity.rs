#[cfg(debug_assertions)]
use common::generic_consts::Random;
use common::types::{PointOffsetType, ScoreType};
use segment::common::operation_error::OperationResult;
use segment::data_types::vectors::{QueryVector, VectorInternal};
#[cfg(debug_assertions)]
use segment::vector_storage::VectorStorageRead;
use segment::vector_storage::{RawScorer, VectorStorageEnum, new_raw_scorer};

/// Scores MMR candidates against a selected candidate.
///
/// The similarity between candidates `i` and `j` is the score of `j`'s stored
/// vector for a query of `i`'s vector.
pub enum SimilarityToSelected<'storage> {
    /// Dense and sparse metrics are symmetric, so the selected candidate is the
    /// query and the others are scored against it in one batch. For cosine this
    /// relies on the vectors being normalized, as they are stored, so the query
    /// preprocessing leaves them unchanged.
    Symmetric {
        storage: &'storage VectorStorageEnum,
        vectors: Vec<VectorInternal>,
    },
    /// Multi-vector MaxSim is not symmetric: each candidate is its own query,
    /// so candidates are scored against the selected one one at a time.
    PerCandidate {
        /// Scorers for each vector, parallel to the vectors in the storage.
        scorers: Vec<Box<dyn RawScorer + 'storage>>,
    },
}

impl<'storage> SimilarityToSelected<'storage> {
    /// `vectors` are the vectors in `storage`, in the same order.
    pub fn new(
        vectors: Vec<VectorInternal>,
        storage: &'storage VectorStorageEnum,
    ) -> OperationResult<Self> {
        #[cfg(debug_assertions)]
        {
            for (i, vector) in vectors.iter().enumerate() {
                let stored_vector = storage.get_vector::<Random>(i as u32);
                assert_eq!(stored_vector.to_owned(), *vector);
            }
        }

        match vectors.first() {
            Some(VectorInternal::MultiDense(_)) => {
                let scorers = vectors
                    .into_iter()
                    .map(|vector| new_raw_scorer(QueryVector::Nearest(vector), storage))
                    .collect::<OperationResult<Vec<_>>>()?;
                Ok(Self::PerCandidate { scorers })
            }
            Some(VectorInternal::Dense(_) | VectorInternal::Sparse(_)) | None => {
                Ok(Self::Symmetric { storage, vectors })
            }
        }
    }

    /// Set `similarities[i]` to the similarity between `candidates[i]` and
    /// `selected`.
    pub fn score(
        &self,
        selected: PointOffsetType,
        candidates: &[PointOffsetType],
        similarities: &mut [ScoreType],
    ) -> OperationResult<()> {
        match self {
            Self::Symmetric { storage, vectors } => {
                let query = QueryVector::Nearest(vectors[selected as usize].clone());
                new_raw_scorer(query, storage)?.score_points(candidates, similarities)
            }
            Self::PerCandidate { scorers } => {
                for (similarity, &candidate) in similarities.iter_mut().zip(candidates) {
                    *similarity = scorers[candidate as usize].score_point(selected)?;
                }
                Ok(())
            }
        }
    }
}
