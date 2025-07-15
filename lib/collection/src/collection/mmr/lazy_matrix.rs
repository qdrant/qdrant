use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::{PointOffsetType, ScoreType};
use segment::data_types::vectors::{QueryVector, VectorInternal};
use segment::vector_storage::{RawScorer, VectorStorageEnum, new_raw_scorer};

use crate::common::symmetric_matrix::SymmetricMatrix;
use crate::operations::types::{CollectionError, CollectionResult};

/// Compute the similarity matrix lazily.
///
/// Uses a symmetric matrix as a cache layer to compute similarities only once,
/// but only those which are requested.
pub struct LazyMatrix<'storage> {
    scorers: Vec<Box<dyn RawScorer + 'storage>>,
    // perf: can this Option<ScoreType> be smaller? SymmetricMatrix allocates nearly m*m/2 values
    matrix: SymmetricMatrix<Option<ScoreType>>,
}

impl<'storage> LazyMatrix<'storage> {
    /// Create a new lazy matrix from a list of vectors and a storage.
    ///
    /// Returns None if there are less than two vectors.
    pub fn new(
        vectors: Vec<VectorInternal>,
        storage: &'storage VectorStorageEnum,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Self> {
        let matrix = SymmetricMatrix::new(vectors.len(), None).ok_or_else(|| {
            CollectionError::service_error(
                "There are less than 2 points for building a similarity matrix",
            )
        })?;

        // Prepare all scorers
        let scorers = vectors
            .into_iter()
            .map(|vector| {
                let query = QueryVector::Nearest(vector);
                Ok(new_raw_scorer(
                    query,
                    storage,
                    hw_measurement_acc.get_counter_cell(),
                )?)
            })
            .collect::<CollectionResult<Vec<_>>>()?;
        Ok(Self { scorers, matrix })
    }

    pub fn get_similarity(&mut self, i: usize, j: usize) -> ScoreType {
        if let Some(similarity) = self.matrix.get(i, j) {
            return *similarity;
        }
        let similarity = self.compute_similarity(i, j);
        self.matrix.set(i, j, Some(similarity));
        similarity
    }

    fn compute_similarity(&self, i: usize, j: usize) -> ScoreType {
        self.scorers[i].score_point(j as PointOffsetType)
    }
}
