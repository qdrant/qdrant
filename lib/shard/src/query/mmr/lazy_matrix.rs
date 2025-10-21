use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::{PointOffsetType, ScoreType};
use segment::common::operation_error::OperationResult;
use segment::data_types::vectors::{QueryVector, VectorInternal};
#[cfg(debug_assertions)]
use segment::vector_storage::{Random, VectorStorage};
use segment::vector_storage::{RawScorer, VectorStorageEnum, new_raw_scorer};

/// Compute the similarity matrix lazily.
///
/// Uses a symmetric matrix as a cache layer to compute similarities only once,
/// but only those which are requested.
pub struct LazyMatrix<'storage> {
    /// Vec of scorers for each vector. position is parallel to the vectors in the referenced storage.
    scorers: Vec<Box<dyn RawScorer + 'storage>>,

    // perf: can this Option<ScoreType> be smaller?
    //       this allocates m*m values. For 1000x1000 it takes 8MB.
    /// similarity matrix with possibly unallocated values.
    matrix: Vec<Vec<Option<ScoreType>>>,
}

impl<'storage> LazyMatrix<'storage> {
    /// Create a new lazy matrix from a list of vectors and a storage.
    ///
    /// Returns an error if there are less than two vectors.
    pub fn new(
        vectors: Vec<VectorInternal>,
        storage: &'storage VectorStorageEnum,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> OperationResult<Self> {
        #[cfg(debug_assertions)]
        {
            for (i, vector) in vectors.iter().enumerate() {
                let stored_vector = storage.get_vector::<Random>(i as u32);
                assert_eq!(stored_vector.to_owned(), *vector);
            }
        }

        let matrix = vec![vec![None; vectors.len()]; vectors.len()];

        // Prepare all scorers
        let scorers = vectors
            .into_iter()
            .map(|vector| {
                let query = QueryVector::Nearest(vector);
                new_raw_scorer(query, storage, hw_measurement_acc.get_counter_cell())
            })
            .collect::<OperationResult<Vec<_>>>()?;

        Ok(Self { scorers, matrix })
    }

    pub fn get_similarity(&mut self, i: usize, j: usize) -> ScoreType {
        if let Some(similarity) = self.matrix[i][j] {
            return similarity;
        }
        let similarity = self.compute_similarity(i, j);
        self.matrix[i][j] = Some(similarity);
        similarity
    }

    fn compute_similarity(&self, i: usize, j: usize) -> ScoreType {
        self.scorers[i].score_point(j as PointOffsetType)
    }
}
