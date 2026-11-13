use pairwise::{Aux, Combine as _};
use quantization::encoded_vectors_u8::ScalarCombine;

use crate::vector_storage::matrix_scorer::DenseCombine;

/// How the inner product of two stored records and their values become a score: the combine
/// value the CPU kernels score these records with, as it is. Code that scores elsewhere reads
/// its formula from the variant, and its constants from the value's getters.
#[derive(Clone, Copy, Debug)]
pub enum StoredCombine {
    /// Original vectors, see [`DenseCombine`].
    Dense(DenseCombine),
    /// Scalar quantization, see [`ScalarCombine`] and its getters.
    Scalar(ScalarCombine),
}

impl StoredCombine {
    /// The score of a pair, through the wrapped value's own score.
    pub fn score(self, dot: f32, aux_a: &Aux, aux_b: &Aux) -> f32 {
        match self {
            StoredCombine::Dense(combine) => combine.score(dot, aux_a, aux_b),
            StoredCombine::Scalar(combine) => combine.score(dot, aux_a, aux_b),
        }
    }
}
