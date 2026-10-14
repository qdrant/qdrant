/// How the inner product of two decoded vectors (see [`KernelElem::dot`]) and one value per
/// vector combine into the score of the storage's `score_internal`, in its order of operations.
///
/// [`KernelElem::dot`]: crate::KernelElem::dot
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Combine {
    /// `dot`
    Plain,
    /// `2·dot − aux_a − aux_b`, with the squared norm as `aux`.
    Euclid,
    /// `dot · aux_a · aux_b`, with the inverse norm as `aux`.
    Cos,
    /// `mult · dot + (aux_a − shift) + aux_b`, with the per-vector offset as `aux`.
    Affine { mult: f32, shift: f32 },
    /// Binary quantization, with `dot` the number of differing bits: `(dim − dot) − dot`, or
    /// its negation.
    Hamming { dim: f32, negate: bool },
}

impl Combine {
    /// Score of the pair `(a, b)`. Not symmetric in `aux` for every variant, so a pushed pair
    /// is combined once per direction.
    #[inline]
    pub fn score(self, dot: f32, aux_a: f32, aux_b: f32) -> f32 {
        match self {
            Combine::Plain => dot,
            Combine::Euclid => 2.0 * dot - aux_a - aux_b,
            Combine::Cos => dot * aux_a * aux_b,
            Combine::Affine { mult, shift } => mult * dot + (aux_a - shift) + aux_b,
            Combine::Hamming { dim, negate } => {
                let equal = dim - dot;
                if negate { dot - equal } else { equal - dot }
            }
        }
    }
}
