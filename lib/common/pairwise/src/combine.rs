/// Values of one vector for [`Combine`]. Every variant but [`Combine::Turbo`] uses only the
/// first.
pub type Aux = [f32; 3];

/// How the inner product of two decoded vectors (see [`KernelElem::dot`]) and the values of
/// each vector combine into the score of the storage's `score_internal`, in its order of
/// operations.
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
    /// TurboQuant, as `TurboQuantizer::score_symmetric`, with `[sf, l2², xm]` as `aux`: the
    /// inner product becomes `raw` as `dot` says, then the score is `raw · sf_a · sf_b`, or
    /// with `l2`, `l2²_a + l2²_b − 2·sf_a·sf_b·raw`. Negated if `invert`.
    Turbo {
        dot: TurboDot,
        l2: bool,
        invert: bool,
    },
}

/// How [`Combine::Turbo`] turns the inner product into the centroid dot product `raw`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TurboDot {
    /// 1 bit, with `dot` the number of differing bits: `centroid_sq · (bits − 2·dot)`.
    Signs { bits: f32, centroid_sq: f32 },
    /// `dot / div`: the codebook scale squared for codebooks, 1 for the integer grids.
    Scaled { div: f32 },
    /// TQ+, with weighted codes: `dot / div + xm_a + xm_b − mm`.
    Corrected { div: f32, mm: f32 },
}

impl Combine {
    /// Score of the pair `(a, b)`. Not symmetric in `aux` for every variant, so a pushed pair
    /// is combined once per direction.
    ///
    /// Always inlined, so each loop over a row folds it to one variant's formula; with plain
    /// `#[inline]`, the TurboQuant variants made it a call per pair.
    #[inline(always)]
    pub fn score(self, dot: f32, aux_a: &Aux, aux_b: &Aux) -> f32 {
        match self {
            Combine::Plain => dot,
            Combine::Euclid => 2.0 * dot - aux_a[0] - aux_b[0],
            Combine::Cos => dot * aux_a[0] * aux_b[0],
            Combine::Affine { mult, shift } => mult * dot + (aux_a[0] - shift) + aux_b[0],
            Combine::Hamming { dim, negate } => {
                let equal = dim - dot;
                if negate { dot - equal } else { equal - dot }
            }
            Combine::Turbo { dot: d, l2, invert } => {
                let raw = match d {
                    TurboDot::Signs { bits, centroid_sq } => centroid_sq * (bits - 2.0 * dot),
                    TurboDot::Scaled { div } => dot / div,
                    TurboDot::Corrected { div, mm } => dot / div + aux_a[2] + aux_b[2] - mm,
                };
                let score = if l2 {
                    aux_a[1] + aux_b[1] - 2.0 * aux_a[0] * aux_b[0] * raw
                } else {
                    raw * aux_a[0] * aux_b[0]
                };
                if invert { -score } else { score }
            }
        }
    }
}
