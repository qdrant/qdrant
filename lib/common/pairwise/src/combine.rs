/// Values of one vector that its scores need besides the inner product, as many as the storage
/// needs: an offset, a norm, or TurboQuant's scaling factor, squared length, and correction.
/// Unused values are 0.
pub type Aux = [f32; 3];

/// The last step of a storage's `score_internal`: the score of a pair from the inner product of
/// their decoded vectors (see [`KernelElem::dot`]) and the values each was decoded with.
///
/// The kernels compute inner products only and know no storage's formula. A storage implements
/// this by calling what its `score_internal` calls after its own inner product, so both score
/// alike by construction.
///
/// Not symmetric in `aux` for every storage, so a pushed pair is combined once per direction.
/// Kernels call [`Self::score`] for every pair: implementations should be `#[inline(always)]`,
/// so a loop over a row folds to one formula (with plain `#[inline]`, TurboQuant's became a
/// call per pair). A storage that picks between formulas at runtime implements
/// [`CombineDispatch`] instead, with one `Combine` per formula.
///
/// [`KernelElem::dot`]: crate::KernelElem::dot
pub trait Combine: Copy + Send + Sync {
    fn score(self, dot: f32, aux_a: &Aux, aux_b: &Aux) -> f32;
}

/// What operands give the kernels to combine with: one [`Combine`], or a choice between several
/// made at runtime, such as one per distance or bit width. The kernels dispatch once per row of
/// scores, so their loop over the row compiles once per formula instead of branching per pair,
/// which cost up to 10% of a 1-bit TurboQuant leaf and 4% of an f32 one.
///
/// Implementations should be `#[inline(always)]` too, so the loops compile into the kernels'
/// `#[target_feature]` functions.
pub trait CombineDispatch: Copy + Send + Sync {
    /// Calls `f` with the formula to score with.
    fn dispatch<F: CombineFn>(self, f: F) -> F::Output;
}

/// A single formula is its own choice.
impl<C: Combine> CombineDispatch for C {
    #[inline(always)]
    fn dispatch<F: CombineFn>(self, f: F) -> F::Output {
        f.call(self)
    }
}

/// A function of whichever [`Combine`] a [`CombineDispatch`] chooses.
pub trait CombineFn {
    type Output;

    fn call<C: Combine>(self, combine: C) -> Self::Output;
}

/// The score of one pair, for code that combines a pair at a time.
pub(crate) struct ScorePair<'a> {
    pub dot: f32,
    pub aux_a: &'a Aux,
    pub aux_b: &'a Aux,
}

impl CombineFn for ScorePair<'_> {
    type Output = f32;

    #[inline(always)]
    fn call<C: Combine>(self, combine: C) -> f32 {
        combine.score(self.dot, self.aux_a, self.aux_b)
    }
}
