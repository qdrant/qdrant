/// The last step of a storage's `score_internal`: the score of a pair from the inner product of
/// their decoded vectors (see [`KernelElem::dot`]) and the value each was decoded with.
///
/// The kernels compute inner products only and know no storage's formula. A storage implements
/// this by calling what its `score_internal` calls after its own inner product, so both score
/// alike by construction.
///
/// Not symmetric in `aux` for every storage, so a pushed pair is combined once per direction.
///
/// [`KernelElem::dot`]: crate::KernelElem::dot
pub trait Combine: Copy + Send + Sync {
    fn score(self, dot: f32, aux_a: f32, aux_b: f32) -> f32;
}
