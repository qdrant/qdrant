//! Dot products of one query with a batch of stored vectors, blocked over the batch.
//!
//! The query is cut into blocks of [`BLOCK`] floats. A block is loaded into registers once
//! and multiplied with the same block of every vector of a pass before the next block, so
//! the query loads are shared by the whole pass. While a vector's block is scored, the cache
//! lines of its next block are prefetched, so they are on their way during the visits to the
//! other vectors of the pass.
//!
//! Every kernel keeps the partial sums of the one-vector kernel that
//! `DotProductMetric::similarity` uses on the same CPU, and adds the same products to them in
//! the same order. So the scores are bit for bit the same: a point scores the same alone, in a
//! batch, in a full scan or on another replica of the same CPU kind.

#[cfg(target_arch = "x86_64")]
mod avx;
#[cfg(target_arch = "x86_64")]
mod avx512;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
mod neon;
#[cfg(test)]
mod tests;

use common::types::ScoreType;

use super::metric::default_similarity_batch;
use super::simple::DotProductMetric;
use crate::data_types::vectors::VectorElementType;

/// Floats of the query held in registers at a time: 256 bytes, 4 cache lines.
const BLOCK: usize = 64;

/// Most vectors in one pass over the query blocks. More vectors share each query load, but
/// each one is another memory stream in flight. 16 is about the number of new neighbors of
/// one HNSW expansion, so a typical HNSW batch is one pass; larger batches are split into
/// passes of equal size.
const MAX_PASS: usize = 16;

/// A kernel that scores one pass.
///
/// # Safety
/// The CPU must support the kernel's instruction set. `query` has at least [`BLOCK`] floats,
/// every vector as many as `query`, `scores` one slot per vector, and there are at most
/// [`MAX_PASS`] vectors.
type PassKernel = unsafe fn(&[VectorElementType], &[&[VectorElementType]], &mut [ScoreType]);

/// `DotProductMetric::similarity` of `query` with each of `vectors`, written to `scores`.
pub(crate) fn dot_similarity_batch(
    query: &[VectorElementType],
    vectors: &[impl AsRef<[VectorElementType]>],
    scores: &mut [ScoreType],
) {
    debug_assert_eq!(vectors.len(), scores.len());

    // A single vector shares nothing, and a query shorter than a block has no block
    if vectors.len() > 1
        && query.len() >= BLOCK
        && let Some(kernel) = pass_kernel()
    {
        score_in_passes(kernel, query, vectors, scores);
        return;
    }

    default_similarity_batch::<_, DotProductMetric>(query, vectors, scores);
}

/// The blocked kernel for this CPU's widest supported instruction set, if any. It reproduces
/// the kernel of `DotProductMetric::similarity`, so it needs the same CPU features.
fn pass_kernel() -> Option<PassKernel> {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx") && is_x86_feature_detected!("fma") {
            if is_x86_feature_detected!("avx512f") {
                return Some(avx512::dot_similarity_blocked_avx512);
            }
            return Some(avx::dot_similarity_blocked_avx);
        }
    }

    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            return Some(neon::dot_similarity_blocked_neon);
        }
    }

    None
}

/// Scores `vectors` with `kernel` in passes of equal size, at most [`MAX_PASS`] each.
///
/// `kernel` must be supported by this CPU and `query` have at least [`BLOCK`] floats.
fn score_in_passes(
    kernel: PassKernel,
    query: &[VectorElementType],
    vectors: &[impl AsRef<[VectorElementType]>],
    scores: &mut [ScoreType],
) {
    debug_assert!(query.len() >= BLOCK);
    let passes = vectors.len().div_ceil(MAX_PASS);
    let pass_size = vectors.len().div_ceil(passes);

    let mut pass: [&[VectorElementType]; MAX_PASS] = [&[]; MAX_PASS];
    for (vectors, scores) in vectors.chunks(pass_size).zip(scores.chunks_mut(pass_size)) {
        let pass = &mut pass[..vectors.len()];
        for (slot, vector) in pass.iter_mut().zip(vectors) {
            *slot = vector.as_ref();
            // The kernels read `query.len()` floats of every vector
            assert_eq!(slot.len(), query.len());
        }
        // SAFETY: the caller picked a kernel this CPU supports and a query of at least one
        // block; the pass has at most `MAX_PASS` vectors of the query's length and one score
        // slot each.
        unsafe { kernel(query, pass, scores) };
    }
}

/// Prefetches the cache lines that start inside the block at `block`: 4 lines of 64 bytes.
/// A line that starts before `block` belongs to the block before.
#[cfg(any(
    target_arch = "x86_64",
    all(target_arch = "aarch64", target_feature = "neon")
))]
#[inline(always)]
fn prefetch_block(block: *const VectorElementType) {
    /// Cache line size assumed, in bytes.
    const CACHE_LINE: usize = 64;

    let block = block.cast::<u8>();
    let first = block.wrapping_add(block.addr().wrapping_neg() % CACHE_LINE);
    for line in 0..BLOCK * size_of::<VectorElementType>() / CACHE_LINE {
        common::prefetch::prefetch_read(first.wrapping_add(line * CACHE_LINE));
    }
}
