//! NEON kernels for [`QuerySimd`] on aarch64.
//!
//! The codebook is stored signed (`Encoding::codebook` as `i8`), so
//! `vmull_s8` and `SDOT` operate on true `i8 × i8` products — no bias
//! correction — and the query halves use the full i8 range (see the module
//! docs of [`super`]).

use core::arch::aarch64::*;

use super::{Code, PLANE_BLOCK, QueryPlanes, QuerySimd, encoding};

/// Packed data bytes per NEON block: one 128-bit register of codes.
const BLOCK_128: usize = 16;
const _: () = assert!(PLANE_BLOCK.is_multiple_of(BLOCK_128));

/// The codebook as a `TBL` table for the width packing `PLANES` codes per
/// byte.
const fn codebook<const PLANES: usize>() -> [Code; 16] {
    encoding(PLANES).codebook
}

/// Mask of one code in the low bits of a byte.
const fn code_mask<const PLANES: usize>() -> u8 {
    ((1u16 << (8 / PLANES)) - 1) as u8
}

/// One [`BLOCK_128`]-byte block of every query plane.  Loaded once per
/// block and shared by all vectors scored against it.
#[derive(Clone, Copy)]
struct QueryBlock128<const PLANES: usize> {
    low: [int8x16_t; PLANES],
    high: [int8x16_t; PLANES],
}

impl<const PLANES: usize> QueryBlock128<PLANES> {
    /// # Safety
    /// `offset + BLOCK_128` must not exceed the plane length.
    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn load(planes: &QueryPlanes<PLANES>, offset: usize) -> Self {
        let mut block = Self {
            low: [vdupq_n_s8(0); PLANES],
            high: [vdupq_n_s8(0); PLANES],
        };
        for (k, low) in block.low.iter_mut().enumerate() {
            *low = unsafe { load_plane_128(&planes.low[k], offset) };
        }
        for (k, high) in block.high.iter_mut().enumerate() {
            *high = unsafe { load_plane_128(&planes.high[k], offset) };
        }
        block
    }
}

/// # Safety
/// `offset + BLOCK_128 <= plane.len()`.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn load_plane_128(plane: &[i8], offset: usize) -> int8x16_t {
    debug_assert!(offset + BLOCK_128 <= plane.len());
    unsafe { vld1q_s8(plane.as_ptr().add(offset)) }
}

/// The next plane's codes moved into the low bits of every byte.  The shift
/// count is an immediate, hence the match over the widths.
#[inline]
#[target_feature(enable = "neon")]
fn next_plane_128<const PLANES: usize>(codes: uint8x16_t) -> uint8x16_t {
    match PLANES {
        2 => vshrq_n_u8(codes, 4),
        4 => vshrq_n_u8(codes, 2),
        _ => vshrq_n_u8(codes, 1),
    }
}

/// Codebook values addressed by the low code of every byte of `codes`.
#[inline]
#[target_feature(enable = "neon")]
fn lookup_codes<const PLANES: usize>(codes: uint8x16_t) -> int8x16_t {
    let table = const { codebook::<PLANES>() };
    let codebook = unsafe { vld1q_s8(table.as_ptr()) };
    vqtbl1q_s8(
        codebook,
        vandq_u8(codes, vdupq_n_u8(const { code_mask::<PLANES>() })),
    )
}

/// `acc[lane] += Σ₄ a · b` over each lane's four i8 pairs (`SDOT`).  Inline
/// asm because `vdotq_s32` is still unstable (rust-lang/rust#117224).
///
/// # Safety
/// CPU must support `dotprod`.
#[inline]
#[target_feature(enable = "neon,dotprod")]
unsafe fn sdot(mut acc: int32x4_t, a: int8x16_t, b: int8x16_t) -> int32x4_t {
    unsafe {
        core::arch::asm!(
            "sdot {acc:v}.4s, {a:v}.16b, {b:v}.16b",
            acc = inout(vreg) acc,
            a = in(vreg) a,
            b = in(vreg) b,
            options(pure, nomem, nostack, preserves_flags),
        );
    }
    acc
}

/// i32 accumulators of one vector: two independent low/high chains, plane
/// `k` feeding pair `k & 1`, which keeps the multiply-accumulate latency
/// off the critical path at every width.
///
/// i32 lane bound: each block adds at most `4 · 127 · 128 = 65 024` per
/// plane to a lane, so overflow needs ~33 K blocks ≈ 500 K packed bytes.
#[derive(Clone, Copy)]
struct Acc128 {
    low: [int32x4_t; 2],
    high: [int32x4_t; 2],
}

impl Acc128 {
    #[inline]
    #[target_feature(enable = "neon")]
    fn zero() -> Self {
        let zero = vdupq_n_s32(0);
        Self {
            low: [zero; 2],
            high: [zero; 2],
        }
    }

    /// Fold one block of packed `codes` (16 bytes) into the accumulators
    /// with `SDOT`: plane by plane, the codes are shifted down, masked to
    /// one code per byte and looked up in the codebook.
    ///
    /// # Safety
    /// CPU must support `dotprod`.
    #[inline]
    #[target_feature(enable = "neon,dotprod")]
    unsafe fn accumulate_sdot<const PLANES: usize>(
        &mut self,
        codes: uint8x16_t,
        query: QueryBlock128<PLANES>,
    ) {
        let mut shifted = codes;
        for k in 0..PLANES {
            let values = lookup_codes::<PLANES>(shifted);
            self.low[k & 1] = unsafe { sdot(self.low[k & 1], values, query.low[k]) };
            self.high[k & 1] = unsafe { sdot(self.high[k & 1], values, query.high[k]) };
            shifted = next_plane_128::<PLANES>(shifted);
        }
    }

    /// Per-lane `(low, high)` query-half totals.
    #[inline]
    #[target_feature(enable = "neon")]
    fn fold(self) -> (int32x4_t, int32x4_t) {
        (
            vaddq_s32(self.low[0], self.low[1]),
            vaddq_s32(self.high[0], self.high[1]),
        )
    }
}

impl<const PLANES: usize> QuerySimd<PLANES> {
    /// ARMv8.2-A Dot Product variant over the query planes: one 128-bit
    /// register of packed codes per block, one `TBL` codebook lookup per
    /// plane and `SDOT` into independent accumulators (see [`Acc128`]).
    /// The last partial block runs on a zero-padded copy of the remaining
    /// bytes.
    ///
    /// # Safety
    /// CPU must support `neon` and `dotprod`.
    #[target_feature(enable = "neon,dotprod")]
    pub unsafe fn dotprod_raw_neon_sdot(&self, vector: &[u8]) -> i64 {
        assert_eq!(
            vector.len(),
            self.vector_bytes,
            "QuerySimd<{PLANES}>::dotprod_raw_neon_sdot: vector length mismatch ({} vs expected {})",
            vector.len(),
            self.vector_bytes,
        );

        unsafe { Self::reduce_neon(self.accumulate_neon_sdot(vector.as_ptr())) }
    }

    /// Block loop of the SDOT kernels over the vector at `data`.
    ///
    /// # Safety
    /// CPU must support `dotprod`; `data` must be readable for
    /// `self.vector_bytes` bytes.
    #[inline]
    #[target_feature(enable = "neon,dotprod")]
    unsafe fn accumulate_neon_sdot(&self, data: *const u8) -> Acc128 {
        unsafe {
            let mut acc = Acc128::zero();

            let full_blocks = self.vector_bytes / BLOCK_128;
            for block in 0..full_blocks {
                let offset = block * BLOCK_128;
                let query = QueryBlock128::load(&self.planes, offset);
                acc.accumulate_sdot(vld1q_u8(data.add(offset)), query);
            }

            let tail = self.vector_bytes % BLOCK_128;
            if tail > 0 {
                let offset = full_blocks * BLOCK_128;
                let query = QueryBlock128::load(&self.planes, offset);
                let mut block = [0u8; BLOCK_128];
                std::ptr::copy_nonoverlapping(data.add(offset), block.as_mut_ptr(), tail);
                acc.accumulate_sdot(vld1q_u8(block.as_ptr()), query);
            }

            acc
        }
    }

    /// Raw dot product from one vector's accumulators.  Horizontal adds are
    /// single instructions here, so nothing is gained by fusing them.
    #[inline]
    #[target_feature(enable = "neon")]
    fn reduce_neon(acc: Acc128) -> i64 {
        let (low, high) = acc.fold();
        i64::from(vaddvq_s32(low)) + Self::ENCODING.query_high_coef * i64::from(vaddvq_s32(high))
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::QuerySimd;
    use super::super::shared::{parity_dims, random_inputs};

    /// The kernel must reproduce the scalar reference bit-exactly at every
    /// parity dim of every width.
    fn neon_sdot_matches_scalar<const PLANES: usize>() {
        let mut rng = StdRng::seed_from_u64(7);
        for dim in parity_dims::<PLANES>() {
            let (query, vector) = random_inputs::<PLANES>(&mut rng, dim);
            let scalar = query.dotprod_raw(&vector);
            let sdot = unsafe { query.dotprod_raw_neon_sdot(&vector) };
            assert_eq!(
                scalar, sdot,
                "PLANES={PLANES} dim={dim}: scalar {scalar} != sdot {sdot}"
            );
        }
    }

    #[test]
    fn test_neon_sdot_matches_scalar() {
        if !std::arch::is_aarch64_feature_detected!("dotprod") {
            return;
        }
        neon_sdot_matches_scalar::<2>();
        neon_sdot_matches_scalar::<4>();
        neon_sdot_matches_scalar::<8>();
    }

    /// Saturation safety at an extreme dim (64K) under the worst-case load:
    /// the query maxed out and every code at the max-magnitude codebook
    /// slot (all-ones bytes at every width).  The scalar reference is i64
    /// throughout; a SIMD mismatch proves some intermediate saturated or
    /// overflowed.
    fn saturation_safety_64k<const PLANES: usize>() {
        let dim = 65_536;
        let query = QuerySimd::<PLANES>::new(&vec![1.0_f32; dim]);
        let vector = vec![0xFF_u8; dim / PLANES];
        let scalar = query.dotprod_raw(&vector);
        let sdot = unsafe { query.dotprod_raw_neon_sdot(&vector) };
        assert_eq!(scalar, sdot, "PLANES={PLANES}: sdot disagrees");
    }

    #[test]
    fn test_saturation_safety_64k() {
        if !std::arch::is_aarch64_feature_detected!("dotprod") {
            return;
        }
        saturation_safety_64k::<2>();
        saturation_safety_64k::<4>();
        saturation_safety_64k::<8>();
    }
}
