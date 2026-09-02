//! NEON kernels for [`QuerySimd`] on aarch64.
//!
//! The codebook is stored signed (`Encoding::codebook` as `i8`), so
//! `vmull_s8` and `SDOT` operate on true `i8 × i8` products — no bias
//! correction — and the query bytes use the full i8 range (see the module
//! docs of [`super`]).

use core::arch::aarch64::*;

use super::{Code, PLANE_BLOCK, QueryPlanes, QuerySimd, encoding, tail_block};

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
struct QueryBlock128<const PLANES: usize, const QUERY_BYTES: usize> {
    bytes: [[int8x16_t; PLANES]; QUERY_BYTES],
}

impl<const PLANES: usize, const QUERY_BYTES: usize> QueryBlock128<PLANES, QUERY_BYTES> {
    /// # Safety
    /// `offset + BLOCK_128` must not exceed the plane length.
    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn load(planes: &QueryPlanes<PLANES, QUERY_BYTES>, offset: usize) -> Self {
        let mut block = Self {
            bytes: [[vdupq_n_s8(0); PLANES]; QUERY_BYTES],
        };
        for (b, byte_planes) in block.bytes.iter_mut().enumerate() {
            for (k, plane) in byte_planes.iter_mut().enumerate() {
                *plane = unsafe { load_plane_128(planes.plane(b, k), offset) };
            }
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

/// `acc[lane] += Σ₄ a · b` without `dotprod`: widening i8 multiplies
/// (`vmull_s8`, exact in i16) pairwise-added into the i32 lanes
/// (`vpadalq_s16`).  Same per-lane bound as [`sdot`].
#[inline]
#[target_feature(enable = "neon")]
fn mul_add(acc: int32x4_t, a: int8x16_t, b: int8x16_t) -> int32x4_t {
    let acc = vpadalq_s16(acc, vmull_s8(vget_low_s8(a), vget_low_s8(b)));
    vpadalq_s16(acc, vmull_high_s8(a, b))
}

/// i32 accumulators of one vector: per query byte two independent chains,
/// plane `k` feeding chain `k & 1`, which keeps the multiply-accumulate
/// latency off the critical path at every width.
///
/// i32 lane bound: each block adds at most `4 · 127 · 128 = 65 024` per
/// plane to a lane, so overflow needs ~33 K blocks ≈ 500 K packed bytes.
#[derive(Clone, Copy)]
struct Acc128<const QUERY_BYTES: usize> {
    bytes: [[int32x4_t; 2]; QUERY_BYTES],
}

impl<const QUERY_BYTES: usize> Acc128<QUERY_BYTES> {
    #[inline]
    #[target_feature(enable = "neon")]
    fn zero() -> Self {
        Self {
            bytes: [[vdupq_n_s32(0); 2]; QUERY_BYTES],
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
        query: QueryBlock128<PLANES, QUERY_BYTES>,
    ) {
        let mut shifted = codes;
        for k in 0..PLANES {
            let values = lookup_codes::<PLANES>(shifted);
            for (acc, planes) in self.bytes.iter_mut().zip(&query.bytes) {
                acc[k & 1] = unsafe { sdot(acc[k & 1], values, planes[k]) };
            }
            shifted = next_plane_128::<PLANES>(shifted);
        }
    }

    /// [`Self::accumulate_sdot`] for CPUs without `dotprod`.
    #[inline]
    #[target_feature(enable = "neon")]
    fn accumulate_mull<const PLANES: usize>(
        &mut self,
        codes: uint8x16_t,
        query: QueryBlock128<PLANES, QUERY_BYTES>,
    ) {
        let mut shifted = codes;
        for k in 0..PLANES {
            let values = lookup_codes::<PLANES>(shifted);
            for (acc, planes) in self.bytes.iter_mut().zip(&query.bytes) {
                acc[k & 1] = mul_add(acc[k & 1], values, planes[k]);
            }
            shifted = next_plane_128::<PLANES>(shifted);
        }
    }

    /// Per-lane totals of every query byte.
    #[inline]
    #[target_feature(enable = "neon")]
    fn fold(self) -> [int32x4_t; QUERY_BYTES] {
        self.bytes.map(|[a, b]| vaddq_s32(a, b))
    }
}

impl<const PLANES: usize, const QUERY_BYTES: usize> QuerySimd<PLANES, QUERY_BYTES> {
    /// ARM NEON over the query planes for CPUs without `dotprod`: the
    /// [`Self::dotprod_raw_neon_sdot`] block loop with `vmull_s8 →
    /// vpadalq_s16` in place of `SDOT`.
    ///
    /// # Safety
    /// CPU must support the `neon` feature (always true on aarch64).
    #[target_feature(enable = "neon")]
    pub unsafe fn dotprod_raw_neon(&self, vector: &[u8]) -> i64 {
        assert_eq!(
            vector.len(),
            self.vector_bytes,
            "QuerySimd<{PLANES}, {QUERY_BYTES}>::dotprod_raw_neon: vector length mismatch ({} \
             vs expected {})",
            vector.len(),
            self.vector_bytes,
        );

        unsafe {
            let [acc] = self.accumulate_neon::<1>(vector.as_ptr(), 0);
            Self::reduce_neon(acc)
        }
    }

    /// Batch counterpart of [`Self::dotprod_raw_neon`] with the float
    /// reconstruction applied — see `dotprod_batch` for the layout contract
    /// (asserted there).  Same grouping policy as
    /// [`Self::dotprod_batch_neon_sdot`].
    ///
    /// # Safety
    /// `data` must hold `out.len()` vectors at `stride`.
    #[target_feature(enable = "neon")]
    pub unsafe fn dotprod_batch_neon(&self, data: &[u8], stride: usize, out: &mut [f32]) {
        unsafe {
            let (groups, rest) = out.as_chunks_mut::<GROUP_128>();
            let interleave = self.vector_bytes <= INTERLEAVE_MAX_BYTES;
            let mut v = 0;
            for group in groups {
                let accs = if interleave {
                    self.accumulate_neon::<GROUP_128>(data.as_ptr().add(v * stride), stride)
                } else {
                    let mut accs = [Acc128::zero(); GROUP_128];
                    for (i, acc) in accs.iter_mut().enumerate() {
                        let [single] =
                            self.accumulate_neon::<1>(data.as_ptr().add((v + i) * stride), stride);
                        *acc = single;
                    }
                    accs
                };
                vst1q_f32(group.as_mut_ptr(), self.postprocess_x4_neon(accs));
                v += GROUP_128;
            }
            for out in rest {
                let [acc] = self.accumulate_neon::<1>(data.as_ptr().add(v * stride), stride);
                *out = self.postprocess(Self::reduce_neon(acc));
                v += 1;
            }
        }
    }

    /// [`Self::accumulate_neon_sdot`] for CPUs without `dotprod`.
    ///
    /// # Safety
    /// `data` must be readable for `(N - 1) * stride + self.vector_bytes`
    /// bytes.
    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn accumulate_neon<const N: usize>(
        &self,
        data: *const u8,
        stride: usize,
    ) -> [Acc128<QUERY_BYTES>; N] {
        unsafe {
            let mut accs = [Acc128::zero(); N];

            let full_blocks = self.vector_bytes / BLOCK_128;
            for block in 0..full_blocks {
                let offset = block * BLOCK_128;
                let query = QueryBlock128::load(&self.planes, offset);
                for (v, acc) in accs.iter_mut().enumerate() {
                    acc.accumulate_mull(vld1q_u8(data.add(v * stride + offset)), query);
                }
            }

            let tail = self.vector_bytes % BLOCK_128;
            if tail > 0 {
                let offset = full_blocks * BLOCK_128;
                let query = QueryBlock128::load(&self.planes, offset);
                for (v, acc) in accs.iter_mut().enumerate() {
                    let block = tail_block::<BLOCK_128>(data.add(v * stride + offset), tail);
                    acc.accumulate_mull(vld1q_u8(block.as_ptr()), query);
                }
            }

            accs
        }
    }

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
            "QuerySimd<{PLANES}, {QUERY_BYTES}>::dotprod_raw_neon_sdot: vector length mismatch \
             ({} vs expected {})",
            vector.len(),
            self.vector_bytes,
        );

        unsafe {
            let [acc] = self.accumulate_neon_sdot::<1>(vector.as_ptr(), 0);
            Self::reduce_neon(acc)
        }
    }

    /// Batch counterpart of [`Self::dotprod_raw_neon_sdot`] with the float
    /// reconstruction applied — see `dotprod_batch` for the layout contract
    /// (asserted there).
    ///
    /// Vectors up to [`INTERLEAVE_MAX_BYTES`] are scored in groups of
    /// [`GROUP_128`]: the group shares each query block load, and its
    /// independent accumulators keep the multi-cycle `SDOT` latency off the
    /// critical path.  Longer vectors walk the block loop one vector at a
    /// time so the hardware prefetcher sees a single sequential stream.
    ///
    /// Either way the reduction is shared across the group (see
    /// [`Self::postprocess_x4_neon`]): the per-vector chain of horizontal
    /// add, scalar combine, convert, multiply and store retires in order
    /// behind the next vector's block loop, so one four-wide pass per group
    /// replaces four serial ones.
    ///
    /// # Safety
    /// CPU must support `dotprod`; `data` must hold `out.len()` vectors at
    /// `stride`.
    #[target_feature(enable = "neon,dotprod")]
    pub unsafe fn dotprod_batch_neon_sdot(&self, data: &[u8], stride: usize, out: &mut [f32]) {
        unsafe {
            let (groups, rest) = out.as_chunks_mut::<GROUP_128>();
            let interleave = self.vector_bytes <= INTERLEAVE_MAX_BYTES;
            let mut v = 0;
            for group in groups {
                let accs = if interleave {
                    self.accumulate_neon_sdot::<GROUP_128>(data.as_ptr().add(v * stride), stride)
                } else {
                    let mut accs = [Acc128::zero(); GROUP_128];
                    for (i, acc) in accs.iter_mut().enumerate() {
                        let [single] = self
                            .accumulate_neon_sdot::<1>(data.as_ptr().add((v + i) * stride), stride);
                        *acc = single;
                    }
                    accs
                };
                vst1q_f32(group.as_mut_ptr(), self.postprocess_x4_neon(accs));
                v += GROUP_128;
            }
            for out in rest {
                let [acc] = self.accumulate_neon_sdot::<1>(data.as_ptr().add(v * stride), stride);
                *out = self.postprocess(Self::reduce_neon(acc));
                v += 1;
            }
        }
    }

    /// Block loop of the SDOT kernels over `N` vectors stored `stride` bytes
    /// apart starting at `data`.  Every query block is loaded once and folded
    /// into all `N` accumulator sets.
    ///
    /// # Safety
    /// CPU must support `dotprod`; `data` must be readable for
    /// `(N - 1) * stride + self.vector_bytes` bytes.
    #[inline]
    #[target_feature(enable = "neon,dotprod")]
    unsafe fn accumulate_neon_sdot<const N: usize>(
        &self,
        data: *const u8,
        stride: usize,
    ) -> [Acc128<QUERY_BYTES>; N] {
        unsafe {
            let mut accs = [Acc128::zero(); N];

            let full_blocks = self.vector_bytes / BLOCK_128;
            for block in 0..full_blocks {
                let offset = block * BLOCK_128;
                let query = QueryBlock128::load(&self.planes, offset);
                for (v, acc) in accs.iter_mut().enumerate() {
                    acc.accumulate_sdot(vld1q_u8(data.add(v * stride + offset)), query);
                }
            }

            let tail = self.vector_bytes % BLOCK_128;
            if tail > 0 {
                let offset = full_blocks * BLOCK_128;
                let query = QueryBlock128::load(&self.planes, offset);
                for (v, acc) in accs.iter_mut().enumerate() {
                    let block = tail_block::<BLOCK_128>(data.add(v * stride + offset), tail);
                    acc.accumulate_sdot(vld1q_u8(block.as_ptr()), query);
                }
            }

            accs
        }
    }

    /// Raw dot product from one vector's accumulators.  Horizontal adds are
    /// single instructions here, so nothing is gained by fusing them.
    #[inline]
    #[target_feature(enable = "neon")]
    fn reduce_neon(acc: Acc128<QUERY_BYTES>) -> i64 {
        Self::combine_bytes(acc.fold().map(|total| i64::from(vaddvq_s32(total))))
    }

    /// [`Self::postprocess`] of a group's accumulators in one pass: per query
    /// byte, two rounds of pairwise adds turn the four vectors' lane totals
    /// into one register of per-vector sums (the same wrapping i32 sum as
    /// `vaddvq_s32`), which are widened to i64, combined across the query
    /// bytes, bias-corrected and converted through f64 — exact, with a
    /// single rounding to f32 like the scalar `as f32`.
    #[inline]
    #[target_feature(enable = "neon")]
    fn postprocess_x4_neon(&self, accs: [Acc128<QUERY_BYTES>; GROUP_128]) -> float32x4_t {
        let totals = accs.map(|acc| acc.fold());
        let low = sum_lanes_x4(totals.map(|total| total[0]));
        let mut lo = vmovl_s32(vget_low_s32(low));
        let mut hi = vmovl_high_s32(low);
        if QUERY_BYTES == 2 {
            let high = sum_lanes_x4(totals.map(|total| total[1]));
            let high_lo = vmovl_s32(vget_low_s32(high));
            let high_hi = vmovl_high_s32(high);
            // The radix is a power of two (128 or 256); the shift is an immediate.
            let (scaled_lo, scaled_hi) = match const { encoding(PLANES).query_high_coef } {
                128 => (vshlq_n_s64::<7>(high_lo), vshlq_n_s64::<7>(high_hi)),
                _ => (vshlq_n_s64::<8>(high_lo), vshlq_n_s64::<8>(high_hi)),
            };
            lo = vaddq_s64(lo, scaled_lo);
            hi = vaddq_s64(hi, scaled_hi);
        }
        let bias = vdupq_n_s64(self.bias_correction);
        let lo = vcvtq_f64_s64(vsubq_s64(lo, bias));
        let hi = vcvtq_f64_s64(vsubq_s64(hi, bias));
        let scores = vcvt_high_f32_f64(vcvt_f32_f64(lo), hi);
        vmulq_n_f32(scores, self.postprocess_scale)
    }
}

/// The lane sums of four i32 vectors, one per output lane: the first
/// pairwise add leaves `[a0+a1, a2+a3, b0+b1, b2+b3]`, the second finishes
/// each vector's sum.
#[inline]
#[target_feature(enable = "neon")]
fn sum_lanes_x4([a, b, c, d]: [int32x4_t; 4]) -> int32x4_t {
    vpaddq_s32(vpaddq_s32(a, b), vpaddq_s32(c, d))
}

/// Vectors per interleaved group of the NEON batch kernels: 4 × 4
/// accumulators plus the query block and codebook fit the 32 vector
/// registers.
const GROUP_128: usize = 4;

/// Longest encoded vector (bytes) the NEON batch kernels score in
/// interleaved groups: four cache lines per vector — the value measured for
/// the AVX-512 kernel; not yet tuned on ARM hardware.
const INTERLEAVE_MAX_BYTES: usize = 256;

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::super::shared::random_bytes;
    use super::super::QuerySimd;
    use super::super::shared::{parity_dims, random_inputs};

    /// Every kernel the host supports must reproduce the scalar reference
    /// bit-exactly at every parity dim.
    fn kernels_match_scalar<const PLANES: usize, const QUERY_BYTES: usize>() {
        let has_dotprod = std::arch::is_aarch64_feature_detected!("dotprod");
        let mut rng = StdRng::seed_from_u64(7);
        for dim in parity_dims::<PLANES>() {
            let (query, vector) = random_inputs::<PLANES, QUERY_BYTES>(&mut rng, dim);
            let scalar = query.dotprod_raw(&vector);
            let tag = format!("PLANES={PLANES} QUERY_BYTES={QUERY_BYTES} dim={dim}");
            unsafe {
                let neon = query.dotprod_raw_neon(&vector);
                assert_eq!(scalar, neon, "{tag}: scalar {scalar} != neon {neon}");
                if has_dotprod {
                    let sdot = query.dotprod_raw_neon_sdot(&vector);
                    assert_eq!(scalar, sdot, "{tag}: scalar {scalar} != sdot {sdot}");
                }
            }
        }
    }

    #[test]
    fn test_kernels_match_scalar() {
        kernels_match_scalar::<2, 2>();
        kernels_match_scalar::<4, 2>();
        kernels_match_scalar::<8, 1>();
        kernels_match_scalar::<8, 2>();
    }

    /// Saturation safety at an extreme dim (64K) under the worst-case load:
    /// the query maxed out and every code at the max-magnitude codebook
    /// slot (all-ones bytes at every width).  The scalar reference is i64
    /// throughout; a SIMD mismatch proves some intermediate saturated or
    /// overflowed.
    fn saturation_safety_64k<const PLANES: usize, const QUERY_BYTES: usize>() {
        let dim = 65_536;
        let query = QuerySimd::<PLANES, QUERY_BYTES>::new(&vec![1.0_f32; dim]);
        let vector = vec![0xFF_u8; dim / PLANES];
        let scalar = query.dotprod_raw(&vector);
        let tag = format!("PLANES={PLANES} QUERY_BYTES={QUERY_BYTES}");
        unsafe {
            let neon = query.dotprod_raw_neon(&vector);
            assert_eq!(scalar, neon, "{tag}: neon disagrees");

            if std::arch::is_aarch64_feature_detected!("dotprod") {
                let sdot = query.dotprod_raw_neon_sdot(&vector);
                assert_eq!(scalar, sdot, "{tag}: sdot disagrees");
            }
        }
    }

    #[test]
    fn test_saturation_safety_64k() {
        saturation_safety_64k::<2, 2>();
        saturation_safety_64k::<4, 2>();
        saturation_safety_64k::<8, 1>();
        saturation_safety_64k::<8, 2>();
    }

    /// The batch kernels must reproduce the scalar reference for every
    /// parity dim (interleaved groups and the per-vector remainder) at a
    /// stride equal to and larger than the vector.
    fn batch_kernels_match_scalar<const PLANES: usize, const QUERY_BYTES: usize>() {
        let has_dotprod = std::arch::is_aarch64_feature_detected!("dotprod");
        let mut rng = StdRng::seed_from_u64(7);
        for dim in parity_dims::<PLANES>() {
            let (query, _) = random_inputs::<PLANES, QUERY_BYTES>(&mut rng, dim);
            let vector_bytes = dim / PLANES;
            for stride in [vector_bytes, vector_bytes + 4] {
                let count = 11;
                let data = random_bytes(&mut rng, count * stride);
                let expected: Vec<f32> = (0..count)
                    .map(|v| {
                        query.postprocess(query.dotprod_raw(&data[v * stride..][..vector_bytes]))
                    })
                    .collect();
                let tag =
                    format!("PLANES={PLANES} QUERY_BYTES={QUERY_BYTES} dim={dim} stride={stride}");

                let mut actual = vec![0.0; count];
                unsafe { query.dotprod_batch_neon(&data, stride, &mut actual) };
                assert_eq!(expected, actual, "{tag}: neon batch");

                if has_dotprod {
                    let mut actual = vec![0.0; count];
                    unsafe { query.dotprod_batch_neon_sdot(&data, stride, &mut actual) };
                    assert_eq!(expected, actual, "{tag}: sdot batch");
                }
            }
        }
    }

    #[test]
    fn test_batch_kernels_match_scalar() {
        batch_kernels_match_scalar::<2, 2>();
        batch_kernels_match_scalar::<4, 2>();
        batch_kernels_match_scalar::<8, 1>();
        batch_kernels_match_scalar::<8, 2>();
    }
}
