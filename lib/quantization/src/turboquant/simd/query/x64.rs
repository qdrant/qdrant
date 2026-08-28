//! x86_64 kernels for [`QuerySimd`].
//!
//! The codebook is stored unsigned (`Encoding::codebook` as `u8`), which
//! is what `maddubs` / `VPDPBUSD` expect in their `u8` operand slot; the
//! signed shift `c_signed = c_u − offset` is undone by the per-query
//! `bias_correction`.  See the module docs of [`super`] for the query
//! ranges that keep the `maddubs` pair sums inside i16.

use core::arch::x86_64::*;

use super::{Code, PLANE_BLOCK, QueryPlanes, QuerySimd, encoding};

/// Packed data bytes per AVX-512 block: one ZMM of codes.
const BLOCK_512: usize = 64;
const _: () = assert!(PLANE_BLOCK.is_multiple_of(BLOCK_512));

/// The codebook as a shuffle table for the width packing `PLANES` codes per
/// byte.
const fn codebook<const PLANES: usize>() -> [Code; 16] {
    encoding(PLANES).codebook
}

/// Mask of one code in the low bits of a byte.
const fn code_mask<const PLANES: usize>() -> i8 {
    ((1u16 << (8 / PLANES)) - 1) as i8
}

/// One [`BLOCK_512`]-byte block of every query plane.
#[derive(Clone, Copy)]
struct QueryBlock512<const PLANES: usize> {
    low: [__m512i; PLANES],
    high: [__m512i; PLANES],
}

impl<const PLANES: usize> QueryBlock512<PLANES> {
    /// # Safety
    /// CPU must support `avx512f`; `offset + BLOCK_512` must not exceed the
    /// plane length.
    #[inline]
    #[target_feature(enable = "avx512f")]
    unsafe fn load(planes: &QueryPlanes<PLANES>, offset: usize) -> Self {
        let mut block = Self {
            low: [_mm512_setzero_si512(); PLANES],
            high: [_mm512_setzero_si512(); PLANES],
        };
        for (k, low) in block.low.iter_mut().enumerate() {
            *low = unsafe { load_plane_512(&planes.low[k], offset) };
        }
        for (k, high) in block.high.iter_mut().enumerate() {
            *high = unsafe { load_plane_512(&planes.high[k], offset) };
        }
        block
    }
}

/// # Safety
/// CPU must support `avx512f`; `offset + BLOCK_512 <= plane.len()`.
#[inline]
#[target_feature(enable = "avx512f")]
unsafe fn load_plane_512(plane: &[i8], offset: usize) -> __m512i {
    debug_assert!(offset + BLOCK_512 <= plane.len());
    unsafe { _mm512_loadu_si512(plane.as_ptr().add(offset).cast::<__m512i>()) }
}

/// The next plane's codes moved into the low bits of every byte: `codes >>
/// bits` within 16-bit lanes, so the caller masks off what crosses over
/// from the neighboring byte.  The shift count is an immediate, hence the
/// match over the widths.
#[inline]
#[target_feature(enable = "avx512bw")]
unsafe fn next_plane_512<const PLANES: usize>(codes: __m512i) -> __m512i {
    match PLANES {
        2 => _mm512_srli_epi16(codes, 4),
        4 => _mm512_srli_epi16(codes, 2),
        _ => _mm512_srli_epi16(codes, 1),
    }
}

/// `VPDPBUSD` accumulators of one vector: two independent low/high chains,
/// plane `k` feeding pair `k & 1`.  Two pairs are enough to keep the
/// multiply-accumulate latency off the critical path at every width
/// (`PLANES / 2` dependent steps per block against `2 · PLANES` issued).
///
/// i32 lane bound: each `VPDPBUSD` adds at most `4 · 255 · 64 = 65 280`
/// (`4 · 128 · 128 = 65 536` for the 1-bit encoding) to a lane, so overflow
/// needs ~32 K blocks ≈ 2 M packed bytes — far beyond any real input.
#[derive(Clone, Copy)]
struct Acc512 {
    low: [__m512i; 2],
    high: [__m512i; 2],
}

impl Acc512 {
    /// # Safety
    /// CPU must support `avx512f`.
    #[inline]
    #[target_feature(enable = "avx512f")]
    unsafe fn zero() -> Self {
        let zero = _mm512_setzero_si512();
        Self {
            low: [zero; 2],
            high: [zero; 2],
        }
    }

    /// Fold one block of packed `codes` (64 bytes) into the accumulators:
    /// plane by plane, the codes are shifted down, masked to one code per
    /// byte and looked up in the codebook.
    ///
    /// # Safety
    /// CPU must support `avx512f`, `avx512bw`, and `avx512vnni`.
    #[inline]
    #[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
    unsafe fn accumulate<const PLANES: usize>(
        &mut self,
        codes: __m512i,
        query: QueryBlock512<PLANES>,
    ) {
        let table = const { codebook::<PLANES>() };
        let codebook =
            _mm512_broadcast_i32x4(unsafe { _mm_loadu_si128(table.as_ptr().cast::<__m128i>()) });
        let mask = _mm512_set1_epi8(const { code_mask::<PLANES>() });
        let mut shifted = codes;
        for k in 0..PLANES {
            let values = _mm512_shuffle_epi8(codebook, _mm512_and_si512(shifted, mask));
            self.low[k & 1] = _mm512_dpbusd_epi32(self.low[k & 1], values, query.low[k]);
            self.high[k & 1] = _mm512_dpbusd_epi32(self.high[k & 1], values, query.high[k]);
            shifted = unsafe { next_plane_512::<PLANES>(shifted) };
        }
    }

    /// Per-lane `(low, high)` query-half totals.
    ///
    /// # Safety
    /// CPU must support `avx512f`.
    #[inline]
    #[target_feature(enable = "avx512f")]
    unsafe fn fold(self) -> (__m512i, __m512i) {
        (
            _mm512_add_epi32(self.low[0], self.low[1]),
            _mm512_add_epi32(self.high[0], self.high[1]),
        )
    }
}

impl<const PLANES: usize> QuerySimd<PLANES> {
    /// AVX-512 VNNI (Ice Lake Xeon+, Zen 4+) over the query planes: one ZMM
    /// of packed codes per block, one `vpshufb` codebook lookup per plane
    /// and `VPDPBUSD` into independent accumulators (see [`Acc512`]).  The
    /// last partial block is a masked load whose dead lanes multiply
    /// against the planes' zero padding.
    ///
    /// # Safety
    /// CPU must support `avx512f`, `avx512bw`, and `avx512vnni`.
    #[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
    pub unsafe fn dotprod_raw_avx512_vnni(&self, vector: &[u8]) -> i64 {
        assert_eq!(
            vector.len(),
            self.vector_bytes,
            "QuerySimd<{PLANES}>::dotprod_raw_avx512_vnni: vector length mismatch ({} vs expected {})",
            vector.len(),
            self.vector_bytes,
        );

        unsafe { Self::reduce_avx512(self.accumulate_avx512(vector.as_ptr())) }
    }

    /// Block loop of the AVX-512 kernels over the vector at `data`.
    ///
    /// # Safety
    /// CPU must support `avx512f`, `avx512bw`, and `avx512vnni`; `data` must
    /// be readable for `self.vector_bytes` bytes.
    #[inline]
    #[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
    unsafe fn accumulate_avx512(&self, data: *const u8) -> Acc512 {
        unsafe {
            let mut acc = Acc512::zero();

            let full_blocks = self.vector_bytes / BLOCK_512;
            for block in 0..full_blocks {
                let offset = block * BLOCK_512;
                let query = QueryBlock512::load(&self.planes, offset);
                let codes = _mm512_loadu_si512(data.add(offset).cast::<__m512i>());
                acc.accumulate(codes, query);
            }

            let tail = self.vector_bytes % BLOCK_512;
            if tail > 0 {
                let offset = full_blocks * BLOCK_512;
                let query = QueryBlock512::load(&self.planes, offset);
                let mask: __mmask64 = (1 << tail) - 1;
                let codes = _mm512_maskz_loadu_epi8(mask, data.add(offset).cast::<i8>());
                acc.accumulate(codes, query);
            }

            acc
        }
    }

    /// Raw dot product from one vector's accumulators.
    ///
    /// # Safety
    /// CPU must support `avx512f`.
    #[inline]
    #[target_feature(enable = "avx512f")]
    unsafe fn reduce_avx512(acc: Acc512) -> i64 {
        unsafe {
            let (low, high) = acc.fold();
            i64::from(_mm512_reduce_add_epi32(low))
                + Self::ENCODING.query_high_coef * i64::from(_mm512_reduce_add_epi32(high))
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::QuerySimd;
    use super::super::shared::{parity_dims, random_inputs};

    fn has_avx512_vnni() -> bool {
        std::is_x86_feature_detected!("avx512f")
            && std::is_x86_feature_detected!("avx512bw")
            && std::is_x86_feature_detected!("avx512vnni")
    }

    /// The kernel must reproduce the scalar reference bit-exactly at every
    /// parity dim of every width.
    fn avx512_vnni_matches_scalar<const PLANES: usize>() {
        let mut rng = StdRng::seed_from_u64(7);
        for dim in parity_dims::<PLANES>() {
            let (query, vector) = random_inputs::<PLANES>(&mut rng, dim);
            let scalar = query.dotprod_raw(&vector);
            let vnni512 = unsafe { query.dotprod_raw_avx512_vnni(&vector) };
            assert_eq!(
                scalar, vnni512,
                "PLANES={PLANES} dim={dim}: scalar {scalar} != avx512_vnni {vnni512}"
            );
        }
    }

    #[test]
    fn test_avx512_vnni_matches_scalar() {
        if !has_avx512_vnni() {
            return;
        }
        avx512_vnni_matches_scalar::<2>();
        avx512_vnni_matches_scalar::<4>();
        avx512_vnni_matches_scalar::<8>();
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
        let vnni512 = unsafe { query.dotprod_raw_avx512_vnni(&vector) };
        assert_eq!(scalar, vnni512, "PLANES={PLANES}: avx512_vnni disagrees");
    }

    #[test]
    fn test_saturation_safety_64k() {
        if !has_avx512_vnni() {
            return;
        }
        saturation_safety_64k::<2>();
        saturation_safety_64k::<4>();
        saturation_safety_64k::<8>();
    }
}
