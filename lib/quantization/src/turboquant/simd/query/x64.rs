//! x86_64 kernels for [`QuerySimd`].
//!
//! The codebook is stored unsigned (`Encoding::codebook` as `u8`), which
//! is what `maddubs` / `VPDPBUSD` expect in their `u8` operand slot; the
//! signed shift `c_signed = c_u − offset` is undone by the per-query
//! `bias_correction`.  See the module docs of [`super`] for the query
//! ranges that keep the `maddubs` pair sums inside i16.

use core::arch::x86_64::*;

use super::{Code, PLANE_BLOCK, QueryPlanes, QuerySimd, encoding};

/// Packed data bytes per SSE block: one XMM of codes.
const BLOCK_128: usize = 16;
const _: () = assert!(PLANE_BLOCK.is_multiple_of(BLOCK_128));

/// Packed data bytes per AVX2 block: one YMM of codes.
const BLOCK_256: usize = 32;
const _: () = assert!(PLANE_BLOCK.is_multiple_of(BLOCK_256));

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

/// One [`BLOCK_128`]-byte block of every query plane.
#[derive(Clone, Copy)]
struct QueryBlock128<const PLANES: usize> {
    low: [__m128i; PLANES],
    high: [__m128i; PLANES],
}

impl<const PLANES: usize> QueryBlock128<PLANES> {
    /// # Safety
    /// CPU must support `sse2`; `offset + BLOCK_128` must not exceed the
    /// plane length.
    #[inline]
    #[target_feature(enable = "sse2")]
    unsafe fn load(planes: &QueryPlanes<PLANES>, offset: usize) -> Self {
        let mut block = Self {
            low: [_mm_setzero_si128(); PLANES],
            high: [_mm_setzero_si128(); PLANES],
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
/// CPU must support `sse2`; `offset + BLOCK_128 <= plane.len()`.
#[inline]
#[target_feature(enable = "sse2")]
unsafe fn load_plane_128(plane: &[i8], offset: usize) -> __m128i {
    debug_assert!(offset + BLOCK_128 <= plane.len());
    unsafe { _mm_loadu_si128(plane.as_ptr().add(offset).cast::<__m128i>()) }
}

/// [`next_plane_512`] on XMM.
#[inline]
#[target_feature(enable = "sse2")]
unsafe fn next_plane_128<const PLANES: usize>(codes: __m128i) -> __m128i {
    match PLANES {
        2 => _mm_srli_epi16(codes, 4),
        4 => _mm_srli_epi16(codes, 2),
        _ => _mm_srli_epi16(codes, 1),
    }
}

/// `maddubs → madd` accumulators of one vector; the 128-bit form of
/// [`Acc256`], with the same integer bounds.
#[derive(Clone, Copy)]
struct Acc128 {
    low: [__m128i; 2],
    high: [__m128i; 2],
}

impl Acc128 {
    /// # Safety
    /// CPU must support `sse2`.
    #[inline]
    #[target_feature(enable = "sse2")]
    unsafe fn zero() -> Self {
        let zero = _mm_setzero_si128();
        Self {
            low: [zero; 2],
            high: [zero; 2],
        }
    }

    /// Fold one block of packed `codes` (16 bytes) into the accumulators.
    ///
    /// # Safety
    /// CPU must support `ssse3` and `sse4.1`.
    #[inline]
    #[target_feature(enable = "sse4.1,ssse3")]
    unsafe fn accumulate<const PLANES: usize>(
        &mut self,
        codes: __m128i,
        query: QueryBlock128<PLANES>,
    ) {
        let table = const { codebook::<PLANES>() };
        let codebook = unsafe { _mm_loadu_si128(table.as_ptr().cast::<__m128i>()) };
        let mask = _mm_set1_epi8(const { code_mask::<PLANES>() });
        let ones = _mm_set1_epi16(1);
        let mut shifted = codes;
        for k in 0..PLANES {
            let values = _mm_shuffle_epi8(codebook, _mm_and_si128(shifted, mask));
            let dot = |acc: __m128i, query: __m128i| {
                _mm_add_epi32(acc, _mm_madd_epi16(_mm_maddubs_epi16(values, query), ones))
            };
            self.low[k & 1] = dot(self.low[k & 1], query.low[k]);
            self.high[k & 1] = dot(self.high[k & 1], query.high[k]);
            shifted = unsafe { next_plane_128::<PLANES>(shifted) };
        }
    }

    /// Per-lane `(low, high)` query-half totals.
    ///
    /// # Safety
    /// CPU must support `sse2`.
    #[inline]
    #[target_feature(enable = "sse2")]
    unsafe fn fold(self) -> (__m128i, __m128i) {
        (
            _mm_add_epi32(self.low[0], self.low[1]),
            _mm_add_epi32(self.high[0], self.high[1]),
        )
    }
}

/// One [`BLOCK_256`]-byte block of every query plane.
#[derive(Clone, Copy)]
struct QueryBlock256<const PLANES: usize> {
    low: [__m256i; PLANES],
    high: [__m256i; PLANES],
}

impl<const PLANES: usize> QueryBlock256<PLANES> {
    /// # Safety
    /// CPU must support `avx2`; `offset + BLOCK_256` must not exceed the
    /// plane length.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn load(planes: &QueryPlanes<PLANES>, offset: usize) -> Self {
        let mut block = Self {
            low: [_mm256_setzero_si256(); PLANES],
            high: [_mm256_setzero_si256(); PLANES],
        };
        for (k, low) in block.low.iter_mut().enumerate() {
            *low = unsafe { load_plane_256(&planes.low[k], offset) };
        }
        for (k, high) in block.high.iter_mut().enumerate() {
            *high = unsafe { load_plane_256(&planes.high[k], offset) };
        }
        block
    }
}

/// # Safety
/// CPU must support `avx2`; `offset + BLOCK_256 <= plane.len()`.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn load_plane_256(plane: &[i8], offset: usize) -> __m256i {
    debug_assert!(offset + BLOCK_256 <= plane.len());
    unsafe { _mm256_loadu_si256(plane.as_ptr().add(offset).cast::<__m256i>()) }
}

/// [`next_plane_512`] on YMM.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn next_plane_256<const PLANES: usize>(codes: __m256i) -> __m256i {
    match PLANES {
        2 => _mm256_srli_epi16(codes, 4),
        4 => _mm256_srli_epi16(codes, 2),
        _ => _mm256_srli_epi16(codes, 1),
    }
}

/// `maddubs → madd` accumulators of one vector; the same two-pair shape as
/// [`Acc512`].
///
/// The `maddubs` pair sums stay inside i16 by the module-level query bound
/// (`|pair| ≤ 2 · 255 · 64 = 32 640`, or exactly `2 · 128 · 128 = 32 768`
/// at the negative end for the 1-bit encoding); `madd` against ones then
/// adds at most 65 280 (65 536) per i32 lane per plane, the same bound as
/// VNNI.
#[derive(Clone, Copy)]
struct Acc256 {
    low: [__m256i; 2],
    high: [__m256i; 2],
}

impl Acc256 {
    /// # Safety
    /// CPU must support `avx2`.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn zero() -> Self {
        let zero = _mm256_setzero_si256();
        Self {
            low: [zero; 2],
            high: [zero; 2],
        }
    }

    /// Fold one block of packed `codes` (32 bytes) into the accumulators.
    ///
    /// # Safety
    /// CPU must support `avx2`.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn accumulate<const PLANES: usize>(
        &mut self,
        codes: __m256i,
        query: QueryBlock256<PLANES>,
    ) {
        let table = const { codebook::<PLANES>() };
        let codebook = _mm256_broadcastsi128_si256(unsafe {
            _mm_loadu_si128(table.as_ptr().cast::<__m128i>())
        });
        let mask = _mm256_set1_epi8(const { code_mask::<PLANES>() });
        let ones = _mm256_set1_epi16(1);
        let mut shifted = codes;
        for k in 0..PLANES {
            let values = _mm256_shuffle_epi8(codebook, _mm256_and_si256(shifted, mask));
            let dot = |acc: __m256i, query: __m256i| {
                _mm256_add_epi32(
                    acc,
                    _mm256_madd_epi16(_mm256_maddubs_epi16(values, query), ones),
                )
            };
            self.low[k & 1] = dot(self.low[k & 1], query.low[k]);
            self.high[k & 1] = dot(self.high[k & 1], query.high[k]);
            shifted = unsafe { next_plane_256::<PLANES>(shifted) };
        }
    }

    /// Per-lane `(low, high)` query-half totals.
    ///
    /// # Safety
    /// CPU must support `avx2`.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn fold(self) -> (__m256i, __m256i) {
        (
            _mm256_add_epi32(self.low[0], self.low[1]),
            _mm256_add_epi32(self.high[0], self.high[1]),
        )
    }
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
    /// x86_64 SSE4.1 + SSSE3 over the query planes: one XMM of packed codes
    /// per block, the 128-bit form of the AVX2 kernel.
    ///
    /// # Safety
    /// CPU must support `ssse3` and `sse4.1`.
    #[target_feature(enable = "sse4.1,ssse3")]
    pub unsafe fn dotprod_raw_sse(&self, vector: &[u8]) -> i64 {
        assert_eq!(
            vector.len(),
            self.vector_bytes,
            "QuerySimd<{PLANES}>::dotprod_raw_sse: vector length mismatch ({} vs expected {})",
            vector.len(),
            self.vector_bytes,
        );

        unsafe {
            let (low, high) = self.accumulate_sse(vector.as_ptr()).fold();
            i64::from(hsum_i32_sse(low))
                + Self::ENCODING.query_high_coef * i64::from(hsum_i32_sse(high))
        }
    }

    /// Block loop of the SSE kernels over the vector at `data`.
    ///
    /// # Safety
    /// CPU must support `ssse3` and `sse4.1`; `data` must be readable for
    /// `self.vector_bytes` bytes.
    #[inline]
    #[target_feature(enable = "sse4.1,ssse3")]
    unsafe fn accumulate_sse(&self, data: *const u8) -> Acc128 {
        unsafe {
            let mut acc = Acc128::zero();

            let full_blocks = self.vector_bytes / BLOCK_128;
            for block in 0..full_blocks {
                let offset = block * BLOCK_128;
                let query = QueryBlock128::load(&self.planes, offset);
                let codes = _mm_loadu_si128(data.add(offset).cast::<__m128i>());
                acc.accumulate(codes, query);
            }

            let tail = self.vector_bytes % BLOCK_128;
            if tail > 0 {
                let offset = full_blocks * BLOCK_128;
                let query = QueryBlock128::load(&self.planes, offset);
                let mut block = [0u8; BLOCK_128];
                std::ptr::copy_nonoverlapping(data.add(offset), block.as_mut_ptr(), tail);
                let codes = _mm_loadu_si128(block.as_ptr().cast::<__m128i>());
                acc.accumulate(codes, query);
            }

            acc
        }
    }

    /// x86_64 AVX2 over the query planes: one YMM of packed codes per
    /// block, one `vpshufb` codebook lookup per plane and `maddubs → madd`
    /// products into independent accumulators (see [`Acc256`]).  The last
    /// partial block runs on a zero-padded copy of the remaining bytes.
    ///
    /// # Safety
    /// CPU must support `avx2`.
    #[target_feature(enable = "avx2")]
    pub unsafe fn dotprod_raw_avx2(&self, vector: &[u8]) -> i64 {
        assert_eq!(
            vector.len(),
            self.vector_bytes,
            "QuerySimd<{PLANES}>::dotprod_raw_avx2: vector length mismatch ({} vs expected {})",
            vector.len(),
            self.vector_bytes,
        );

        unsafe { Self::reduce_avx2(self.accumulate_avx2(vector.as_ptr())) }
    }

    /// Block loop of the AVX2 kernels over the vector at `data`.
    ///
    /// # Safety
    /// CPU must support `avx2`; `data` must be readable for
    /// `self.vector_bytes` bytes.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn accumulate_avx2(&self, data: *const u8) -> Acc256 {
        unsafe {
            let mut acc = Acc256::zero();

            let full_blocks = self.vector_bytes / BLOCK_256;
            for block in 0..full_blocks {
                let offset = block * BLOCK_256;
                let query = QueryBlock256::load(&self.planes, offset);
                let codes = _mm256_loadu_si256(data.add(offset).cast::<__m256i>());
                acc.accumulate(codes, query);
            }

            let tail = self.vector_bytes % BLOCK_256;
            if tail > 0 {
                let offset = full_blocks * BLOCK_256;
                let query = QueryBlock256::load(&self.planes, offset);
                let mut block = [0u8; BLOCK_256];
                std::ptr::copy_nonoverlapping(data.add(offset), block.as_mut_ptr(), tail);
                let codes = _mm256_loadu_si256(block.as_ptr().cast::<__m256i>());
                acc.accumulate(codes, query);
            }

            acc
        }
    }

    /// Raw dot product from one vector's accumulators.
    ///
    /// # Safety
    /// CPU must support `avx2`.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn reduce_avx2(acc: Acc256) -> i64 {
        unsafe {
            let (low, high) = acc.fold();
            i64::from(hsum_i32_avx2(low))
                + Self::ENCODING.query_high_coef * i64::from(hsum_i32_avx2(high))
        }
    }

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

#[target_feature(enable = "sse2")]
unsafe fn hsum_i32_sse(v: __m128i) -> i32 {
    let v = _mm_add_epi32(v, _mm_shuffle_epi32(v, 0x4E));
    let v = _mm_add_epi32(v, _mm_shuffle_epi32(v, 0xB1));
    _mm_cvtsi128_si32(v)
}

#[inline]
#[target_feature(enable = "avx2")]
unsafe fn hsum_i32_avx2(v: __m256i) -> i32 {
    unsafe {
        hsum_i32_sse(_mm_add_epi32(
            _mm256_castsi256_si128(v),
            _mm256_extracti128_si256(v, 1),
        ))
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::QuerySimd;
    use super::super::shared::{parity_dims, random_inputs};

    fn has_sse() -> bool {
        std::is_x86_feature_detected!("ssse3") && std::is_x86_feature_detected!("sse4.1")
    }

    /// The SSE kernel must reproduce the scalar reference bit-exactly at
    /// every parity dim of every width.
    fn sse_matches_scalar<const PLANES: usize>() {
        let mut rng = StdRng::seed_from_u64(7);
        for dim in parity_dims::<PLANES>() {
            let (query, vector) = random_inputs::<PLANES>(&mut rng, dim);
            let scalar = query.dotprod_raw(&vector);
            let sse = unsafe { query.dotprod_raw_sse(&vector) };
            assert_eq!(
                scalar, sse,
                "PLANES={PLANES} dim={dim}: scalar {scalar} != sse {sse}"
            );
        }
    }

    #[test]
    fn test_sse_matches_scalar() {
        if !has_sse() {
            return;
        }
        sse_matches_scalar::<2>();
        sse_matches_scalar::<4>();
        sse_matches_scalar::<8>();
    }

    /// The AVX2 kernel must reproduce the scalar reference bit-exactly at
    /// every parity dim of every width.
    fn avx2_matches_scalar<const PLANES: usize>() {
        let mut rng = StdRng::seed_from_u64(7);
        for dim in parity_dims::<PLANES>() {
            let (query, vector) = random_inputs::<PLANES>(&mut rng, dim);
            let scalar = query.dotprod_raw(&vector);
            let avx2 = unsafe { query.dotprod_raw_avx2(&vector) };
            assert_eq!(
                scalar, avx2,
                "PLANES={PLANES} dim={dim}: scalar {scalar} != avx2 {avx2}"
            );
        }
    }

    #[test]
    fn test_avx2_matches_scalar() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        avx2_matches_scalar::<2>();
        avx2_matches_scalar::<4>();
        avx2_matches_scalar::<8>();
    }

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
        unsafe {
            if has_sse() {
                let sse = query.dotprod_raw_sse(&vector);
                assert_eq!(scalar, sse, "PLANES={PLANES}: sse disagrees");
            }
            if std::is_x86_feature_detected!("avx2") {
                let avx2 = query.dotprod_raw_avx2(&vector);
                assert_eq!(scalar, avx2, "PLANES={PLANES}: avx2 disagrees");
            }
            if has_avx512_vnni() {
                let vnni512 = query.dotprod_raw_avx512_vnni(&vector);
                assert_eq!(scalar, vnni512, "PLANES={PLANES}: avx512_vnni disagrees");
            }
        }
    }

    #[test]
    fn test_saturation_safety_64k() {
        saturation_safety_64k::<2>();
        saturation_safety_64k::<4>();
        saturation_safety_64k::<8>();
    }
}
