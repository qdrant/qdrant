//! x86_64 SIMD paths for [`Query4bitSimd`].
//!
//! The codebook is stored as unsigned u8 ∈ [0, 255] (`CODEBOOK_U8`), which is
//! exactly what `_mm_maddubs_epi16` / `VPDPBUSD` expect in their u8 operand
//! slot; the signed shift `c_signed = c_u − 128` is undone by
//! `Query4bitSimd`'s per-query `bias_correction`.  Query halves are 7-bit
//! signed to keep the maddubs pair sum under i16 saturation:
//!   c_u ≤ 255, q ∈ [−64, 63] → |pair| ≤ 2·255·64 = 32 640 < 32 767.
//! `QUERY_HIGH_COEF = 128` here — half the aarch64 value because the query
//! halves cover a 7-bit range.

use super::{CODEBOOK_SCALE, CODEBOOK_U8, QUERY_HIGH_COEF, Query4bitSimd};

impl Query4bitSimd {
    /// x86_64 SSE4.1 + SSSE3 implementation of [`Query4bitSimd::dotprod_raw`].
    ///
    /// # Safety
    /// CPU must support `ssse3` and `sse4.1`.
    #[target_feature(enable = "sse4.1,ssse3")]
    pub unsafe fn dotprod_raw_sse(&self, vector: &[u8]) -> i64 {
        use core::arch::x86_64::*;

        assert_eq!(
            vector.len(),
            self.expected_vector_bytes(),
            "Query4bitSimd::dotprod_raw_sse: vector length mismatch ({} vs expected {})",
            vector.len(),
            self.expected_vector_bytes(),
        );

        unsafe {
            let codebook = _mm_loadu_si128(CODEBOOK_U8.as_ptr().cast::<__m128i>());
            let ones = _mm_set1_epi16(1);
            let nibble_mask = _mm_set1_epi8(0x0F);
            let mut acc_low = _mm_setzero_si128();
            let mut acc_high = _mm_setzero_si128();

            for (chunk_idx, [low, high]) in self.query_data.iter().enumerate() {
                let v_packed =
                    _mm_loadl_epi64(vector.as_ptr().add(chunk_idx * 8).cast::<__m128i>());
                let v_lo = _mm_and_si128(v_packed, nibble_mask);
                let v_hi = _mm_and_si128(_mm_srli_epi16(v_packed, 4), nibble_mask);
                let v = _mm_unpacklo_epi8(v_lo, v_hi);
                let c_u = _mm_shuffle_epi8(codebook, v);

                let q_low = _mm_loadu_si128(low.as_ptr().cast::<__m128i>());
                let q_high = _mm_loadu_si128(high.as_ptr().cast::<__m128i>());

                let prod_low = _mm_maddubs_epi16(c_u, q_low);
                let prod_high = _mm_maddubs_epi16(c_u, q_high);
                acc_low = _mm_add_epi32(acc_low, _mm_madd_epi16(prod_low, ones));
                acc_high = _mm_add_epi32(acc_high, _mm_madd_epi16(prod_high, ones));
            }

            // Tail: one extra SSE chunk on a zero-padded 8-byte scratch.
            if let Some(buf) = self.tail_chunk_scratch(vector) {
                let v_packed = _mm_loadl_epi64(buf.as_ptr().cast::<__m128i>());
                let v_lo = _mm_and_si128(v_packed, nibble_mask);
                let v_hi = _mm_and_si128(_mm_srli_epi16(v_packed, 4), nibble_mask);
                let v = _mm_unpacklo_epi8(v_lo, v_hi);
                let c_u = _mm_shuffle_epi8(codebook, v);
                let q_low = _mm_loadu_si128(self.tail_low.as_ptr().cast::<__m128i>());
                let q_high = _mm_loadu_si128(self.tail_high.as_ptr().cast::<__m128i>());
                let prod_low = _mm_maddubs_epi16(c_u, q_low);
                let prod_high = _mm_maddubs_epi16(c_u, q_high);
                acc_low = _mm_add_epi32(acc_low, _mm_madd_epi16(prod_low, ones));
                acc_high = _mm_add_epi32(acc_high, _mm_madd_epi16(prod_high, ones));
            }

            i64::from(hsum_i32_sse(acc_low)) + QUERY_HIGH_COEF * i64::from(hsum_i32_sse(acc_high))
        }
    }

    /// x86_64 AVX2 implementation over the even/odd query planes: 32 packed
    /// bytes (64 dims) per iteration.  One mask yields the 32 even-dim
    /// codebook indices, one shift+mask the 32 odd-dim ones, so the per-dim
    /// shuffle work is two `vpshufb` per 64 dims; the four `maddubs → madd`
    /// products go into four independent accumulators to keep the pipeline
    /// full.  `maddubs` pair sums stay inside i16 by the module-level query
    /// bound (`|pair| ≤ 2·255·64 = 32 640 < 32 767`).
    ///
    /// # Safety
    /// CPU must support `avx2`.
    #[target_feature(enable = "avx2")]
    pub unsafe fn dotprod_raw_avx2(&self, vector: &[u8]) -> i64 {
        assert_eq!(
            vector.len(),
            self.expected_vector_bytes(),
            "Query4bitSimd::dotprod_raw_avx2: vector length mismatch ({} vs expected {})",
            vector.len(),
            self.expected_vector_bytes(),
        );

        unsafe { self.dotprod_raw_avx2_core(vector) }
    }

    /// Batch counterpart of [`Self::dotprod_raw_avx2`], with the float
    /// reconstruction applied — see `dotprod_batch` for the layout contract
    /// (length preconditions are asserted there).
    ///
    /// # Safety
    /// CPU must support `avx2`.
    #[target_feature(enable = "avx2")]
    pub(super) unsafe fn dotprod_batch_avx2(&self, data: &[u8], stride: usize, out: &mut [f32]) {
        unsafe {
            for (v, out) in out.iter_mut().enumerate() {
                let raw = self.dotprod_raw_avx2_core(&data[v * stride..]);
                *out = self.postprocess_scale * (raw - self.bias_correction) as f32;
            }
        }
    }

    /// Shared core of the AVX2 kernels.  Reads exactly
    /// `expected_vector_bytes()` from the front of `vector` — callers
    /// guarantee the slice is at least that long.
    ///
    /// # Safety
    /// CPU must support `avx2`.
    #[target_feature(enable = "avx2")]
    unsafe fn dotprod_raw_avx2_core(&self, vector: &[u8]) -> i64 {
        use core::arch::x86_64::*;

        unsafe {
            let codebook_128 = _mm_loadu_si128(CODEBOOK_U8.as_ptr().cast::<__m128i>());
            let codebook = _mm256_broadcastsi128_si256(codebook_128);
            let ones = _mm256_set1_epi16(1);
            let nibble_mask = _mm256_set1_epi8(0x0F);
            let mut acc_even_low = _mm256_setzero_si256();
            let mut acc_even_high = _mm256_setzero_si256();
            let mut acc_odd_low = _mm256_setzero_si256();
            let mut acc_odd_high = _mm256_setzero_si256();

            let stride = self.plane_stride;
            let planes = self.planes.as_ptr();
            let half = self.expected_vector_bytes();

            // A local macro rather than a closure: closures don't reliably
            // inherit the enclosing `#[target_feature]`, which would demote
            // the intrinsics inside to non-inlined calls.
            macro_rules! step {
                ($v_packed:expr, $offset:expr) => {{
                    let v_packed = $v_packed;
                    let offset = $offset;
                    let idx_even = _mm256_and_si256(v_packed, nibble_mask);
                    let idx_odd = _mm256_and_si256(_mm256_srli_epi16(v_packed, 4), nibble_mask);
                    let c_even = _mm256_shuffle_epi8(codebook, idx_even);
                    let c_odd = _mm256_shuffle_epi8(codebook, idx_odd);

                    let q_even_low = _mm256_loadu_si256(planes.add(offset).cast::<__m256i>());
                    let q_even_high =
                        _mm256_loadu_si256(planes.add(stride + offset).cast::<__m256i>());
                    let q_odd_low =
                        _mm256_loadu_si256(planes.add(2 * stride + offset).cast::<__m256i>());
                    let q_odd_high =
                        _mm256_loadu_si256(planes.add(3 * stride + offset).cast::<__m256i>());

                    let prod_el = _mm256_maddubs_epi16(c_even, q_even_low);
                    let prod_eh = _mm256_maddubs_epi16(c_even, q_even_high);
                    let prod_ol = _mm256_maddubs_epi16(c_odd, q_odd_low);
                    let prod_oh = _mm256_maddubs_epi16(c_odd, q_odd_high);
                    acc_even_low = _mm256_add_epi32(acc_even_low, _mm256_madd_epi16(prod_el, ones));
                    acc_even_high =
                        _mm256_add_epi32(acc_even_high, _mm256_madd_epi16(prod_eh, ones));
                    acc_odd_low = _mm256_add_epi32(acc_odd_low, _mm256_madd_epi16(prod_ol, ones));
                    acc_odd_high = _mm256_add_epi32(acc_odd_high, _mm256_madd_epi16(prod_oh, ones));
                }};
            }

            let full_blocks = half / 32;
            for i in 0..full_blocks {
                let v_packed = _mm256_loadu_si256(vector.as_ptr().add(32 * i).cast::<__m256i>());
                step!(v_packed, 32 * i);
            }

            // Last partial data block via a zero-padded scratch: the matching
            // query-plane bytes past `half` are zero, so the scratch's zero
            // data lanes contribute nothing either way.
            let rem = half - full_blocks * 32;
            if rem > 0 {
                let mut buf = [0_u8; 32];
                buf[..rem].copy_from_slice(&vector[full_blocks * 32..half]);
                let v_packed = _mm256_loadu_si256(buf.as_ptr().cast::<__m256i>());
                step!(v_packed, 32 * full_blocks);
            }

            let acc_low = _mm256_add_epi32(acc_even_low, acc_odd_low);
            let acc_high = _mm256_add_epi32(acc_even_high, acc_odd_high);
            let sum_low = _mm_add_epi32(
                _mm256_castsi256_si128(acc_low),
                _mm256_extracti128_si256(acc_low, 1),
            );
            let sum_high = _mm_add_epi32(
                _mm256_castsi256_si128(acc_high),
                _mm256_extracti128_si256(acc_high, 1),
            );
            i64::from(hsum_i32_sse(sum_low)) + QUERY_HIGH_COEF * i64::from(hsum_i32_sse(sum_high))
        }
    }

    /// AVX-512 VNNI (Ice Lake Xeon+, Zen 4+) over the even/odd query planes:
    /// 64 packed bytes (128 dims) per iteration.  One mask yields the 64
    /// even-dim codebook indices, one shift+mask the 64 odd-dim ones — two
    /// `vpshufb` of shuffle work per 128 dims — and the four `VPDPBUSD` land
    /// in four independent accumulators, so throughput isn't bound by the
    /// multi-cycle `VPDPBUSD` latency the way a single accumulator chain is.
    /// The last partial block uses a masked load; its dead lanes multiply
    /// against the planes' zero padding.
    ///
    /// i32 lane bound: each `VPDPBUSD` adds ≤ 4·255·64 = 65 280 per lane, so
    /// overflow would need ~2¹⁵ blocks ≈ 4M dims — far beyond any real input.
    ///
    /// # Safety
    /// CPU must support `avx512f`, `avx512bw`, and `avx512vnni`.
    #[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
    pub unsafe fn dotprod_raw_avx512_vnni(&self, vector: &[u8]) -> i64 {
        assert_eq!(
            vector.len(),
            self.expected_vector_bytes(),
            "Query4bitSimd::dotprod_raw_avx512_vnni: vector length mismatch ({} vs expected {})",
            vector.len(),
            self.expected_vector_bytes(),
        );

        unsafe { self.dotprod_raw_avx512_vnni_core(vector) }
    }

    /// Batch counterpart of [`Self::dotprod_raw_avx512_vnni`], with the float
    /// reconstruction applied — see `dotprod_batch` for the layout contract
    /// (length preconditions are asserted there).
    ///
    /// # Safety
    /// CPU must support `avx512f`, `avx512bw`, and `avx512vnni`.
    #[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
    pub(super) unsafe fn dotprod_batch_avx512_vnni(
        &self,
        data: &[u8],
        stride: usize,
        out: &mut [f32],
    ) {
        unsafe {
            for (v, out) in out.iter_mut().enumerate() {
                let raw = self.dotprod_raw_avx512_vnni_core(&data[v * stride..]);
                *out = self.postprocess_scale * (raw - self.bias_correction) as f32;
            }
        }
    }

    /// Shared core of the AVX-512 kernels.  Reads exactly
    /// `expected_vector_bytes()` from the front of `vector` — callers
    /// guarantee the slice is at least that long.
    ///
    /// # Safety
    /// CPU must support `avx512f`, `avx512bw`, and `avx512vnni`.
    #[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
    unsafe fn dotprod_raw_avx512_vnni_core(&self, vector: &[u8]) -> i64 {
        use core::arch::x86_64::*;

        unsafe {
            let codebook_128 = _mm_loadu_si128(CODEBOOK_U8.as_ptr().cast::<__m128i>());
            let codebook = _mm512_broadcast_i32x4(codebook_128);
            let nibble_mask = _mm512_set1_epi8(0x0F);
            let mut acc_even_low = _mm512_setzero_si512();
            let mut acc_even_high = _mm512_setzero_si512();
            let mut acc_odd_low = _mm512_setzero_si512();
            let mut acc_odd_high = _mm512_setzero_si512();

            let stride = self.plane_stride;
            let planes = self.planes.as_ptr();
            let half = self.expected_vector_bytes();

            // A local macro rather than a closure: closures don't reliably
            // inherit the enclosing `#[target_feature]`, which would demote
            // the intrinsics inside to non-inlined calls.
            macro_rules! step {
                ($v_packed:expr, $offset:expr) => {{
                    let v_packed = $v_packed;
                    let offset = $offset;
                    let idx_even = _mm512_and_si512(v_packed, nibble_mask);
                    let idx_odd = _mm512_and_si512(_mm512_srli_epi16(v_packed, 4), nibble_mask);
                    let c_even = _mm512_shuffle_epi8(codebook, idx_even);
                    let c_odd = _mm512_shuffle_epi8(codebook, idx_odd);

                    let q_even_low = _mm512_loadu_si512(planes.add(offset).cast::<__m512i>());
                    let q_even_high =
                        _mm512_loadu_si512(planes.add(stride + offset).cast::<__m512i>());
                    let q_odd_low =
                        _mm512_loadu_si512(planes.add(2 * stride + offset).cast::<__m512i>());
                    let q_odd_high =
                        _mm512_loadu_si512(planes.add(3 * stride + offset).cast::<__m512i>());

                    acc_even_low = _mm512_dpbusd_epi32(acc_even_low, c_even, q_even_low);
                    acc_even_high = _mm512_dpbusd_epi32(acc_even_high, c_even, q_even_high);
                    acc_odd_low = _mm512_dpbusd_epi32(acc_odd_low, c_odd, q_odd_low);
                    acc_odd_high = _mm512_dpbusd_epi32(acc_odd_high, c_odd, q_odd_high);
                }};
            }

            let full_blocks = half / 64;
            for i in 0..full_blocks {
                let v_packed = _mm512_loadu_si512(vector.as_ptr().add(64 * i).cast::<__m512i>());
                step!(v_packed, 64 * i);
            }

            let rem = half - full_blocks * 64;
            if rem > 0 {
                let mask = (1_u64 << rem) - 1;
                let v_packed =
                    _mm512_maskz_loadu_epi8(mask, vector.as_ptr().add(64 * full_blocks).cast());
                step!(v_packed, 64 * full_blocks);
            }

            let acc_low = _mm512_add_epi32(acc_even_low, acc_odd_low);
            let acc_high = _mm512_add_epi32(acc_even_high, acc_odd_high);
            i64::from(_mm512_reduce_add_epi32(acc_low))
                + QUERY_HIGH_COEF * i64::from(_mm512_reduce_add_epi32(acc_high))
        }
    }
}

#[target_feature(enable = "sse2")]
unsafe fn hsum_i32_sse(v: core::arch::x86_64::__m128i) -> i32 {
    use core::arch::x86_64::*;
    let v = _mm_add_epi32(v, _mm_shuffle_epi32(v, 0x4E));
    let v = _mm_add_epi32(v, _mm_shuffle_epi32(v, 0xB1));
    _mm_cvtsi128_si32(v)
}

// ------------------------------------------------------------------
// score_4bit_internal — both operands signed, so we can't reuse
// `maddubs` / `VPDPBUSD`.  The honest path widens the signed codebook
// bytes to i16 and uses `madd_epi16` (signed × signed → i32 pair-sum);
// AVX-512 VNNI's `VPDPWSSD` is the fused equivalent on ZMM.
//
// We load `CODEBOOK_U8` and XOR with 0x80 to recover the signed i8 form
// (= c_u − 128).  The resulting `c_signed` lives in [−128, 127], so:
//   per product:       |c_a · c_b| ≤ 128·128 = 16 384
//   madd_epi16 pair:   ≤ 2·16 384 = 32 768 < i32::MAX ✓
//   i32 acc at 64K:    ≤ 16 384·16 384 = 268 M ≪ i32::MAX
// Far away from saturation in every intermediate.
// ------------------------------------------------------------------

/// SSE4.1 + SSSE3 implementation of [`super::score_4bit_internal`].
///
/// # Safety
/// CPU must support `ssse3` and `sse4.1`.
#[target_feature(enable = "sse4.1,ssse3")]
pub unsafe fn score_4bit_internal_sse(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::x86_64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_4bit_internal_sse: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    unsafe {
        let codebook_i8 = _mm_xor_si128(
            _mm_loadu_si128(CODEBOOK_U8.as_ptr().cast::<__m128i>()),
            _mm_set1_epi8(-128i8),
        );
        let nibble_mask = _mm_set1_epi8(0x0F);
        let mut acc = _mm_setzero_si128();

        let n_full = a.len() / 8;
        for i in 0..n_full {
            let va = _mm_loadl_epi64(a.as_ptr().add(i * 8).cast::<__m128i>());
            let va_lo = _mm_and_si128(va, nibble_mask);
            let va_hi = _mm_and_si128(_mm_srli_epi16(va, 4), nibble_mask);
            let a_idx = _mm_unpacklo_epi8(va_lo, va_hi);
            let c_a_i8 = _mm_shuffle_epi8(codebook_i8, a_idx);

            let vb = _mm_loadl_epi64(b.as_ptr().add(i * 8).cast::<__m128i>());
            let vb_lo = _mm_and_si128(vb, nibble_mask);
            let vb_hi = _mm_and_si128(_mm_srli_epi16(vb, 4), nibble_mask);
            let b_idx = _mm_unpacklo_epi8(vb_lo, vb_hi);
            let c_b_i8 = _mm_shuffle_epi8(codebook_i8, b_idx);

            let c_a_lo = _mm_cvtepi8_epi16(c_a_i8);
            let c_a_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_a_i8, 8));
            let c_b_lo = _mm_cvtepi8_epi16(c_b_i8);
            let c_b_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_b_i8, 8));

            let prod_lo = _mm_madd_epi16(c_a_lo, c_b_lo);
            let prod_hi = _mm_madd_epi16(c_a_hi, c_b_hi);
            acc = _mm_add_epi32(acc, _mm_add_epi32(prod_lo, prod_hi));
        }

        let simd_bytes = n_full * 8;
        let sum = i64::from(hsum_i32_sse(acc))
            + super::score_4bit_internal_integer(&a[simd_bytes..], &b[simd_bytes..]);
        sum as f32 / (CODEBOOK_SCALE * CODEBOOK_SCALE)
    }
}

/// AVX2 implementation of [`super::score_4bit_internal`].  32 elements per
/// iteration (16 bytes from each source) using YMM widening and
/// `_mm256_madd_epi16`.
///
/// # Safety
/// CPU must support `avx2`.
#[target_feature(enable = "avx2")]
pub unsafe fn score_4bit_internal_avx2(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::x86_64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_4bit_internal_avx2: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    unsafe {
        let codebook_i8_128 = _mm_xor_si128(
            _mm_loadu_si128(CODEBOOK_U8.as_ptr().cast::<__m128i>()),
            _mm_set1_epi8(-128i8),
        );
        let codebook_i8 = _mm256_broadcastsi128_si256(codebook_i8_128);
        let nibble_mask = _mm_set1_epi8(0x0F);
        let mut acc = _mm256_setzero_si256();

        let n_iters = a.len() / 16;
        for i in 0..n_iters {
            let va = _mm_loadu_si128(a.as_ptr().add(16 * i).cast::<__m128i>());
            let va_lo = _mm_and_si128(va, nibble_mask);
            let va_hi = _mm_and_si128(_mm_srli_epi16(va, 4), nibble_mask);
            let a_idx_0 = _mm_unpacklo_epi8(va_lo, va_hi);
            let a_idx_1 = _mm_unpackhi_epi8(va_lo, va_hi);
            let a_idx_256 = _mm256_inserti128_si256(_mm256_castsi128_si256(a_idx_0), a_idx_1, 1);
            let c_a_i8 = _mm256_shuffle_epi8(codebook_i8, a_idx_256);

            let vb = _mm_loadu_si128(b.as_ptr().add(16 * i).cast::<__m128i>());
            let vb_lo = _mm_and_si128(vb, nibble_mask);
            let vb_hi = _mm_and_si128(_mm_srli_epi16(vb, 4), nibble_mask);
            let b_idx_0 = _mm_unpacklo_epi8(vb_lo, vb_hi);
            let b_idx_1 = _mm_unpackhi_epi8(vb_lo, vb_hi);
            let b_idx_256 = _mm256_inserti128_si256(_mm256_castsi128_si256(b_idx_0), b_idx_1, 1);
            let c_b_i8 = _mm256_shuffle_epi8(codebook_i8, b_idx_256);

            // Widen i8×32 into two i16×16 halves.
            let c_a_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(c_a_i8));
            let c_a_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(c_a_i8, 1));
            let c_b_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(c_b_i8));
            let c_b_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(c_b_i8, 1));

            let prod_lo = _mm256_madd_epi16(c_a_lo, c_b_lo);
            let prod_hi = _mm256_madd_epi16(c_a_hi, c_b_hi);
            acc = _mm256_add_epi32(acc, _mm256_add_epi32(prod_lo, prod_hi));
        }

        let acc_lo = _mm256_castsi256_si128(acc);
        let acc_hi = _mm256_extracti128_si256(acc, 1);
        let simd_bytes = n_iters * 16;
        let sum = i64::from(hsum_i32_sse(_mm_add_epi32(acc_lo, acc_hi)))
            + super::score_4bit_internal_integer(&a[simd_bytes..], &b[simd_bytes..]);
        sum as f32 / (CODEBOOK_SCALE * CODEBOOK_SCALE)
    }
}

/// AVX-512 VNNI implementation of [`super::score_4bit_internal`].  Uses
/// `VPDPWSSD` — the signed-signed fused counterpart of `VPDPBUSD` —
/// available as part of AVX-512 VNNI on Ice Lake Xeon+, Sapphire Rapids,
/// and Zen 4+.  32 elements per iteration: widen 32 i8 → 32 i16 in one
/// ZMM each, then a single `dpwssd` accumulates 16 pair-products into
/// 16 i32 lanes.
///
/// # Safety
/// CPU must support `avx512f`, `avx512bw`, and `avx512vnni`.
#[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
pub unsafe fn score_4bit_internal_avx512_vnni(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::x86_64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_4bit_internal_avx512_vnni: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    unsafe {
        let codebook_i8_128 = _mm_xor_si128(
            _mm_loadu_si128(CODEBOOK_U8.as_ptr().cast::<__m128i>()),
            _mm_set1_epi8(-128i8),
        );
        let codebook_i8_256 = _mm256_broadcastsi128_si256(codebook_i8_128);
        let nibble_mask = _mm_set1_epi8(0x0F);
        let mut acc = _mm512_setzero_si512();

        // 32 elements (= 16 bytes from each source) per iter.  Shuffle stays
        // on 256-bit (32 codebook values fit exactly), then we widen to 512.
        let n_iters = a.len() / 16;
        for i in 0..n_iters {
            let va = _mm_loadu_si128(a.as_ptr().add(16 * i).cast::<__m128i>());
            let va_lo = _mm_and_si128(va, nibble_mask);
            let va_hi = _mm_and_si128(_mm_srli_epi16(va, 4), nibble_mask);
            let a_idx_0 = _mm_unpacklo_epi8(va_lo, va_hi);
            let a_idx_1 = _mm_unpackhi_epi8(va_lo, va_hi);
            let a_idx_256 = _mm256_inserti128_si256(_mm256_castsi128_si256(a_idx_0), a_idx_1, 1);
            let c_a_i8_256 = _mm256_shuffle_epi8(codebook_i8_256, a_idx_256);

            let vb = _mm_loadu_si128(b.as_ptr().add(16 * i).cast::<__m128i>());
            let vb_lo = _mm_and_si128(vb, nibble_mask);
            let vb_hi = _mm_and_si128(_mm_srli_epi16(vb, 4), nibble_mask);
            let b_idx_0 = _mm_unpacklo_epi8(vb_lo, vb_hi);
            let b_idx_1 = _mm_unpackhi_epi8(vb_lo, vb_hi);
            let b_idx_256 = _mm256_inserti128_si256(_mm256_castsi128_si256(b_idx_0), b_idx_1, 1);
            let c_b_i8_256 = _mm256_shuffle_epi8(codebook_i8_256, b_idx_256);

            // Widen 32 i8 → 32 i16, one ZMM each.
            let c_a_i16 = _mm512_cvtepi8_epi16(c_a_i8_256);
            let c_b_i16 = _mm512_cvtepi8_epi16(c_b_i8_256);

            // VPDPWSSD: acc[lane] += a[2·lane]·b[2·lane] + a[2·lane+1]·b[2·lane+1]
            // with every operand interpreted as signed i16.
            acc = _mm512_dpwssd_epi32(acc, c_a_i16, c_b_i16);
        }

        let acc_256_lo = _mm512_castsi512_si256(acc);
        let acc_256_hi = _mm512_extracti64x4_epi64(acc, 1);
        let acc_256 = _mm256_add_epi32(acc_256_lo, acc_256_hi);
        let acc_128 = _mm_add_epi32(
            _mm256_castsi256_si128(acc_256),
            _mm256_extracti128_si256(acc_256, 1),
        );
        let simd_bytes = n_iters * 16;
        let sum = i64::from(hsum_i32_sse(acc_128))
            + super::score_4bit_internal_integer(&a[simd_bytes..], &b[simd_bytes..]);
        sum as f32 / (CODEBOOK_SCALE * CODEBOOK_SCALE)
    }
}

// ------------------------------------------------------------------
// score_4bit_internal_weighted — TQ+ symmetric path with per-coord
// `D'²` weighting. `weights[i]` is `i16` (non-negative, capped at
// `i16::MAX − 1` — see `ErrorCorrection::d_prime_sq_i16` doc), which
// matches the SIMD load directly and feeds the very efficient
// `madd_epi16` pair-sum.
//
// Per-coord product bound:
//   |c_a · c_b · w| ≤ 128·128·32 766 ≈ 5.37e8
//   madd pair sum: ≤ 2 · 5.37e8 ≈ 1.07e9 < i32::MAX (2.15e9) ✓
// At dim=64 K the i32 sum reaches ≈ 1.7e10 — overflow if accumulated
// in i32. We widen each `madd_epi16` result to i64 lanes immediately
// (`_mm256_cvtepi32_epi64`) and accumulate into an i64 ymm reg, with
// far enough headroom for any practical dim.
// ------------------------------------------------------------------

/// SSE4.1 + SSSE3 weighted variant of [`super::score_4bit_internal`].
///
/// # Safety
/// CPU must support `ssse3` and `sse4.1`.
#[target_feature(enable = "sse4.1,ssse3")]
pub unsafe fn score_4bit_internal_weighted_sse(a: &[u8], b: &[u8], weights: &[i16]) -> i64 {
    use core::arch::x86_64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_4bit_internal_weighted_sse: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );
    assert_eq!(
        weights.len(),
        2 * a.len(),
        "score_4bit_internal_weighted_sse: weights length {} != 2 · a.len() {}",
        weights.len(),
        2 * a.len(),
    );

    unsafe {
        let codebook_i8 = _mm_xor_si128(
            _mm_loadu_si128(CODEBOOK_U8.as_ptr().cast::<__m128i>()),
            _mm_set1_epi8(-128i8),
        );
        let nibble_mask = _mm_set1_epi8(0x0F);
        let mut acc = _mm_setzero_si128(); // 2 i64 lanes

        // 8 bytes from each source = 16 coords per iter.
        let n_full = a.len() / 8;
        for i in 0..n_full {
            let va = _mm_loadl_epi64(a.as_ptr().add(i * 8).cast::<__m128i>());
            let va_lo = _mm_and_si128(va, nibble_mask);
            let va_hi = _mm_and_si128(_mm_srli_epi16(va, 4), nibble_mask);
            let a_idx = _mm_unpacklo_epi8(va_lo, va_hi);
            let c_a_i8 = _mm_shuffle_epi8(codebook_i8, a_idx);

            let vb = _mm_loadl_epi64(b.as_ptr().add(i * 8).cast::<__m128i>());
            let vb_lo = _mm_and_si128(vb, nibble_mask);
            let vb_hi = _mm_and_si128(_mm_srli_epi16(vb, 4), nibble_mask);
            let b_idx = _mm_unpacklo_epi8(vb_lo, vb_hi);
            let c_b_i8 = _mm_shuffle_epi8(codebook_i8, b_idx);

            // Widen i8×16 → i16×8 (low half only — the high half is zero
            // because `unpacklo_epi8` placed all 16 indices in the low 16
            // bytes already; for SSE we process 16 coords per iter via two
            // halves of the i8 register).
            let c_a_lo = _mm_cvtepi8_epi16(c_a_i8);
            let c_a_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_a_i8, 8));
            let c_b_lo = _mm_cvtepi8_epi16(c_b_i8);
            let c_b_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_b_i8, 8));

            // c_a × c_b in i16 (max 16129 fits).
            let prod_lo = _mm_mullo_epi16(c_a_lo, c_b_lo);
            let prod_hi = _mm_mullo_epi16(c_a_hi, c_b_hi);

            // Load 16 i16 weights = 32 bytes = 2 xmm regs.
            let w_lo = _mm_loadu_si128(weights.as_ptr().add(16 * i).cast::<__m128i>());
            let w_hi = _mm_loadu_si128(weights.as_ptr().add(16 * i + 8).cast::<__m128i>());

            // Pair-sum (prod[2k]·w[2k] + prod[2k+1]·w[2k+1]) → 4 i32 lanes each.
            let pw_lo = _mm_madd_epi16(prod_lo, w_lo);
            let pw_hi = _mm_madd_epi16(prod_hi, w_hi);
            let pw = _mm_add_epi32(pw_lo, pw_hi);

            // Widen 4 i32 → 2 i64 + 2 i64, accumulate.
            let pw_lo_i64 = _mm_cvtepi32_epi64(pw);
            let pw_hi_i64 = _mm_cvtepi32_epi64(_mm_srli_si128(pw, 8));
            acc = _mm_add_epi64(acc, pw_lo_i64);
            acc = _mm_add_epi64(acc, pw_hi_i64);
        }

        // Hsum 2 i64 lanes.
        let mut tmp = [0i64; 2];
        _mm_storeu_si128(tmp.as_mut_ptr().cast::<__m128i>(), acc);
        let simd_sum = tmp[0] + tmp[1];

        // Tail.
        let simd_bytes = n_full * 8;
        let tail = super::score_4bit_internal_weighted_scalar(
            &a[simd_bytes..],
            &b[simd_bytes..],
            &weights[2 * simd_bytes..],
        );
        simd_sum + tail
    }
}

/// AVX2 weighted variant of [`super::score_4bit_internal`]. 32 coords per
/// iteration; `madd_epi16` pair-sums i16 weighted products into i32 lanes
/// then widens to i64 for accumulation.
///
/// # Safety
/// CPU must support `avx2`.
#[target_feature(enable = "avx2")]
pub unsafe fn score_4bit_internal_weighted_avx2(a: &[u8], b: &[u8], weights: &[i16]) -> i64 {
    use core::arch::x86_64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_4bit_internal_weighted_avx2: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );
    assert_eq!(
        weights.len(),
        2 * a.len(),
        "score_4bit_internal_weighted_avx2: weights length {} != 2 · a.len() {}",
        weights.len(),
        2 * a.len(),
    );

    unsafe {
        let codebook_i8_128 = _mm_xor_si128(
            _mm_loadu_si128(CODEBOOK_U8.as_ptr().cast::<__m128i>()),
            _mm_set1_epi8(-128i8),
        );
        let codebook_i8 = _mm256_broadcastsi128_si256(codebook_i8_128);
        let nibble_mask = _mm_set1_epi8(0x0F);
        let mut acc = _mm256_setzero_si256(); // 4 i64 lanes

        // 16 bytes from each source = 32 coords per iter.
        let n_iters = a.len() / 16;
        for i in 0..n_iters {
            let va = _mm_loadu_si128(a.as_ptr().add(16 * i).cast::<__m128i>());
            let va_lo = _mm_and_si128(va, nibble_mask);
            let va_hi = _mm_and_si128(_mm_srli_epi16(va, 4), nibble_mask);
            let a_idx_0 = _mm_unpacklo_epi8(va_lo, va_hi);
            let a_idx_1 = _mm_unpackhi_epi8(va_lo, va_hi);
            let a_idx_256 = _mm256_inserti128_si256(_mm256_castsi128_si256(a_idx_0), a_idx_1, 1);
            let c_a_i8 = _mm256_shuffle_epi8(codebook_i8, a_idx_256);

            let vb = _mm_loadu_si128(b.as_ptr().add(16 * i).cast::<__m128i>());
            let vb_lo = _mm_and_si128(vb, nibble_mask);
            let vb_hi = _mm_and_si128(_mm_srli_epi16(vb, 4), nibble_mask);
            let b_idx_0 = _mm_unpacklo_epi8(vb_lo, vb_hi);
            let b_idx_1 = _mm_unpackhi_epi8(vb_lo, vb_hi);
            let b_idx_256 = _mm256_inserti128_si256(_mm256_castsi128_si256(b_idx_0), b_idx_1, 1);
            let c_b_i8 = _mm256_shuffle_epi8(codebook_i8, b_idx_256);

            // Widen i8×32 → i16×16 + i16×16.
            let c_a_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(c_a_i8));
            let c_a_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(c_a_i8, 1));
            let c_b_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(c_b_i8));
            let c_b_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(c_b_i8, 1));

            // c_a × c_b in i16 (max 16129 fits).
            let prod_lo = _mm256_mullo_epi16(c_a_lo, c_b_lo);
            let prod_hi = _mm256_mullo_epi16(c_a_hi, c_b_hi);

            // Load 32 i16 weights = 64 bytes = 2 ymm regs.
            let w_lo = _mm256_loadu_si256(weights.as_ptr().add(32 * i).cast::<__m256i>());
            let w_hi = _mm256_loadu_si256(weights.as_ptr().add(32 * i + 16).cast::<__m256i>());

            // Pair-sum into i32: 8 i32 lanes per madd.
            let pw_lo = _mm256_madd_epi16(prod_lo, w_lo);
            let pw_hi = _mm256_madd_epi16(prod_hi, w_hi);
            let pw = _mm256_add_epi32(pw_lo, pw_hi);

            // Widen 8 i32 → 4 i64 + 4 i64, accumulate.
            let pw_lo_i64 = _mm256_cvtepi32_epi64(_mm256_castsi256_si128(pw));
            let pw_hi_i64 = _mm256_cvtepi32_epi64(_mm256_extracti128_si256(pw, 1));
            acc = _mm256_add_epi64(acc, pw_lo_i64);
            acc = _mm256_add_epi64(acc, pw_hi_i64);
        }

        // Hsum 4 i64 lanes.
        let acc_lo = _mm256_castsi256_si128(acc);
        let acc_hi = _mm256_extracti128_si256(acc, 1);
        let summed = _mm_add_epi64(acc_lo, acc_hi);
        let mut tmp = [0i64; 2];
        _mm_storeu_si128(tmp.as_mut_ptr().cast::<__m128i>(), summed);
        let simd_sum = tmp[0] + tmp[1];

        // Tail.
        let simd_bytes = n_iters * 16;
        let tail = super::score_4bit_internal_weighted_scalar(
            &a[simd_bytes..],
            &b[simd_bytes..],
            &weights[2 * simd_bytes..],
        );
        simd_sum + tail
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::super::shared::pack_codes;
    use super::super::shared::{PARITY_DIMS, random_inputs};
    use super::super::{
        Query4bitSimd, score_4bit_internal_scalar, score_4bit_internal_weighted_scalar,
    };
    use super::{
        score_4bit_internal_avx2, score_4bit_internal_avx512_vnni, score_4bit_internal_sse,
        score_4bit_internal_weighted_avx2, score_4bit_internal_weighted_sse,
    };

    /// Build deterministic non-negative i16 weights of length `2 · vec_bytes`
    /// for parity tests of the weighted kernels.
    fn random_weights(rng: &mut StdRng, vec_bytes: usize) -> Vec<i16> {
        use rand::RngExt;
        (0..2 * vec_bytes)
            .map(|_| rng.random_range(0..=i16::MAX))
            .collect()
    }

    #[test]
    fn test_sse_matches_scalar() {
        if !std::is_x86_feature_detected!("ssse3") || !std::is_x86_feature_detected!("sse4.1") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (simd_query, vector) = random_inputs(&mut rng, dim);
            let scalar = simd_query.dotprod_raw(&vector);
            let sse = unsafe { simd_query.dotprod_raw_sse(&vector) };
            assert_eq!(scalar, sse, "scalar {scalar} != sse {sse} at dim {dim}");
        }
    }

    #[test]
    fn test_avx2_matches_scalar() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (simd_query, vector) = random_inputs(&mut rng, dim);
            let scalar = simd_query.dotprod_raw(&vector);
            let avx2 = unsafe { simd_query.dotprod_raw_avx2(&vector) };
            assert_eq!(scalar, avx2, "scalar {scalar} != avx2 {avx2} at dim {dim}");
        }
    }

    #[test]
    fn test_avx512_vnni_matches_scalar() {
        if !(std::is_x86_feature_detected!("avx512f")
            && std::is_x86_feature_detected!("avx512bw")
            && std::is_x86_feature_detected!("avx512vnni"))
        {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (simd_query, vector) = random_inputs(&mut rng, dim);
            let scalar = simd_query.dotprod_raw(&vector);
            let vnni512 = unsafe { simd_query.dotprod_raw_avx512_vnni(&vector) };
            assert_eq!(
                scalar, vnni512,
                "scalar {scalar} != avx512_vnni {vnni512} at dim {dim}"
            );
        }
    }

    /// Single saturation-safety check at an extreme dim (64K) with the
    /// worst-case combination: query maxed out and every lane of the vector
    /// pointing at the extreme-magnitude codebook slot.  Scalar is the
    /// reference (i64 throughout, saturation-free by construction); each
    /// SIMD path must match it exactly.  A mismatch proves that some
    /// intermediate integer saturated or overflowed.
    #[test]
    fn test_saturation_safety_64k() {
        let dim = 65_536;
        let query = vec![1.0_f32; dim];
        let indices: Vec<u8> = vec![15; dim]; // CODEBOOK_U8[15] = 255 (max magnitude)
        let vector = pack_codes(&indices, 4);

        let q = Query4bitSimd::new(&query);
        let scalar = q.dotprod_raw(&vector);

        unsafe {
            if std::is_x86_feature_detected!("ssse3") && std::is_x86_feature_detected!("sse4.1") {
                let sse = q.dotprod_raw_sse(&vector);
                assert_eq!(scalar, sse, "sse disagrees at dim={dim}");
            }
            if std::is_x86_feature_detected!("avx2") {
                let avx2 = q.dotprod_raw_avx2(&vector);
                assert_eq!(scalar, avx2, "avx2 disagrees at dim={dim}");
            }
            if std::is_x86_feature_detected!("avx512f")
                && std::is_x86_feature_detected!("avx512bw")
                && std::is_x86_feature_detected!("avx512vnni")
            {
                let v512 = q.dotprod_raw_avx512_vnni(&vector);
                assert_eq!(scalar, v512, "avx512_vnni disagrees at dim={dim}");
            }
        }
    }

    /// Parity: each x86 `score_4bit_internal` variant must reproduce the
    /// scalar reference bit-exactly.  Both sides compute `Σ c_signed_a ·
    /// c_signed_b / c_scale²` with deterministic ordering, so the f32
    /// outputs are identical.
    #[test]
    fn test_score_sse_matches_scalar() {
        if !std::is_x86_feature_detected!("ssse3") || !std::is_x86_feature_detected!("sse4.1") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (_, vec_a) = random_inputs(&mut rng, dim);
            let (_, vec_b) = random_inputs(&mut rng, dim);
            let scalar = score_4bit_internal_scalar(&vec_a, &vec_b);
            let sse = unsafe { score_4bit_internal_sse(&vec_a, &vec_b) };
            assert_eq!(
                scalar, sse,
                "score: scalar {scalar} != sse {sse} at dim {dim}"
            );
        }
    }

    #[test]
    fn test_score_avx2_matches_scalar() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (_, vec_a) = random_inputs(&mut rng, dim);
            let (_, vec_b) = random_inputs(&mut rng, dim);
            let scalar = score_4bit_internal_scalar(&vec_a, &vec_b);
            let avx2 = unsafe { score_4bit_internal_avx2(&vec_a, &vec_b) };
            assert_eq!(
                scalar, avx2,
                "score: scalar {scalar} != avx2 {avx2} at dim {dim}"
            );
        }
    }

    #[test]
    fn test_score_avx512_vnni_matches_scalar() {
        if !(std::is_x86_feature_detected!("avx512f")
            && std::is_x86_feature_detected!("avx512bw")
            && std::is_x86_feature_detected!("avx512vnni"))
        {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (_, vec_a) = random_inputs(&mut rng, dim);
            let (_, vec_b) = random_inputs(&mut rng, dim);
            let scalar = score_4bit_internal_scalar(&vec_a, &vec_b);
            let vnni512 = unsafe { score_4bit_internal_avx512_vnni(&vec_a, &vec_b) };
            assert_eq!(
                scalar, vnni512,
                "score: scalar {scalar} != avx512_vnni {vnni512} at dim {dim}"
            );
        }
    }

    /// Parity for the weighted kernel: every x86 backend must match the
    /// scalar reference bit-exactly across our matryoshka corner-case dims.
    #[test]
    fn test_score_weighted_sse_matches_scalar() {
        if !std::is_x86_feature_detected!("ssse3") || !std::is_x86_feature_detected!("sse4.1") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(0xBEEF);
        for &dim in PARITY_DIMS {
            let (_, vec_a) = random_inputs(&mut rng, dim);
            let (_, vec_b) = random_inputs(&mut rng, dim);
            let weights = random_weights(&mut rng, vec_a.len());
            let scalar = score_4bit_internal_weighted_scalar(&vec_a, &vec_b, &weights);
            let sse = unsafe { score_4bit_internal_weighted_sse(&vec_a, &vec_b, &weights) };
            assert_eq!(
                scalar, sse,
                "weighted: scalar {scalar} != sse {sse} at dim {dim}"
            );
        }
    }

    #[test]
    fn test_score_weighted_avx2_matches_scalar() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(0xBEEF);
        for &dim in PARITY_DIMS {
            let (_, vec_a) = random_inputs(&mut rng, dim);
            let (_, vec_b) = random_inputs(&mut rng, dim);
            let weights = random_weights(&mut rng, vec_a.len());
            let scalar = score_4bit_internal_weighted_scalar(&vec_a, &vec_b, &weights);
            let avx2 = unsafe { score_4bit_internal_weighted_avx2(&vec_a, &vec_b, &weights) };
            assert_eq!(
                scalar, avx2,
                "weighted: scalar {scalar} != avx2 {avx2} at dim {dim}"
            );
        }
    }

    /// Saturation-safety for the weighted kernel: every x86 backend at 64K
    /// dims with worst-case inputs (max-magnitude codebook + max-magnitude
    /// i16 weight) must match the i64 scalar reference.
    #[test]
    fn test_score_weighted_saturation_safety_64k() {
        let dim = 65_536;
        let indices: Vec<u8> = vec![15; dim];
        let vec_a = pack_codes(&indices, 4);
        let vec_b = pack_codes(&indices, 4);
        let max_weight: i16 = i16::MAX;
        let weights: Vec<i16> = vec![max_weight; dim];

        let scalar = score_4bit_internal_weighted_scalar(&vec_a, &vec_b, &weights);

        unsafe {
            if std::is_x86_feature_detected!("ssse3") && std::is_x86_feature_detected!("sse4.1") {
                let sse = score_4bit_internal_weighted_sse(&vec_a, &vec_b, &weights);
                assert_eq!(scalar, sse, "weighted score sse disagrees at dim={dim}");
            }
            if std::is_x86_feature_detected!("avx2") {
                let avx2 = score_4bit_internal_weighted_avx2(&vec_a, &vec_b, &weights);
                assert_eq!(scalar, avx2, "weighted score avx2 disagrees at dim={dim}");
            }
        }
    }

    /// Saturation-safety at 64K for all x86 score paths simultaneously.
    /// Both vectors are every index 15 → `c_signed = 127`, every product is
    /// `127² = 16 129`, every madd pair ≤ 32 258 (i32-safe, nowhere near i16
    /// because we already widen).  Total = 16 384·16 129 ≈ 264 M, fits i32
    /// with ~8× headroom.
    #[test]
    fn test_score_saturation_safety_64k() {
        let dim = 65_536;
        let indices: Vec<u8> = vec![15; dim]; // CODEBOOK_U8[15] = 255 → signed 127
        let vec_a = pack_codes(&indices, 4);
        let vec_b = pack_codes(&indices, 4);

        let scalar = score_4bit_internal_scalar(&vec_a, &vec_b);

        unsafe {
            if std::is_x86_feature_detected!("ssse3") && std::is_x86_feature_detected!("sse4.1") {
                let sse = score_4bit_internal_sse(&vec_a, &vec_b);
                assert_eq!(scalar, sse, "score sse disagrees at dim={dim}");
            }
            if std::is_x86_feature_detected!("avx2") {
                let avx2 = score_4bit_internal_avx2(&vec_a, &vec_b);
                assert_eq!(scalar, avx2, "score avx2 disagrees at dim={dim}");
            }
            if std::is_x86_feature_detected!("avx512f")
                && std::is_x86_feature_detected!("avx512bw")
                && std::is_x86_feature_detected!("avx512vnni")
            {
                let vnni = score_4bit_internal_avx512_vnni(&vec_a, &vec_b);
                assert_eq!(scalar, vnni, "score avx512_vnni disagrees at dim={dim}");
            }
        }
    }
}
