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

    /// x86_64 AVX2 implementation.  `query_data` stores `[low, high]` as 32
    /// contiguous i8 bytes per chunk, so a single YMM load grabs both halves.
    /// Codebook is broadcast to both 128-bit lanes; maddubs pairs the upper
    /// lane with `high` and the lower lane with `low`, producing i32 sums
    /// split cleanly by lane at the end.
    ///
    /// # Safety
    /// CPU must support `avx2`.
    #[target_feature(enable = "avx2")]
    pub unsafe fn dotprod_raw_avx2(&self, vector: &[u8]) -> i64 {
        use core::arch::x86_64::*;

        assert_eq!(
            vector.len(),
            self.expected_vector_bytes(),
            "Query4bitSimd::dotprod_raw_avx2: vector length mismatch ({} vs expected {})",
            vector.len(),
            self.expected_vector_bytes(),
        );

        unsafe {
            let codebook_128 = _mm_loadu_si128(CODEBOOK_U8.as_ptr().cast::<__m128i>());
            let codebook = _mm256_broadcastsi128_si256(codebook_128);
            let ones = _mm256_set1_epi16(1);
            let ones_128 = _mm_set1_epi16(1);
            let nibble_mask = _mm_set1_epi8(0x0F);
            let mut acc = _mm256_setzero_si256();

            for (chunk_idx, chunk) in self.query_data.iter().enumerate() {
                let low_high = _mm256_loadu_si256(chunk.as_ptr().cast::<__m256i>());

                let v_packed =
                    _mm_loadl_epi64(vector.as_ptr().add(chunk_idx * 8).cast::<__m128i>());
                let v_lo = _mm_and_si128(v_packed, nibble_mask);
                let v_hi = _mm_and_si128(_mm_srli_epi16(v_packed, 4), nibble_mask);
                let v128 = _mm_unpacklo_epi8(v_lo, v_hi);
                let v = _mm256_broadcastsi128_si256(v128);
                let c = _mm256_shuffle_epi8(codebook, v);

                let prods = _mm256_maddubs_epi16(c, low_high);
                acc = _mm256_add_epi32(acc, _mm256_madd_epi16(prods, ones));
            }

            let mut acc_low = _mm256_castsi256_si128(acc);
            let mut acc_high = _mm256_extracti128_si256(acc, 1);

            // Tail: one extra SSE chunk on a zero-padded scratch — matches
            // the SSE variant's post-loop kernel.
            if let Some(buf) = self.tail_chunk_scratch(vector) {
                let v_packed = _mm_loadl_epi64(buf.as_ptr().cast::<__m128i>());
                let v_lo = _mm_and_si128(v_packed, nibble_mask);
                let v_hi = _mm_and_si128(_mm_srli_epi16(v_packed, 4), nibble_mask);
                let v = _mm_unpacklo_epi8(v_lo, v_hi);
                let c_u = _mm_shuffle_epi8(codebook_128, v);
                let q_low = _mm_loadu_si128(self.tail_low.as_ptr().cast::<__m128i>());
                let q_high = _mm_loadu_si128(self.tail_high.as_ptr().cast::<__m128i>());
                let prod_low = _mm_maddubs_epi16(c_u, q_low);
                let prod_high = _mm_maddubs_epi16(c_u, q_high);
                acc_low = _mm_add_epi32(acc_low, _mm_madd_epi16(prod_low, ones_128));
                acc_high = _mm_add_epi32(acc_high, _mm_madd_epi16(prod_high, ones_128));
            }

            i64::from(hsum_i32_sse(acc_low)) + QUERY_HIGH_COEF * i64::from(hsum_i32_sse(acc_high))
        }
    }

    /// AVX-512 VNNI (Ice Lake Xeon+, Zen 4+): 2 chunks per iteration.  Two
    /// consecutive `[low, high]` entries = 64 bytes = one ZMM load.  `VPDPBUSD`
    /// fuses the 4-wide u8×i8 dot with i32 accumulation; the ZMM layout puts
    /// [low_a, high_a, low_b, high_b] into lanes 0..3.
    ///
    /// # Safety
    /// CPU must support `avx512f`, `avx512bw`, and `avx512vnni`.
    #[target_feature(enable = "avx512f,avx512bw,avx512vnni,sse4.1,ssse3")]
    pub unsafe fn dotprod_raw_avx512_vnni(&self, vector: &[u8]) -> i64 {
        use core::arch::x86_64::*;

        assert_eq!(
            vector.len(),
            self.expected_vector_bytes(),
            "Query4bitSimd::dotprod_raw_avx512_vnni: vector length mismatch ({} vs expected {})",
            vector.len(),
            self.expected_vector_bytes(),
        );

        unsafe {
            let codebook_128 = _mm_loadu_si128(CODEBOOK_U8.as_ptr().cast::<__m128i>());
            let codebook_512 = _mm512_broadcast_i32x4(codebook_128);
            let nibble_mask_128 = _mm_set1_epi8(0x0F);
            let ones_128 = _mm_set1_epi16(1);
            let mut acc = _mm512_setzero_si512();

            let chunks = self.query_data.as_slice();
            let n_pairs = chunks.len() / 2;

            for i in 0..n_pairs {
                let pair_ptr = chunks.as_ptr().add(2 * i).cast::<__m512i>();
                let low_high_pair = _mm512_loadu_si512(pair_ptr);

                let v_packed_16 = _mm_loadu_si128(vector.as_ptr().add(16 * i).cast::<__m128i>());
                let v_lo = _mm_and_si128(v_packed_16, nibble_mask_128);
                let v_hi = _mm_and_si128(_mm_srli_epi16(v_packed_16, 4), nibble_mask_128);
                let v_chunk_a = _mm_unpacklo_epi8(v_lo, v_hi);
                let v_chunk_b = _mm_unpackhi_epi8(v_lo, v_hi);

                let v_dup_a = _mm256_broadcastsi128_si256(v_chunk_a);
                let v_dup_b = _mm256_broadcastsi128_si256(v_chunk_b);
                let v_512 = _mm512_inserti64x4(_mm512_castsi256_si512(v_dup_a), v_dup_b, 1);

                let c_512 = _mm512_shuffle_epi8(codebook_512, v_512);
                acc = _mm512_dpbusd_epi32(acc, c_512, low_high_pair);
            }

            let acc_256_lo = _mm512_castsi512_si256(acc);
            let acc_256_hi = _mm512_extracti64x4_epi64(acc, 1);
            let lane_a_low = _mm256_castsi256_si128(acc_256_lo);
            let lane_a_high = _mm256_extracti128_si256(acc_256_lo, 1);
            let lane_b_low = _mm256_castsi256_si128(acc_256_hi);
            let lane_b_high = _mm256_extracti128_si256(acc_256_hi, 1);
            let mut sum_low_xmm = _mm_add_epi32(lane_a_low, lane_b_low);
            let mut sum_high_xmm = _mm_add_epi32(lane_a_high, lane_b_high);

            // Odd leftover chunk via SSE-style `maddubs + madd_epi16` — 1 chunk
            // is too narrow to benefit from VNNI here.  Only reached when
            // `query_data.len()` is odd (dim not a multiple of 32).
            if chunks.len() % 2 == 1 {
                let tail_chunk = 2 * n_pairs;
                let [low, high] = chunks[tail_chunk];

                let v_packed =
                    _mm_loadl_epi64(vector.as_ptr().add(tail_chunk * 8).cast::<__m128i>());
                let v_lo = _mm_and_si128(v_packed, nibble_mask_128);
                let v_hi = _mm_and_si128(_mm_srli_epi16(v_packed, 4), nibble_mask_128);
                let v = _mm_unpacklo_epi8(v_lo, v_hi);
                let c_u = _mm_shuffle_epi8(codebook_128, v);

                let q_low = _mm_loadu_si128(low.as_ptr().cast::<__m128i>());
                let q_high = _mm_loadu_si128(high.as_ptr().cast::<__m128i>());
                let prod_low = _mm_maddubs_epi16(c_u, q_low);
                let prod_high = _mm_maddubs_epi16(c_u, q_high);
                sum_low_xmm = _mm_add_epi32(sum_low_xmm, _mm_madd_epi16(prod_low, ones_128));
                sum_high_xmm = _mm_add_epi32(sum_high_xmm, _mm_madd_epi16(prod_high, ones_128));
            }

            // Tail dims (< 14 remaining after the optional leftover chunk):
            // one SSE chunk on a zero-padded scratch, same as the SSE variant.
            if let Some(buf) = self.tail_chunk_scratch(vector) {
                let v_packed = _mm_loadl_epi64(buf.as_ptr().cast::<__m128i>());
                let v_lo = _mm_and_si128(v_packed, nibble_mask_128);
                let v_hi = _mm_and_si128(_mm_srli_epi16(v_packed, 4), nibble_mask_128);
                let v = _mm_unpacklo_epi8(v_lo, v_hi);
                let c_u = _mm_shuffle_epi8(codebook_128, v);

                let q_low = _mm_loadu_si128(self.tail_low.as_ptr().cast::<__m128i>());
                let q_high = _mm_loadu_si128(self.tail_high.as_ptr().cast::<__m128i>());
                let prod_low = _mm_maddubs_epi16(c_u, q_low);
                let prod_high = _mm_maddubs_epi16(c_u, q_high);
                sum_low_xmm = _mm_add_epi32(sum_low_xmm, _mm_madd_epi16(prod_low, ones_128));
                sum_high_xmm = _mm_add_epi32(sum_high_xmm, _mm_madd_epi16(prod_high, ones_128));
            }

            i64::from(hsum_i32_sse(sum_low_xmm))
                + QUERY_HIGH_COEF * i64::from(hsum_i32_sse(sum_high_xmm))
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

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::super::shared::pack_codes;
    use super::super::shared::{PARITY_DIMS, random_inputs};
    use super::super::{Query4bitSimd, score_4bit_internal_scalar};
    use super::{
        score_4bit_internal_avx2, score_4bit_internal_avx512_vnni, score_4bit_internal_sse,
    };

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
