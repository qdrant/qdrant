//! x86_64 SIMD paths for [`Query2bitSimd`] and [`super::score_2bit_internal`].
//!
//! Storage / encoding mirror [`super::query4bit::x64`]: unsigned `CODEBOOK_U8`
//! consumed as the `u8` operand of `maddubs` / `VPDPBUSD`, with the query
//! quantized to 7-bit signed halves (K=128) so the `u8 × i8 → i16` pair-sum
//! never saturates.  The only new machinery is the 2-bit → centroid unpack,
//! which uses a pair-table trick analogous to [`super::arm`]:
//!
//!   * `PAIR_TABLE_EVEN_U8[nibble] = CODEBOOK_U8[nibble & 0b11]`
//!   * `PAIR_TABLE_ODD_U8[nibble]  = CODEBOOK_U8[(nibble >> 2) & 0b11]`
//!
//! Per 4 packed data bytes: split low/high nibbles, `pshufb` both pair tables
//! with the nibble indices, interleave via `punpcklbw` → 16 centroid bytes
//! in natural dim order.  From there on the pipeline is identical to 4-bit.

use super::{CODEBOOK_SCALE, CODEBOOK_U8, QUERY_HIGH_COEF, Query2bitSimd};

/// `PAIR_TABLE_EVEN_U8[nibble]` = `CODEBOOK_U8[nibble & 0b11]`.
const PAIR_TABLE_EVEN_U8: [u8; 16] = {
    let mut tbl = [0_u8; 16];
    let mut k = 0;
    while k < 16 {
        tbl[k] = CODEBOOK_U8[k & 0b11];
        k += 1;
    }
    tbl
};

/// `PAIR_TABLE_ODD_U8[nibble]` = `CODEBOOK_U8[(nibble >> 2) & 0b11]`.
const PAIR_TABLE_ODD_U8: [u8; 16] = {
    let mut tbl = [0_u8; 16];
    let mut k = 0;
    while k < 16 {
        tbl[k] = CODEBOOK_U8[(k >> 2) & 0b11];
        k += 1;
    }
    tbl
};

/// Unpack 4 packed data bytes into a natural-order `__m128i` of 16 centroid
/// `u8` values (one per code).
#[inline]
#[target_feature(enable = "sse4.1,ssse3")]
unsafe fn unpack_16_codes_sse(bytes4: *const u8) -> core::arch::x86_64::__m128i {
    use core::arch::x86_64::*;
    unsafe {
        // Load 4 bytes into low 32 bits.
        let data = _mm_cvtsi32_si128(bytes4.cast::<i32>().read_unaligned());

        let low_mask = _mm_set1_epi8(0x0F);
        let lo_nibs = _mm_and_si128(data, low_mask);
        let hi_nibs = _mm_and_si128(_mm_srli_epi16(data, 4), low_mask);

        // Interleave low/high nibbles of the first 4 lanes:
        //   [lo(b0), hi(b0), lo(b1), hi(b1), lo(b2), hi(b2), lo(b3), hi(b3), …]
        let pair_indices = _mm_unpacklo_epi8(lo_nibs, hi_nibs);

        let t_even = _mm_loadu_si128(PAIR_TABLE_EVEN_U8.as_ptr().cast::<__m128i>());
        let t_odd = _mm_loadu_si128(PAIR_TABLE_ODD_U8.as_ptr().cast::<__m128i>());
        let c_even = _mm_shuffle_epi8(t_even, pair_indices);
        let c_odd = _mm_shuffle_epi8(t_odd, pair_indices);

        // Natural dim order: [c0, c1, c2, c3, c4, ..., c15].
        _mm_unpacklo_epi8(c_even, c_odd)
    }
}

#[target_feature(enable = "sse2")]
unsafe fn hsum_i32_sse(v: core::arch::x86_64::__m128i) -> i32 {
    use core::arch::x86_64::*;
    let v = _mm_add_epi32(v, _mm_shuffle_epi32(v, 0x4E));
    let v = _mm_add_epi32(v, _mm_shuffle_epi32(v, 0xB1));
    _mm_cvtsi128_si32(v)
}

impl Query2bitSimd {
    /// SSE4.1 + SSSE3 implementation of [`Query2bitSimd::dotprod_raw`].
    ///
    /// # Safety
    /// CPU must support `ssse3` and `sse4.1`.
    #[target_feature(enable = "sse4.1,ssse3")]
    pub unsafe fn dotprod_raw_sse(&self, vector: &[u8]) -> i64 {
        use core::arch::x86_64::*;

        assert_eq!(
            vector.len(),
            self.expected_vector_bytes(),
            "Query2bitSimd::dotprod_raw_sse: vector length mismatch ({} vs expected {})",
            vector.len(),
            self.expected_vector_bytes(),
        );

        unsafe {
            let mut acc_low = _mm_setzero_si128();
            let mut acc_high = _mm_setzero_si128();
            let ones = _mm_set1_epi16(1);

            for (chunk_idx, [low, high]) in self.query_data.iter().enumerate() {
                let c = unpack_16_codes_sse(vector.as_ptr().add(chunk_idx * 4));

                let q_low = _mm_loadu_si128(low.as_ptr().cast::<__m128i>());
                let q_high = _mm_loadu_si128(high.as_ptr().cast::<__m128i>());

                let prod_low = _mm_maddubs_epi16(c, q_low);
                let prod_high = _mm_maddubs_epi16(c, q_high);

                acc_low = _mm_add_epi32(acc_low, _mm_madd_epi16(prod_low, ones));
                acc_high = _mm_add_epi32(acc_high, _mm_madd_epi16(prod_high, ones));
            }

            let sum_low = i64::from(hsum_i32_sse(acc_low));
            let sum_high = i64::from(hsum_i32_sse(acc_high));
            sum_low + QUERY_HIGH_COEF * sum_high + self.dotprod_raw_tail(vector)
        }
    }

    /// AVX2 implementation.  Built on top of the SSE unpack (`_mm_shuffle_epi8`
    /// stays 128-bit-lane-scoped on AVX2, so doubling up to YMM for 8 bytes of
    /// data at once requires extra lane-management that costs more than the
    /// unroll saves).  We call the SSE unpack twice per iteration and pair the
    /// `maddubs` / `madd_epi16` paths on 256-bit vectors where they're cheap.
    ///
    /// # Safety
    /// CPU must support `avx2`, `ssse3` and `sse4.1`.
    #[target_feature(enable = "avx2,sse4.1,ssse3")]
    pub unsafe fn dotprod_raw_avx2(&self, vector: &[u8]) -> i64 {
        use core::arch::x86_64::*;

        assert_eq!(
            vector.len(),
            self.expected_vector_bytes(),
            "Query2bitSimd::dotprod_raw_avx2: vector length mismatch ({} vs expected {})",
            vector.len(),
            self.expected_vector_bytes(),
        );

        unsafe {
            let ones = _mm256_set1_epi16(1);
            let mut acc_low = _mm256_setzero_si256();
            let mut acc_high = _mm256_setzero_si256();

            let n_chunks = self.query_data.len();
            let n_pairs = n_chunks / 2;

            // 2× unroll: fold two SSE chunks into one YMM accumulation per iter.
            for p in 0..n_pairs {
                let [qa_lo, qa_hi] = &self.query_data[2 * p];
                let [qb_lo, qb_hi] = &self.query_data[2 * p + 1];
                let c_a = unpack_16_codes_sse(vector.as_ptr().add(4 * (2 * p)));
                let c_b = unpack_16_codes_sse(vector.as_ptr().add(4 * (2 * p + 1)));
                let c = _mm256_set_m128i(c_b, c_a);

                let q_lo_a = _mm_loadu_si128(qa_lo.as_ptr().cast::<__m128i>());
                let q_lo_b = _mm_loadu_si128(qb_lo.as_ptr().cast::<__m128i>());
                let q_hi_a = _mm_loadu_si128(qa_hi.as_ptr().cast::<__m128i>());
                let q_hi_b = _mm_loadu_si128(qb_hi.as_ptr().cast::<__m128i>());
                let q_low = _mm256_set_m128i(q_lo_b, q_lo_a);
                let q_high = _mm256_set_m128i(q_hi_b, q_hi_a);

                let prod_low = _mm256_maddubs_epi16(c, q_low);
                let prod_high = _mm256_maddubs_epi16(c, q_high);
                acc_low = _mm256_add_epi32(acc_low, _mm256_madd_epi16(prod_low, ones));
                acc_high = _mm256_add_epi32(acc_high, _mm256_madd_epi16(prod_high, ones));
            }

            // Fold YMM accumulators into XMM.
            let mut sum_low_sse = _mm_add_epi32(
                _mm256_castsi256_si128(acc_low),
                _mm256_extracti128_si256(acc_low, 1),
            );
            let mut sum_high_sse = _mm_add_epi32(
                _mm256_castsi256_si128(acc_high),
                _mm256_extracti128_si256(acc_high, 1),
            );

            // Tail: odd chunk count → one extra chunk via SSE.
            if n_chunks % 2 == 1 {
                let idx = 2 * n_pairs;
                let [q_lo_t, q_hi_t] = &self.query_data[idx];
                let c = unpack_16_codes_sse(vector.as_ptr().add(4 * idx));
                let q_low = _mm_loadu_si128(q_lo_t.as_ptr().cast::<__m128i>());
                let q_high = _mm_loadu_si128(q_hi_t.as_ptr().cast::<__m128i>());
                let prod_low = _mm_maddubs_epi16(c, q_low);
                let prod_high = _mm_maddubs_epi16(c, q_high);
                let ones = _mm_set1_epi16(1);
                sum_low_sse = _mm_add_epi32(sum_low_sse, _mm_madd_epi16(prod_low, ones));
                sum_high_sse = _mm_add_epi32(sum_high_sse, _mm_madd_epi16(prod_high, ones));
            }

            let sum_low = i64::from(hsum_i32_sse(sum_low_sse));
            let sum_high = i64::from(hsum_i32_sse(sum_high_sse));
            sum_low + QUERY_HIGH_COEF * sum_high + self.dotprod_raw_tail(vector)
        }
    }

    /// AVX-512 + VNNI implementation — uses `VPDPBUSD` on 512-bit ZMM for
    /// fused `u8 × i8 → i32` MAC.  Processes 4 chunks (64 codes) per iter.
    ///
    /// Tail handling falls back to SSE `maddubs + madd_epi16` rather than the
    /// narrower 128/256-bit VPDPBUSD variants, because those need `avx512vl`.
    ///
    /// # Safety
    /// CPU must support `avx512f`, `avx512bw`, `avx512vnni`, `ssse3`, `sse4.1`.
    #[target_feature(enable = "avx512f,avx512bw,avx512vnni,sse4.1,ssse3")]
    pub unsafe fn dotprod_raw_avx512_vnni(&self, vector: &[u8]) -> i64 {
        use core::arch::x86_64::*;

        assert_eq!(
            vector.len(),
            self.expected_vector_bytes(),
            "Query2bitSimd::dotprod_raw_avx512_vnni: vector length mismatch ({} vs expected {})",
            vector.len(),
            self.expected_vector_bytes(),
        );

        unsafe {
            let mut acc_low = _mm512_setzero_si512();
            let mut acc_high = _mm512_setzero_si512();

            let n_chunks = self.query_data.len();
            let n_quads = n_chunks / 4;

            for q in 0..n_quads {
                let base = 4 * q;

                // Unpack 4 × 16 centroids from 16 packed data bytes.
                let c_0 = unpack_16_codes_sse(vector.as_ptr().add(4 * base));
                let c_1 = unpack_16_codes_sse(vector.as_ptr().add(4 * (base + 1)));
                let c_2 = unpack_16_codes_sse(vector.as_ptr().add(4 * (base + 2)));
                let c_3 = unpack_16_codes_sse(vector.as_ptr().add(4 * (base + 3)));
                let c_ab = _mm256_set_m128i(c_1, c_0);
                let c_cd = _mm256_set_m128i(c_3, c_2);
                let c = _mm512_inserti64x4(_mm512_castsi256_si512(c_ab), c_cd, 1);

                // Load 4 × 16 query-low i8 and 4 × 16 query-high i8 into ZMMs.
                let q_lo_0 = _mm_loadu_si128(self.query_data[base][0].as_ptr().cast::<__m128i>());
                let q_lo_1 =
                    _mm_loadu_si128(self.query_data[base + 1][0].as_ptr().cast::<__m128i>());
                let q_lo_2 =
                    _mm_loadu_si128(self.query_data[base + 2][0].as_ptr().cast::<__m128i>());
                let q_lo_3 =
                    _mm_loadu_si128(self.query_data[base + 3][0].as_ptr().cast::<__m128i>());
                let q_lo_ab = _mm256_set_m128i(q_lo_1, q_lo_0);
                let q_lo_cd = _mm256_set_m128i(q_lo_3, q_lo_2);
                let q_low = _mm512_inserti64x4(_mm512_castsi256_si512(q_lo_ab), q_lo_cd, 1);

                let q_hi_0 = _mm_loadu_si128(self.query_data[base][1].as_ptr().cast::<__m128i>());
                let q_hi_1 =
                    _mm_loadu_si128(self.query_data[base + 1][1].as_ptr().cast::<__m128i>());
                let q_hi_2 =
                    _mm_loadu_si128(self.query_data[base + 2][1].as_ptr().cast::<__m128i>());
                let q_hi_3 =
                    _mm_loadu_si128(self.query_data[base + 3][1].as_ptr().cast::<__m128i>());
                let q_hi_ab = _mm256_set_m128i(q_hi_1, q_hi_0);
                let q_hi_cd = _mm256_set_m128i(q_hi_3, q_hi_2);
                let q_high = _mm512_inserti64x4(_mm512_castsi256_si512(q_hi_ab), q_hi_cd, 1);

                acc_low = _mm512_dpbusd_epi32(acc_low, c, q_low);
                acc_high = _mm512_dpbusd_epi32(acc_high, c, q_high);
            }

            let mut total_low = i64::from(_mm512_reduce_add_epi32(acc_low));
            let mut total_high = i64::from(_mm512_reduce_add_epi32(acc_high));

            // Tail (0..3 chunks) via plain SSE `maddubs + madd_epi16` —
            // avoids pulling in avx512vl for the narrow VPDPBUSD variants.
            let tail_start = n_quads * 4;
            let ones = _mm_set1_epi16(1);
            for p in tail_start..n_chunks {
                let [q_lo_chunk, q_hi_chunk] = &self.query_data[p];
                let c = unpack_16_codes_sse(vector.as_ptr().add(4 * p));
                let q_low = _mm_loadu_si128(q_lo_chunk.as_ptr().cast::<__m128i>());
                let q_high = _mm_loadu_si128(q_hi_chunk.as_ptr().cast::<__m128i>());
                let prod_low = _mm_maddubs_epi16(c, q_low);
                let prod_high = _mm_maddubs_epi16(c, q_high);
                total_low += i64::from(hsum_i32_sse(_mm_madd_epi16(prod_low, ones)));
                total_high += i64::from(hsum_i32_sse(_mm_madd_epi16(prod_high, ones)));
            }

            total_low + QUERY_HIGH_COEF * total_high + self.dotprod_raw_tail(vector)
        }
    }
}

// ------------------------------------------------------------------
// score_2bit_internal — both operands signed, same widen-to-i16
// pattern as query4bit's score_4bit_internal.  XOR 0x80 converts the
// CODEBOOK_U8 pair-table bytes back to signed i8 in-register.
// ------------------------------------------------------------------

/// SSE4.1 + SSSE3 implementation of [`super::score_2bit_internal`].
///
/// # Safety
/// CPU must support `ssse3` and `sse4.1`.
#[target_feature(enable = "sse4.1,ssse3")]
pub unsafe fn score_2bit_internal_sse(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::x86_64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_2bit_internal_sse: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    unsafe {
        let shift = _mm_set1_epi8(-128_i8);
        let mut acc = _mm_setzero_si128();
        let n_full = a.len() / 4;
        for i in 0..n_full {
            let c_a_u8 = unpack_16_codes_sse(a.as_ptr().add(i * 4));
            let c_b_u8 = unpack_16_codes_sse(b.as_ptr().add(i * 4));
            let c_a = _mm_xor_si128(c_a_u8, shift);
            let c_b = _mm_xor_si128(c_b_u8, shift);

            let c_a_lo = _mm_cvtepi8_epi16(c_a);
            let c_a_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_a, 8));
            let c_b_lo = _mm_cvtepi8_epi16(c_b);
            let c_b_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_b, 8));

            let prod_lo = _mm_madd_epi16(c_a_lo, c_b_lo);
            let prod_hi = _mm_madd_epi16(c_a_hi, c_b_hi);
            acc = _mm_add_epi32(acc, _mm_add_epi32(prod_lo, prod_hi));
        }
        let simd_bytes = n_full * 4;
        let acc_i64 = i64::from(hsum_i32_sse(acc))
            + super::score_2bit_internal_integer(&a[simd_bytes..], &b[simd_bytes..]);
        acc_i64 as f32 / (CODEBOOK_SCALE * CODEBOOK_SCALE)
    }
}

/// AVX2 implementation of [`super::score_2bit_internal`].
///
/// # Safety
/// CPU must support `avx2`, `ssse3`, `sse4.1`.
#[target_feature(enable = "avx2,sse4.1,ssse3")]
pub unsafe fn score_2bit_internal_avx2(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::x86_64::*;

    assert_eq!(a.len(), b.len());

    unsafe {
        let shift256 = _mm256_set1_epi8(-128_i8);
        let mut acc = _mm256_setzero_si256();

        let n_chunks = a.len() / 4;
        let n_pairs = n_chunks / 2;

        for p in 0..n_pairs {
            let off_a0 = 4 * (2 * p);
            let off_a1 = 4 * (2 * p + 1);
            let c_a = _mm256_set_m128i(
                unpack_16_codes_sse(a.as_ptr().add(off_a1)),
                unpack_16_codes_sse(a.as_ptr().add(off_a0)),
            );
            let c_b = _mm256_set_m128i(
                unpack_16_codes_sse(b.as_ptr().add(off_a1)),
                unpack_16_codes_sse(b.as_ptr().add(off_a0)),
            );
            let c_a = _mm256_xor_si256(c_a, shift256);
            let c_b = _mm256_xor_si256(c_b, shift256);

            let c_a_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(c_a));
            let c_a_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(c_a, 1));
            let c_b_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(c_b));
            let c_b_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(c_b, 1));

            let prod_lo = _mm256_madd_epi16(c_a_lo, c_b_lo);
            let prod_hi = _mm256_madd_epi16(c_a_hi, c_b_hi);
            acc = _mm256_add_epi32(acc, _mm256_add_epi32(prod_lo, prod_hi));
        }

        let mut acc_sse = _mm_add_epi32(
            _mm256_castsi256_si128(acc),
            _mm256_extracti128_si256(acc, 1),
        );
        if n_chunks % 2 == 1 {
            let off = 4 * (2 * n_pairs);
            let shift = _mm_set1_epi8(-128_i8);
            let c_a = _mm_xor_si128(unpack_16_codes_sse(a.as_ptr().add(off)), shift);
            let c_b = _mm_xor_si128(unpack_16_codes_sse(b.as_ptr().add(off)), shift);
            let c_a_lo = _mm_cvtepi8_epi16(c_a);
            let c_a_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_a, 8));
            let c_b_lo = _mm_cvtepi8_epi16(c_b);
            let c_b_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_b, 8));
            acc_sse = _mm_add_epi32(acc_sse, _mm_madd_epi16(c_a_lo, c_b_lo));
            acc_sse = _mm_add_epi32(acc_sse, _mm_madd_epi16(c_a_hi, c_b_hi));
        }
        let simd_bytes = n_chunks * 4;
        let acc_i64 = i64::from(hsum_i32_sse(acc_sse))
            + super::score_2bit_internal_integer(&a[simd_bytes..], &b[simd_bytes..]);
        acc_i64 as f32 / (CODEBOOK_SCALE * CODEBOOK_SCALE)
    }
}

// ------------------------------------------------------------------
// score_2bit_internal_weighted — TQ+ symmetric path with per-coord
// `D'²` weighting. Same structure as the 4-bit weighted kernel: i16
// weights → `madd_epi16` pair-sum → widen to i64 for accumulation.
//
// Per-coord product bound (2-bit codebook):
//   |c_a · c_b · w| ≤ 127·127·32 766 ≈ 5.28e8
//   madd pair sum: ≤ 2 · 5.28e8 ≈ 1.06e9 < i32::MAX ✓
// Same i32 → i64 widening pattern as the 4-bit path.
// ------------------------------------------------------------------

/// SSE4.1 + SSSE3 weighted variant of [`super::score_2bit_internal`].
///
/// # Safety
/// CPU must support `ssse3` and `sse4.1`.
#[target_feature(enable = "sse4.1,ssse3")]
pub unsafe fn score_2bit_internal_weighted_sse(a: &[u8], b: &[u8], weights: &[i16]) -> i64 {
    use core::arch::x86_64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_2bit_internal_weighted_sse: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );
    assert_eq!(
        weights.len(),
        4 * a.len(),
        "score_2bit_internal_weighted_sse: weights length {} != 4 · a.len() {}",
        weights.len(),
        4 * a.len(),
    );

    unsafe {
        let shift = _mm_set1_epi8(-128_i8);
        let mut acc = _mm_setzero_si128(); // 2 i64 lanes

        // 4 bytes from each source = 16 coords per iter.
        let n_full = a.len() / 4;
        for i in 0..n_full {
            let c_a_u8 = unpack_16_codes_sse(a.as_ptr().add(i * 4));
            let c_b_u8 = unpack_16_codes_sse(b.as_ptr().add(i * 4));
            let c_a = _mm_xor_si128(c_a_u8, shift);
            let c_b = _mm_xor_si128(c_b_u8, shift);

            let c_a_lo = _mm_cvtepi8_epi16(c_a);
            let c_a_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_a, 8));
            let c_b_lo = _mm_cvtepi8_epi16(c_b);
            let c_b_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_b, 8));

            let prod_lo = _mm_mullo_epi16(c_a_lo, c_b_lo);
            let prod_hi = _mm_mullo_epi16(c_a_hi, c_b_hi);

            let w_lo = _mm_loadu_si128(weights.as_ptr().add(16 * i).cast::<__m128i>());
            let w_hi = _mm_loadu_si128(weights.as_ptr().add(16 * i + 8).cast::<__m128i>());

            let pw_lo = _mm_madd_epi16(prod_lo, w_lo);
            let pw_hi = _mm_madd_epi16(prod_hi, w_hi);
            let pw = _mm_add_epi32(pw_lo, pw_hi);

            let pw_lo_i64 = _mm_cvtepi32_epi64(pw);
            let pw_hi_i64 = _mm_cvtepi32_epi64(_mm_srli_si128(pw, 8));
            acc = _mm_add_epi64(acc, pw_lo_i64);
            acc = _mm_add_epi64(acc, pw_hi_i64);
        }

        let mut tmp = [0i64; 2];
        _mm_storeu_si128(tmp.as_mut_ptr().cast::<__m128i>(), acc);
        let simd_sum = tmp[0] + tmp[1];

        let simd_bytes = n_full * 4;
        let tail = super::score_2bit_internal_weighted_scalar(
            &a[simd_bytes..],
            &b[simd_bytes..],
            &weights[4 * simd_bytes..],
        );
        simd_sum + tail
    }
}

/// AVX2 weighted variant of [`super::score_2bit_internal`]. 32 coords per
/// iteration (two 16-coord SSE chunks fed through one ymm madd).
///
/// # Safety
/// CPU must support `avx2`, `ssse3`, `sse4.1`.
#[target_feature(enable = "avx2,sse4.1,ssse3")]
pub unsafe fn score_2bit_internal_weighted_avx2(a: &[u8], b: &[u8], weights: &[i16]) -> i64 {
    use core::arch::x86_64::*;

    assert_eq!(a.len(), b.len());
    assert_eq!(weights.len(), 4 * a.len());

    unsafe {
        let shift256 = _mm256_set1_epi8(-128_i8);
        let mut acc = _mm256_setzero_si256(); // 4 i64 lanes

        let n_chunks = a.len() / 4;
        let n_pairs = n_chunks / 2;

        // Process pairs of 4-byte chunks (32 coords) per iteration.
        for p in 0..n_pairs {
            let off_0 = 4 * (2 * p);
            let off_1 = 4 * (2 * p + 1);
            let c_a = _mm256_set_m128i(
                unpack_16_codes_sse(a.as_ptr().add(off_1)),
                unpack_16_codes_sse(a.as_ptr().add(off_0)),
            );
            let c_b = _mm256_set_m128i(
                unpack_16_codes_sse(b.as_ptr().add(off_1)),
                unpack_16_codes_sse(b.as_ptr().add(off_0)),
            );
            let c_a = _mm256_xor_si256(c_a, shift256);
            let c_b = _mm256_xor_si256(c_b, shift256);

            let c_a_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(c_a));
            let c_a_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(c_a, 1));
            let c_b_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(c_b));
            let c_b_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(c_b, 1));

            let prod_lo = _mm256_mullo_epi16(c_a_lo, c_b_lo);
            let prod_hi = _mm256_mullo_epi16(c_a_hi, c_b_hi);

            // Weights are laid out in coord order; for AVX2's interleaved
            // pair-of-chunks layout we load `[chunk_0_weights, chunk_1_weights]`.
            // Coords for (off_0, off_1) span weight indices `[16·(2p)..16·(2p+2)]`.
            let w_lo = _mm256_loadu_si256(weights.as_ptr().add(16 * (2 * p)).cast::<__m256i>());
            let w_hi =
                _mm256_loadu_si256(weights.as_ptr().add(16 * (2 * p) + 16).cast::<__m256i>());

            let pw_lo = _mm256_madd_epi16(prod_lo, w_lo);
            let pw_hi = _mm256_madd_epi16(prod_hi, w_hi);
            let pw = _mm256_add_epi32(pw_lo, pw_hi);

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
        let mut simd_sum = tmp[0] + tmp[1];

        // Trailing single chunk if `n_chunks` is odd.
        if n_chunks % 2 == 1 {
            let off = 4 * (2 * n_pairs);
            let w_off = 16 * (2 * n_pairs);
            let shift = _mm_set1_epi8(-128_i8);
            let c_a = _mm_xor_si128(unpack_16_codes_sse(a.as_ptr().add(off)), shift);
            let c_b = _mm_xor_si128(unpack_16_codes_sse(b.as_ptr().add(off)), shift);
            let c_a_lo = _mm_cvtepi8_epi16(c_a);
            let c_a_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_a, 8));
            let c_b_lo = _mm_cvtepi8_epi16(c_b);
            let c_b_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_b, 8));
            let prod_lo = _mm_mullo_epi16(c_a_lo, c_b_lo);
            let prod_hi = _mm_mullo_epi16(c_a_hi, c_b_hi);
            let w_lo = _mm_loadu_si128(weights.as_ptr().add(w_off).cast::<__m128i>());
            let w_hi = _mm_loadu_si128(weights.as_ptr().add(w_off + 8).cast::<__m128i>());
            let pw = _mm_add_epi32(_mm_madd_epi16(prod_lo, w_lo), _mm_madd_epi16(prod_hi, w_hi));
            let pw_lo_i64 = _mm_cvtepi32_epi64(pw);
            let pw_hi_i64 = _mm_cvtepi32_epi64(_mm_srli_si128(pw, 8));
            let mut t = [0i64; 2];
            _mm_storeu_si128(
                t.as_mut_ptr().cast::<__m128i>(),
                _mm_add_epi64(pw_lo_i64, pw_hi_i64),
            );
            simd_sum += t[0] + t[1];
        }

        let simd_bytes = n_chunks * 4;
        let tail = super::score_2bit_internal_weighted_scalar(
            &a[simd_bytes..],
            &b[simd_bytes..],
            &weights[4 * simd_bytes..],
        );
        simd_sum + tail
    }
}

/// AVX-512 + VNNI implementation — uses `VPDPWSSD` for fused i16×i16 MAC.
///
/// # Safety
/// CPU must support `avx512f`, `avx512bw`, `avx512vnni`, `ssse3`, `sse4.1`.
#[target_feature(enable = "avx512f,avx512bw,avx512vnni,sse4.1,ssse3")]
pub unsafe fn score_2bit_internal_avx512_vnni(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::x86_64::*;

    assert_eq!(a.len(), b.len());

    unsafe {
        // Full-width 512-bit VNNI.  Process 4 chunks (64 codes) per iter.
        let shift512 = _mm512_set1_epi8(-128_i8);
        let mut acc = _mm512_setzero_si512();

        let n_chunks = a.len() / 4;
        let n_quads = n_chunks / 4;

        for q in 0..n_quads {
            let base = 4 * q;
            let c_a_0 = unpack_16_codes_sse(a.as_ptr().add(4 * base));
            let c_a_1 = unpack_16_codes_sse(a.as_ptr().add(4 * (base + 1)));
            let c_a_2 = unpack_16_codes_sse(a.as_ptr().add(4 * (base + 2)));
            let c_a_3 = unpack_16_codes_sse(a.as_ptr().add(4 * (base + 3)));
            let c_b_0 = unpack_16_codes_sse(b.as_ptr().add(4 * base));
            let c_b_1 = unpack_16_codes_sse(b.as_ptr().add(4 * (base + 1)));
            let c_b_2 = unpack_16_codes_sse(b.as_ptr().add(4 * (base + 2)));
            let c_b_3 = unpack_16_codes_sse(b.as_ptr().add(4 * (base + 3)));

            let c_a_ab = _mm256_set_m128i(c_a_1, c_a_0);
            let c_a_cd = _mm256_set_m128i(c_a_3, c_a_2);
            let c_a = _mm512_inserti64x4(_mm512_castsi256_si512(c_a_ab), c_a_cd, 1);
            let c_b_ab = _mm256_set_m128i(c_b_1, c_b_0);
            let c_b_cd = _mm256_set_m128i(c_b_3, c_b_2);
            let c_b = _mm512_inserti64x4(_mm512_castsi256_si512(c_b_ab), c_b_cd, 1);

            // Unwind the `+128` shift to recover signed i8.
            let c_a = _mm512_xor_si512(c_a, shift512);
            let c_b = _mm512_xor_si512(c_b, shift512);

            // Widen i8 → i16 in both halves of each ZMM.
            let c_a_lo = _mm512_cvtepi8_epi16(_mm512_castsi512_si256(c_a));
            let c_a_hi = _mm512_cvtepi8_epi16(_mm512_extracti64x4_epi64(c_a, 1));
            let c_b_lo = _mm512_cvtepi8_epi16(_mm512_castsi512_si256(c_b));
            let c_b_hi = _mm512_cvtepi8_epi16(_mm512_extracti64x4_epi64(c_b, 1));

            acc = _mm512_dpwssd_epi32(acc, c_a_lo, c_b_lo);
            acc = _mm512_dpwssd_epi32(acc, c_a_hi, c_b_hi);
        }

        let mut acc_total = i64::from(_mm512_reduce_add_epi32(acc));

        // Tail (0..3 chunks) via SSE `madd_epi16` — no avx512vl needed.
        let tail_start = n_quads * 4;
        let shift_128 = _mm_set1_epi8(-128_i8);
        for p in tail_start..n_chunks {
            let c_a = _mm_xor_si128(unpack_16_codes_sse(a.as_ptr().add(4 * p)), shift_128);
            let c_b = _mm_xor_si128(unpack_16_codes_sse(b.as_ptr().add(4 * p)), shift_128);
            let c_a_lo = _mm_cvtepi8_epi16(c_a);
            let c_a_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_a, 8));
            let c_b_lo = _mm_cvtepi8_epi16(c_b);
            let c_b_hi = _mm_cvtepi8_epi16(_mm_srli_si128(c_b, 8));
            let prod = _mm_add_epi32(
                _mm_madd_epi16(c_a_lo, c_b_lo),
                _mm_madd_epi16(c_a_hi, c_b_hi),
            );
            acc_total += i64::from(hsum_i32_sse(prod));
        }

        // Scalar tail for any bytes past the last SIMD chunk (a.len() % 4).
        let simd_bytes = n_chunks * 4;
        acc_total += super::score_2bit_internal_integer(&a[simd_bytes..], &b[simd_bytes..]);
        acc_total as f32 / (CODEBOOK_SCALE * CODEBOOK_SCALE)
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::super::shared::pack_codes;
    use super::super::shared::{PARITY_DIMS, random_inputs};
    use super::super::{
        Query2bitSimd, score_2bit_internal_scalar, score_2bit_internal_weighted_scalar,
    };
    use super::*;

    /// Build deterministic non-negative i16 weights of length `4 · vec_bytes`
    /// for parity tests of the 2-bit weighted kernel.
    fn random_weights(rng: &mut StdRng, vec_bytes: usize) -> Vec<i16> {
        use rand::RngExt;
        (0..4 * vec_bytes)
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
            let got = unsafe { simd_query.dotprod_raw_sse(&vector) };
            assert_eq!(scalar, got, "sse mismatch at dim {dim}");
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
            let got = unsafe { simd_query.dotprod_raw_avx2(&vector) };
            assert_eq!(scalar, got, "avx2 mismatch at dim {dim}");
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
            let got = unsafe { simd_query.dotprod_raw_avx512_vnni(&vector) };
            assert_eq!(scalar, got, "avx512 mismatch at dim {dim}");
        }
    }

    #[test]
    fn test_saturation_safety_64k() {
        let dim = 65_536;
        let query = vec![1.0_f32; dim];
        let indices: Vec<u8> = vec![3; dim]; // max-magnitude centroid
        let vector = pack_codes(&indices, 2);

        let q = Query2bitSimd::new(&query);
        let scalar = q.dotprod_raw(&vector);

        unsafe {
            if std::is_x86_feature_detected!("ssse3") && std::is_x86_feature_detected!("sse4.1") {
                assert_eq!(scalar, q.dotprod_raw_sse(&vector));
            }
            if std::is_x86_feature_detected!("avx2") {
                assert_eq!(scalar, q.dotprod_raw_avx2(&vector));
            }
            if std::is_x86_feature_detected!("avx512f")
                && std::is_x86_feature_detected!("avx512bw")
                && std::is_x86_feature_detected!("avx512vnni")
            {
                assert_eq!(scalar, q.dotprod_raw_avx512_vnni(&vector));
            }
        }
    }

    #[test]
    fn test_score_sse_matches_scalar() {
        if !std::is_x86_feature_detected!("ssse3") || !std::is_x86_feature_detected!("sse4.1") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (_, vec_a) = random_inputs(&mut rng, dim);
            let (_, vec_b) = random_inputs(&mut rng, dim);
            let scalar = score_2bit_internal_scalar(&vec_a, &vec_b);
            let sse = unsafe { score_2bit_internal_sse(&vec_a, &vec_b) };
            assert_eq!(
                scalar.to_bits(),
                sse.to_bits(),
                "score sse mismatch at dim {dim}",
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
            let scalar = score_2bit_internal_scalar(&vec_a, &vec_b);
            let got = unsafe { score_2bit_internal_avx2(&vec_a, &vec_b) };
            assert_eq!(
                scalar.to_bits(),
                got.to_bits(),
                "score avx2 mismatch at dim {dim}",
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
            let scalar = score_2bit_internal_scalar(&vec_a, &vec_b);
            let got = unsafe { score_2bit_internal_avx512_vnni(&vec_a, &vec_b) };
            assert_eq!(
                scalar.to_bits(),
                got.to_bits(),
                "score avx512 mismatch at dim {dim}",
            );
        }
    }

    /// Parity for the 2-bit weighted kernel: every x86 backend must match
    /// the scalar reference bit-exactly across our matryoshka corner-case dims.
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
            let scalar = score_2bit_internal_weighted_scalar(&vec_a, &vec_b, &weights);
            let sse = unsafe { score_2bit_internal_weighted_sse(&vec_a, &vec_b, &weights) };
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
            let scalar = score_2bit_internal_weighted_scalar(&vec_a, &vec_b, &weights);
            let avx2 = unsafe { score_2bit_internal_weighted_avx2(&vec_a, &vec_b, &weights) };
            assert_eq!(
                scalar, avx2,
                "weighted: scalar {scalar} != avx2 {avx2} at dim {dim}"
            );
        }
    }

    /// Saturation-safety for the 2-bit weighted kernel at 64K dims: every
    /// x86 backend must match the i64 scalar reference under worst-case
    /// inputs (max-magnitude codebook + max-magnitude i16 weight).
    #[test]
    fn test_score_weighted_saturation_safety_64k() {
        let dim = 65_536;
        let indices: Vec<u8> = vec![3; dim];
        let vec_a = pack_codes(&indices, 2);
        let vec_b = pack_codes(&indices, 2);
        let max_weight: i16 = i16::MAX;
        let weights: Vec<i16> = vec![max_weight; dim];

        let scalar = score_2bit_internal_weighted_scalar(&vec_a, &vec_b, &weights);

        unsafe {
            if std::is_x86_feature_detected!("ssse3") && std::is_x86_feature_detected!("sse4.1") {
                let sse = score_2bit_internal_weighted_sse(&vec_a, &vec_b, &weights);
                assert_eq!(scalar, sse, "weighted score sse disagrees at dim={dim}");
            }
            if std::is_x86_feature_detected!("avx2") {
                let avx2 = score_2bit_internal_weighted_avx2(&vec_a, &vec_b, &weights);
                assert_eq!(scalar, avx2, "weighted score avx2 disagrees at dim={dim}");
            }
        }
    }

    #[test]
    fn test_score_saturation_safety_64k() {
        let dim = 65_536;
        let indices: Vec<u8> = vec![3; dim];
        let vec_a = pack_codes(&indices, 2);
        let vec_b = pack_codes(&indices, 2);
        let scalar = score_2bit_internal_scalar(&vec_a, &vec_b);

        unsafe {
            if std::is_x86_feature_detected!("ssse3") && std::is_x86_feature_detected!("sse4.1") {
                assert_eq!(
                    scalar.to_bits(),
                    score_2bit_internal_sse(&vec_a, &vec_b).to_bits()
                );
            }
            if std::is_x86_feature_detected!("avx2") {
                assert_eq!(
                    scalar.to_bits(),
                    score_2bit_internal_avx2(&vec_a, &vec_b).to_bits()
                );
            }
            if std::is_x86_feature_detected!("avx512f")
                && std::is_x86_feature_detected!("avx512bw")
                && std::is_x86_feature_detected!("avx512vnni")
            {
                assert_eq!(
                    scalar.to_bits(),
                    score_2bit_internal_avx512_vnni(&vec_a, &vec_b).to_bits(),
                );
            }
        }
    }
}
