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

use super::{CODEBOOK_U8, QUERY_HIGH_COEF, Query4bitSimd};

impl Query4bitSimd {
    /// x86_64 SSE4.1 + SSSE3 implementation of [`Query4bitSimd::dotprod_raw`].
    ///
    /// # Safety
    /// CPU must support `ssse3` and `sse4.1`.
    #[target_feature(enable = "sse4.1,ssse3")]
    pub unsafe fn dotprod_raw_sse(&self, vector: &[u8]) -> i64 {
        use core::arch::x86_64::*;

        unsafe {
            let codebook = _mm_loadu_si128(CODEBOOK_U8.as_ptr().cast::<__m128i>());
            let ones = _mm_set1_epi16(1);
            let nibble_mask = _mm_set1_epi8(0x0F);
            let mut acc_low = _mm_setzero_si128();
            let mut acc_high = _mm_setzero_si128();

            for ([low, high], v_chunk) in self.query_data.iter().zip(vector.chunks_exact(8)) {
                let v_packed = _mm_loadl_epi64(v_chunk.as_ptr().cast::<__m128i>());
                let v_lo = _mm_and_si128(v_packed, nibble_mask);
                let v_hi = _mm_and_si128(_mm_srli_epi16(v_packed, 4), nibble_mask);
                let v = _mm_unpacklo_epi8(v_lo, v_hi);
                let c_u = _mm_shuffle_epi8(codebook, v);

                let q_low = _mm_loadu_si128(low.as_ptr().cast::<__m128i>());
                let q_high = _mm_loadu_si128(high.as_ptr().cast::<__m128i>());

                // maddubs(u8 c_u, i8 q_*): pair sum ≤ 2·255·64 = 32 640 < 32 767.
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

        unsafe {
            let codebook_128 = _mm_loadu_si128(CODEBOOK_U8.as_ptr().cast::<__m128i>());
            let codebook = _mm256_broadcastsi128_si256(codebook_128);
            let ones = _mm256_set1_epi16(1);
            let nibble_mask = _mm_set1_epi8(0x0F);
            // Lanes 0..3 accumulate low-half contribution; lanes 4..7 accumulate high-half.
            let mut acc = _mm256_setzero_si256();

            for (chunk, v_chunk) in self.query_data.iter().zip(vector.chunks_exact(8)) {
                // `chunk` is `[low, high]`, 32 contiguous i8 bytes — fill one YMM.
                let low_high = _mm256_loadu_si256(chunk.as_ptr().cast::<__m256i>());

                let v_packed = _mm_loadl_epi64(v_chunk.as_ptr().cast::<__m128i>());
                let v_lo = _mm_and_si128(v_packed, nibble_mask);
                let v_hi = _mm_and_si128(_mm_srli_epi16(v_packed, 4), nibble_mask);
                let v128 = _mm_unpacklo_epi8(v_lo, v_hi);
                let v = _mm256_broadcastsi128_si256(v128);
                let c = _mm256_shuffle_epi8(codebook, v);

                // maddubs(u8 c, i8 low_high) — same c in both lanes, so lane 0 = c·low,
                // lane 1 = c·high.  Saturation budget identical to the SSE path.
                let prods = _mm256_maddubs_epi16(c, low_high);
                acc = _mm256_add_epi32(acc, _mm256_madd_epi16(prods, ones));
            }

            let acc_low = _mm256_castsi256_si128(acc);
            let acc_high = _mm256_extracti128_si256(acc, 1);
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
    #[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
    pub unsafe fn dotprod_raw_avx512_vnni(&self, vector: &[u8]) -> i64 {
        use core::arch::x86_64::*;

        unsafe {
            let codebook_128 = _mm_loadu_si128(CODEBOOK_U8.as_ptr().cast::<__m128i>());
            let codebook_512 = _mm512_broadcast_i32x4(codebook_128);
            let nibble_mask_128 = _mm_set1_epi8(0x0F);
            let mut acc = _mm512_setzero_si512();

            // `query_data.len()` is guaranteed even by the dim-%32 precondition,
            // so every pair is consumed and there is no tail to handle.
            let chunks = self.query_data.as_slice();
            let n_pairs = chunks.len() / 2;

            for i in 0..n_pairs {
                // Two consecutive [[i8;16];2] entries → 64 bytes:
                // [low_a, high_a, low_b, high_b].
                let pair_ptr = chunks.as_ptr().add(2 * i).cast::<__m512i>();
                let low_high_pair = _mm512_loadu_si512(pair_ptr);

                // 16 packed bytes → 32 nibbles (2 chunks × 16 indices each).
                let v_packed_16 = _mm_loadu_si128(vector.as_ptr().add(16 * i).cast::<__m128i>());
                let v_lo = _mm_and_si128(v_packed_16, nibble_mask_128);
                let v_hi = _mm_and_si128(_mm_srli_epi16(v_packed_16, 4), nibble_mask_128);
                let v_chunk_a = _mm_unpacklo_epi8(v_lo, v_hi); // 16 indices for chunk 2i
                let v_chunk_b = _mm_unpackhi_epi8(v_lo, v_hi); // 16 indices for chunk 2i+1

                // `[v_a, v_a, v_b, v_b]` lines up with the codebook shuffle for
                // each 128-bit lane of `low_high_pair`.
                let v_dup_a = _mm256_broadcastsi128_si256(v_chunk_a);
                let v_dup_b = _mm256_broadcastsi128_si256(v_chunk_b);
                let v_512 = _mm512_inserti64x4(_mm512_castsi256_si512(v_dup_a), v_dup_b, 1);

                let c_512 = _mm512_shuffle_epi8(codebook_512, v_512);
                // dpbusd(acc, u8, i8): `c_512` is u8 (≤ 255), `low_high_pair` is i8.
                acc = _mm512_dpbusd_epi32(acc, c_512, low_high_pair);
            }

            // Reduce 16 i32 lanes: [0..3]=chunk-a low, [4..7]=chunk-a high,
            //                      [8..11]=chunk-b low, [12..15]=chunk-b high.
            let acc_256_lo = _mm512_castsi512_si256(acc);
            let acc_256_hi = _mm512_extracti64x4_epi64(acc, 1);
            let lane_a_low = _mm256_castsi256_si128(acc_256_lo);
            let lane_a_high = _mm256_extracti128_si256(acc_256_lo, 1);
            let lane_b_low = _mm256_castsi256_si128(acc_256_hi);
            let lane_b_high = _mm256_extracti128_si256(acc_256_hi, 1);

            let sum_low = i64::from(hsum_i32_sse(_mm_add_epi32(lane_a_low, lane_b_low)));
            let sum_high = i64::from(hsum_i32_sse(_mm_add_epi32(lane_a_high, lane_b_high)));

            sum_low + QUERY_HIGH_COEF * sum_high
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

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::Query4bitSimd;
    use super::super::shared::{PARITY_DIMS, pack_nibbles, random_inputs};

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

    /// Single saturation-safety check at an extreme dim (16K) with the
    /// worst-case combination: query maxed out and every lane of the vector
    /// pointing at the extreme-magnitude codebook slot.  Scalar is the
    /// reference (i64 throughout, saturation-free by construction); each
    /// SIMD path must match it exactly.  A mismatch proves that some
    /// intermediate integer saturated or overflowed.
    #[test]
    fn test_saturation_safety_16k() {
        let dim = 16_384;
        let query = vec![1.0_f32; dim];
        let indices: Vec<u8> = vec![15; dim]; // CODEBOOK_U8[15] = 255 (max magnitude)
        let vector = pack_nibbles(&indices);

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
}
