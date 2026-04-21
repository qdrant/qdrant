/// 4-bit codebook (Lloyd-Max optimal for N(0, 1), see `turboquant::lloyd_max`)
/// pre-quantized to **u8** at compile time with scale `63 / max|c|` and then
/// shifted by `+64` into `[0, 127]` (7-bit unsigned).  Storing unsigned lets us
/// feed `c_u` directly as the u8 operand of `_mm_maddubs_epi16` / `VPDPBUSD`,
/// with the query as the signed i8 operand.  The sign information that the
/// earlier i8 layout carried directly is recovered via `sum_q_int`, a
/// **per-query** constant (not per-vector), which leaves `dotprod` as just
/// `SIMD_dot_raw − bias_correction` with no extra pass over the vector.
///
/// Consistency with `CENTROIDS_4BIT` is guarded by `test_codebook_matches_lloyd_max`.
const CODEBOOK_U8: [u8; 16] = [
    1, 16, 27, 35, 42, 49, 55, 61, 67, 73, 79, 86, 93, 101, 112, 127,
];

/// Additive offset that recovers the signed codebook: `c_signed[k] = c_u[k] − 64`.
/// Range `c_signed ∈ [−63, 63]` (7-bit signed), so `Σ q_int · c_signed` has the
/// same dynamic range analysis as the old i8 path on balanced inputs.
const CODEBOOK_OFFSET: i64 = 64;

/// `max|c|` over `CENTROIDS_4BIT` — the extreme centroid.  Combined with the
/// per-query `q_scale` it fixes the float→integer reconstruction factor.
const CODEBOOK_ABS_MAX: f32 = 2.733;

/// Codebook scale: `c_scale = 63 / max|c|` so `|c_signed| ≤ 63 < 64 = OFFSET`.
/// Kept as a separate constant to make the reconstruction formula obvious.
const CODEBOOK_SCALE: f32 = 63.0 / CODEBOOK_ABS_MAX;

pub struct Query4bitSimd {
    /// Query encoded as a balanced signed-i8 split
    /// `q_signed = 256 · high + low`, each chunk stored as `[low, high]`.
    /// Both halves are signed — `low, high ∈ [−128, 127]` — so the metric
    /// routines don't need to track per-half sign gymnastics.
    query_data: Vec<[[i8; 16]; 2]>,
    /// `1 / (q_scale · c_scale)` — prefactor from integer to float dot product.
    postprocess_scale: f32,
    /// `CODEBOOK_OFFSET · Σ q_signed[j]` — subtract from `dot_raw` to recover
    /// the true signed integer dot product.  Computed once in `new()`; doesn't
    /// depend on the PQ vector, so `dotprod` does zero per-vector scalar work.
    bias_correction: i64,
}

impl Query4bitSimd {
    /// Query dim must be a multiple of 32.  The NEON SDOT and AVX-512 VNNI paths
    /// both consume 2 query_data chunks (= 32 query elements) per iteration; with
    /// `dim % 32 == 0` we get `query_data.len()` even and can drop all tail
    /// handling.  Every PQ dimension we ship (128, 256, 384, 512, 768, 1024,
    /// 1536, 2048, 4096) already satisfies this.
    pub fn new(data: &[f32]) -> Self {
        assert!(
            data.len().is_multiple_of(32),
            "Query4bitSimd requires query dim to be a multiple of 32 (got {})",
            data.len(),
        );

        // Scale query to ~15.9-bit signed integer `q_signed ∈ [−32 639, 32 639]`.
        // Balanced decomposition `q_signed = 256 · high + low` then fits both
        // halves in i8 (balanced-signed, |low| ≤ 128, |high| ≤ 127).
        //
        // Saturation budget on `maddubs(c_u u8, low_i8)` per pair:
        //   c_u ≤ 127, low ∈ [−128, 127] → |pair| ≤ 2·127·128 = 32 512 < 32 767 ✓
        let q_abs_max = data
            .iter()
            .copied()
            .map(f32::abs)
            .fold(0.0_f32, f32::max)
            .max(f32::EPSILON);
        let q_scale = 32639.0 / q_abs_max;

        let num_chunks = data.len() / 16;
        let mut query_data: Vec<[[i8; 16]; 2]> = Vec::with_capacity(num_chunks);
        let mut sum_q_signed: i64 = 0;
        for chunk_idx in 0..num_chunks {
            let mut low = [0_i8; 16];
            let mut high = [0_i8; 16];
            for i in 0..16 {
                let q_signed = (data[chunk_idx * 16 + i] * q_scale)
                    .round()
                    .clamp(-32639.0, 32639.0) as i32;
                // Balanced split: ensure `low ∈ [−128, 127]` (not the default
                // Rust `%` behaviour, which mirrors the sign of the dividend).
                let l_mod = q_signed.rem_euclid(256);
                let l = if l_mod >= 128 { l_mod - 256 } else { l_mod } as i8;
                let h = ((q_signed - i32::from(l)) / 256) as i8;
                low[i] = l;
                high[i] = h;
                sum_q_signed += i64::from(q_signed);
            }
            query_data.push([low, high]);
        }

        // `dot_raw` = Σ q_signed · c_u = Σ q_signed · (c_signed + OFFSET)
        //           = Σ q_signed · c_signed + OFFSET · sum_q_signed
        // True dot ≈ (Σ q_signed · c_signed) / (q_scale · c_scale)
        //         = (dot_raw − OFFSET · sum_q_signed) / (q_scale · c_scale).
        let postprocess_scale = 1.0 / (q_scale * CODEBOOK_SCALE);
        let bias_correction = CODEBOOK_OFFSET * sum_q_signed;

        Self {
            query_data,
            postprocess_scale,
            bias_correction,
        }
    }

    pub fn dotprod(&self, vector: &[u8]) -> f32 {
        // No per-vector correction loop: `bias_correction` was baked in at `new()`.
        let dot_raw = self.dotprod_raw_best(vector);
        self.postprocess_scale * (dot_raw - self.bias_correction) as f32
    }

    #[inline]
    fn dotprod_raw_best(&self, vector: &[u8]) -> i64 {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx512f")
                && std::is_x86_feature_detected!("avx512bw")
                && std::is_x86_feature_detected!("avx512vnni")
            {
                return unsafe { self.dotprod_raw_avx512_vnni(vector) };
            }
            if std::is_x86_feature_detected!("avx2") {
                return unsafe { self.dotprod_raw_avx2(vector) };
            }
            if std::is_x86_feature_detected!("sse4.1") && std::is_x86_feature_detected!("ssse3") {
                return unsafe { self.dotprod_raw_sse(vector) };
            }
        }
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            if std::arch::is_aarch64_feature_detected!("dotprod") {
                return unsafe { self.dotprod_raw_neon_sdot(vector) };
            }
            return unsafe { self.dotprod_raw_neon(vector) };
        }
        #[allow(unreachable_code)]
        self.dotprod_raw(vector)
    }

    /// Compute `Σ q_int[j] · c_u[v[j]]` over 16 lanes per chunk.  The
    /// `CODEBOOK_OFFSET · sum_q_int` bias is subtracted by the caller
    /// (`Query4bitSimd::dotprod`) from a precomputed per-query constant.
    ///
    /// `vector` is PQ-encoded with two 4-bit codebook indices packed per byte:
    /// the low nibble is the index for the even lane (j = 2k), the high nibble
    /// for the odd lane (j = 2k + 1).
    pub fn dotprod_raw(&self, vector: &[u8]) -> i64 {
        let mut acc_low: i64 = 0;
        let mut acc_high: i64 = 0;
        for ([low, high], v) in self.query_data.iter().zip(vector.chunks(8)) {
            for i in 0..16 {
                let byte = v[i / 2];
                let idx = if i & 1 == 0 { byte & 0x0F } else { byte >> 4 };
                let c_u = i64::from(CODEBOOK_U8[idx as usize]);
                acc_low += i64::from(low[i]) * c_u;
                acc_high += i64::from(high[i]) * c_u;
            }
        }
        acc_low + 256 * acc_high
    }

    /// ARM NEON implementation of [`Query4bitSimd::dotprod_raw`].
    ///
    /// # Safety
    /// CPU must support the `neon` feature (always true on aarch64).
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    #[target_feature(enable = "neon")]
    pub unsafe fn dotprod_raw_neon(&self, vector: &[u8]) -> i64 {
        use core::arch::aarch64::*;

        unsafe {
            // `CODEBOOK_U8` values are ≤ 127, so reinterpreting as i8 preserves
            // them for `vqtbl1q_s8` + `vmull_s8` on the same register.
            let codebook = vld1q_s8(CODEBOOK_U8.as_ptr().cast::<i8>());
            let mut acc_low = vdupq_n_s32(0);
            let mut acc_high = vdupq_n_s32(0);

            let nibble_mask = vdup_n_u8(0x0F);
            for ([low, high], v_chunk) in self.query_data.iter().zip(vector.chunks_exact(8)) {
                // Unpack 8 packed bytes into 16 nibble indices:
                //   [byte0.lo, byte0.hi, byte1.lo, byte1.hi, ..., byte7.lo, byte7.hi].
                let v_packed = vld1_u8(v_chunk.as_ptr());
                let v_lo = vand_u8(v_packed, nibble_mask);
                let v_hi = vshr_n_u8(v_packed, 4);
                let v = vcombine_u8(vzip1_u8(v_lo, v_hi), vzip2_u8(v_lo, v_hi));

                // Gather 16 codebook values as i8 (values 0..=127 → non-negative).
                let c = vqtbl1q_s8(codebook, v);

                // Signed i8 query halves — one 16-byte load per half.
                let q_low = vld1q_s8(low.as_ptr());
                let q_high = vld1q_s8(high.as_ptr());

                // i8 × i8 → i16; per product magnitude ≤ 127 × 128 = 16 256 fits in i16.
                let prod_low_lo = vmull_s8(vget_low_s8(q_low), vget_low_s8(c));
                let prod_low_hi = vmull_high_s8(q_low, c);
                let prod_high_lo = vmull_s8(vget_low_s8(q_high), vget_low_s8(c));
                let prod_high_hi = vmull_high_s8(q_high, c);

                // Pairwise-add the i16×8 products into the i32×4 accumulators.
                acc_low = vpadalq_s16(acc_low, prod_low_lo);
                acc_low = vpadalq_s16(acc_low, prod_low_hi);
                acc_high = vpadalq_s16(acc_high, prod_high_lo);
                acc_high = vpadalq_s16(acc_high, prod_high_hi);
            }

            // dot_raw = Σ (low + 256·high) · c_u = acc_low + 256 · acc_high.
            i64::from(vaddvq_s32(acc_low)) + 256 * i64::from(vaddvq_s32(acc_high))
        }
    }

    /// ARMv8.2-A Dot Product variant. Uses `SDOT` to sum four i8×i8 products per
    /// i32 lane per instruction, emitted via inline asm because `vdotq_s32` is
    /// still unstable (rust-lang/rust#117224).
    ///
    /// 2× unrolled: two chunks per iteration with four independent `i32x4`
    /// accumulators (two for low, two for high) break the single dependency chain
    /// of a naive implementation.  On Apple M-series SDOT has ~3-cycle latency at
    /// 4/cycle throughput, and four parallel chains lift throughput ~7–20% over a
    /// 1× version (larger dims benefit more — latency dominates there).
    ///
    /// # Safety
    /// CPU must support `neon` and `dotprod`.
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    #[target_feature(enable = "neon,dotprod")]
    pub unsafe fn dotprod_raw_neon_sdot(&self, vector: &[u8]) -> i64 {
        use core::arch::aarch64::*;

        unsafe {
            // `CODEBOOK_U8` values ≤ 127 → i8 reinterpret preserves them.
            let codebook = vld1q_s8(CODEBOOK_U8.as_ptr().cast::<i8>());
            let mut acc_low_0 = vdupq_n_s32(0);
            let mut acc_low_1 = vdupq_n_s32(0);
            let mut acc_high_0 = vdupq_n_s32(0);
            let mut acc_high_1 = vdupq_n_s32(0);
            let nibble_mask_q = vdupq_n_u8(0x0F);

            // `query_data.len()` is guaranteed even by the dim-%32 precondition,
            // so every pair is consumed and there is no tail to handle.
            let chunks = self.query_data.as_slice();
            let n_pairs = chunks.len() / 2;

            for i in 0..n_pairs {
                let [low_0, high_0] = chunks[2 * i];
                let [low_1, high_1] = chunks[2 * i + 1];

                // One 16-byte load covers both chunks' 8 packed bytes each.
                let v_packed = vld1q_u8(vector.as_ptr().add(16 * i));
                let v_lo = vandq_u8(v_packed, nibble_mask_q);
                let v_hi = vshrq_n_u8(v_packed, 4);
                let v_0 = vzip1q_u8(v_lo, v_hi);
                let v_1 = vzip2q_u8(v_lo, v_hi);
                let c_0 = vqtbl1q_s8(codebook, v_0);
                let c_1 = vqtbl1q_s8(codebook, v_1);

                let q_low_0 = vld1q_s8(low_0.as_ptr());
                let q_high_0 = vld1q_s8(high_0.as_ptr());
                let q_low_1 = vld1q_s8(low_1.as_ptr());
                let q_high_1 = vld1q_s8(high_1.as_ptr());

                // Four independent SDOT dependency chains — 2× unroll for ILP.
                // i8 × i8: codebook values ∈ [0, 127] are non-negative in i8,
                // query halves ∈ [-128, 127] are genuinely signed.
                core::arch::asm!(
                    "sdot {acc:v}.4s, {a:v}.16b, {b:v}.16b",
                    acc = inout(vreg) acc_low_0,
                    a = in(vreg) q_low_0,
                    b = in(vreg) c_0,
                    options(pure, nomem, nostack, preserves_flags),
                );
                core::arch::asm!(
                    "sdot {acc:v}.4s, {a:v}.16b, {b:v}.16b",
                    acc = inout(vreg) acc_low_1,
                    a = in(vreg) q_low_1,
                    b = in(vreg) c_1,
                    options(pure, nomem, nostack, preserves_flags),
                );
                core::arch::asm!(
                    "sdot {acc:v}.4s, {a:v}.16b, {b:v}.16b",
                    acc = inout(vreg) acc_high_0,
                    a = in(vreg) q_high_0,
                    b = in(vreg) c_0,
                    options(pure, nomem, nostack, preserves_flags),
                );
                core::arch::asm!(
                    "sdot {acc:v}.4s, {a:v}.16b, {b:v}.16b",
                    acc = inout(vreg) acc_high_1,
                    a = in(vreg) q_high_1,
                    b = in(vreg) c_1,
                    options(pure, nomem, nostack, preserves_flags),
                );
            }

            let acc_low = vaddq_s32(acc_low_0, acc_low_1);
            let acc_high = vaddq_s32(acc_high_0, acc_high_1);
            i64::from(vaddvq_s32(acc_low)) + 256 * i64::from(vaddvq_s32(acc_high))
        }
    }

    /// x86_64 SSE4.1 + SSSE3 implementation of [`Query4bitSimd::dotprod_raw`].
    ///
    /// # Safety
    /// CPU must support `ssse3` and `sse4.1` (check via `is_x86_feature_detected!`).
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse4.1,ssse3")]
    pub unsafe fn dotprod_raw_sse(&self, vector: &[u8]) -> i64 {
        use core::arch::x86_64::*;

        unsafe {
            // 16 u8 codebook entries live in one xmm register, gathered by PSHUFB.
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

                // maddubs(u8 c_u, i8 q_*): pair sum ≤ 2·127·128 = 32 512 < 32 767.
                let prod_low = _mm_maddubs_epi16(c_u, q_low);
                let prod_high = _mm_maddubs_epi16(c_u, q_high);
                acc_low = _mm_add_epi32(acc_low, _mm_madd_epi16(prod_low, ones));
                acc_high = _mm_add_epi32(acc_high, _mm_madd_epi16(prod_high, ones));
            }

            i64::from(hsum_i32_sse(acc_low)) + 256 * i64::from(hsum_i32_sse(acc_high))
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
    #[cfg(target_arch = "x86_64")]
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
            i64::from(hsum_i32_sse(acc_low)) + 256 * i64::from(hsum_i32_sse(acc_high))
        }
    }

    /// AVX-512 VNNI (Ice Lake Xeon+, Zen 4+): 2 chunks per iteration.  Two
    /// consecutive `[low, high]` entries = 64 bytes = one ZMM load.  `VPDPBUSD`
    /// fuses the 4-wide u8×i8 dot with i32 accumulation; the ZMM layout puts
    /// [low_a, high_a, low_b, high_b] into lanes 0..3.
    ///
    /// # Safety
    /// CPU must support `avx512f`, `avx512bw`, and `avx512vnni`.
    #[cfg(target_arch = "x86_64")]
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
                // dpbusd(acc, u8, i8): `c_512` is u8 (≤ 127), `low_high_pair` is i8.
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

            sum_low + 256 * sum_high
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn hsum_i32_sse(v: core::arch::x86_64::__m128i) -> i32 {
    use core::arch::x86_64::*;
    let v = _mm_add_epi32(v, _mm_shuffle_epi32(v, 0x4E));
    let v = _mm_add_epi32(v, _mm_shuffle_epi32(v, 0xB1));
    _mm_cvtsi128_si32(v)
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::prelude::StdRng;
    use rand::seq::SliceRandom;
    use rand_distr::{Distribution, StandardNormal};

    use super::*;
    use crate::turboquant::lloyd_max;

    /// Packs a sequence of 4-bit indices (each in [0, 15]) two per byte:
    /// low nibble → even lane, high nibble → odd lane.
    fn pack_nibbles(indices: &[u8]) -> Vec<u8> {
        assert_eq!(indices.len() % 2, 0);
        indices
            .chunks_exact(2)
            .map(|p| p[0] | (p[1] << 4))
            .collect()
    }

    fn sample_normal_vec(rng: &mut StdRng, len: usize) -> Vec<f32> {
        (0..len).map(|_| StandardNormal.sample(rng)).collect()
    }

    /// Map each raw float to the index of its nearest centroid in `CENTROIDS_4BIT`.
    fn encode_to_nearest_centroid(centroids: &[f32], raw: &[f32]) -> Vec<u8> {
        raw.iter()
            .map(|&v| {
                centroids
                    .iter()
                    .enumerate()
                    .min_by(|a, b| (a.1 - v).abs().partial_cmp(&(b.1 - v).abs()).unwrap())
                    .map(|(k, _)| k as u8)
                    .unwrap()
            })
            .collect()
    }

    /// Parity-test helper: query ~ N(0,1), balanced index distribution (each centroid
    /// appears dim/16 times, shuffled).  Distribution of indices doesn't affect
    /// scalar-vs-SIMD parity; we just need non-trivial data.
    fn random_inputs(rng: &mut StdRng, dim: usize) -> (Query4bitSimd, Vec<u8>) {
        let query = sample_normal_vec(rng, dim);
        let mut indices: Vec<u8> = (0..dim).map(|i| (i % 16) as u8).collect();
        indices.shuffle(rng);
        (Query4bitSimd::new(&query), pack_nibbles(&indices))
    }

    /// The compile-time `CODEBOOK_U8` must reproduce what the runtime recipe
    /// (`(c * c_scale).round().clamp(-63, 63) + 64`) would build from
    /// `CENTROIDS_4BIT`.  Keeps the hardcoded constant in sync with `lloyd_max`.
    #[test]
    fn test_codebook_matches_lloyd_max() {
        let centroids = lloyd_max::get_centroids(4);
        assert_eq!(centroids.len(), 16);

        let c_abs_max = centroids
            .iter()
            .copied()
            .map(f32::abs)
            .fold(0.0_f32, f32::max);
        assert!(
            (CODEBOOK_ABS_MAX - c_abs_max).abs() < 1e-6,
            "CODEBOOK_ABS_MAX ({CODEBOOK_ABS_MAX}) != max|CENTROIDS_4BIT| ({c_abs_max})"
        );

        let c_scale = 63.0 / c_abs_max;
        let offset = CODEBOOK_OFFSET as i32;
        let quantized: [u8; 16] = std::array::from_fn(|k| {
            let signed = (centroids[k] * c_scale).round().clamp(-63.0, 63.0) as i32;
            (signed + offset) as u8
        });
        assert_eq!(
            quantized, CODEBOOK_U8,
            "const CODEBOOK_U8 drifted from CENTROIDS_4BIT Lloyd-Max quantization",
        );
    }

    /// Reconstruction accuracy on realistic PQ inputs:
    /// query ∼ N(0,1), vector drawn from N(0,1) then mapped to nearest centroid.
    /// We compare `simd.dotprod()` against the "ideal" PQ dot (sum of `q[j] * c[v[j]]`
    /// with float-precision centroid lookup) — the error our SIMD path adds over a
    /// hypothetical perfect-precision PQ should be tiny.
    #[test]
    fn test_dotprod_matches_float() {
        let mut rng = StdRng::seed_from_u64(42);
        let dim = 256;
        let n_trials = 64;

        let centroids = lloyd_max::get_centroids(4);

        for _ in 0..n_trials {
            let query = sample_normal_vec(&mut rng, dim);
            let v_raw = sample_normal_vec(&mut rng, dim);
            let indices = encode_to_nearest_centroid(centroids, &v_raw);
            let v_pq: Vec<f32> = indices.iter().map(|&k| centroids[k as usize]).collect();

            let pq_dot: f32 = query.iter().zip(v_pq.iter()).map(|(a, b)| a * b).sum();
            let simd_dot = Query4bitSimd::new(&query).dotprod(&pack_nibbles(&indices));

            // SIMD quantization error scales like √d × σ_q × ε_c.  For d=256,
            // unit-variance query and i8 codebook precision, typical error is
            // ≲0.1 and 3σ tail excursions stay under 0.5.
            assert!(
                (pq_dot - simd_dot).abs() < 0.5,
                "simd_dot {simd_dot} too far from ideal PQ dot {pq_dot}",
            );
        }
    }

    /// Quantitative proof that i8 codebook + 14-bit query precision are plenty:
    /// the RMS error our SIMD path adds is an order of magnitude below the error
    /// PQ centroid snapping itself introduces.  If this invariant ever flips,
    /// something in the quantization pipeline lost precision.
    #[test]
    fn test_simd_noise_below_pq_noise() {
        let mut rng = StdRng::seed_from_u64(123);
        let dim = 256;
        let n_trials = 256;

        let centroids = lloyd_max::get_centroids(4);

        let mut sq_pq_noise = 0.0_f64;
        let mut sq_simd_noise = 0.0_f64;

        for _ in 0..n_trials {
            let query = sample_normal_vec(&mut rng, dim);
            let v_raw = sample_normal_vec(&mut rng, dim);
            let indices = encode_to_nearest_centroid(centroids, &v_raw);
            let v_pq: Vec<f32> = indices.iter().map(|&k| centroids[k as usize]).collect();

            let true_dot: f64 = query
                .iter()
                .zip(v_raw.iter())
                .map(|(a, b)| f64::from(*a) * f64::from(*b))
                .sum();
            let pq_dot: f64 = query
                .iter()
                .zip(v_pq.iter())
                .map(|(a, b)| f64::from(*a) * f64::from(*b))
                .sum();
            let simd_dot = f64::from(Query4bitSimd::new(&query).dotprod(&pack_nibbles(&indices)));

            sq_pq_noise += (pq_dot - true_dot).powi(2);
            sq_simd_noise += (simd_dot - pq_dot).powi(2);
        }

        let rms_pq_noise = (sq_pq_noise / f64::from(n_trials)).sqrt();
        let rms_simd_noise = (sq_simd_noise / f64::from(n_trials)).sqrt();

        // Print for easy comparison across encoding variants.
        eprintln!(
            "NOISE at dim={dim}: pq_rms={rms_pq_noise:.4} simd_rms={rms_simd_noise:.4} \
             ratio={:.2}×",
            rms_pq_noise / rms_simd_noise,
        );

        // Expect RMS(pq_noise) ≈ √(d · D_4) ≈ √(256 · 0.0115) ≈ 1.7.
        // With the 7-bit-unsigned codebook + 8-bit-signed query encoding the
        // SIMD noise is ~0.16 (≈ 0.04 × 2^2 over the 14+8 scheme that traded
        // ~6 bits of query precision for a per-query bias correction).  Ratio
        // 5× keeps "SIMD < PQ by nearly an order of magnitude" as a regression
        // guard without being so tight that normal distribution tails flake it.
        assert!(
            rms_simd_noise * 5.0 < rms_pq_noise,
            "SIMD noise RMS {rms_simd_noise:.4} should be << PQ noise RMS \
             {rms_pq_noise:.4} (ratio {:.2}×)",
            rms_pq_noise / rms_simd_noise,
        );
    }

    /// All test dims below are multiples of 32 — enforced by `Query4bitSimd::new`.
    const PARITY_DIMS: &[usize] = &[32, 128, 256, 1024, 2048];

    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    #[test]
    fn test_neon_matches_scalar() {
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (simd_query, vector) = random_inputs(&mut rng, dim);
            let scalar = simd_query.dotprod_raw(&vector);
            let neon = unsafe { simd_query.dotprod_raw_neon(&vector) };
            assert_eq!(scalar, neon, "scalar {scalar} != neon {neon} at dim {dim}");
        }
    }

    #[cfg(target_arch = "x86_64")]
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

    #[cfg(target_arch = "x86_64")]
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

    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    #[test]
    fn test_neon_sdot_matches_scalar() {
        if !std::arch::is_aarch64_feature_detected!("dotprod") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (simd_query, vector) = random_inputs(&mut rng, dim);
            let scalar = simd_query.dotprod_raw(&vector);
            let sdot = unsafe { simd_query.dotprod_raw_neon_sdot(&vector) };
            assert_eq!(scalar, sdot, "scalar {scalar} != sdot {sdot} at dim {dim}");
        }
    }

    #[cfg(target_arch = "x86_64")]
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

    /// Run `dotprod_raw` on every SIMD backend available at runtime and assert that
    /// each result equals the scalar baseline (which uses i64 throughout and cannot
    /// overflow at any practical PQ dimension).  A mismatch implies saturation or
    /// integer overflow in one of the SIMD paths.
    fn assert_all_simd_match_scalar(query: &Query4bitSimd, vector: &[u8]) {
        let expected = query.dotprod_raw(vector);

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            let neon = unsafe { query.dotprod_raw_neon(vector) };
            assert_eq!(expected, neon, "neon mismatch (expected {expected})");

            if std::arch::is_aarch64_feature_detected!("dotprod") {
                let sdot = unsafe { query.dotprod_raw_neon_sdot(vector) };
                assert_eq!(expected, sdot, "sdot mismatch (expected {expected})");
            }
        }

        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("ssse3") && std::is_x86_feature_detected!("sse4.1") {
                let sse = unsafe { query.dotprod_raw_sse(vector) };
                assert_eq!(expected, sse, "sse mismatch (expected {expected})");
            }
            if std::is_x86_feature_detected!("avx2") {
                let avx2 = unsafe { query.dotprod_raw_avx2(vector) };
                assert_eq!(expected, avx2, "avx2 mismatch (expected {expected})");
            }
            if std::is_x86_feature_detected!("avx512f")
                && std::is_x86_feature_detected!("avx512bw")
                && std::is_x86_feature_detected!("avx512vnni")
            {
                let v512 = unsafe { query.dotprod_raw_avx512_vnni(vector) };
                assert_eq!(expected, v512, "avx512_vnni mismatch (expected {expected})");
            }
        }
    }

    /// Build the worst-case positive-accumulation inputs.
    ///
    /// - Query: all 1.0 → `q_signed = 8191`, `q_biased = 16383`, low = high = 127 (7-bit max).
    /// - Vector: every index 15 → codebook lookup always yields `CODEBOOK_INT[15] = 127`.
    /// - Every `_mm_maddubs_epi16` pair sees (127 u8) × (127 i8) + (127 u8) × (127 i8)
    ///   = 32 258 — 509 below i16 saturation.
    fn max_positive_inputs(dim: usize) -> (Query4bitSimd, Vec<u8>) {
        let query = vec![1.0_f32; dim];
        let indices: Vec<u8> = vec![15; dim];
        (Query4bitSimd::new(&query), pack_nibbles(&indices))
    }

    /// Mirror of `max_positive_inputs` with every index 0 → `CODEBOOK_INT[0] = −127`.
    /// Every maddubs pair = −32 258, one above the i16 minimum of −32 768.
    fn max_negative_inputs(dim: usize) -> (Query4bitSimd, Vec<u8>) {
        let query = vec![1.0_f32; dim];
        let indices: Vec<u8> = vec![0; dim];
        (Query4bitSimd::new(&query), pack_nibbles(&indices))
    }

    /// `_mm_maddubs_epi16` saturates to i16 (±32 767).  The design guarantees
    /// low/high ∈ [0, 127] and codebook_int ∈ [−127, 127], so the worst-case pair
    /// sum is 127×127 + 127×127 = 32 258.  A SIMD/scalar mismatch here would mean
    /// saturation corrupted the result.
    #[test]
    fn test_overflow_maddubs_near_saturation() {
        // Positive: pair = +32 258 < +32 767 → no saturation.
        let (q, v) = max_positive_inputs(32);
        assert_all_simd_match_scalar(&q, &v);

        // Negative: pair = −32 258 > −32 768 → no saturation.
        let (q, v) = max_negative_inputs(32);
        assert_all_simd_match_scalar(&q, &v);
    }

    /// At dim = 4 096 each i32 accumulator lane carries
    /// 256 chunks × 4 products × 16 129 = 16 516 096 — far below i32 max (2.1 B).
    /// The horizontal sum across four lanes reaches 66 064 384, also i32-safe.
    #[test]
    fn test_overflow_i32_accumulator_depth() {
        let (q, v) = max_positive_inputs(4096);
        assert_all_simd_match_scalar(&q, &v);

        let (q, v) = max_negative_inputs(4096);
        assert_all_simd_match_scalar(&q, &v);
    }
}
