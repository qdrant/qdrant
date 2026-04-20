/// 4-bit codebook (Lloyd-Max optimal for N(0, 1), see `turboquant::lloyd_max`)
/// pre-quantized to i8 at compile time with scale `127 / max|c|`.  The
/// distribution is symmetric around zero so the per-query shift term vanishes
/// (no `postprocess_shift` is needed on reconstruction).  Consistency with
/// `CENTROIDS_4BIT` is guarded by `test_codebook_matches_lloyd_max`.
const CODEBOOK_INT: [i8; 16] = [
    -127, -96, -75, -58, -44, -31, -18, -6, 6, 18, 31, 44, 58, 75, 96, 127,
];

/// `max|c|` over `CENTROIDS_4BIT` — the extreme centroid.  Together with the
/// per-query `q_scale` it fixes the float→integer reconstruction factor.
const CODEBOOK_ABS_MAX: f32 = 2.733;

pub struct Query4bitSimd {
    query_data: Vec<[[u8; 16]; 2]>,
    postprocess_scale: f32,
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

        // Scale query to a signed 14-bit integer `q_signed ∈ [-8191, 8191]`; store it
        // biased by 8192 so each 7-bit half (low/high) fits into a u8 and keeps
        // `_mm_maddubs_epi16` within i16 without saturation.
        let q_abs_max = data
            .iter()
            .copied()
            .map(f32::abs)
            .fold(0.0_f32, f32::max)
            .max(f32::EPSILON);
        let q_scale = 8191.0 / q_abs_max;

        let num_chunks = data.len() / 16;
        let mut query_data = Vec::with_capacity(num_chunks);
        for chunk_idx in 0..num_chunks {
            let mut low = [0u8; 16];
            let mut high = [0u8; 16];
            for i in 0..16 {
                let q_signed = (data[chunk_idx * 16 + i] * q_scale)
                    .round()
                    .clamp(-8191.0, 8191.0) as i32;
                let q_biased = (q_signed + 8192) as u32;
                low[i] = (q_biased & 0x7F) as u8;
                high[i] = (q_biased >> 7) as u8;
            }
            query_data.push([low, high]);
        }

        // dotprod_raw computes `sum_j q_biased[j] * c_int[v[j]]`.  Unpacking the bias:
        //   dot_raw = sum_j q_signed * c_int[v[j]] + 8192 * sum_j c_int[v[j]]
        // Reconstruction to float: `true ≈ 1/(q_scale * c_scale) * dot_raw`.
        // For the symmetric CODEBOOK_INT, `sum_j c_int[v[j]]` has expectation
        // zero for any PQ-like vector, so the bias term washes out.  With
        // `c_scale = 127/CODEBOOK_ABS_MAX`, the prefactor is
        //   postprocess_scale = CODEBOOK_ABS_MAX / (127 * q_scale).
        let postprocess_scale = CODEBOOK_ABS_MAX / (127.0 * q_scale);

        Self {
            query_data,
            postprocess_scale,
        }
    }

    pub fn dotprod(&self, vector: &[u8]) -> f32 {
        // `dot_raw` = Σ q_biased[j] · c_int[v[j]]
        //           = Σ q_signed · c_int + 8192 · Σ c_int[v[j]]
        // The second term is the bias introduced by storing `q_biased = q_signed + 8192`.
        // Its expectation is zero for balanced PQ, but the variance over a finite-length
        // vector is non-negligible (RMS ~ √d × σ_c_int), so we subtract it explicitly.
        let dot_raw = self.dotprod_raw_best(vector);
        let sum_c_v = sum_codebook_over_vector(vector);
        self.postprocess_scale * (dot_raw - 8192 * sum_c_v) as f32
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

    /// Compute `sum_j q_biased[j] * c_int[v[j]]` over 16 lanes per chunk.
    ///
    /// `vector` is PQ-encoded with two 4-bit codebook indices packed per byte:
    /// the low nibble is the index for the even lane (j = 2k), the high nibble
    /// for the odd lane (j = 2k + 1).
    pub fn dotprod_raw(&self, vector: &[u8]) -> i64 {
        let mut acc_low: i64 = 0;
        let mut acc_high: i64 = 0;
        for (&[low, high], v) in self.query_data.iter().zip(vector.chunks(8)) {
            for i in 0..16 {
                let byte = v[i / 2];
                let idx = if i & 1 == 0 { byte & 0x0F } else { byte >> 4 };
                let codebook_value = i64::from(CODEBOOK_INT[idx as usize]);
                acc_low += i64::from(low[i]) * codebook_value;
                acc_high += i64::from(high[i]) * codebook_value;
            }
        }
        acc_low + (acc_high.abs() << 7) * acc_high.signum()
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
            // 16-entry i8 codebook fits exactly into a single NEON register, used as a
            // lookup table by `vqtbl1q_s8`.
            let codebook = vld1q_s8(CODEBOOK_INT.as_ptr());
            let mut acc_low = vdupq_n_s32(0);
            let mut acc_high = vdupq_n_s32(0);

            let nibble_mask = vdup_n_u8(0x0F);
            for (&[low, high], v_chunk) in self.query_data.iter().zip(vector.chunks_exact(8)) {
                // Unpack 8 packed bytes into 16 nibble indices:
                //   [byte0.lo, byte0.hi, byte1.lo, byte1.hi, ..., byte7.lo, byte7.hi].
                let v_packed = vld1_u8(v_chunk.as_ptr());
                let v_lo = vand_u8(v_packed, nibble_mask);
                let v_hi = vshr_n_u8(v_packed, 4);
                let v = vcombine_u8(vzip1_u8(v_lo, v_hi), vzip2_u8(v_lo, v_hi));

                // Gather 16 signed codebook values indexed by the 4-bit PQ codes.
                let c = vqtbl1q_s8(codebook, v);

                // low/high are 7-bit unsigned so reinterpret as i8 is value-preserving.
                let low_s = vreinterpretq_s8_u8(vld1q_u8(low.as_ptr()));
                let high_s = vreinterpretq_s8_u8(vld1q_u8(high.as_ptr()));

                // i8 × i8 → i16; per product magnitude ≤ 127 × 128 = 16256 fits in i16.
                let prod_low_lo = vmull_s8(vget_low_s8(low_s), vget_low_s8(c));
                let prod_low_hi = vmull_high_s8(low_s, c);
                let prod_high_lo = vmull_s8(vget_low_s8(high_s), vget_low_s8(c));
                let prod_high_hi = vmull_high_s8(high_s, c);

                // Pairwise-add the i16×8 products into i32×4 accumulators.
                acc_low = vpadalq_s16(acc_low, prod_low_lo);
                acc_low = vpadalq_s16(acc_low, prod_low_hi);
                acc_high = vpadalq_s16(acc_high, prod_high_lo);
                acc_high = vpadalq_s16(acc_high, prod_high_hi);
            }

            let acc_low_sum = i64::from(vaddvq_s32(acc_low));
            let acc_high_sum = i64::from(vaddvq_s32(acc_high));
            acc_low_sum + acc_high_sum * 128
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
            let codebook = vld1q_s8(CODEBOOK_INT.as_ptr());
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
                // vzip1q_u8 interleaves the low 8 bytes of each input → 16 indices
                // for chunk 2i; vzip2q_u8 does the same for bytes 8..15 → chunk 2i+1.
                let v_0 = vzip1q_u8(v_lo, v_hi);
                let v_1 = vzip2q_u8(v_lo, v_hi);
                let c_0 = vqtbl1q_s8(codebook, v_0);
                let c_1 = vqtbl1q_s8(codebook, v_1);

                let low_s_0 = vreinterpretq_s8_u8(vld1q_u8(low_0.as_ptr()));
                let high_s_0 = vreinterpretq_s8_u8(vld1q_u8(high_0.as_ptr()));
                let low_s_1 = vreinterpretq_s8_u8(vld1q_u8(low_1.as_ptr()));
                let high_s_1 = vreinterpretq_s8_u8(vld1q_u8(high_1.as_ptr()));

                // Four independent SDOT dependency chains.
                core::arch::asm!(
                    "sdot {acc:v}.4s, {a:v}.16b, {b:v}.16b",
                    acc = inout(vreg) acc_low_0,
                    a = in(vreg) low_s_0,
                    b = in(vreg) c_0,
                    options(pure, nomem, nostack, preserves_flags),
                );
                core::arch::asm!(
                    "sdot {acc:v}.4s, {a:v}.16b, {b:v}.16b",
                    acc = inout(vreg) acc_low_1,
                    a = in(vreg) low_s_1,
                    b = in(vreg) c_1,
                    options(pure, nomem, nostack, preserves_flags),
                );
                core::arch::asm!(
                    "sdot {acc:v}.4s, {a:v}.16b, {b:v}.16b",
                    acc = inout(vreg) acc_high_0,
                    a = in(vreg) high_s_0,
                    b = in(vreg) c_0,
                    options(pure, nomem, nostack, preserves_flags),
                );
                core::arch::asm!(
                    "sdot {acc:v}.4s, {a:v}.16b, {b:v}.16b",
                    acc = inout(vreg) acc_high_1,
                    a = in(vreg) high_s_1,
                    b = in(vreg) c_1,
                    options(pure, nomem, nostack, preserves_flags),
                );
            }

            let acc_low = vaddq_s32(acc_low_0, acc_low_1);
            let acc_high = vaddq_s32(acc_high_0, acc_high_1);
            let acc_low_sum = i64::from(vaddvq_s32(acc_low));
            let acc_high_sum = i64::from(vaddvq_s32(acc_high));
            acc_low_sum + acc_high_sum * 128
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
            // 16 i8 codebook entries live in one xmm register, gathered by PSHUFB.
            let codebook = _mm_loadu_si128(CODEBOOK_INT.as_ptr().cast::<__m128i>());
            let ones = _mm_set1_epi16(1);
            let nibble_mask = _mm_set1_epi8(0x0F);
            let mut acc_low = _mm_setzero_si128();
            let mut acc_high = _mm_setzero_si128();

            for (&[low, high], v_chunk) in self.query_data.iter().zip(vector.chunks_exact(8)) {
                // Unpack 8 packed bytes → 16 nibble indices in the low 16 lanes.
                let v_packed = _mm_loadl_epi64(v_chunk.as_ptr().cast::<__m128i>());
                let v_lo = _mm_and_si128(v_packed, nibble_mask);
                let v_hi = _mm_and_si128(_mm_srli_epi16(v_packed, 4), nibble_mask);
                let v = _mm_unpacklo_epi8(v_lo, v_hi);
                let c = _mm_shuffle_epi8(codebook, v);

                let low_vec = _mm_loadu_si128(low.as_ptr().cast::<__m128i>());
                let high_vec = _mm_loadu_si128(high.as_ptr().cast::<__m128i>());

                // maddubs: u8×i8 → i16, horizontally pair-summed. Per pair ≤ 32512 fits i16.
                let prod_low = _mm_maddubs_epi16(low_vec, c);
                let prod_high = _mm_maddubs_epi16(high_vec, c);

                // madd_epi16 with ones pair-sums i16×8 → i32×4, accumulate.
                acc_low = _mm_add_epi32(acc_low, _mm_madd_epi16(prod_low, ones));
                acc_high = _mm_add_epi32(acc_high, _mm_madd_epi16(prod_high, ones));
            }

            let sum_low = i64::from(hsum_i32_sse(acc_low));
            let sum_high = i64::from(hsum_i32_sse(acc_high));
            sum_low + sum_high * 128
        }
    }

    /// x86_64 AVX2 implementation: stacks low/high 7-bit query halves into two
    /// 16-byte lanes of a YMM register, does `maddubs_epi16 + madd_epi16 → add`.
    ///
    /// 2× unrolled: two chunks per iteration with two independent YMM
    /// accumulators break the two-stage dependency chain (`maddubs → madd → acc`,
    /// ~10 cycles on Zen 4).  Measured `dotprod_raw_avx2_x2` as strictly ≥ the
    /// non-unrolled baseline on Zen 4, so the 1× version was removed.
    ///
    /// # Safety
    /// CPU must support `avx2`.
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    pub unsafe fn dotprod_raw_avx2(&self, vector: &[u8]) -> i64 {
        use core::arch::x86_64::*;

        unsafe {
            let codebook_128 = _mm_loadu_si128(CODEBOOK_INT.as_ptr().cast::<__m128i>());
            let codebook = _mm256_broadcastsi128_si256(codebook_128);
            let ones = _mm256_set1_epi16(1);
            let nibble_mask = _mm_set1_epi8(0x0F);
            let mut acc0 = _mm256_setzero_si256();
            let mut acc1 = _mm256_setzero_si256();

            // `query_data.len()` is guaranteed even by the dim-%32 precondition,
            // so every pair is consumed and there is no tail to handle.
            let chunks = self.query_data.as_slice();
            let n_pairs = chunks.len() / 2;

            for i in 0..n_pairs {
                // 2 × 32-byte query_data loads.
                let lh0 = _mm256_loadu_si256(chunks.as_ptr().add(2 * i).cast::<__m256i>());
                let lh1 = _mm256_loadu_si256(chunks.as_ptr().add(2 * i + 1).cast::<__m256i>());

                // One 16-byte vector load covers both chunks' 8 packed bytes each.
                let v_packed = _mm_loadu_si128(vector.as_ptr().add(16 * i).cast::<__m128i>());
                let v_lo = _mm_and_si128(v_packed, nibble_mask);
                let v_hi = _mm_and_si128(_mm_srli_epi16(v_packed, 4), nibble_mask);
                let v128_0 = _mm_unpacklo_epi8(v_lo, v_hi); // 16 indices for chunk 2i
                let v128_1 = _mm_unpackhi_epi8(v_lo, v_hi); // 16 indices for chunk 2i+1
                let v0 = _mm256_broadcastsi128_si256(v128_0);
                let v1 = _mm256_broadcastsi128_si256(v128_1);
                let c0 = _mm256_shuffle_epi8(codebook, v0);
                let c1 = _mm256_shuffle_epi8(codebook, v1);

                let prods0 = _mm256_maddubs_epi16(lh0, c0);
                let prods1 = _mm256_maddubs_epi16(lh1, c1);
                acc0 = _mm256_add_epi32(acc0, _mm256_madd_epi16(prods0, ones));
                acc1 = _mm256_add_epi32(acc1, _mm256_madd_epi16(prods1, ones));
            }

            let acc = _mm256_add_epi32(acc0, acc1);
            let acc_low = _mm256_castsi256_si128(acc);
            let acc_high = _mm256_extracti128_si256(acc, 1);
            let sum_low = i64::from(hsum_i32_sse(acc_low));
            let sum_high = i64::from(hsum_i32_sse(acc_high));
            sum_low + sum_high * 128
        }
    }

    /// AVX-512 VNNI (Ice Lake Xeon+, Zen 4+).  Processes 2 chunks per iteration
    /// in a 512-bit accumulator using `VPDPBUSD` on ZMM: the
    /// `[low0, high0, low1, high1]` layout of two consecutive query_data entries
    /// fits a 64-byte load exactly.  On Zen 4 ZMM `VPDPBUSD` is throughput
    /// 0.5/cycle (double-pumped on a 256-bit FPU), so even a single acc chain
    /// saturates the pipeline — explicit unroll gave no speedup when measured.
    ///
    /// # Safety
    /// CPU must support `avx512f`, `avx512bw`, and `avx512vnni`.
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
    pub unsafe fn dotprod_raw_avx512_vnni(&self, vector: &[u8]) -> i64 {
        use core::arch::x86_64::*;

        unsafe {
            let codebook_128 = _mm_loadu_si128(CODEBOOK_INT.as_ptr().cast::<__m128i>());
            let codebook_512 = _mm512_broadcast_i32x4(codebook_128);
            let nibble_mask_128 = _mm_set1_epi8(0x0F);
            let mut acc = _mm512_setzero_si512();

            // `query_data.len()` is guaranteed even by the dim-%32 precondition,
            // so every pair is consumed and there is no tail to handle.
            let chunks = self.query_data.as_slice();
            let n_pairs = chunks.len() / 2;

            for i in 0..n_pairs {
                // Two consecutive [[u8;16];2] entries → 64 bytes: [low0, high0, low1, high1].
                let pair_ptr = chunks.as_ptr().add(2 * i).cast::<__m512i>();
                let low_high_pair = _mm512_loadu_si512(pair_ptr);

                // 16 packed bytes → 32 nibbles (2 chunks × 16 indices each).
                let v_packed_16 = _mm_loadu_si128(vector.as_ptr().add(16 * i).cast::<__m128i>());
                let v_lo = _mm_and_si128(v_packed_16, nibble_mask_128);
                let v_hi = _mm_and_si128(_mm_srli_epi16(v_packed_16, 4), nibble_mask_128);
                let v_chunk_a = _mm_unpacklo_epi8(v_lo, v_hi); // 16 indices for chunk 2i
                let v_chunk_b = _mm_unpackhi_epi8(v_lo, v_hi); // 16 indices for chunk 2i+1

                // Arrange into [v_a, v_a, v_b, v_b] so each 128-bit lane of codebook
                // gets the right index set for its half of `low_high_pair`.
                let v_dup_a = _mm256_broadcastsi128_si256(v_chunk_a);
                let v_dup_b = _mm256_broadcastsi128_si256(v_chunk_b);
                let v_512 = _mm512_inserti64x4(_mm512_castsi256_si512(v_dup_a), v_dup_b, 1);

                let c_512 = _mm512_shuffle_epi8(codebook_512, v_512);
                acc = _mm512_dpbusd_epi32(acc, low_high_pair, c_512);
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

            sum_low + sum_high * 128
        }
    }
}

/// Σ c_int[v[j]] over a packed PQ vector.  Each byte holds two 4-bit centroid
/// indices — low nibble for the even lane, high nibble for the odd lane.  This
/// sum is needed to cancel the `8192 × Σ c_int` bias that the `q_biased`
/// encoding bakes into `dot_raw`.
#[inline]
fn sum_codebook_over_vector(vector: &[u8]) -> i64 {
    let mut sum: i64 = 0;
    for &byte in vector {
        sum += i64::from(CODEBOOK_INT[(byte & 0x0F) as usize]);
        sum += i64::from(CODEBOOK_INT[(byte >> 4) as usize]);
    }
    sum
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

    /// The compile-time `CODEBOOK_INT` must reproduce what `new` used to build at
    /// runtime from `CENTROIDS_4BIT`.  Runs the same quantization recipe and
    /// compares element-wise.
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

        let c_scale = 127.0 / c_abs_max;
        let quantized: [i8; 16] =
            std::array::from_fn(|k| (centroids[k] * c_scale).round().clamp(-127.0, 127.0) as i8);
        assert_eq!(
            quantized, CODEBOOK_INT,
            "const CODEBOOK_INT drifted from CENTROIDS_4BIT Lloyd-Max quantization",
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

        // Expect RMS(pq_noise) ≈ √(d · D_4) ≈ √(256 · 0.0115) ≈ 1.7
        //        RMS(simd_noise) ≈ √(d) · σ_q · σ_codebook_quant ≈ 0.04
        // Factor ≥ 10× guarantees i8 precision is not the bottleneck.
        assert!(
            rms_simd_noise * 10.0 < rms_pq_noise,
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
