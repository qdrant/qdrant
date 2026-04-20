pub struct Query4bitSimd {
    query_data: Vec<[[u8; 16]; 2]>,
    codebook: [i8; 16],
    postprocess_scale: f32,
    postprocess_shift: f32,
}

impl Query4bitSimd {
    /// Query dim must be a multiple of 32.  The NEON SDOT and AVX-512 VNNI paths
    /// both consume 2 query_data chunks (= 32 query elements) per iteration; with
    /// `dim % 32 == 0` we get `query_data.len()` even and can drop all tail
    /// handling.  Every PQ dimension we ship (128, 256, 384, 512, 768, 1024,
    /// 1536, 2048, 4096) already satisfies this.
    pub fn new(data: &[f32], codebook: &[f32; 16]) -> Self {
        assert!(
            data.len() % 32 == 0,
            "Query4bitSimd requires query dim to be a multiple of 32 (got {})",
            data.len(),
        );

        // Subtract the centroid mean before quantizing — the shift lives in the
        // codebook itself, so reconstructing a dot product needs only a per-query
        // constant (no per-vector side data).
        let c_shift: f32 = codebook.iter().sum::<f32>() / codebook.len() as f32;
        let c_centered: [f32; 16] = std::array::from_fn(|k| codebook[k] - c_shift);

        let c_abs_max = c_centered
            .iter()
            .copied()
            .map(f32::abs)
            .fold(0.0_f32, f32::max)
            .max(f32::EPSILON);
        let c_scale = 127.0 / c_abs_max;
        let codebook_int: [i8; 16] =
            std::array::from_fn(|k| (c_centered[k] * c_scale).round().clamp(-127.0, 127.0) as i8);

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

        // dotprod_raw computes `sum_j q_biased[j] * c_int[v[j]]`. Unpacking the bias:
        //   dot_raw = sum_j q_signed * c_int[v[j]] + 8192 * sum_j c_int[v[j]]
        // Float dot reconstruction:
        //   true = sum_j q[j] * c[v[j]]
        //        = sum_j q[j] * c_centered[v[j]] + c_shift * sum_j q[j]
        //        ≈ (1 / (q_scale * c_scale)) * sum_j q_signed * c_int[v[j]] + c_shift * sum_q
        // The residual `(8192 / (q_scale*c_scale)) * sum_j c_int[v[j]]` has expectation
        // zero by construction of the centered codebook (and is exactly zero when the
        // vector uses each centroid equally).
        let postprocess_scale = 1.0 / (q_scale * c_scale);
        let postprocess_shift = c_shift * data.iter().sum::<f32>();

        Self {
            query_data,
            codebook: codebook_int,
            postprocess_scale,
            postprocess_shift,
        }
    }

    pub fn dotprod(&self, vector: &[u8]) -> f32 {
        let raw = self.dotprod_raw_best(vector);
        self.postprocess_scale * raw as f32 + self.postprocess_shift
    }

    #[inline]
    fn dotprod_raw_best(&self, vector: &[u8]) -> i64 {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avxvnni") && std::is_x86_feature_detected!("avx2") {
                return unsafe { self.dotprod_raw_avx_vnni(vector) };
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
                let codebook_value = self.codebook[idx as usize] as i64;
                acc_low += low[i] as i64 * codebook_value;
                acc_high += high[i] as i64 * codebook_value;
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
            let codebook = vld1q_s8(self.codebook.as_ptr());
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

            let acc_low_sum = vaddvq_s32(acc_low) as i64;
            let acc_high_sum = vaddvq_s32(acc_high) as i64;
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
            let codebook = vld1q_s8(self.codebook.as_ptr());
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
            let acc_low_sum = vaddvq_s32(acc_low) as i64;
            let acc_high_sum = vaddvq_s32(acc_high) as i64;
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
            let codebook = _mm_loadu_si128(self.codebook.as_ptr() as *const __m128i);
            let ones = _mm_set1_epi16(1);
            let nibble_mask = _mm_set1_epi8(0x0F);
            let mut acc_low = _mm_setzero_si128();
            let mut acc_high = _mm_setzero_si128();

            for (&[low, high], v_chunk) in self.query_data.iter().zip(vector.chunks_exact(8)) {
                // Unpack 8 packed bytes → 16 nibble indices in the low 16 lanes.
                let v_packed = _mm_loadl_epi64(v_chunk.as_ptr() as *const __m128i);
                let v_lo = _mm_and_si128(v_packed, nibble_mask);
                let v_hi = _mm_and_si128(_mm_srli_epi16(v_packed, 4), nibble_mask);
                let v = _mm_unpacklo_epi8(v_lo, v_hi);
                let c = _mm_shuffle_epi8(codebook, v);

                let low_vec = _mm_loadu_si128(low.as_ptr() as *const __m128i);
                let high_vec = _mm_loadu_si128(high.as_ptr() as *const __m128i);

                // maddubs: u8×i8 → i16, horizontally pair-summed. Per pair ≤ 32512 fits i16.
                let prod_low = _mm_maddubs_epi16(low_vec, c);
                let prod_high = _mm_maddubs_epi16(high_vec, c);

                // madd_epi16 with ones pair-sums i16×8 → i32×4, accumulate.
                acc_low = _mm_add_epi32(acc_low, _mm_madd_epi16(prod_low, ones));
                acc_high = _mm_add_epi32(acc_high, _mm_madd_epi16(prod_high, ones));
            }

            let sum_low = hsum_i32_sse(acc_low) as i64;
            let sum_high = hsum_i32_sse(acc_high) as i64;
            sum_low + sum_high * 128
        }
    }

    /// x86_64 AVX2 implementation: processes low and high 7-bit halves of one chunk
    /// together by stacking them as two 16-byte lanes of a single ymm register.
    ///
    /// # Safety
    /// CPU must support `avx2` (check via `is_x86_feature_detected!("avx2")`).
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    pub unsafe fn dotprod_raw_avx2(&self, vector: &[u8]) -> i64 {
        use core::arch::x86_64::*;

        unsafe {
            let codebook_128 = _mm_loadu_si128(self.codebook.as_ptr() as *const __m128i);
            let codebook = _mm256_broadcastsi128_si256(codebook_128);
            let ones = _mm256_set1_epi16(1);
            let nibble_mask = _mm_set1_epi8(0x0F);
            // Lanes 0..3 accumulate low-byte contribution; lanes 4..7 accumulate high-byte.
            let mut acc = _mm256_setzero_si256();

            for (chunk, v_chunk) in self.query_data.iter().zip(vector.chunks_exact(8)) {
                // One query_data entry is stored as `[low[16], high[16]]` contiguously,
                // so a single 32-byte load gives us the u8 operand for both halves.
                let low_high = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);

                // Unpack 8 packed bytes → 16 nibble indices in a __m128i, broadcast to 256.
                let v_packed = _mm_loadl_epi64(v_chunk.as_ptr() as *const __m128i);
                let v_lo = _mm_and_si128(v_packed, nibble_mask);
                let v_hi = _mm_and_si128(_mm_srli_epi16(v_packed, 4), nibble_mask);
                let v128 = _mm_unpacklo_epi8(v_lo, v_hi);
                let v = _mm256_broadcastsi128_si256(v128);
                let c = _mm256_shuffle_epi8(codebook, v);

                let prods = _mm256_maddubs_epi16(low_high, c);
                acc = _mm256_add_epi32(acc, _mm256_madd_epi16(prods, ones));
            }

            let acc_low = _mm256_castsi256_si128(acc);
            let acc_high = _mm256_extracti128_si256(acc, 1);
            let sum_low = hsum_i32_sse(acc_low) as i64;
            let sum_high = hsum_i32_sse(acc_high) as i64;
            sum_low + sum_high * 128
        }
    }

    /// 2× unrolled AVX2: two chunks per iteration with two independent YMM
    /// accumulators, breaking the `maddubs + madd_epi16 → acc` dependency chain.
    /// The chain on AVX2 is two stages deep (~10 cycles on Zen 4), so two parallel
    /// chains can in principle double throughput of this path.
    ///
    /// # Safety
    /// CPU must support `avx2`.
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    pub unsafe fn dotprod_raw_avx2_x2(&self, vector: &[u8]) -> i64 {
        use core::arch::x86_64::*;

        unsafe {
            let codebook_128 = _mm_loadu_si128(self.codebook.as_ptr() as *const __m128i);
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
                let lh0 = _mm256_loadu_si256(chunks.as_ptr().add(2 * i) as *const __m256i);
                let lh1 = _mm256_loadu_si256(chunks.as_ptr().add(2 * i + 1) as *const __m256i);

                // One 16-byte vector load covers both chunks' 8 packed bytes each.
                let v_packed = _mm_loadu_si128(vector.as_ptr().add(16 * i) as *const __m128i);
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
            let sum_low = hsum_i32_sse(acc_low) as i64;
            let sum_high = hsum_i32_sse(acc_high) as i64;
            sum_low + sum_high * 128
        }
    }

    /// AVX-VNNI (Alder Lake+, Zen 4+): a single `VPDPBUSD` replaces the
    /// `maddubs_epi16 + madd_epi16` pair used by the AVX2 path.
    ///
    /// # Safety
    /// CPU must support `avxvnni` (and therefore `avx2`).
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avxvnni,avx2")]
    pub unsafe fn dotprod_raw_avx_vnni(&self, vector: &[u8]) -> i64 {
        use core::arch::x86_64::*;

        unsafe {
            let codebook_128 = _mm_loadu_si128(self.codebook.as_ptr() as *const __m128i);
            let codebook = _mm256_broadcastsi128_si256(codebook_128);
            let nibble_mask = _mm_set1_epi8(0x0F);
            let mut acc = _mm256_setzero_si256();

            for (chunk, v_chunk) in self.query_data.iter().zip(vector.chunks_exact(8)) {
                let low_high = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);

                let v_packed = _mm_loadl_epi64(v_chunk.as_ptr() as *const __m128i);
                let v_lo = _mm_and_si128(v_packed, nibble_mask);
                let v_hi = _mm_and_si128(_mm_srli_epi16(v_packed, 4), nibble_mask);
                let v128 = _mm_unpacklo_epi8(v_lo, v_hi);
                let v = _mm256_broadcastsi128_si256(v128);
                let c = _mm256_shuffle_epi8(codebook, v);

                // VPDPBUSD: acc[i] += sum_{k=0..3} (low_high[4i+k] as u8) * (c[4i+k] as i8).
                acc = _mm256_dpbusd_avx_epi32(acc, low_high, c);
            }

            let acc_low = _mm256_castsi256_si128(acc);
            let acc_high = _mm256_extracti128_si256(acc, 1);
            let sum_low = hsum_i32_sse(acc_low) as i64;
            let sum_high = hsum_i32_sse(acc_high) as i64;
            sum_low + sum_high * 128
        }
    }

    /// 2× unrolled AVX-VNNI: two chunks per iteration with two independent YMM
    /// accumulators, breaking the single `VPDPBUSD → acc` dependency chain.
    /// On Zen 4 YMM `VPDPBUSD` has latency 4 and throughput 1/cycle — saturation
    /// needs 4 ops in flight; two chains roughly double utilization versus the
    /// 1× path which relies on OoO decoupling alone.
    ///
    /// # Safety
    /// CPU must support `avxvnni` (and therefore `avx2`).
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avxvnni,avx2")]
    pub unsafe fn dotprod_raw_avx_vnni_x2(&self, vector: &[u8]) -> i64 {
        use core::arch::x86_64::*;

        unsafe {
            let codebook_128 = _mm_loadu_si128(self.codebook.as_ptr() as *const __m128i);
            let codebook = _mm256_broadcastsi128_si256(codebook_128);
            let nibble_mask = _mm_set1_epi8(0x0F);
            let mut acc0 = _mm256_setzero_si256();
            let mut acc1 = _mm256_setzero_si256();

            let chunks = self.query_data.as_slice();
            let n_pairs = chunks.len() / 2;

            for i in 0..n_pairs {
                let lh0 = _mm256_loadu_si256(chunks.as_ptr().add(2 * i) as *const __m256i);
                let lh1 = _mm256_loadu_si256(chunks.as_ptr().add(2 * i + 1) as *const __m256i);

                let v_packed = _mm_loadu_si128(vector.as_ptr().add(16 * i) as *const __m128i);
                let v_lo = _mm_and_si128(v_packed, nibble_mask);
                let v_hi = _mm_and_si128(_mm_srli_epi16(v_packed, 4), nibble_mask);
                let v128_0 = _mm_unpacklo_epi8(v_lo, v_hi);
                let v128_1 = _mm_unpackhi_epi8(v_lo, v_hi);
                let v0 = _mm256_broadcastsi128_si256(v128_0);
                let v1 = _mm256_broadcastsi128_si256(v128_1);
                let c0 = _mm256_shuffle_epi8(codebook, v0);
                let c1 = _mm256_shuffle_epi8(codebook, v1);

                acc0 = _mm256_dpbusd_avx_epi32(acc0, lh0, c0);
                acc1 = _mm256_dpbusd_avx_epi32(acc1, lh1, c1);
            }

            let acc = _mm256_add_epi32(acc0, acc1);
            let acc_low = _mm256_castsi256_si128(acc);
            let acc_high = _mm256_extracti128_si256(acc, 1);
            let sum_low = hsum_i32_sse(acc_low) as i64;
            let sum_high = hsum_i32_sse(acc_high) as i64;
            sum_low + sum_high * 128
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
    use rand::prelude::StdRng;
    use rand::seq::SliceRandom;
    use rand::{RngExt, SeedableRng};

    use super::*;

    /// Packs a sequence of 4-bit indices (each in [0, 15]) two per byte:
    /// low nibble → even lane, high nibble → odd lane.
    fn pack_nibbles(indices: &[u8]) -> Vec<u8> {
        assert_eq!(indices.len() % 2, 0);
        indices
            .chunks_exact(2)
            .map(|p| p[0] | (p[1] << 4))
            .collect()
    }

    fn random_inputs(rng: &mut StdRng, dim: usize) -> (Query4bitSimd, Vec<u8>) {
        let codebook: [f32; 16] = std::array::from_fn(|_| rng.random_range(-1.0_f32..1.0));
        let query: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0_f32..1.0)).collect();
        let mut indices: Vec<u8> = (0..dim).map(|i| (i % 16) as u8).collect();
        indices.shuffle(rng);
        (
            Query4bitSimd::new(&query, &codebook),
            pack_nibbles(&indices),
        )
    }

    #[test]
    fn test_dotprod_matches_float() {
        let mut rng = StdRng::seed_from_u64(42);
        let dim = 256;
        let n_vectors = 32;

        let codebook: [f32; 16] = std::array::from_fn(|_| rng.random_range(-1.0_f32..1.0));
        let query: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0_f32..1.0)).collect();

        let simd_query = Query4bitSimd::new(&query, &codebook);

        let codebook_abs_max = codebook
            .iter()
            .copied()
            .map(f32::abs)
            .fold(0.0_f32, f32::max);
        let query_abs_sum: f32 = query.iter().map(|q: &f32| q.abs()).sum();
        let tolerance = 0.02 * query_abs_sum * codebook_abs_max + 1e-3;

        // Balanced PQ vector: each centroid appears `dim/16` times, shuffled. This is
        // what well-trained PQ codebooks approximate and makes the residual bias term
        // vanish structurally.
        let base: Vec<u8> = (0..dim).map(|i| (i % 16) as u8).collect();

        for _ in 0..n_vectors {
            let mut indices = base.clone();
            indices.shuffle(&mut rng);

            let true_dot: f32 = query
                .iter()
                .zip(indices.iter())
                .map(|(&q, &v)| q * codebook[v as usize])
                .sum();
            let packed = pack_nibbles(&indices);
            let quant_dot = simd_query.dotprod(&packed);

            assert!(
                (true_dot - quant_dot).abs() < tolerance,
                "quant dot {} too far from true dot {} (tol {})",
                quant_dot,
                true_dot,
                tolerance,
            );
        }
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

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_avx2_x2_matches_scalar() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (simd_query, vector) = random_inputs(&mut rng, dim);
            let scalar = simd_query.dotprod_raw(&vector);
            let avx2_x2 = unsafe { simd_query.dotprod_raw_avx2_x2(&vector) };
            assert_eq!(
                scalar, avx2_x2,
                "scalar {scalar} != avx2_x2 {avx2_x2} at dim {dim}"
            );
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
    fn test_avx_vnni_matches_scalar() {
        if !std::is_x86_feature_detected!("avxvnni") || !std::is_x86_feature_detected!("avx2") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (simd_query, vector) = random_inputs(&mut rng, dim);
            let scalar = simd_query.dotprod_raw(&vector);
            let vnni = unsafe { simd_query.dotprod_raw_avx_vnni(&vector) };
            assert_eq!(
                scalar, vnni,
                "scalar {scalar} != avx_vnni {vnni} at dim {dim}"
            );
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_avx_vnni_x2_matches_scalar() {
        if !std::is_x86_feature_detected!("avxvnni") || !std::is_x86_feature_detected!("avx2") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &dim in PARITY_DIMS {
            let (simd_query, vector) = random_inputs(&mut rng, dim);
            let scalar = simd_query.dotprod_raw(&vector);
            let vnni_x2 = unsafe { simd_query.dotprod_raw_avx_vnni_x2(&vector) };
            assert_eq!(
                scalar, vnni_x2,
                "scalar {scalar} != avx_vnni_x2 {vnni_x2} at dim {dim}"
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
            if std::is_x86_feature_detected!("avxvnni") && std::is_x86_feature_detected!("avx2") {
                let vnni = unsafe { query.dotprod_raw_avx_vnni(vector) };
                assert_eq!(expected, vnni, "avx_vnni mismatch (expected {expected})");
            }
        }
    }

    /// Build the worst-case positive-accumulation inputs.
    ///
    /// - Query: all 1.0 → `q_signed = 8191`, `q_biased = 16383`, low = high = 127 (7-bit max).
    /// - Codebook: entries 0 and 1 both land at `codebook_int = +127`; the remaining 14
    ///   entries are small negatives that balance the centroid mean to zero.
    /// - Vector: alternating indices 0 and 1 → every `_mm_maddubs_epi16` pair sees
    ///   (127 u8) × (127 i8) + (127 u8) × (127 i8) = 32 258 — 509 below i16 saturation.
    fn max_positive_inputs(dim: usize) -> (Query4bitSimd, Vec<u8>) {
        // Fourteen entries at −1/7 cancel the two entries at +1.0 → c_shift = 0,
        // c_abs_max = 1.0, c_scale = 127 → codebook_int[0] = codebook_int[1] = 127.
        let mut codebook = [-1.0_f32 / 7.0; 16];
        codebook[0] = 1.0;
        codebook[1] = 1.0;
        let query = vec![1.0_f32; dim];
        let indices: Vec<u8> = (0..dim).map(|i| (i % 2) as u8).collect();
        (
            Query4bitSimd::new(&query, &codebook),
            pack_nibbles(&indices),
        )
    }

    /// Mirror of `max_positive_inputs` with codebook_int[0] = codebook_int[1] = −127.
    /// Every maddubs pair = −32 258, one above the i16 minimum of −32 768.
    fn max_negative_inputs(dim: usize) -> (Query4bitSimd, Vec<u8>) {
        let mut codebook = [1.0_f32 / 7.0; 16];
        codebook[0] = -1.0;
        codebook[1] = -1.0;
        let query = vec![1.0_f32; dim];
        let indices: Vec<u8> = (0..dim).map(|i| (i % 2) as u8).collect();
        (
            Query4bitSimd::new(&query, &codebook),
            pack_nibbles(&indices),
        )
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
