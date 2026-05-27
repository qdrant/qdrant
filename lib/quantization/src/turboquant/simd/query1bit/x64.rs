//! x86_64 popcount paths for [`super::score_1bit_internal`].
//!
//! Three backends, picked at runtime by the dispatcher in `mod.rs`:
//!
//! * **SSE4.1 / SSSE3** — `pshufb`-based nibble-lookup popcount (Muła): split
//!   each byte into its two nibbles, `pshufb` a 16-entry popcount table,
//!   sum.  `psadbw` against zero horizontally sums 8 bytes into a u16 lane,
//!   which we accumulate into a u64 pair.
//!
//! * **AVX2** — same Muła trick on 32-byte YMM registers.
//!
//! * **AVX-512 VPOPCNTDQ** — `vpopcntq` in hardware: 8 × u64 popcounts per
//!   instruction, summed into a 512-bit u64 accumulator and reduced with
//!   `_mm512_reduce_add_epi64`.
//!
//! All three use a u64 accumulator so no intermediate can saturate at any
//! reasonable vector size (see `test_score_*_overflow_safety_64k`).

/// 16-byte popcount-of-nibble lookup table.  Index = nibble value,
/// value = number of 1-bits.  Broadcast to YMM/ZMM as needed.
const NIBBLE_POPCNT: [i8; 16] = [0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4];

/// Raw popcount of `a ⊕ b` using 16-byte SSE pshufb-nibble-lookup chunks
/// plus a scalar byte tail.  Shared between [`score_1bit_internal_sse`] and
/// the tail path of [`score_1bit_internal_avx512_vpopcntdq`].
///
/// # Safety
/// `a.len() == b.len()`.  CPU must support `ssse3` and `sse4.1`.
#[inline]
#[target_feature(enable = "sse4.1,ssse3")]
unsafe fn popcount_sse(a: &[u8], b: &[u8]) -> u64 {
    use core::arch::x86_64::*;

    unsafe {
        let lookup = _mm_loadu_si128(NIBBLE_POPCNT.as_ptr().cast::<__m128i>());
        let low_mask = _mm_set1_epi8(0x0F);
        let zero = _mm_setzero_si128();
        let mut acc = _mm_setzero_si128();

        let chunks = a.len() / 16;
        for i in 0..chunks {
            let va = _mm_loadu_si128(a.as_ptr().add(i * 16).cast::<__m128i>());
            let vb = _mm_loadu_si128(b.as_ptr().add(i * 16).cast::<__m128i>());
            let x = _mm_xor_si128(va, vb);

            let lo = _mm_and_si128(x, low_mask);
            let hi = _mm_and_si128(_mm_srli_epi16(x, 4), low_mask);
            let cnt_lo = _mm_shuffle_epi8(lookup, lo);
            let cnt_hi = _mm_shuffle_epi8(lookup, hi);
            // Per-byte popcount ≤ 8; two halves summed ≤ 16 per u8 — fits u8.
            let cnt = _mm_add_epi8(cnt_lo, cnt_hi);

            // `psadbw(cnt, 0)` horizontally sums 8 bytes into each u64 lane
            // (max 8 · 16 = 128 per lane per chunk — zero overflow risk).
            acc = _mm_add_epi64(acc, _mm_sad_epu8(cnt, zero));
        }

        let lo = _mm_cvtsi128_si64(acc) as u64;
        let hi = _mm_cvtsi128_si64(_mm_unpackhi_epi64(acc, acc)) as u64;
        let mut popcnt = lo + hi;

        let tail_start = chunks * 16;
        for i in tail_start..a.len() {
            popcnt += u64::from((a[i] ^ b[i]).count_ones());
        }
        popcnt
    }
}

/// SSE4.1 + SSSE3 implementation of [`super::score_1bit_internal`].
///
/// # Safety
/// CPU must support `ssse3` and `sse4.1`.
#[target_feature(enable = "sse4.1,ssse3")]
pub unsafe fn score_1bit_internal_sse(a: &[u8], b: &[u8]) -> f32 {
    assert_eq!(
        a.len(),
        b.len(),
        "score_1bit_internal_sse: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );
    super::popcount_to_score(a.len(), unsafe { popcount_sse(a, b) })
}

/// AVX2 implementation of [`super::score_1bit_internal`].
///
/// Tail after the 32-byte bulk loop (up to 31 bytes) is routed through
/// [`popcount_sse`] — at most 1 SSE chunk + scalar bytes, still cheaper
/// than a 31-iteration scalar loop on short vectors.
///
/// # Safety
/// CPU must support `avx2`, `ssse3`, and `sse4.1`.
#[target_feature(enable = "avx2,sse4.1,ssse3")]
pub unsafe fn score_1bit_internal_avx2(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::x86_64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_1bit_internal_avx2: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    unsafe {
        // Broadcast the 16-byte lookup into both 128-bit halves — pshufb
        // operates per-lane, so each half needs its own copy of the table.
        let lookup_half = _mm_loadu_si128(NIBBLE_POPCNT.as_ptr().cast::<__m128i>());
        let lookup = _mm256_set_m128i(lookup_half, lookup_half);
        let low_mask = _mm256_set1_epi8(0x0F);
        let zero = _mm256_setzero_si256();
        let mut acc = _mm256_setzero_si256();

        let chunks = a.len() / 32;
        for i in 0..chunks {
            let va = _mm256_loadu_si256(a.as_ptr().add(i * 32).cast::<__m256i>());
            let vb = _mm256_loadu_si256(b.as_ptr().add(i * 32).cast::<__m256i>());
            let x = _mm256_xor_si256(va, vb);

            let lo = _mm256_and_si256(x, low_mask);
            let hi = _mm256_and_si256(_mm256_srli_epi16(x, 4), low_mask);
            let cnt_lo = _mm256_shuffle_epi8(lookup, lo);
            let cnt_hi = _mm256_shuffle_epi8(lookup, hi);
            let cnt = _mm256_add_epi8(cnt_lo, cnt_hi);

            let sum64 = _mm256_sad_epu8(cnt, zero);
            acc = _mm256_add_epi64(acc, sum64);
        }

        // Reduce 4 × u64 → scalar u64 via two 128-bit halves.
        let lo128 = _mm256_castsi256_si128(acc);
        let hi128 = _mm256_extracti128_si256(acc, 1);
        let sum128 = _mm_add_epi64(lo128, hi128);
        let lo = _mm_cvtsi128_si64(sum128) as u64;
        let hi = _mm_cvtsi128_si64(_mm_unpackhi_epi64(sum128, sum128)) as u64;
        let mut popcnt = lo + hi;

        let tail_start = chunks * 32;
        popcnt += popcount_sse(&a[tail_start..], &b[tail_start..]);

        super::popcount_to_score(a.len(), popcnt)
    }
}

/// AVX-512 VPOPCNTDQ implementation of [`super::score_1bit_internal`].
///
/// Tail after the 64-byte bulk loop (up to 63 bytes) is handled via the
/// [`popcount_sse`] helper — 3 SSE chunks + scalar bytes is ~10× cheaper
/// than a 63-iteration scalar loop when the tail is non-trivial.
///
/// # Safety
/// CPU must support `avx512f`, `avx512vpopcntdq`, `ssse3`, and `sse4.1`.
#[target_feature(enable = "avx512f,avx512vpopcntdq,sse4.1,ssse3")]
pub unsafe fn score_1bit_internal_avx512_vpopcntdq(a: &[u8], b: &[u8]) -> f32 {
    use core::arch::x86_64::*;

    assert_eq!(
        a.len(),
        b.len(),
        "score_1bit_internal_avx512_vpopcntdq: vector length mismatch ({} vs {})",
        a.len(),
        b.len(),
    );

    unsafe {
        let mut acc = _mm512_setzero_si512();
        let chunks = a.len() / 64;
        for i in 0..chunks {
            let va = _mm512_loadu_si512(a.as_ptr().add(i * 64).cast::<__m512i>());
            let vb = _mm512_loadu_si512(b.as_ptr().add(i * 64).cast::<__m512i>());
            let x = _mm512_xor_si512(va, vb);
            let cnt = _mm512_popcnt_epi64(x);
            acc = _mm512_add_epi64(acc, cnt);
        }
        let mut popcnt = _mm512_reduce_add_epi64(acc) as u64;

        let tail_start = chunks * 64;
        popcnt += popcount_sse(&a[tail_start..], &b[tail_start..]);

        super::popcount_to_score(a.len(), popcnt)
    }
}

impl<const BITS: usize> super::Query1bitSimd<BITS> {
    /// SSE4.1 + SSSE3 implementation of
    /// [`super::Query1bitSimd::dotprod_raw`].
    ///
    /// Per block: load 16-byte data chunk, then for each of `BITS` planes
    /// `pshufb`-nibble-lookup popcount of `AND` reduced via `psadbw` into
    /// u64 pair; accumulate in `[__m128i; BITS]` regs (one per plane).
    ///
    /// # Safety
    /// CPU must support `ssse3` and `sse4.1`.
    #[target_feature(enable = "sse4.1,ssse3")]
    pub unsafe fn dotprod_raw_sse(&self, vector: &[u8]) -> i64 {
        use core::arch::x86_64::*;

        unsafe {
            let lookup = _mm_loadu_si128(NIBBLE_POPCNT.as_ptr().cast::<__m128i>());
            let low_mask = _mm_set1_epi8(0x0F);
            let zero = _mm_setzero_si128();
            let mut acc: [__m128i; BITS] = core::array::from_fn(|_| _mm_setzero_si128());

            // Main loop: full blocks from the vector directly.
            for block_idx in 0..self.num_full_blocks() {
                let data = _mm_loadu_si128(
                    vector
                        .as_ptr()
                        .add(block_idx * super::BLOCK_BYTES)
                        .cast::<__m128i>(),
                );
                let block_base = block_idx * BITS * super::BLOCK_BYTES;
                for (b, acc_b) in acc.iter_mut().enumerate() {
                    let plane = _mm_loadu_si128(
                        self.planes
                            .as_ptr()
                            .add(block_base + b * super::BLOCK_BYTES)
                            .cast::<__m128i>(),
                    );
                    let x = _mm_and_si128(data, plane);
                    let lo = _mm_and_si128(x, low_mask);
                    let hi = _mm_and_si128(_mm_srli_epi16(x, 4), low_mask);
                    let cnt_lo = _mm_shuffle_epi8(lookup, lo);
                    let cnt_hi = _mm_shuffle_epi8(lookup, hi);
                    let cnt = _mm_add_epi8(cnt_lo, cnt_hi);
                    *acc_b = _mm_add_epi64(*acc_b, _mm_sad_epu8(cnt, zero));
                }
            }

            // Partial tail block via zero-padded stack buffer.
            if let Some((buf, block_idx)) = self.tail_block_scratch(vector) {
                let data = _mm_loadu_si128(buf.as_ptr().cast::<__m128i>());
                let block_base = block_idx * BITS * super::BLOCK_BYTES;
                for (b, acc_b) in acc.iter_mut().enumerate() {
                    let plane = _mm_loadu_si128(
                        self.planes
                            .as_ptr()
                            .add(block_base + b * super::BLOCK_BYTES)
                            .cast::<__m128i>(),
                    );
                    let x = _mm_and_si128(data, plane);
                    let lo = _mm_and_si128(x, low_mask);
                    let hi = _mm_and_si128(_mm_srli_epi16(x, 4), low_mask);
                    let cnt_lo = _mm_shuffle_epi8(lookup, lo);
                    let cnt_hi = _mm_shuffle_epi8(lookup, hi);
                    let cnt = _mm_add_epi8(cnt_lo, cnt_hi);
                    *acc_b = _mm_add_epi64(*acc_b, _mm_sad_epu8(cnt, zero));
                }
            }

            reduce_planes::<BITS>(&acc)
        }
    }

    /// AVX-512 VPOPCNTDQ (on XMM via AVX-512VL) implementation of
    /// [`super::Query1bitSimd::dotprod_raw`].
    ///
    /// Replaces the Muła nibble-lookup with hardware `_mm_popcnt_epi64`
    /// (one instruction per block per plane).  Block stays at 16 bytes
    /// since the interleave layout keeps plane chunks at 16-byte granularity.
    ///
    /// # Safety
    /// CPU must support `avx512vl` and `avx512vpopcntdq` (and thus SSE2 for
    /// the XMM load/store pairs).
    #[target_feature(enable = "avx512vl,avx512vpopcntdq")]
    pub unsafe fn dotprod_raw_avx512_vpopcntdq(&self, vector: &[u8]) -> i64 {
        use core::arch::x86_64::*;

        unsafe {
            let mut acc: [__m128i; BITS] = core::array::from_fn(|_| _mm_setzero_si128());

            for block_idx in 0..self.num_full_blocks() {
                let data = _mm_loadu_si128(
                    vector
                        .as_ptr()
                        .add(block_idx * super::BLOCK_BYTES)
                        .cast::<__m128i>(),
                );
                let block_base = block_idx * BITS * super::BLOCK_BYTES;
                for (b, acc_b) in acc.iter_mut().enumerate() {
                    let plane = _mm_loadu_si128(
                        self.planes
                            .as_ptr()
                            .add(block_base + b * super::BLOCK_BYTES)
                            .cast::<__m128i>(),
                    );
                    let cnt = _mm_popcnt_epi64(_mm_and_si128(data, plane));
                    *acc_b = _mm_add_epi64(*acc_b, cnt);
                }
            }

            // Partial tail block via zero-padded stack buffer.
            if let Some((buf, block_idx)) = self.tail_block_scratch(vector) {
                let data = _mm_loadu_si128(buf.as_ptr().cast::<__m128i>());
                let block_base = block_idx * BITS * super::BLOCK_BYTES;
                for (b, acc_b) in acc.iter_mut().enumerate() {
                    let plane = _mm_loadu_si128(
                        self.planes
                            .as_ptr()
                            .add(block_base + b * super::BLOCK_BYTES)
                            .cast::<__m128i>(),
                    );
                    let cnt = _mm_popcnt_epi64(_mm_and_si128(data, plane));
                    *acc_b = _mm_add_epi64(*acc_b, cnt);
                }
            }

            reduce_planes::<BITS>(&acc)
        }
    }
}

/// Reduce `[__m128i; BITS]` plane accumulators (each holding 2 × u64
/// popcount lanes) into the weighted `v_dot_q` integer sum.
///
/// # Safety
/// Caller must have enabled at least SSE2 (true of every caller here).
#[inline]
#[target_feature(enable = "sse2")]
unsafe fn reduce_planes<const BITS: usize>(acc: &[core::arch::x86_64::__m128i; BITS]) -> i64 {
    use core::arch::x86_64::*;

    let mut v_dot_q: i64 = 0;
    for (b, acc_b) in acc.iter().enumerate() {
        let lo = _mm_cvtsi128_si64(*acc_b) as u64;
        let hi = _mm_cvtsi128_si64(_mm_unpackhi_epi64(*acc_b, *acc_b)) as u64;
        let popcnt = lo + hi;
        let w_b: i64 = if b == BITS - 1 {
            -(1i64 << (BITS - 1))
        } else {
            1i64 << b
        };
        v_dot_q += w_b * popcnt as i64;
    }
    v_dot_q
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::super::shared::random_bytes;
    use super::super::score_1bit_internal_scalar;
    use super::super::shared::PARITY_BYTE_LENS;
    use super::*;

    #[test]
    fn test_score_sse_matches_scalar() {
        if !std::is_x86_feature_detected!("ssse3") || !std::is_x86_feature_detected!("sse4.1") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &byte_len in PARITY_BYTE_LENS {
            let a = random_bytes(&mut rng, byte_len);
            let b = random_bytes(&mut rng, byte_len);
            let scalar = score_1bit_internal_scalar(&a, &b);
            let got = unsafe { score_1bit_internal_sse(&a, &b) };
            assert_eq!(
                scalar.to_bits(),
                got.to_bits(),
                "sse mismatch at byte_len={byte_len}",
            );
        }
    }

    #[test]
    fn test_score_avx2_matches_scalar() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &byte_len in PARITY_BYTE_LENS {
            let a = random_bytes(&mut rng, byte_len);
            let b = random_bytes(&mut rng, byte_len);
            let scalar = score_1bit_internal_scalar(&a, &b);
            let got = unsafe { score_1bit_internal_avx2(&a, &b) };
            assert_eq!(
                scalar.to_bits(),
                got.to_bits(),
                "avx2 mismatch at byte_len={byte_len}",
            );
        }
    }

    #[test]
    fn test_score_avx512_vpopcntdq_matches_scalar() {
        if !(std::is_x86_feature_detected!("avx512f")
            && std::is_x86_feature_detected!("avx512vpopcntdq"))
        {
            return;
        }
        let mut rng = StdRng::seed_from_u64(7);
        for &byte_len in PARITY_BYTE_LENS {
            let a = random_bytes(&mut rng, byte_len);
            let b = random_bytes(&mut rng, byte_len);
            let scalar = score_1bit_internal_scalar(&a, &b);
            let got = unsafe { score_1bit_internal_avx512_vpopcntdq(&a, &b) };
            assert_eq!(
                scalar.to_bits(),
                got.to_bits(),
                "avx512 mismatch at byte_len={byte_len}",
            );
        }
    }

    /// Overflow safety at 64 KiB (524 288 bits) with `a = all 0xFF`,
    /// `b = all 0x00` — every bit disagrees, so `popcnt = n_bits`.  Each
    /// SIMD path must match scalar (u64 throughout) exactly; a mismatch
    /// would mean an intermediate `u16`/`u32`/u64 lane overflowed.
    #[test]
    fn test_score_overflow_safety_64k() {
        let byte_len = 65_536 / 8;
        let a = vec![0xFF_u8; byte_len];
        let b = vec![0x00_u8; byte_len];
        let scalar = score_1bit_internal_scalar(&a, &b);

        unsafe {
            if std::is_x86_feature_detected!("ssse3") && std::is_x86_feature_detected!("sse4.1") {
                let sse = score_1bit_internal_sse(&a, &b);
                assert_eq!(scalar.to_bits(), sse.to_bits(), "sse overflow at 64k");
            }
            if std::is_x86_feature_detected!("avx2") {
                let avx2 = score_1bit_internal_avx2(&a, &b);
                assert_eq!(scalar.to_bits(), avx2.to_bits(), "avx2 overflow at 64k");
            }
            if std::is_x86_feature_detected!("avx512f")
                && std::is_x86_feature_detected!("avx512vpopcntdq")
            {
                let avx512 = score_1bit_internal_avx512_vpopcntdq(&a, &b);
                assert_eq!(scalar.to_bits(), avx512.to_bits(), "avx512 overflow at 64k");
            }
        }
    }

    /// Parity of `Query1bitSimd::dotprod_raw_{sse, avx512_vpopcntdq}` vs
    /// the scalar kernel across several BITS values and dims.
    #[test]
    fn test_query_dotprod_x86_matches_scalar() {
        use rand_distr::{Distribution, StandardNormal};

        use super::super::Query1bitSimd;

        fn check<const BITS: usize>(dim: usize, seed: u64) {
            let mut rng = StdRng::seed_from_u64(seed);
            let query: Vec<f32> = (0..dim).map(|_| StandardNormal.sample(&mut rng)).collect();
            let data = random_bytes(&mut rng, dim / 8);
            let q = Query1bitSimd::<BITS>::new(&query);
            let scalar = q.dotprod_raw(&data);

            if std::is_x86_feature_detected!("ssse3") && std::is_x86_feature_detected!("sse4.1") {
                let sse = unsafe { q.dotprod_raw_sse(&data) };
                assert_eq!(scalar, sse, "BITS={BITS} dim={dim}: sse mismatch");
            }
            if std::is_x86_feature_detected!("avx512vl")
                && std::is_x86_feature_detected!("avx512vpopcntdq")
            {
                let avx512 = unsafe { q.dotprod_raw_avx512_vpopcntdq(&data) };
                assert_eq!(scalar, avx512, "BITS={BITS} dim={dim}: avx512 mismatch");
            }
        }

        for &dim in &[128usize, 256, 384, 512, 1024, 2048] {
            check::<8>(dim, 0xCAFE);
            check::<10>(dim, 0xBEEF);
            check::<12>(dim, 0xDEAD);
        }
    }

    /// Overflow safety at dim=64K with max-magnitude query against all-1
    /// data.  Each SIMD path (when available on the CPU) must match scalar
    /// exactly; a mismatch would mean an intermediate `u32` per-plane
    /// accumulator (or the u64 lane in the VPOPCNTDQ variant) saturated.
    #[test]
    fn test_query_dotprod_x86_overflow_safety_64k() {
        use super::super::Query1bitSimd;

        let dim = 65_536;
        let query = vec![1.0_f32; dim];
        let data = vec![0xFFu8; dim / 8];

        fn check<const BITS: usize>(query: &[f32], data: &[u8]) {
            let q = Query1bitSimd::<BITS>::new(query);
            let scalar = q.dotprod_raw(data);

            if std::is_x86_feature_detected!("ssse3") && std::is_x86_feature_detected!("sse4.1") {
                let sse = unsafe { q.dotprod_raw_sse(data) };
                assert_eq!(scalar, sse, "BITS={BITS} sse overflow at 64k");
            }
            if std::is_x86_feature_detected!("avx512vl")
                && std::is_x86_feature_detected!("avx512vpopcntdq")
            {
                let avx512 = unsafe { q.dotprod_raw_avx512_vpopcntdq(data) };
                assert_eq!(scalar, avx512, "BITS={BITS} avx512 overflow at 64k");
            }
        }

        check::<8>(&query, &data);
        check::<16>(&query, &data);
    }
}
