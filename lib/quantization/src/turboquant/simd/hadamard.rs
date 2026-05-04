//! SIMD Fast Walsh–Hadamard Transform on `f64`.
//!
//! Bit-equal to the scalar reference at
//! [`crate::turboquant::rotation::in_place_walsh_hadamard_transform`] — same
//! pair order, single add/sub ops per butterfly, no FMA, no associativity
//! reordering. Verified by [`tests`] across multiple seeds at every size in
//! `[4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192]`.
//!
//! Variants:
//! * x86_64 / AVX2 ([`simd_x86`]):
//!   * [`simd_x86::wht_avx2`] — fuses stages h=1, 2 in 4-element AVX2 blocks
//!     (one YMM); each h ≥ 4 stage is its own pass over the array.
//!   * [`simd_x86::wht_avx2_radix16_4x`] — radix-16 inner kernel + 4×
//!     hand-unrolled outer butterfly loop for h ≥ 16.
//! * aarch64 / NEON ([`simd_arm`]):
//!   * [`simd_arm::wht_neon`] — fuses stages h=1, 2 in 4-element NEON blocks
//!     (two `float64x2_t`); each h ≥ 4 stage is its own pass over the array.
//!   * [`simd_arm::wht_neon_radix16_4x`] — radix-16 inner kernel + 4×
//!     hand-unrolled outer butterfly loop for h ≥ 16.

#[cfg(target_arch = "x86_64")]
pub mod simd_x86 {
    use core::arch::x86_64::*;

    use super::scalar_wht;

    /// Apply the WHT stages h=1 and h=2 to a single 4-element YMM, in register.
    ///
    /// Input  `v  = [v0, v1, v2, v3]` (4 contiguous doubles).
    /// Output    `= [v0+v1+v2+v3, v0-v1+v2-v3, v0+v1-v2-v3, v0-v1-v2+v3]`,
    /// matching the scalar reference's stages h=1 and h=2 over those 4
    /// elements with single add/sub ops in the same pair order — bit-equal.
    #[inline]
    #[target_feature(enable = "avx,avx2")]
    unsafe fn wht_h1h2_in_ymm(v: __m256d) -> __m256d {
        // h = 1: pair butterfly within each 2-element half.
        //   v       = [v0,    v1,    v2,    v3   ]
        //   v_swap1 = [v1,    v0,    v3,    v2   ]
        //   addsub  = [v0-v1, v0+v1, v2-v3, v2+v3]
        //   h1      = [v0+v1, v0-v1, v2+v3, v2-v3]
        let v_swap1 = _mm256_permute_pd(v, 0b0101);
        let h1 = _mm256_permute_pd(_mm256_addsub_pd(v, v_swap1), 0b0101);

        // h = 2: butterfly between the two 2-element halves.
        //   h1      = [a,   b,   c,   d  ]   (a=v0+v1, b=v0-v1, c=v2+v3, d=v2-v3)
        //   v_swap2 = [c,   d,   a,   b  ]
        //   plus    = [a+c, b+d, c+a, d+b]
        //   minus   = [c-a, d-b, a-c, b-d]   (note: v_swap2 - h1)
        //   h2      = [a+c, b+d, a-c, b-d]
        let v_swap2 = _mm256_permute2f128_pd(h1, h1, 0x01);
        let plus = _mm256_add_pd(h1, v_swap2);
        let minus = _mm256_sub_pd(v_swap2, h1);
        _mm256_blend_pd(plus, minus, 0b1100)
    }

    /// AVX2 in-place Fast Walsh–Hadamard Transform on `f64`.
    ///
    /// Length must be a power of two. Operates by:
    /// * `n < 8`  — scalar fallback (too small for register-width work).
    /// * `h ∈ {1, 2}` — in-register butterflies on 4-element blocks.
    /// * `h ≥ 4`  — straight-line vectorized butterflies between two SIMD
    ///   lanes of 4 doubles each.
    ///
    /// The pair structure of each stage is identical to the scalar loop, so
    /// the output is bit-equal to `in_place_walsh_hadamard_transform` (no
    /// floating-point reassociation).
    ///
    /// # Safety
    /// CPU must support `avx` and `avx2`.
    #[target_feature(enable = "avx,avx2")]
    pub unsafe fn wht_avx2(x: &mut [f64]) {
        let n = x.len();
        debug_assert!(n.is_power_of_two(), "WHT requires power-of-2 length");

        if n < 8 {
            scalar_wht(x);
            return;
        }

        unsafe {
            // Stages h = 1 and h = 2 fused into one pass over 4-element blocks.
            let mut i = 0;
            while i + 4 <= n {
                let p = x.as_mut_ptr().add(i);
                _mm256_storeu_pd(p, wht_h1h2_in_ymm(_mm256_loadu_pd(p)));
                i += 4;
            }

            // Stages h ≥ 4: each butterfly pair is `(x[k], x[k+h])` and both
            // sides are 4-double-aligned within the stride, so a plain
            // load/add/sub/store at lane width works.
            outer_butterfly_stages(x, 4);
        }
    }

    /// In-register radix-16 WHT kernel: applies stages h=1, 2, 4, 8 to 16
    /// contiguous f64 starting at `p`, fused into one load / compute / store
    /// pass over 4 YMM regs.
    ///
    /// Pair members at h=4 and h=8 match the scalar memory layout exactly
    /// (each YMM holds 4 contiguous doubles, so `(r0,r1)` is positions 0–3
    /// vs 4–7 and `(r0..r1, r2..r3)` is 0–7 vs 8–15) — bit-equal output to
    /// the scalar reference.
    ///
    /// # Safety
    /// `p` must point to ≥ 16 writable f64. CPU must support `avx,avx2`.
    #[inline]
    #[target_feature(enable = "avx,avx2")]
    unsafe fn radix16_block(p: *mut f64) {
        unsafe {
            let r0 = _mm256_loadu_pd(p);
            let r1 = _mm256_loadu_pd(p.add(4));
            let r2 = _mm256_loadu_pd(p.add(8));
            let r3 = _mm256_loadu_pd(p.add(12));

            // Stages h=1, h=2 applied independently to each YMM — 4 parallel
            // dep chains, perfect ILP.
            let r0 = wht_h1h2_in_ymm(r0);
            let r1 = wht_h1h2_in_ymm(r1);
            let r2 = wht_h1h2_in_ymm(r2);
            let r3 = wht_h1h2_in_ymm(r3);

            // Stage h=4: register-to-register butterflies between paired YMMs
            // (r0,r1) and (r2,r3). No permutes.
            let s0 = _mm256_add_pd(r0, r1);
            let s1 = _mm256_sub_pd(r0, r1);
            let s2 = _mm256_add_pd(r2, r3);
            let s3 = _mm256_sub_pd(r2, r3);

            // Stage h=8: register-to-register butterflies between (s0,s2) and
            // (s1,s3). Maintains scalar pair order: positions [0..7] paired
            // with [8..15].
            let t0 = _mm256_add_pd(s0, s2);
            let t1 = _mm256_add_pd(s1, s3);
            let t2 = _mm256_sub_pd(s0, s2);
            let t3 = _mm256_sub_pd(s1, s3);

            _mm256_storeu_pd(p, t0);
            _mm256_storeu_pd(p.add(4), t1);
            _mm256_storeu_pd(p.add(8), t2);
            _mm256_storeu_pd(p.add(12), t3);
        }
    }

    /// Run the standard SIMD outer butterfly loop for stages h = `start_h`,
    /// 2·`start_h`, …, n/2.
    ///
    /// # Safety
    /// CPU must support `avx,avx2`. `start_h` must be ≥ 4 and a power of two,
    /// `x.len()` must be a power of two ≥ 2·`start_h` (otherwise the loop
    /// body never runs, which is fine).
    #[inline]
    #[target_feature(enable = "avx,avx2")]
    unsafe fn outer_butterfly_stages(x: &mut [f64], start_h: usize) {
        unsafe {
            let n = x.len();
            let mut h = start_h;
            while h < n {
                let stride = 2 * h;
                let mut block = 0;
                while block < n {
                    let mut j = 0;
                    while j < h {
                        let pa = x.as_mut_ptr().add(block + j);
                        let pb = x.as_mut_ptr().add(block + j + h);
                        let a = _mm256_loadu_pd(pa);
                        let b = _mm256_loadu_pd(pb);
                        _mm256_storeu_pd(pa, _mm256_add_pd(a, b));
                        _mm256_storeu_pd(pb, _mm256_sub_pd(a, b));
                        j += 4;
                    }
                    block += stride;
                }
                h *= 2;
            }
        }
    }

    /// 4× hand-unrolled outer butterfly loop. Inner iteration processes 16
    /// doubles (4 YMM pairs) — 8 in-flight loads, 4 adds, 4 subs, 8 stores per
    /// iteration. At `h = 16` the unrolled body covers the whole stage in one
    /// iteration with no loop. At larger h it improves ILP by giving the OoO
    /// engine four independent dep chains.
    ///
    /// # Safety
    /// CPU must support `avx,avx2`. `start_h` must be ≥ 16 and a power of two,
    /// `x.len()` must be a power of two ≥ 2·`start_h`. Caller guarantees h is
    /// always a multiple of 16 inside the inner loop, so no scalar tail.
    #[inline]
    #[target_feature(enable = "avx,avx2")]
    unsafe fn outer_butterfly_stages_4x(x: &mut [f64], start_h: usize) {
        unsafe {
            let n = x.len();
            let mut h = start_h;
            while h < n {
                let stride = 2 * h;
                let mut block = 0;
                while block < n {
                    let mut j = 0;
                    while j < h {
                        let p = x.as_mut_ptr().add(block + j);
                        let q = p.add(h);

                        let a0 = _mm256_loadu_pd(p);
                        let a1 = _mm256_loadu_pd(p.add(4));
                        let a2 = _mm256_loadu_pd(p.add(8));
                        let a3 = _mm256_loadu_pd(p.add(12));
                        let b0 = _mm256_loadu_pd(q);
                        let b1 = _mm256_loadu_pd(q.add(4));
                        let b2 = _mm256_loadu_pd(q.add(8));
                        let b3 = _mm256_loadu_pd(q.add(12));

                        _mm256_storeu_pd(p, _mm256_add_pd(a0, b0));
                        _mm256_storeu_pd(p.add(4), _mm256_add_pd(a1, b1));
                        _mm256_storeu_pd(p.add(8), _mm256_add_pd(a2, b2));
                        _mm256_storeu_pd(p.add(12), _mm256_add_pd(a3, b3));
                        _mm256_storeu_pd(q, _mm256_sub_pd(a0, b0));
                        _mm256_storeu_pd(q.add(4), _mm256_sub_pd(a1, b1));
                        _mm256_storeu_pd(q.add(8), _mm256_sub_pd(a2, b2));
                        _mm256_storeu_pd(q.add(12), _mm256_sub_pd(a3, b3));

                        j += 16;
                    }
                    block += stride;
                }
                h *= 2;
            }
        }
    }

    /// AVX2 WHT with radix-16 in-register fusion *and* 4×-unrolled outer
    /// butterfly stages.
    ///
    /// # Safety
    /// CPU must support `avx` and `avx2`. Length must be a power of two.
    #[target_feature(enable = "avx,avx2")]
    pub unsafe fn wht_avx2_radix16_4x(x: &mut [f64]) {
        let n = x.len();
        debug_assert!(n.is_power_of_two(), "WHT requires power-of-2 length");

        if n < 8 {
            scalar_wht(x);
            return;
        }

        unsafe {
            if n == 8 {
                let p = x.as_mut_ptr();
                let v0 = wht_h1h2_in_ymm(_mm256_loadu_pd(p));
                let v1 = wht_h1h2_in_ymm(_mm256_loadu_pd(p.add(4)));
                _mm256_storeu_pd(p, _mm256_add_pd(v0, v1));
                _mm256_storeu_pd(p.add(4), _mm256_sub_pd(v0, v1));
                return;
            }

            let mut i = 0;
            while i + 16 <= n {
                radix16_block(x.as_mut_ptr().add(i));
                i += 16;
            }

            // n ≥ 16: outer stages start at h=16, always a multiple of 16.
            // n=16 has no outer stages (h=16 is == n, loop doesn't fire).
            outer_butterfly_stages_4x(x, 16);
        }
    }
}

#[cfg(target_arch = "aarch64")]
pub mod simd_arm {
    use core::arch::aarch64::*;

    use super::scalar_wht;

    /// Apply the WHT stages h=1 and h=2 to 4 contiguous f64 at `p`, returning
    /// the result in 2 NEON registers in natural `[pos i, pos i+1]` order.
    ///
    /// Input  `[a0, a1, a2, a3]`. Output:
    ///   `r0 = [a0+a1+a2+a3, a0-a1+a2-a3]`  (positions 0, 1 post-h=2)
    ///   `r1 = [a0+a1-a2-a3, a0-a1-a2+a3]`  (positions 2, 3 post-h=2)
    ///
    /// Uses `vld2q_f64` to deinterleave the load — the h=1 pair grouping
    /// `(a0,a1), (a2,a3)` comes "for free" with the load — then `vuzp1q_f64`
    /// / `vuzp2q_f64` to re-interleave to natural order before h=2.
    /// Output is bit-equal to the scalar reference's stages h=1 and h=2 over
    /// those 4 elements with single add/sub ops in the same pair order.
    ///
    /// # Safety
    /// `p` must point to ≥ 4 readable f64. CPU must support `neon`.
    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn wht_h1h2_block_neon(p: *mut f64) -> [float64x2_t; 2] {
        unsafe {
            // val.0 = [a0, a2], val.1 = [a1, a3] (deinterleaved by vld2q).
            let val = vld2q_f64(p);
            // h = 1: butterfly within each (a0,a1) and (a2,a3) pair.
            //   e = [a0+a1, a2+a3]  = post-h=1 [pos 0, pos 2]
            //   o = [a0-a1, a2-a3]  = post-h=1 [pos 1, pos 3]
            let e = vaddq_f64(val.0, val.1);
            let o = vsubq_f64(val.0, val.1);
            // Re-interleave to natural [pos 0, pos 1] / [pos 2, pos 3] form.
            let lo = vuzp1q_f64(e, o);
            let hi = vuzp2q_f64(e, o);
            // h = 2: cross-vector butterfly between (lo, hi).
            [vaddq_f64(lo, hi), vsubq_f64(lo, hi)]
        }
    }

    /// NEON in-place Fast Walsh–Hadamard Transform on `f64`.
    ///
    /// Length must be a power of two. Operates by:
    /// * `n < 8`  — scalar fallback (too small for register-width work).
    /// * `h ∈ {1, 2}` — `wht_h1h2_block_neon` per 4-element block.
    /// * `h ≥ 4`  — straight-line vectorized butterflies between two SIMD
    ///   lanes of 2 doubles each.
    ///
    /// The pair structure of each stage is identical to the scalar loop, so
    /// the output is bit-equal to `in_place_walsh_hadamard_transform` (no
    /// floating-point reassociation).
    ///
    /// # Safety
    /// CPU must support `neon`.
    #[target_feature(enable = "neon")]
    pub unsafe fn wht_neon(x: &mut [f64]) {
        let n = x.len();
        debug_assert!(n.is_power_of_two(), "WHT requires power-of-2 length");

        if n < 8 {
            scalar_wht(x);
            return;
        }

        unsafe {
            // Stages h = 1 and h = 2 fused into one pass over 4-element blocks.
            let mut i = 0;
            while i + 4 <= n {
                let p = x.as_mut_ptr().add(i);
                let [r0, r1] = wht_h1h2_block_neon(p);
                vst1q_f64(p, r0);
                vst1q_f64(p.add(2), r1);
                i += 4;
            }

            // Stages h ≥ 4: each butterfly pair is `(x[k], x[k+h])` and both
            // sides are 2-double-aligned within the stride, so a plain
            // load/add/sub/store at lane width works.
            outer_butterfly_stages(x, 4);
        }
    }

    /// In-register radix-8 WHT kernel: applies stages h=1, 2, 4 to 8
    /// contiguous f64 at `p`, fused into one load / compute / store pass over
    /// 4 NEON regs.
    ///
    /// # Safety
    /// `p` must point to ≥ 8 writable f64. CPU must support `neon`.
    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn radix8_block(p: *mut f64) {
        unsafe {
            // h = 1, h = 2 in two parallel 4-element subblocks.
            let [r0, r1] = wht_h1h2_block_neon(p);
            let [r2, r3] = wht_h1h2_block_neon(p.add(4));

            // h = 4: cross-block butterfly. Pair members are positions 0..3
            // vs 4..7 — lane-wise in scalar pair order.
            let t0 = vaddq_f64(r0, r2);
            let t1 = vaddq_f64(r1, r3);
            let t2 = vsubq_f64(r0, r2);
            let t3 = vsubq_f64(r1, r3);

            vst1q_f64(p, t0);
            vst1q_f64(p.add(2), t1);
            vst1q_f64(p.add(4), t2);
            vst1q_f64(p.add(6), t3);
        }
    }

    /// In-register radix-16 WHT kernel: applies stages h=1, 2, 4, 8 to 16
    /// contiguous f64 starting at `p`, fused into one load / compute / store
    /// pass over 8 NEON regs.
    ///
    /// Pair members at h=4 and h=8 match the scalar memory layout exactly
    /// (each NEON reg holds 2 contiguous doubles, paired post-h=2 vectors
    /// `(r0..r1, r2..r3)` are positions 0–3 vs 4–7, paired blocks
    /// `(r0..r3, r4..r7)` are 0–7 vs 8–15) — bit-equal output to the scalar
    /// reference.
    ///
    /// # Safety
    /// `p` must point to ≥ 16 writable f64. CPU must support `neon`.
    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn radix16_block(p: *mut f64) {
        unsafe {
            // h = 1, h = 2 in four parallel 4-element subblocks — 4 parallel
            // dep chains, perfect ILP. After this each (r_2k, r_2k+1) pair
            // holds 4 consecutive post-h=2 positions.
            let [r0, r1] = wht_h1h2_block_neon(p);
            let [r2, r3] = wht_h1h2_block_neon(p.add(4));
            let [r4, r5] = wht_h1h2_block_neon(p.add(8));
            let [r6, r7] = wht_h1h2_block_neon(p.add(12));

            // h = 4: register-to-register butterflies between paired sub-blocks
            // (r0..r1, r2..r3) and (r4..r5, r6..r7). No permutes.
            let s0 = vaddq_f64(r0, r2);
            let s1 = vaddq_f64(r1, r3);
            let s2 = vsubq_f64(r0, r2);
            let s3 = vsubq_f64(r1, r3);
            let s4 = vaddq_f64(r4, r6);
            let s5 = vaddq_f64(r5, r7);
            let s6 = vsubq_f64(r4, r6);
            let s7 = vsubq_f64(r5, r7);

            // h = 8: register-to-register butterflies between (s0..s3) and
            // (s4..s7). Maintains scalar pair order: positions [0..7] paired
            // with [8..15].
            let t0 = vaddq_f64(s0, s4);
            let t1 = vaddq_f64(s1, s5);
            let t2 = vaddq_f64(s2, s6);
            let t3 = vaddq_f64(s3, s7);
            let t4 = vsubq_f64(s0, s4);
            let t5 = vsubq_f64(s1, s5);
            let t6 = vsubq_f64(s2, s6);
            let t7 = vsubq_f64(s3, s7);

            vst1q_f64(p, t0);
            vst1q_f64(p.add(2), t1);
            vst1q_f64(p.add(4), t2);
            vst1q_f64(p.add(6), t3);
            vst1q_f64(p.add(8), t4);
            vst1q_f64(p.add(10), t5);
            vst1q_f64(p.add(12), t6);
            vst1q_f64(p.add(14), t7);
        }
    }

    /// Run the standard SIMD outer butterfly loop for stages h = `start_h`,
    /// 2·`start_h`, …, n/2.
    ///
    /// # Safety
    /// CPU must support `neon`. `start_h` must be ≥ 2 and a power of two,
    /// `x.len()` must be a power of two ≥ 2·`start_h` (otherwise the loop
    /// body never runs, which is fine).
    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn outer_butterfly_stages(x: &mut [f64], start_h: usize) {
        unsafe {
            let n = x.len();
            let mut h = start_h;
            while h < n {
                let stride = 2 * h;
                let mut block = 0;
                while block < n {
                    let mut j = 0;
                    while j < h {
                        let pa = x.as_mut_ptr().add(block + j);
                        let pb = x.as_mut_ptr().add(block + j + h);
                        let a = vld1q_f64(pa);
                        let b = vld1q_f64(pb);
                        vst1q_f64(pa, vaddq_f64(a, b));
                        vst1q_f64(pb, vsubq_f64(a, b));
                        j += 2;
                    }
                    block += stride;
                }
                h *= 2;
            }
        }
    }

    /// 4× hand-unrolled outer butterfly loop. Inner iteration processes 8
    /// doubles (4 NEON pairs) — 8 in-flight loads, 4 adds, 4 subs, 8 stores
    /// per iteration. At `h = 8` the unrolled body covers the whole stage in
    /// one iteration with no loop. At larger h it improves ILP by giving the
    /// OoO engine four independent dep chains.
    ///
    /// # Safety
    /// CPU must support `neon`. `start_h` must be ≥ 8 and a power of two,
    /// `x.len()` must be a power of two ≥ 2·`start_h`. Caller guarantees h is
    /// always a multiple of 8 inside the inner loop, so no scalar tail.
    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn outer_butterfly_stages_4x(x: &mut [f64], start_h: usize) {
        unsafe {
            let n = x.len();
            let mut h = start_h;
            while h < n {
                let stride = 2 * h;
                let mut block = 0;
                while block < n {
                    let mut j = 0;
                    while j < h {
                        let p = x.as_mut_ptr().add(block + j);
                        let q = p.add(h);

                        let a0 = vld1q_f64(p);
                        let a1 = vld1q_f64(p.add(2));
                        let a2 = vld1q_f64(p.add(4));
                        let a3 = vld1q_f64(p.add(6));
                        let b0 = vld1q_f64(q);
                        let b1 = vld1q_f64(q.add(2));
                        let b2 = vld1q_f64(q.add(4));
                        let b3 = vld1q_f64(q.add(6));

                        vst1q_f64(p, vaddq_f64(a0, b0));
                        vst1q_f64(p.add(2), vaddq_f64(a1, b1));
                        vst1q_f64(p.add(4), vaddq_f64(a2, b2));
                        vst1q_f64(p.add(6), vaddq_f64(a3, b3));
                        vst1q_f64(q, vsubq_f64(a0, b0));
                        vst1q_f64(q.add(2), vsubq_f64(a1, b1));
                        vst1q_f64(q.add(4), vsubq_f64(a2, b2));
                        vst1q_f64(q.add(6), vsubq_f64(a3, b3));

                        j += 8;
                    }
                    block += stride;
                }
                h *= 2;
            }
        }
    }

    /// NEON WHT with radix-16 in-register fusion *and* 4×-unrolled outer
    /// butterfly stages.
    ///
    /// # Safety
    /// CPU must support `neon`. Length must be a power of two.
    #[target_feature(enable = "neon")]
    pub unsafe fn wht_neon_radix16_4x(x: &mut [f64]) {
        let n = x.len();
        debug_assert!(n.is_power_of_two(), "WHT requires power-of-2 length");

        if n < 8 {
            scalar_wht(x);
            return;
        }

        unsafe {
            if n == 8 {
                radix8_block(x.as_mut_ptr());
                return;
            }

            let mut i = 0;
            while i + 16 <= n {
                radix16_block(x.as_mut_ptr().add(i));
                i += 16;
            }

            // n ≥ 16: outer stages start at h=16, always a multiple of 8.
            // n=16 has no outer stages (h=16 == n, loop doesn't fire).
            outer_butterfly_stages_4x(x, 16);
        }
    }
}

/// Scalar WHT used as the `n < 8` fallback inside the SIMD routines and
/// as the cross-platform fallback in [`super::wht_dispatch`]. Mirrors
/// the reference implementation in `rotation.rs`.
pub(in crate::turboquant) fn scalar_wht(x: &mut [f64]) {
    let n = x.len();
    let mut h = 1;
    while h < n {
        for i in (0..n).step_by(h * 2) {
            for j in i..i + h {
                let a = x[j];
                let b = x[j + h];
                x[j] = a + b;
                x[j + h] = a - b;
            }
        }
        h *= 2;
    }
}

/// Safe in-place WHT dispatcher: picks the fastest SIMD path supported by the
/// running CPU, or falls back to a scalar pass when no SIMD path is
/// available. Length must be a power of two.
///
/// Output is bit-equal to
/// [`crate::turboquant::rotation::in_place_walsh_hadamard_transform`] on every
/// path — no FMA, same pair order, single add/sub per butterfly.
#[inline]
pub fn wht_dispatch(x: &mut [f64]) {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            // SAFETY: AVX2 confirmed by runtime detection above.
            unsafe { simd_x86::wht_avx2_radix16_4x(x) };
            return;
        }
        scalar_wht(x);
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            // SAFETY: NEON confirmed by runtime detection above.
            unsafe { simd_arm::wht_neon_radix16_4x(x) };
            return;
        }
        scalar_wht(x);
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    scalar_wht(x);
}

#[cfg(test)]
mod tests {
    use rand::prelude::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;
    use crate::turboquant::rotation::in_place_walsh_hadamard_transform;

    /// Power-of-2 sizes from the smallest (n=1, identity) up through the
    /// largest benched size. Covers every distinct path on every backend:
    /// * n ∈ {1, 2}: SIMD variants fall through `n < 8` to the scalar
    ///   fallback (n=1 is identity; n=2 is one butterfly).
    /// * n=4: scalar fallback (still under the n<8 cutoff).
    /// * n=8: hand-unrolled single-block path.
    /// * n=16: single radix16-block path, no outer loop.
    /// * n=32: smallest size where the outer-stage loop fires.
    /// * n ≥ 64: bench territory.
    const SIZES: &[usize] = &[
        1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192,
    ];

    /// Three independently chosen seeds so a bug that survives one input
    /// distribution still trips at least one of these.
    const SEEDS: &[u64] = &[0xCAFE, 0xBEEF, 0xDEAD_BEEF_F00D];

    /// Returns the SIMD variants available on the running CPU. Empty when no
    /// supported backend is available — tests that depend on a SIMD path
    /// should skip in that case.
    #[allow(unused_mut)]
    #[allow(clippy::type_complexity)]
    fn simd_variants() -> Vec<(&'static str, fn(&mut [f64]))> {
        let mut v: Vec<(&'static str, fn(&mut [f64]))> = Vec::new();

        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx2") {
                v.push(("wht_avx2", |x| unsafe { simd_x86::wht_avx2(x) }));
                v.push(("wht_avx2_radix16_4x", |x| unsafe {
                    simd_x86::wht_avx2_radix16_4x(x)
                }));
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            if std::arch::is_aarch64_feature_detected!("neon") {
                v.push(("wht_neon", |x| unsafe { simd_arm::wht_neon(x) }));
                v.push(("wht_neon_radix16_4x", |x| unsafe {
                    simd_arm::wht_neon_radix16_4x(x)
                }));
            }
        }

        v
    }

    fn assert_bit_equal(scalar: &[f64], simd: &[f64], label: &str) {
        for (i, (s, r)) in scalar.iter().zip(simd.iter()).enumerate() {
            assert_eq!(
                s.to_bits(),
                r.to_bits(),
                "{label}: mismatch at i={i}: scalar={s}, simd={r}",
            );
        }
    }

    /// Bit-equal parity: every SIMD variant must produce exactly the scalar
    /// output for every (size, seed). Same ground-truth assertion the bench's
    /// startup parity check used to do.
    #[test]
    fn simd_wht_matches_scalar_bit_equal() {
        let variants = simd_variants();
        if variants.is_empty() {
            eprintln!("skipping: no SIMD variants available on this CPU");
            return;
        }

        for &seed in SEEDS {
            let mut rng = StdRng::seed_from_u64(seed);
            for &n in SIZES {
                let input: Vec<f64> = (0..n).map(|_| rng.random_range(-1.0f64..1.0)).collect();

                let mut scalar = input.clone();
                in_place_walsh_hadamard_transform(&mut scalar);

                for (name, f) in &variants {
                    let mut simd = input.clone();
                    f(&mut simd);
                    assert_bit_equal(&scalar, &simd, &format!("{name} at seed=0x{seed:X}, n={n}"));
                }
            }
        }
    }

    /// Edge-value parity: feed each SIMD variant inputs that the bench's
    /// uniform `[-1, 1)` random data won't cover (zeros, all-ones,
    /// alternating signs, huge magnitudes, denormals). Bit-equal because
    /// every step is a single add/sub in the same order — but assert it
    /// anyway, so a future change that introduces FMA or reorders ops on
    /// this kind of input will trip.
    #[test]
    fn simd_wht_matches_scalar_edge_values() {
        let variants = simd_variants();
        if variants.is_empty() {
            eprintln!("skipping: no SIMD variants available on this CPU");
            return;
        }

        #[allow(clippy::type_complexity)]
        let cases: &[(&str, fn(usize) -> Vec<f64>)] = &[
            ("zeros", |n| vec![0.0; n]),
            ("ones", |n| vec![1.0; n]),
            ("neg_ones", |n| vec![-1.0; n]),
            ("alternating", |n| {
                (0..n)
                    .map(|i| if i % 2 == 0 { 1.0 } else { -1.0 })
                    .collect()
            }),
            // Stay safely below f64::MAX / n so no stage overflows.
            ("huge", |n| vec![1e150; n]),
            ("tiny", |n| vec![1e-300; n]),
            ("denormals", |n| vec![f64::MIN_POSITIVE / 2.0; n]),
            ("mixed_sign_huge", |n| {
                (0..n)
                    .map(|i| if i % 2 == 0 { 1e150 } else { -1e150 })
                    .collect()
            }),
        ];

        for (case_name, build) in cases {
            for &n in SIZES {
                let input = build(n);
                let mut scalar = input.clone();
                in_place_walsh_hadamard_transform(&mut scalar);

                for (variant_name, f) in &variants {
                    let mut simd = input.clone();
                    f(&mut simd);
                    assert_bit_equal(
                        &scalar,
                        &simd,
                        &format!("{variant_name} on '{case_name}' at n={n}"),
                    );
                }
            }
        }
    }

    /// The safe `wht_dispatch` wrapper must agree with the scalar reference
    /// regardless of CPU features. On AVX2 / NEON hardware it routes through
    /// the radix-16 4x kernel; on architectures with no SIMD path it falls
    /// through to scalar.
    #[test]
    fn wht_dispatch_matches_scalar_bit_equal() {
        let mut rng = StdRng::seed_from_u64(0x1234_5678);
        for &n in SIZES {
            let input: Vec<f64> = (0..n).map(|_| rng.random_range(-1.0f64..1.0)).collect();

            let mut scalar = input.clone();
            in_place_walsh_hadamard_transform(&mut scalar);

            let mut dispatched = input.clone();
            wht_dispatch(&mut dispatched);

            assert_bit_equal(&scalar, &dispatched, &format!("wht_dispatch at n={n}"));
        }
    }

    /// `scalar_wht` is the cross-platform fallback `wht_dispatch` uses when
    /// no SIMD path is available. Runtime detection on AVX2- or NEON-enabled
    /// hardware never takes that branch, so test it directly here against
    /// the authoritative reference.
    #[test]
    fn scalar_wht_fallback_matches_reference() {
        let mut rng = StdRng::seed_from_u64(0xFEED_FACE);
        for &n in SIZES {
            let input: Vec<f64> = (0..n).map(|_| rng.random_range(-1.0f64..1.0)).collect();

            let mut reference = input.clone();
            in_place_walsh_hadamard_transform(&mut reference);

            let mut fallback = input.clone();
            scalar_wht(&mut fallback);

            assert_bit_equal(&reference, &fallback, &format!("scalar_wht at n={n}"));
        }
    }
}
