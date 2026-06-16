use ordered_float::NotNan;

use crate::EncodingError;

/// Extended version of P-square one-quantile estimator by Jain & Chlamtac (1985).
///
/// <https://www.cse.wustl.edu/~jain/papers/ftp/psqr.pdf>
/// By default, P-square uses 5 markers to estimate a single quantile.
/// This implementation is extended to support an arbitrary odd number of markers N >= 5
///
/// Usage:
/// ```ignore
/// let mut p2 = P2Quantile::<7>::new(0.99).unwrap();
/// for x in data { p2.push(x).unwrap(); }
/// let q_hat = p2.estimate();
/// ```
pub enum P2Quantile<const N: usize = 7> {
    Linear(P2QuantileLinear<N>),
    Impl(P2QuantileImpl<N>),
}

impl<const N: usize> P2Quantile<N> {
    pub fn new(q: f64) -> Result<Self, EncodingError> {
        const {
            assert!(N >= 5, "P2Quantile requires at least 5 markers");
            assert!(N % 2 == 1, "P2Quantile requires an odd number of markers");
        };
        if q <= 0.0 || q >= 1.0 {
            return Err(EncodingError::EncodingError(
                "Quantile q must be in (0, 1)".to_string(),
            ));
        }
        Ok(Self::Linear(P2QuantileLinear {
            quantile: q,
            observations: Default::default(),
        }))
    }

    /// Push one observation.
    pub fn push(&mut self, x: f64) {
        let Ok(x) = NotNan::new(x) else {
            return;
        };
        if !x.is_finite() {
            return;
        }

        match self {
            P2Quantile::Linear(linear) => {
                // in linear case just collect observations until we have N of them
                linear.observations.push(x);
                if linear.observations.len() == N {
                    *self = P2Quantile::Impl(P2QuantileImpl::new_from_linear(linear));
                }
            }
            P2Quantile::Impl(p2) => p2.push(*x),
        }
    }

    /// Get resulting quantile estimation.
    pub fn estimate(self) -> f64 {
        match self {
            P2Quantile::Linear(linear) => linear.estimate(),
            P2Quantile::Impl(p2) => p2.estimate(),
        }
    }
}

/// Streaming P-square state. Marker fields are stored in **struct-of-arrays**
/// layout (separate `[f64; N]` per attribute) rather than `[Marker; N]` so
/// that the per-push hot loops walk contiguous memory and LLVM auto-
/// vectorizes them under AVX2/AVX-512/NEON. Concretely:
///
/// - `update_desired_position` becomes one fused-multiply-add over the
///   whole `target_probabilities` array.
/// - The `n_position` increment past cell `k` is a masked add over the
///   contiguous `n_positions` slice.
///
/// `adjust` still uses index-by-index access with `Marker` snapshots of
/// neighbors (its parabolic interpolation has cross-marker dependencies
/// and isn't profitably SIMD'd).
pub struct P2QuantileImpl<const N: usize> {
    count: usize,
    heights: [f64; N],
    n_positions: [f64; N],
    n_desired: [f64; N],
    target_probabilities: [f64; N],
}

impl<const N: usize> P2QuantileImpl<N> {
    fn new_from_linear(linear: &P2QuantileLinear<N>) -> Self {
        assert_eq!(linear.observations.len(), N);

        let mut buf = linear.observations.clone();
        buf.sort_unstable();

        let target_probabilities = Self::generate_grid_probabilities(linear.quantile);
        let mut heights = [0.0f64; N];
        let mut n_positions = [0.0f64; N];
        let mut n_desired = [0.0f64; N];
        let count_minus_one = (N - 1) as f64;
        for i in 0..N {
            heights[i] = *buf[i];
            n_positions[i] = (i + 1) as f64;
            // Same as `update_desired_position(N)`, inlined to avoid the
            // method call that forced AoS access in the previous layout.
            n_desired[i] = 1.0 + target_probabilities[i] * count_minus_one;
        }
        P2QuantileImpl {
            count: N,
            heights,
            n_positions,
            n_desired,
            target_probabilities,
        }
    }

    fn estimate(self) -> f64 {
        // `N / 2` marker tracks the target quantile
        self.heights[N / 2]
    }

    fn push(&mut self, x: f64) {
        self.count += 1;

        // 1) Identify cell k and update extreme markers if needed
        // k is the cell index in [0..N - 1]
        let k = if x < self.heights[0] {
            self.heights[0] = x;
            0
        } else if x > self.heights[N - 1] {
            self.heights[N - 1] = x;
            N - 1
        } else {
            self.find_marker(x)
        };

        // 2) Increment n_position for markers above k. SoA layout lets
        // LLVM emit a tight masked-add over a contiguous slice instead
        // of strided AoS accesses.
        for p in self.n_positions[(k + 1)..N].iter_mut() {
            *p += 1.0;
        }

        // 3) Update desired positions for all markers. Pure FMA over
        // contiguous arrays — explicit SIMD intrinsics (NEON / AVX2+FMA)
        // when available, scalar fallback otherwise.
        let count_minus_one = (self.count - 1) as f64;
        update_desired_positions(
            &mut self.n_desired,
            &self.target_probabilities,
            count_minus_one,
        );

        // 4) Adjust interior markers. Branch-heavy parabolic interp
        // with prev/next dependencies — kept scalar.
        for i in 1..(N - 1) {
            adjust_marker(i, &mut self.heights, &mut self.n_positions, &self.n_desired);
        }
    }

    fn find_marker(&self, x: f64) -> usize {
        find_marker_simd::<N>(&self.heights, x)
    }

    /// Generate target probabilities for markers
    /// In the original P-square with 5 markers, the target probabilities are:
    /// p = [0, q/2, q, (1 + q)/2, 1]
    /// This function generalizes this to N markers by placing additional markers
    /// between the second and the middle, and between the middle and the second last.
    fn generate_grid_probabilities(q: f64) -> [f64; N] {
        let mut p = [0.0; N];
        let additional_markers_count = (N - 5) / 2;
        p[0] = 0.0;
        p[1] = q * 0.5;

        // add extended marker probabilities
        for i in 0..additional_markers_count {
            // just lerp between q/2 and be more close to the middle
            let factor = 0.7 + 0.3 * (i + 1) as f64 / (additional_markers_count as f64 + 2.0);
            p[i + 2] = q * factor;
        }

        // middle marker, tracks the required quantile
        p[N / 2] = q;

        // add extended marker probabilities
        for i in 0..additional_markers_count {
            let factor = 0.7
                + 0.3 * (additional_markers_count - i) as f64
                    / (additional_markers_count as f64 + 2.0);
            p[N / 2 + 1 + i] = 1.0 + (q - 1.0) * factor;
        }

        p[N - 2] = 1.0 + (q - 1.0) * 0.5;
        p[N - 1] = 1.0;
        p
    }
}

/// Per-push hot loop: `n_desired[i] = 1.0 + target_probabilities[i] *
/// count_minus_one` for every marker `i`. Pure FMA over two contiguous
/// `[f64; N]` arrays, dispatched to the widest available SIMD on the
/// target arch.
///
/// On aarch64 NEON: 2-lane f64 FMA via `vfmaq_f64` (Apple Silicon does
/// two of these per cycle through dual NEON pipes), scalar tail for
/// odd `N`.
///
/// On x86_64 AVX2+FMA: 4-lane f64 FMA via `_mm256_fmadd_pd`, scalar
/// tail. Runtime-detects the features once via `is_x86_feature_detected!`
/// (cached AtomicBool internally).
///
/// Otherwise: scalar loop. LLVM may auto-vectorize this, but explicit
/// intrinsics give us a stable lower bound on the codegen quality.
#[inline(always)]
fn update_desired_positions<const N: usize>(
    n_desired: &mut [f64; N],
    target_probabilities: &[f64; N],
    count_minus_one: f64,
) {
    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: NEON is part of the aarch64 baseline ABI. The target
        // pointer offsets stay within `[0, N)` thanks to the loop bound.
        unsafe { update_desired_neon(n_desired, target_probabilities, count_minus_one) };
    }

    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("fma")
        {
            // SAFETY: AVX2 + FMA both detected at runtime; pointer
            // offsets stay within `[0, N)`.
            unsafe { update_desired_avx2(n_desired, target_probabilities, count_minus_one) };
        } else {
            update_desired_scalar(n_desired, target_probabilities, count_minus_one);
        }
    }

    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    update_desired_scalar(n_desired, target_probabilities, count_minus_one);
}

#[cfg(not(target_arch = "aarch64"))]
#[inline(always)]
fn update_desired_scalar<const N: usize>(
    n_desired: &mut [f64; N],
    target_probabilities: &[f64; N],
    count_minus_one: f64,
) {
    for i in 0..N {
        n_desired[i] = 1.0 + target_probabilities[i] * count_minus_one;
    }
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn update_desired_neon<const N: usize>(
    n_desired: &mut [f64; N],
    target_probabilities: &[f64; N],
    count_minus_one: f64,
) {
    use std::arch::aarch64::*;
    // SAFETY for the whole body: NEON enabled by `#[target_feature]`,
    // pointer offsets stay in `[0, N)` (loop bound), and `as_ptr` /
    // `as_mut_ptr` give valid same-allocation bases.
    unsafe {
        let cm1 = vdupq_n_f64(count_minus_one);
        let one = vdupq_n_f64(1.0);
        let tp_ptr = target_probabilities.as_ptr();
        let nd_ptr = n_desired.as_mut_ptr();

        // 2-lane chunks. With const-known N LLVM unrolls the while into
        // exactly ⌊N/2⌋ vfma + (N % 2) scalar tail.
        let mut i = 0;
        while i + 2 <= N {
            let tp = vld1q_f64(tp_ptr.add(i));
            let r = vfmaq_f64(one, tp, cm1);
            vst1q_f64(nd_ptr.add(i), r);
            i += 2;
        }
        while i < N {
            n_desired[i] = 1.0 + target_probabilities[i] * count_minus_one;
            i += 1;
        }
    }
}

/// Branchless `find_marker` using SIMD compares + bit-count. Returns the
/// cell index `k` such that `heights[k] <= x <= heights[k+1]`. Equivalent
/// to counting how many of `heights[1..N]` are strictly less than `x`,
/// clamped into `[0, N-1]`.
///
/// Caller has already handled the extreme cases (`x < heights[0]`,
/// `x > heights[N-1]`) before reaching here, so we know
/// `heights[0] <= x <= heights[N-1]`.
#[inline(always)]
fn find_marker_simd<const N: usize>(heights: &[f64; N], x: f64) -> usize {
    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: NEON is part of the aarch64 baseline ABI.
        unsafe { find_marker_neon(heights, x) }
    }

    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            // SAFETY: AVX2 detected at runtime.
            return unsafe { find_marker_avx2(heights, x) };
        }
    }

    #[cfg(not(target_arch = "aarch64"))]
    {
        find_marker_scalar(heights, x)
    }
}

#[cfg(not(target_arch = "aarch64"))]
#[inline(always)]
fn find_marker_scalar<const N: usize>(heights: &[f64; N], x: f64) -> usize {
    for (i, &h) in heights.iter().enumerate().take(N).skip(1) {
        if x <= h {
            return i - 1;
        }
    }
    N - 1
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn find_marker_neon<const N: usize>(heights: &[f64; N], x: f64) -> usize {
    use std::arch::aarch64::*;
    // SAFETY for the whole body: NEON enabled, pointer offsets stay in
    // `[0, N)`. We compare against `heights[1..N]` (N-1 lanes) by
    // strictly-less mask: count how many of those lanes have
    // `heights[i] < x` and that's exactly `k` (one less than the first
    // marker where `x <= heights[i]`).
    unsafe {
        let x_splat = vdupq_n_f64(x);
        let h_ptr = heights.as_ptr().add(1);
        let mut count: u64 = 0;
        let mut i = 0;
        // 2-lane chunks.
        while i + 2 < N {
            let h = vld1q_f64(h_ptr.add(i));
            // mask lane = u64::MAX if heights[i+1] < x else 0.
            let m = vcltq_f64(h, x_splat);
            // Reduce: each set lane is u64::MAX = bitcount 64. Count
            // set lanes by AND-with-1 + horizontal add.
            let bits = vandq_u64(m, vdupq_n_u64(1));
            count += vaddvq_u64(bits);
            i += 2;
        }
        // Scalar tail (at most 1 element for N=7: heights[6]).
        while i < N - 1 {
            if *h_ptr.add(i) < x {
                count += 1;
            }
            i += 1;
        }
        count as usize
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn find_marker_avx2<const N: usize>(heights: &[f64; N], x: f64) -> usize {
    use std::arch::x86_64::*;
    // SAFETY for the whole body: AVX2 enabled, pointer offsets stay in
    // `[0, N)`. Same approach as the NEON path: count how many of
    // `heights[1..N]` are strictly less than `x`.
    unsafe {
        let x_splat = _mm256_set1_pd(x);
        let h_ptr = heights.as_ptr().add(1);
        let mut count: usize = 0;
        let mut i = 0;
        while i + 4 < N {
            let h = _mm256_loadu_pd(h_ptr.add(i));
            // _CMP_LT_OQ: ordered, signaling NaN, less-than. Returns
            // mask of lanes where h < x.
            let m = _mm256_cmp_pd::<_CMP_LT_OQ>(h, x_splat);
            // movemask packs the sign bits of the 4 lanes into a 4-bit
            // mask (0..15); popcount gives the lane count set.
            count += _mm256_movemask_pd(m).count_ones() as usize;
            i += 4;
        }
        while i < N - 1 {
            if *h_ptr.add(i) < x {
                count += 1;
            }
            i += 1;
        }
        count
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn update_desired_avx2<const N: usize>(
    n_desired: &mut [f64; N],
    target_probabilities: &[f64; N],
    count_minus_one: f64,
) {
    use std::arch::x86_64::*;
    // SAFETY for the whole body: AVX2 + FMA enabled by
    // `#[target_feature]`, pointer offsets stay in `[0, N)`.
    unsafe {
        let cm1 = _mm256_set1_pd(count_minus_one);
        let one = _mm256_set1_pd(1.0);
        let tp_ptr = target_probabilities.as_ptr();
        let nd_ptr = n_desired.as_mut_ptr();

        // 4-lane chunks. For N=7 this means one AVX2 FMA covering
        // [0..4] plus a 3-element scalar tail [4..7]. Could overlap
        // [3..7] for two AVX2 ops total (lane 3 written twice,
        // idempotent), but the unrolled scalar tail is small enough
        // that the simpler loop is at least as fast and easier to read.
        let mut i = 0;
        while i + 4 <= N {
            let tp = _mm256_loadu_pd(tp_ptr.add(i));
            let r = _mm256_fmadd_pd(tp, cm1, one);
            _mm256_storeu_pd(nd_ptr.add(i), r);
            i += 4;
        }
        while i < N {
            n_desired[i] = 1.0 + target_probabilities[i] * count_minus_one;
            i += 1;
        }
    }
}

/// Marker adjustment for one interior index `i`. Reads neighbors at
/// `i - 1` and `i + 1`, may iterate (parabolic step until the marker is
/// within ±1 of its desired position). Operates on the SoA arrays
/// directly — no `Marker` snapshot allocation per call.
fn adjust_marker<const N: usize>(
    i: usize,
    heights: &mut [f64; N],
    n_positions: &mut [f64; N],
    n_desired: &[f64; N],
) {
    loop {
        let di = n_desired[i] - n_positions[i];
        if di >= 1.0 && (n_positions[i + 1] - n_positions[i]) > 1.0 {
            adjust_step(i, heights, n_positions, 1.0);
        } else if di <= -1.0 && (n_positions[i - 1] - n_positions[i]) < -1.0 {
            adjust_step(i, heights, n_positions, -1.0);
        } else {
            break;
        }
    }
}

fn adjust_step<const N: usize>(
    i: usize,
    heights: &mut [f64; N],
    n_positions: &mut [f64; N],
    dsign: f64,
) {
    let prev_h = heights[i - 1];
    let next_h = heights[i + 1];
    let prev_n = n_positions[i - 1];
    let next_n = n_positions[i + 1];
    let cur_h = heights[i];
    let cur_n = n_positions[i];

    // Try parabolic prediction
    let denom = next_n - prev_n;
    let mut h_par = cur_h;
    if denom != 0.0 {
        let a = (cur_n - prev_n + dsign) / (next_n - cur_n) * (next_h - cur_h);
        let b = (next_n - cur_n - dsign) / (cur_n - prev_n) * (cur_h - prev_h);
        h_par = cur_h + (a + b) * dsign / denom;
    }

    // If parabolic result is within neighbors, use it; otherwise linear.
    heights[i] = if h_par > prev_h && h_par < next_h && h_par.is_finite() {
        h_par
    } else if dsign > 0.0 {
        cur_h + (next_h - cur_h) / (next_n - cur_n)
    } else {
        cur_h + (prev_h - cur_h) / (prev_n - cur_n)
    };

    n_positions[i] += dsign;
}

pub struct P2QuantileLinear<const N: usize> {
    quantile: f64,
    observations: arrayvec::ArrayVec<NotNan<f64>, N>,
}

impl<const N: usize> P2QuantileLinear<N> {
    /// Simple linear-interpolated sample quantile
    fn estimate(mut self) -> f64 {
        estimate_quantile_from_slice(&mut self.observations, self.quantile)
    }
}

fn estimate_quantile_from_slice(observations: &mut [NotNan<f64>], quantile: f64) -> f64 {
    if observations.is_empty() {
        // No data
        return 0.0;
    }
    if observations.len() == 1 {
        return *observations[0];
    }
    observations.sort_unstable();

    let k = quantile * (observations.len() as f64 - 1.0);
    let lo = k.floor() as usize;
    let hi = k.ceil() as usize;
    if lo == hi {
        *observations[lo]
    } else {
        let frac = k - lo as f64;
        *observations[lo] + frac * (*observations[hi] - *observations[lo])
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use rand_distr::{Poisson, StandardNormal, StudentT};

    use super::*;

    const N: usize = 7;
    const COUNT: usize = 10_000;

    /// SIMD parity for `update_desired_positions`: bit-for-bit identical
    /// output between the platform-specific path and a scalar reference
    /// computed with the **same shape** the dispatcher would have used.
    ///
    /// NEON `vfmaq_f64` and AVX2 `_mm256_fmadd_pd` perform a single-
    /// rounded fused multiply-add (matches `f64::mul_add`); the scalar
    /// fallback in `update_desired_scalar` does `1.0 + a * b` (two
    /// roundings, not necessarily equal to `mul_add`). The reference
    /// here mirrors the dispatch in `update_desired_positions` so the
    /// test stays bit-equal on every supported target — including
    /// non-aarch64 / non-FMA-x86_64 paths where the implementation
    /// itself uses `1.0 + a * b`.
    #[test]
    fn test_update_desired_positions_simd_parity() {
        // Mirrors the cfg/runtime dispatch in `update_desired_positions`.
        // A `true` return means the implementation went through a
        // hardware FMA (single-rounded), so the reference uses
        // `mul_add`. A `false` return means it went through the scalar
        // `1.0 + a * b` form.
        fn impl_uses_fma() -> bool {
            #[cfg(target_arch = "aarch64")]
            {
                true
            }
            #[cfg(target_arch = "x86_64")]
            {
                std::arch::is_x86_feature_detected!("avx2")
                    && std::arch::is_x86_feature_detected!("fma")
            }
            #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
            {
                false
            }
        }

        let fused = impl_uses_fma();
        let mut rng = StdRng::seed_from_u64(0xC0FFEE);
        for _ in 0..256 {
            let target_probabilities: [f64; 7] = std::array::from_fn(|_| rng.random::<f64>());
            let count_minus_one: f64 = rng.random_range(0.0..1e6);

            let mut got = [0.0f64; 7];
            super::update_desired_positions::<7>(&mut got, &target_probabilities, count_minus_one);

            let expected: [f64; 7] = std::array::from_fn(|i| {
                if fused {
                    f64::mul_add(target_probabilities[i], count_minus_one, 1.0)
                } else {
                    1.0 + target_probabilities[i] * count_minus_one
                }
            });

            assert_eq!(
                got, expected,
                "tp={target_probabilities:?} cm1={count_minus_one}",
            );
        }
    }

    /// SIMD parity for `find_marker_simd`: returns the same cell index as
    /// the scalar reference for every probe `x` against a sorted `heights`
    /// array. Covers `x` strictly inside the range, equality with each
    /// boundary, and the inclusive ends.
    #[test]
    fn test_find_marker_simd_parity() {
        let scalar_find = |heights: &[f64; 7], x: f64| -> usize {
            for (i, &h) in heights.iter().enumerate().take(7).skip(1) {
                if x <= h {
                    return i - 1;
                }
            }
            6
        };

        let mut rng = StdRng::seed_from_u64(0xBADF00D);
        for _ in 0..256 {
            // Random sorted heights spanning a generous range.
            let mut h: [f64; 7] = std::array::from_fn(|_| rng.random_range(-100.0..100.0));
            h.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());

            // Probes at boundaries and between them.
            let mut probes: Vec<f64> = h.to_vec();
            for w in h.windows(2) {
                probes.push(0.5 * (w[0] + w[1]));
            }
            probes.push(h[0]);
            probes.push(h[6]);

            for x in probes {
                let got = super::find_marker_simd::<7>(&h, x);
                let want = scalar_find(&h, x);
                assert_eq!(got, want, "h={h:?} x={x}");
            }
        }
    }

    #[test]
    fn test_p_square() {
        // Test P2 quantile estimator on uniformly distributed data
        const QUANTILE: f64 = 0.99;
        // In case of uniform distribution, the theoretical value of quantile is equal to the quantile level
        const THEORETICAL_VALUE: f64 = QUANTILE;
        const ERROR: f64 = 1e-2;
        let mut p2 = P2Quantile::<N>::new(QUANTILE).unwrap();

        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(COUNT);
        for _ in 0..COUNT {
            let value = rng.random::<f64>();
            data.push(value.try_into().unwrap());

            p2.push(value);
        }

        // Take P square estimation
        let p = p2.estimate();

        // Compare with linear estimation
        let linear_p = estimate_quantile_from_slice(data.as_mut_slice(), QUANTILE);
        assert!((p - linear_p).abs() < ERROR);

        // Compare with theoretical value
        assert!((p - THEORETICAL_VALUE).abs() < ERROR);
    }

    #[test]
    fn test_p_square_normal() {
        // Test P2 quantile estimator on normally N(0, 1) distributed data
        // Take percentile corresponding to 2 standard deviations (2 sigmas)
        // It'a approximately 97.72 percentile
        const QUANTILE: f64 = 0.9772;
        // The theoretical value of 97.72 percentile for N(0, 1) is approximately 2 sigmas, i.e., 2.0
        const THEORETICAL_VALUE: f64 = 2.0;
        const ERROR: f64 = 0.1; // allow 5% error (0.1 / 2.0 = 0.05 = 5%)

        let mut p2 = P2Quantile::<N>::new(QUANTILE).unwrap();

        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(COUNT);
        for _ in 0..COUNT {
            let value: f64 = rng.sample(StandardNormal);
            data.push(value.try_into().unwrap());

            p2.push(value);
        }

        // Take P square estimation
        let p = p2.estimate();

        // Compare with linear estimation
        let linear_p = estimate_quantile_from_slice(data.as_mut_slice(), QUANTILE);
        assert!((p - linear_p).abs() < ERROR);

        // Compare with theoretical value
        assert!((p - THEORETICAL_VALUE).abs() < ERROR);
    }

    #[test]
    fn test_p_square_normal_low() {
        // Same as test_p_square_normal but with 100 - 97.72 = 2.28 percentile
        // Test P2 quantile estimator on normally N(0, 1) distributed data
        // Take percentile corresponding to -2 standard deviations (-2 sigmas)
        // It'a approximately 2.28 percentile
        const QUANTILE: f64 = 0.0228;
        // The theoretical value of 2.28 percentile for N(0, 1) is approximately -2 sigmas, i.e., -2.0
        const THEORETICAL_VALUE: f64 = -2.0;
        const ERROR: f64 = 0.1; // allow 5% error (0.1 / 2.0 = 0.05 = 5%)

        let mut p2 = P2Quantile::<N>::new(QUANTILE).unwrap();

        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(COUNT);
        for _ in 0..COUNT {
            let value: f64 = rng.sample(StandardNormal);
            data.push(value.try_into().unwrap());

            p2.push(value);
        }

        // Take P square estimation
        let p = p2.estimate();

        // Compare with linear estimation
        let linear_p = estimate_quantile_from_slice(data.as_mut_slice(), QUANTILE);
        assert!((p - linear_p).abs() < ERROR);

        // Compare with theoretical value
        assert!((p - THEORETICAL_VALUE).abs() < ERROR);
    }

    #[test]
    fn test_p_square_poisson() {
        // Take Poisson-distributed data with mean 2. It's case of non-symmetric and non-normal distribution.
        const QUANTILE: f64 = 0.99;
        // The theoretical value of 99 percentile is 6.0
        const THEORETICAL_VALUE: f64 = 6.0;
        const ERROR: f64 = 0.3; // allow 5% error (0.3 / 6.0 = 0.05 = 5%)

        let mut p2 = P2Quantile::<N>::new(QUANTILE).unwrap();

        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(COUNT);
        for _ in 0..COUNT {
            let value = rng.sample(Poisson::new(2.0).unwrap());
            data.push(value.try_into().unwrap());

            p2.push(value);
        }

        // Take P square estimation
        let p = p2.estimate();

        // Compare with linear estimation
        let linear_p = estimate_quantile_from_slice(data.as_mut_slice(), QUANTILE);
        assert!((p - linear_p).abs() < ERROR);

        // Compare with theoretical value
        assert!((p - THEORETICAL_VALUE).abs() < ERROR);
    }

    #[test]
    fn test_p_square_student() {
        // Corner case test with Student t-distribution with low degrees of freedom (heavy tails)
        // StudentT-distributed data with 2 degrees of freedom has heavy tails and infinite variance.
        const QUANTILE: f64 = 0.99;
        // The theoretical value of 99 percentile is somewhat around 6.9646
        const THEORETICAL_VALUE: f64 = 6.9646;
        const ERROR: f64 = 0.69646; // 10% error because of heavy tails

        let mut p2 = P2Quantile::<N>::new(QUANTILE).unwrap();

        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(COUNT);
        for _ in 0..COUNT {
            let value = rng.sample(StudentT::new(2.0).unwrap());
            data.push(value.try_into().unwrap());

            p2.push(value);
        }

        // Take P square estimation
        let p = p2.estimate();

        // Compare with linear estimation
        let linear_p = estimate_quantile_from_slice(data.as_mut_slice(), QUANTILE);
        assert!((p - linear_p).abs() < ERROR);

        // Compare with theoretical value
        assert!((p - THEORETICAL_VALUE).abs() < ERROR);
    }

    #[test]
    fn test_p_square_zeros() {
        let mut p2 = P2Quantile::<N>::new(0.99).unwrap();
        for _ in 0..COUNT {
            p2.push(0.0);
        }

        // Take P square estimation
        let p = p2.estimate();

        // Should be exactly zero
        assert_eq!(p, 0.0);
    }

    #[test]
    fn test_p_square_linear() {
        let mut p2 = P2Quantile::<N>::new(0.99).unwrap();
        p2.push(0.0);
        p2.push(0.0);
        p2.push(0.0);

        // Take P square estimation
        let p = p2.estimate();

        // Should be exactly zero
        assert_eq!(p, 0.0);
    }

    #[test]
    fn test_p_square_extended_grid() {
        // Check increasing order
        let grid = P2QuantileImpl::<7>::generate_grid_probabilities(0.99);
        for i in 1..grid.len() {
            assert!(grid[i] > grid[i - 1]);
        }

        // Check increasing order
        let grid = P2QuantileImpl::<9>::generate_grid_probabilities(0.99);
        for i in 1..grid.len() {
            assert!(grid[i] > grid[i - 1]);
        }

        // Check increasing order
        let grid = P2QuantileImpl::<11>::generate_grid_probabilities(0.99);
        for i in 1..grid.len() {
            assert!(grid[i] > grid[i - 1]);
        }
    }
}
