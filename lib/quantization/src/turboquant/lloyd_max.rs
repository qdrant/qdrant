#![allow(dead_code)]

const CENTROIDS_1BIT: &[f32] = &[-0.797_884_6, 0.797_884_6];

/// 2-bit (4 centroids): Lloyd-Max for N(0,1)
const CENTROIDS_2BIT: &[f32] = &[-1.510, -0.4528, 0.4528, 1.510];

/// 3-bit (8 centroids): Lloyd-Max for N(0,1)
const CENTROIDS_3BIT: &[f32] = &[
    -2.152, -1.344, -0.7560, -0.2451, 0.2451, 0.7560, 1.344, 2.152,
];

/// 4-bit (16 centroids): Lloyd-Max for N(0,1)
const CENTROIDS_4BIT: &[f32] = &[
    -2.733, -2.069, -1.618, -1.256, -0.9424, -0.6568, -0.3881, -0.1284, 0.1284, 0.3881, 0.6568,
    0.9424, 1.256, 1.618, 2.069, 2.733,
];

/// Return the centroid slice for a given bit-width (1–4).
///
/// # Panics
/// Panics if `bits` is not in 1..=4.
pub fn get_centroids(bits: u8) -> &'static [f32] {
    match bits {
        1 => CENTROIDS_1BIT,
        2 => CENTROIDS_2BIT,
        3 => CENTROIDS_3BIT,
        4 => CENTROIDS_4BIT,
        _ => panic!("unsupported bit-width: {bits}"),
    }
}

#[cfg(test)]
mod tests {
    use std::f64::consts::PI;

    use super::*;

    /// Gaussian approximation N(0, 1/d) for the coordinate distribution
    /// after random rotation of a d-dimensional unit vector.
    /// Accurate for d >= 64.
    fn gaussian_pdf(x: f64, d: usize) -> f64 {
        let sigma2 = 1.0 / d as f64;
        (1.0 / (2.0 * PI * sigma2).sqrt()) * (-x * x / (2.0 * sigma2)).exp()
    }

    /// Composite Simpson's rule for numerical integration of `f` over `[a, b]`.
    fn integrate(f: impl Fn(f64) -> f64, a: f64, b: f64) -> f64 {
        // Use enough points for good accuracy on smooth functions.
        const N: usize = 4096;
        let h = (b - a) / N as f64;

        let mut sum = f(a) + f(b);
        for i in 1..N {
            let x = a + i as f64 * h;
            let weight = if i % 2 == 0 { 2.0 } else { 4.0 };
            sum += weight * f(x);
        }
        sum * h / 3.0
    }

    /// Solve the Lloyd-Max optimal scalar quantizer for N(0, 1/d).
    ///
    /// Returns `2^bits` optimal centroids (sorted) as f32.
    fn solve_lloyd_max(d: usize, bits: u32) -> Vec<f32> {
        const MAX_ITER: usize = 200;
        const TOL: f64 = 1e-10;

        let n_levels = 1usize << bits;
        let sigma = 1.0 / (d as f64).sqrt();

        // Initialize centroids uniformly in [-3.5*sigma, 3.5*sigma].
        let lo = -3.5 * sigma;
        let hi = 3.5 * sigma;
        let mut centroids: Vec<f64> = (0..n_levels)
            .map(|i| lo + (hi - lo) * (i as f64 + 0.5) / n_levels as f64)
            .collect();

        let mut boundaries = Vec::with_capacity(n_levels - 1);

        for _iter in 0..MAX_ITER {
            // Step 1: boundaries = midpoints between adjacent centroids.
            boundaries.clear();
            for i in 0..n_levels - 1 {
                boundaries.push((centroids[i] + centroids[i + 1]) / 2.0);
            }

            // Step 2: update centroids as conditional expectations E[X | X in partition_i].
            let far_lo = lo * 3.0;
            let far_hi = hi * 3.0;

            let mut max_shift: f64 = 0.0;

            for i in 0..n_levels {
                let a = if i == 0 { far_lo } else { boundaries[i - 1] };
                let b = if i == n_levels - 1 {
                    far_hi
                } else {
                    boundaries[i]
                };

                let numerator = integrate(|x| x * gaussian_pdf(x, d), a, b);
                let denominator = integrate(|x| gaussian_pdf(x, d), a, b);

                let new_c = if denominator > 1e-15 {
                    numerator / denominator
                } else {
                    centroids[i]
                };

                max_shift = max_shift.max((new_c - centroids[i]).abs());
                centroids[i] = new_c;
            }

            if max_shift < TOL {
                break;
            }
        }

        centroids.iter().map(|&c| c as f32).collect()
    }

    /// Verify that `solve_lloyd_max` reproduces the hardcoded centroid constants.
    /// The constants are for N(0,1), i.e. d=1 where sigma^2 = 1/d = 1.
    #[test]
    fn test_matches_hardcoded_centroids() {
        let cases: &[(u32, &[f32])] = &[
            (1, CENTROIDS_1BIT),
            (2, CENTROIDS_2BIT),
            (3, CENTROIDS_3BIT),
            (4, CENTROIDS_4BIT),
        ];
        for &(bits, expected) in cases {
            let centroids = solve_lloyd_max(1, bits);
            assert_eq!(
                centroids.len(),
                expected.len(),
                "bits={bits}: wrong number of centroids"
            );
            for (i, (&got, &want)) in centroids.iter().zip(expected).enumerate() {
                assert!(
                    (got - want).abs() < 1e-3,
                    "bits={bits}, centroid[{i}]: got {got}, expected {want}"
                );
            }
        }
    }
}
