#![allow(dead_code)]

const CENTROIDS_1BIT: &[f32] = &[-0.797_884_6, 0.797_884_6];

/// 2-bit (4 centroids): Lloyd-Max for N(0,1)
const CENTROIDS_2BIT: &[f32] = &[-1.510, -0.4528, 0.4528, 1.510];

/// 4-bit (16 centroids): Lloyd-Max for N(0,1)
const CENTROIDS_4BIT: &[f32] = &[
    -2.733, -2.069, -1.618, -1.256, -0.9424, -0.6568, -0.3881, -0.1284, 0.1284, 0.3881, 0.6568,
    0.9424, 1.256, 1.618, 2.069, 2.733,
];

/// Return the centroid slice for a given bit-width (1–4).
///
/// # Panics
/// Panics if `bits` is not in [1, 2, 4]
pub fn get_centroids(bits: u8) -> &'static [f32] {
    match bits {
        1 => CENTROIDS_1BIT,
        2 => CENTROIDS_2BIT,
        4 => CENTROIDS_4BIT,
        _ => unreachable!("unsupported bit-width: {bits}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Standard normal PDF: φ(x) = exp(-x²/2) / √(2π).
    fn std_normal_pdf(x: f64) -> f64 {
        (-0.5 * x * x).exp() / (2.0 * std::f64::consts::PI).sqrt()
    }

    /// Approximate error function (Abramowitz & Stegun 7.1.26, max error 1.5e-7).
    fn erf_approx(x: f64) -> f64 {
        let a = x.abs();
        let t = 1.0 / (1.0 + 0.3275911 * a);
        let poly = t
            * (0.254829592
                + t * (-0.284496736 + t * (1.421413741 + t * (-1.453152027 + t * 1.061405429))));
        let result = 1.0 - poly * (-a * a).exp();
        if x >= 0.0 { result } else { -result }
    }

    /// Standard normal CDF: Φ(x) = (1 + erf(x/√2)) / 2.
    fn std_normal_cdf(x: f64) -> f64 {
        0.5 * (1.0 + erf_approx(x / std::f64::consts::SQRT_2))
    }

    /// Approximate inverse of the standard normal CDF (Abramowitz & Stegun 26.2.23).
    fn inv_std_normal_cdf(p: f64) -> f64 {
        if p <= 0.0 {
            return f64::NEG_INFINITY;
        }
        if p >= 1.0 {
            return f64::INFINITY;
        }
        let t = if p < 0.5 {
            (-2.0 * p.ln()).sqrt()
        } else {
            (-2.0 * (1.0 - p).ln()).sqrt()
        };
        let result = t
            - (2.515517 + 0.802853 * t + 0.010328 * t * t)
                / (1.0 + 1.432788 * t + 0.189269 * t * t + 0.001308 * t * t * t);
        if p < 0.5 { -result } else { result }
    }

    /// E[X | a < X < b] where X ~ N(0, σ²), using the closed-form:
    ///   σ · (φ(a/σ) − φ(b/σ)) / (Φ(b/σ) − Φ(a/σ))
    fn gaussian_conditional_expectation(sigma: f64, a: f64, b: f64) -> f64 {
        let a_std = if a.is_finite() { a / sigma } else { a };
        let b_std = if b.is_finite() { b / sigma } else { b };

        let prob = if a_std.is_infinite() && a_std < 0.0 {
            std_normal_cdf(b_std)
        } else if b_std.is_infinite() && b_std > 0.0 {
            1.0 - std_normal_cdf(a_std)
        } else {
            std_normal_cdf(b_std) - std_normal_cdf(a_std)
        };

        if prob < 1e-15 {
            return if a.is_finite() && b.is_finite() {
                (a + b) / 2.0
            } else if a.is_finite() {
                a + sigma
            } else if b.is_finite() {
                b - sigma
            } else {
                0.0
            };
        }

        let pdf_diff = std_normal_pdf(a_std) - std_normal_pdf(b_std);
        sigma * pdf_diff / prob
    }

    /// Solve the Lloyd-Max optimal scalar quantizer for N(0, 1/d).
    ///
    /// Returns `2^bits` optimal centroids (sorted) as f32.
    fn solve_lloyd_max(d: usize, bits: u32) -> Vec<f32> {
        const MAX_ITER: usize = 200;
        const TOL: f64 = 1e-10;

        let n_levels = 1usize << bits;
        let sigma = 1.0 / (d as f64).sqrt();

        // Initialize boundaries at equal-probability quantiles.
        let mut boundaries: Vec<f64> = (1..n_levels)
            .map(|i| sigma * inv_std_normal_cdf(i as f64 / n_levels as f64))
            .collect();

        let mut centroids = vec![0.0f64; n_levels];

        for _iter in 0..MAX_ITER {
            // Update centroids as conditional expectations.
            let mut max_shift: f64 = 0.0;

            for i in 0..n_levels {
                let a = if i == 0 {
                    f64::NEG_INFINITY
                } else {
                    boundaries[i - 1]
                };
                let b = if i == n_levels - 1 {
                    f64::INFINITY
                } else {
                    boundaries[i]
                };
                let new_c = gaussian_conditional_expectation(sigma, a, b);
                max_shift = max_shift.max((new_c - centroids[i]).abs());
                centroids[i] = new_c;
            }

            if max_shift < TOL {
                break;
            }

            // Update boundaries as midpoints.
            for i in 0..n_levels - 1 {
                boundaries[i] = (centroids[i] + centroids[i + 1]) / 2.0;
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
