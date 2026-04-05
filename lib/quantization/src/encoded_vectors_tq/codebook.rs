use serde::{Deserialize, Serialize};

use super::packing::unpack_index;

/// Number of iterations for Lloyd's algorithm (codebook optimization).
const LLOYD_ITERATIONS: usize = 100;

/// Optimal scalar quantizer codebook for TurboQuant.
///
/// Contains sorted centroid values and decision boundaries for
/// `bits`-bit quantization of coordinates distributed as N(0, 1/padded_dim).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Codebook {
    /// Bits per coordinate (1–6).
    pub bits: usize,
    /// Padded dimension used to compute the codebook.
    pub padded_dim: usize,
    /// Sorted centroid values (2^bits entries).
    pub centroids: Vec<f32>,
    /// Decision boundaries between centroids (2^bits − 1 entries).
    pub boundaries: Vec<f32>,
}

impl Codebook {
    /// Compute optimal centroids and decision boundaries for `bits`-bit scalar
    /// quantization of coordinates distributed as N(0, 1/padded_dim).
    pub fn new(bits: usize, padded_dim: usize) -> Self {
        let d = padded_dim as f64;

        let centroids_f64: Vec<f64> = match bits {
            1 => {
                // Closed-form: ±√(2/(πd))
                let c = (2.0 / (std::f64::consts::PI * d)).sqrt();
                vec![-c, c]
            }
            2 => {
                // Hardcoded optimal centroids from the TurboQuant paper
                let s = d.sqrt();
                vec![-1.51 / s, -0.453 / s, 0.453 / s, 1.51 / s]
            }
            _ => {
                // Lloyd's algorithm on N(0, 1/d)
                let sigma = 1.0 / d.sqrt();
                lloyds_gaussian(1 << bits, sigma)
            }
        };

        let boundaries: Vec<f32> = centroids_f64
            .windows(2)
            .map(|w| ((w[0] + w[1]) / 2.0) as f32)
            .collect();

        let centroids: Vec<f32> = centroids_f64.iter().map(|&c| c as f32).collect();

        Self {
            bits,
            padded_dim,
            centroids,
            boundaries,
        }
    }

    /// Find the partition index for `value` via binary search on sorted boundaries.
    pub fn quantize(&self, value: f32) -> u8 {
        match self
            .boundaries
            .binary_search_by(|b| b.partial_cmp(&value).unwrap_or(std::cmp::Ordering::Equal))
        {
            Ok(i) | Err(i) => i as u8,
        }
    }

    /// Look up the centroid value for the given quantization index.
    pub fn centroid(&self, index: u8) -> f32 {
        self.centroids[(index as usize).min(self.centroids.len() - 1)]
    }

    /// Decode packed indices into raw centroid values (no norm correction).
    pub fn decode_raw(&self, packed: &[u8], padded_dim: usize) -> Vec<f32> {
        let mut y_tilde = Vec::with_capacity(padded_dim);
        for i in 0..padded_dim {
            let idx = unpack_index(packed, i, self.bits) as usize;
            y_tilde.push(self.centroids[idx.min(self.centroids.len() - 1)]);
        }
        y_tilde
    }

    /// Decode packed indices into centroid values, then re-normalize to unit norm.
    ///
    /// Returns the decoded and norm-corrected vector in rotated space.
    pub fn decode_corrected(&self, packed: &[u8], padded_dim: usize) -> Vec<f32> {
        let mut y_tilde = self.decode_raw(packed, padded_dim);
        Self::normalize_to_unit(&mut y_tilde);
        y_tilde
    }

    /// Re-normalize a vector to unit norm in-place.
    pub fn normalize_to_unit(v: &mut [f32]) {
        let norm_sq: f32 = v.iter().map(|&x| x * x).sum();
        let norm = norm_sq.sqrt();
        if norm > 1e-10 {
            for x in v.iter_mut() {
                *x /= norm;
            }
        }
    }

    /// Bytes needed to store `padded_dim` indices at this codebook's bit width.
    pub fn packed_size(&self, padded_dim: usize) -> usize {
        (padded_dim * self.bits + 7) / 8
    }
}

// ============================================================================
// Math helpers
// ============================================================================

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

/// Standard normal PDF: φ(x) = exp(-x²/2) / √(2π)
fn std_normal_pdf(x: f64) -> f64 {
    (-0.5 * x * x).exp() / (2.0 * std::f64::consts::PI).sqrt()
}

/// Standard normal CDF: Φ(x) = (1 + erf(x/√2)) / 2
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

/// E[X | a < X < b] where X ~ N(0, σ²).
///
/// Uses: σ · (φ(a/σ) − φ(b/σ)) / (Φ(b/σ) − Φ(a/σ))
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
        if a.is_finite() && b.is_infinite() {
            return a + sigma;
        } else if a.is_infinite() && b.is_finite() {
            return b - sigma;
        } else if a.is_finite() && b.is_finite() {
            return (a + b) / 2.0;
        } else {
            return 0.0;
        }
    }

    let pdf_diff = std_normal_pdf(a_std) - std_normal_pdf(b_std);
    sigma * pdf_diff / prob
}

/// Lloyd's algorithm for optimal scalar quantization of N(0, σ²).
fn lloyds_gaussian(n_centroids: usize, sigma: f64) -> Vec<f64> {
    let mut boundaries: Vec<f64> = (1..n_centroids)
        .map(|i| {
            let p = i as f64 / n_centroids as f64;
            sigma * inv_std_normal_cdf(p)
        })
        .collect();

    let mut centroids = vec![0.0f64; n_centroids];

    for _ in 0..LLOYD_ITERATIONS {
        centroids[0] = gaussian_conditional_expectation(sigma, f64::NEG_INFINITY, boundaries[0]);
        for i in 1..n_centroids - 1 {
            centroids[i] =
                gaussian_conditional_expectation(sigma, boundaries[i - 1], boundaries[i]);
        }
        centroids[n_centroids - 1] =
            gaussian_conditional_expectation(sigma, boundaries[n_centroids - 2], f64::INFINITY);

        for i in 0..n_centroids - 1 {
            boundaries[i] = (centroids[i] + centroids[i + 1]) / 2.0;
        }
    }

    centroids.sort_by(|a, b| a.partial_cmp(b).unwrap());
    centroids
}
