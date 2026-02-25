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

pub struct P2QuantileImpl<const N: usize> {
    count: usize,
    markers: [Marker; N],
}

impl<const N: usize> P2QuantileImpl<N> {
    fn new_from_linear(linear: &P2QuantileLinear<N>) -> Self {
        assert_eq!(linear.observations.len(), N);

        let mut buf = linear.observations.clone();
        buf.sort_unstable();

        let p = Self::generate_grid_probabilities(linear.quantile);
        let mut markers = [Marker::default(); N];
        for i in 0..N {
            markers[i].height = *buf[i];
            markers[i].target_probability = p[i];
            markers[i].n_position = (i + 1) as f64;
            markers[i].update_desired_position(N);
        }
        P2QuantileImpl { count: N, markers }
    }

    fn estimate(self) -> f64 {
        // `N / 2` marker tracks the target quantile
        self.markers[N / 2].height
    }

    fn push(&mut self, x: f64) {
        self.count += 1;

        // 1) Identify cell k and update extreme markers if needed
        // k is the cell index in [0..N - 1]
        let k = if x < self.markers[0].height {
            // update minimum marker
            self.markers[0].height = x;
            0
        } else if x > self.markers[N - 1].height {
            // update maximum marker
            self.markers[N - 1].height = x;
            N - 1
        } else {
            // otherwise find the markers cell
            self.find_marker(x)
        };

        // 2) Increment positions of markers above k
        for i in (k + 1)..N {
            self.markers[i].n_position += 1.0;
        }

        // 3) Update desired positions
        for i in 0..N {
            self.markers[i].update_desired_position(self.count);
        }

        // 4) Adjust interior markers
        for i in 1..(N - 1) {
            self.markers[i].adjust(self.markers[i - 1], self.markers[i + 1]);
        }
    }

    fn find_marker(&self, x: f64) -> usize {
        for i in 1..N {
            if x <= self.markers[i].height {
                return i - 1;
            }
        }
        N - 1
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

#[derive(Clone, Copy, Default)]
struct Marker {
    height: f64,
    n_position: f64,
    n_desired: f64,
    target_probability: f64,
}

impl Marker {
    fn adjust(&mut self, prev: Marker, next: Marker) {
        loop {
            let di = self.n_desired - self.n_position;
            if di >= 1.0 && (next.n_position - self.n_position) > 1.0 {
                self.adjust_step(&prev, &next, 1.0);
            } else if di <= -1.0 && (prev.n_position - self.n_position) < -1.0 {
                self.adjust_step(&prev, &next, -1.0);
            } else {
                break;
            }
        }
    }

    fn adjust_step(&mut self, prev: &Marker, next: &Marker, dsign: f64) {
        // Try parabolic prediction
        let denom = next.n_position - prev.n_position;
        let mut h_par = self.height;
        if denom != 0.0 {
            let a = (self.n_position - prev.n_position + dsign)
                / (next.n_position - self.n_position)
                * (next.height - self.height);
            let b = (next.n_position - self.n_position - dsign)
                / (self.n_position - prev.n_position)
                * (self.height - prev.height);
            h_par = self.height + (a + b) * dsign / denom;
        }

        // If parabolic result is within neighbors, use it; otherwise linear
        self.height = if h_par > prev.height && h_par < next.height && h_par.is_finite() {
            h_par
        } else {
            // Linear step toward neighbor indicated by dsign
            if dsign > 0.0 {
                self.height + (next.height - self.height) / (next.n_position - self.n_position)
            } else {
                self.height + (prev.height - self.height) / (prev.n_position - self.n_position)
            }
        };

        self.n_position += dsign;
    }

    fn update_desired_position(&mut self, n: usize) {
        self.n_desired = 1.0 + self.target_probability * (n as f64 - 1.0);
    }
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
