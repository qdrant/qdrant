use crate::EncodingError;

/// Extended version of P-square one-quantile estimator by Jain & Chlamtac (1985).
/// https://www.cse.wustl.edu/~jain/papers/ftp/psqr.pdf
/// By default, P-square uses 5 markers to estimate a single quantile.
/// This implementation is extended to support an arbitrary odd number of markers N >= 5
///
/// Usage:
/// let mut p2 = P2Quantile::<7>::new(0.99).unwrap();
/// for x in data { p2.push(x).unwrap(); }
/// let q_hat = p2.estimate();
pub enum P2Quantile<const N: usize = 7> {
    Linear(P2QuantileLinear<N>),
    Impl(P2QuantileImpl<N>),
}

impl<const N: usize> P2Quantile<N> {
    pub fn new(q: f64) -> Result<Self, EncodingError> {
        if N < 5 {
            return Err(EncodingError::EncodingError(
                "P2Quantile requires at least 5 markers".to_string(),
            ));
        }
        if N.is_multiple_of(2) {
            return Err(EncodingError::EncodingError(
                "P2Quantile requires an odd number of markers".to_string(),
            ));
        }
        if q <= 0.0 || q >= 1.0 {
            return Err(EncodingError::EncodingError(
                "Quantile q must be in (0, 1)".to_string(),
            ));
        }
        Ok(Self::Linear(P2QuantileLinear {
            q,
            buf: Default::default(),
        }))
    }

    /// Push one observation.
    pub fn push(&mut self, x: f64) -> Result<(), EncodingError> {
        if !x.is_finite() {
            return Ok(());
        }

        match self {
            P2Quantile::Linear(linear) => {
                linear.buf.push(x);
                if linear.buf.len() == N {
                    *self = P2Quantile::Impl(P2QuantileImpl::new_from_linear(linear));
                }
                Ok(())
            }
            P2Quantile::Impl(p2) => p2.push(x),
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
    n: usize,
    markers: [Marker; N],
}

impl<const N: usize> P2QuantileImpl<N> {
    fn new_from_linear(linear: &P2QuantileLinear<N>) -> Self {
        assert_eq!(linear.buf.len(), N);

        let mut buf = linear.buf.clone();
        buf.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());

        let p = Self::generate_grid_quantiles(linear.q);
        let mut markers = [Marker::default(); N];
        for i in 0..N {
            markers[i].h = buf[i];
            markers[i].p = p[i];
            markers[i].npos = (i + 1) as f64;
            markers[i].update_desired_position(N);
        }
        P2QuantileImpl { n: N, markers }
    }

    fn estimate(self) -> f64 {
        // `N / 2` marker tracks the target quantile
        self.markers[N / 2].h
    }

    fn push(&mut self, x: f64) -> Result<(), EncodingError> {
        self.n += 1;

        // 1) Identify cell k and update extreme markers if needed
        // k is the cell index in [0..N - 1]
        let mut k: Option<usize> = None;
        if x < self.markers[0].h {
            self.markers[0].h = x;
            k = Some(0);
        } else {
            for i in 1..N {
                if x < self.markers[i].h {
                    k = Some(i - 1);
                    break;
                }
            }
        }
        let k = if let Some(k) = k {
            k
        } else {
            self.markers[N - 1].h = x;
            N - 1
        };

        // 2) Increment positions of markers above k
        for i in (k + 1)..N {
            self.markers[i].npos += 1.0;
        }

        // 3) Update desired positions
        for i in 0..N {
            self.markers[i].update_desired_position(self.n);
        }

        // 4) Adjust interior markers
        for i in 1..(N - 1) {
            self.markers[i].adjust(self.markers[i - 1], self.markers[i + 1]);
        }

        Ok(())
    }

    fn generate_grid_quantiles(q: f64) -> smallvec::SmallVec<[f64; N]> {
        let mut p = smallvec::SmallVec::<[f64; N]>::with_capacity(N);
        let additional_markers_count = (N - 5) / 2;
        p.push(0.0);
        p.push(q * 0.5);
        for i in 0..additional_markers_count {
            let factor = 0.7 + 0.3 * (i + 1) as f64 / (additional_markers_count as f64 + 2.0);
            p.push(q * factor);
        }
        p.push(q);
        for i in (0..additional_markers_count).rev() {
            let factor = 0.7 + 0.3 * (i + 1) as f64 / (additional_markers_count as f64 + 2.0);
            p.push(1.0 + (q - 1.0) * factor);
        }
        p.push(1.0 + (q - 1.0) * 0.5);
        p.push(1.0);
        p
    }
}

#[derive(Clone, Copy, Default)]
struct Marker {
    h: f64,
    npos: f64,
    ndes: f64,
    p: f64,
}

impl Marker {
    fn adjust(&mut self, prev: Marker, next: Marker) {
        loop {
            let di = self.ndes - self.npos;
            if di >= 1.0 && (next.npos - self.npos) > 1.0 {
                self.adjust_step(&prev, &next, 1.0);
            } else if di <= -1.0 && (prev.npos - self.npos) < -1.0 {
                self.adjust_step(&prev, &next, -1.0);
            } else {
                break;
            }
        }
    }

    fn adjust_step(&mut self, prev: &Marker, next: &Marker, dsign: f64) {
        // Try parabolic prediction
        let denom = next.npos - prev.npos;
        let mut h_par = self.h;
        if denom != 0.0 {
            let a = (self.npos - prev.npos + dsign) / (next.npos - self.npos) * (next.h - self.h);
            let b = (next.npos - self.npos - dsign) / (self.npos - prev.npos) * (self.h - prev.h);
            h_par = self.h + (a + b) * dsign / denom;
        }

        // If parabolic result is within neighbors, use it; otherwise linear
        self.h = if h_par > prev.h && h_par < next.h && h_par.is_finite() {
            h_par
        } else {
            // Linear step toward neighbor indicated by dsign
            if dsign > 0.0 {
                self.h + (next.h - self.h) / (next.npos - self.npos)
            } else {
                self.h + (prev.h - self.h) / (prev.npos - self.npos)
            }
        };

        self.npos += dsign;
    }

    fn update_desired_position(&mut self, n: usize) {
        self.ndes = 1.0 + self.p * (n as f64 - 1.0);
    }
}

pub struct P2QuantileLinear<const N: usize> {
    q: f64,
    buf: smallvec::SmallVec<[f64; N]>,
}

impl<const N: usize> P2QuantileLinear<N> {
    /// Simple linear-interpolated sample quantile
    fn estimate(mut self) -> f64 {
        let n = self.buf.len();
        if n == 0 {
            // No data
            return 0.0;
        }
        if n == 1 {
            return self.buf[0];
        }
        self.buf.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());

        let k = self.q * (n as f64 - 1.0);
        let lo = k.floor() as usize;
        let hi = k.ceil() as usize;
        if lo == hi {
            self.buf[lo]
        } else {
            let frac = k - lo as f64;
            self.buf[lo] + frac * (self.buf[hi] - self.buf[lo])
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use rand_distr::{Poisson, StandardNormal, StudentT};

    use super::*;

    const N: usize = 7;
    const COUNT: usize = 10_000;

    #[test]
    fn test_p_square() {
        // Test P2 quantile estimator on uniformly distributed data
        const QUANTILE: f64 = 0.99;
        // In case of uniform distribution, the theoretical value of quantile is equal to the quantile level
        const THEOTICAL_VALUE: f64 = QUANTILE;
        const ERROR: f64 = 1e-2;
        let mut p2 = P2Quantile::<N>::new(QUANTILE).unwrap();

        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(COUNT);
        for _ in 0..COUNT {
            let value = rng.random::<f64>();
            data.push(value);

            p2.push(value).unwrap();
        }

        // Take P square estimation
        let p = p2.estimate();

        // Compare with linear estimation
        let linear_p = P2QuantileLinear::<N> {
            q: QUANTILE,
            buf: smallvec::SmallVec::from_slice(data.as_slice()),
        }
        .estimate();
        assert!((p - linear_p).abs() < ERROR);

        // Compare with theoretical value
        assert!((p - THEOTICAL_VALUE).abs() < ERROR);
    }

    #[test]
    fn test_p_square_normal() {
        // Test P2 quantile estimator on normally N(0, 1) distributed data
        // Take percentile corresponding to 2 standard deviations (2 sigmas)
        // It'a approximately 97.72 percentile
        const QUANTILE: f64 = 0.9772;
        // The theoretical value of 97.72 percentile for N(0, 1) is approximately 2 sigmas, i.e., 2.0
        const THEOTICAL_VALUE: f64 = 2.0;
        const ERROR: f64 = 0.1; // allow 5% error (0.1 / 2.0 = 0.05 = 5%)

        let mut p2 = P2Quantile::<N>::new(QUANTILE).unwrap();

        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(COUNT);
        for _ in 0..COUNT {
            let value = rng.sample(StandardNormal);
            data.push(value);

            p2.push(value).unwrap();
        }

        // Take P square estimation
        let p = p2.estimate();

        // Compare with linear estimation
        let linear_p = P2QuantileLinear::<N> {
            q: QUANTILE,
            buf: smallvec::SmallVec::from_slice(data.as_slice()),
        }
        .estimate();
        assert!((p - linear_p).abs() < ERROR);

        // Compare with theoretical value
        assert!((p - THEOTICAL_VALUE).abs() < ERROR);
    }

    #[test]
    fn test_p_square_normal_low() {
        // Same as test_p_square_normal but with 100 - 97.72 = 2.28 percentile
        // Test P2 quantile estimator on normally N(0, 1) distributed data
        // Take percentile corresponding to -2 standard deviations (-2 sigmas)
        // It'a approximately 2.28 percentile
        const QUANTILE: f64 = 0.0228;
        // The theoretical value of 2.28 percentile for N(0, 1) is approximately -2 sigmas, i.e., -2.0
        const THEOTICAL_VALUE: f64 = -2.0;
        const ERROR: f64 = 0.1; // allow 5% error (0.1 / 2.0 = 0.05 = 5%)

        let mut p2 = P2Quantile::<N>::new(QUANTILE).unwrap();

        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(COUNT);
        for _ in 0..COUNT {
            let value = rng.sample(StandardNormal);
            data.push(value);

            p2.push(value).unwrap();
        }

        // Take P square estimation
        let p = p2.estimate();

        // Compare with linear estimation
        let linear_p = P2QuantileLinear::<N> {
            q: QUANTILE,
            buf: smallvec::SmallVec::from_slice(data.as_slice()),
        }
        .estimate();
        assert!((p - linear_p).abs() < ERROR);

        // Compare with theoretical value
        assert!((p - THEOTICAL_VALUE).abs() < ERROR);
    }

    #[test]
    fn test_p_square_poisson() {
        // Take Poisson-distributed data with mean 2. It's case of non-symmetric and non-normal distribution.
        const QUANTILE: f64 = 0.99;
        // The theoretical value of 99 percentile is 6.0
        const THEOTICAL_VALUE: f64 = 6.0;
        const ERROR: f64 = 0.3; // allow 5% error (0.3 / 6.0 = 0.05 = 5%)

        let mut p2 = P2Quantile::<N>::new(QUANTILE).unwrap();

        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(COUNT);
        for _ in 0..COUNT {
            let value = rng.sample(Poisson::new(2.0).unwrap()) as f64;
            data.push(value);

            p2.push(value).unwrap();
        }

        // Take P square estimation
        let p = p2.estimate();

        // Compare with linear estimation
        let linear_p = P2QuantileLinear::<N> {
            q: QUANTILE,
            buf: smallvec::SmallVec::from_slice(data.as_slice()),
        }
        .estimate();
        assert!((p - linear_p).abs() < ERROR);

        // Compare with theoretical value
        assert!((p - THEOTICAL_VALUE).abs() < ERROR);
    }

    #[test]
    fn test_p_square_student() {
        // Corner case test with Student t-distribution with low degrees of freedom (heavy tails)
        // StudentT-distributed data with 2 degrees of freedom has heavy tails and infinite variance.
        const QUANTILE: f64 = 0.99;
        // The theoretical value of 99 percentile is somewhat around 6.9646
        const THEOTICAL_VALUE: f64 = 6.9646;
        const ERROR: f64 = 0.69646; // 10% error because of heavy tails

        let mut p2 = P2Quantile::<N>::new(QUANTILE).unwrap();

        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(COUNT);
        for _ in 0..COUNT {
            let value = rng.sample(StudentT::new(2.0).unwrap()) as f64;
            data.push(value);

            p2.push(value).unwrap();
        }

        // Take P square estimation
        let p = p2.estimate();

        // Compare with linear estimation
        let linear_p = P2QuantileLinear::<N> {
            q: QUANTILE,
            buf: smallvec::SmallVec::from_slice(data.as_slice()),
        }
        .estimate();
        assert!((p - linear_p).abs() < ERROR);

        // Compare with theoretical value
        assert!((p - THEOTICAL_VALUE).abs() < ERROR);
    }

    #[test]
    fn test_p_square_zeros() {
        let mut p2 = P2Quantile::<N>::new(0.99).unwrap();
        for _ in 0..COUNT {
            p2.push(0.0).unwrap();
        }

        // Take P square estimation
        let p = p2.estimate();

        // Should be exactly zero
        assert_eq!(p, 0.0);
    }

    #[test]
    fn test_p_square_linear() {
        let mut p2 = P2Quantile::<N>::new(0.99).unwrap();
        p2.push(0.0).unwrap();
        p2.push(0.0).unwrap();
        p2.push(0.0).unwrap();

        // Take P square estimation
        let p = p2.estimate();

        // Should be exactly zero
        assert_eq!(p, 0.0);
    }
}
