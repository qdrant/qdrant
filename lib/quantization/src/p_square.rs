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
pub struct P2Quantile<const N: usize = 7> {
    q: f64,
    init_buf: Vec<f64>,
    n: usize,
    h: [f64; N],
    npos: [f64; N],
    ndes: [f64; N],
    p: [f64; N],
    initialized: bool,
}

impl<const N: usize> P2Quantile<N> {
    pub fn new(q: f64) -> Result<Self, EncodingError> {
        if N < 5 {
            return Err(EncodingError::ArgumentsError(
                "P2Quantile requires at least 5 markers".to_string(),
            ));
        }
        if N.is_multiple_of(2) {
            return Err(EncodingError::ArgumentsError(
                "P2Quantile requires an odd number of markers".to_string(),
            ));
        }
        if q <= 0.0 || q >= 1.0 {
            return Err(EncodingError::ArgumentsError(
                "Quantile q must be in (0, 1)".to_string(),
            ));
        }
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

        Ok(Self {
            q,
            init_buf: Vec::with_capacity(9),
            n: 0,
            h: [f64::NAN; N],
            npos: [0.0; N],
            ndes: [0.0; N],
            p: p.as_slice().try_into().map_err(|_| EncodingError::EncodingError("Cannot convert vec into array".to_string()))?,
            initialized: false,
        })
    }

    /// Push one observation.
    pub fn push(&mut self, x: f64) -> Result<(), EncodingError> {
        if !x.is_finite() {
            return Ok(());
        }

        if !self.initialized {
            self.init_buf.push(x);
            self.n += 1;

            if self.init_buf.len() == N {
                self.init_buf.sort_by(|a, b| a.partial_cmp(b).unwrap());
                for i in 0..N {
                    self.h[i] = self.init_buf[i];
                }
                self.npos = (1..=N).map(|x| x as f64).collect::<Vec<_>>().try_into().map_err(|_| EncodingError::EncodingError("Cannot convert vec into array".to_string()))?;
                self.update_desired_positions();
                self.initialized = true;
                self.init_buf.clear();
            }
            return Ok(());
        }

        self.n += 1;

        // 1) Identify cell k and update extreme markers if needed
        // k is the cell index in [0..N - 1]
        let mut k: Option<usize> = None;
        if x < self.h[0] {
            self.h[0] = x;
            k = Some(0);
        } else {
            for i in 1..(N - 1) {
                if x < self.h[i] {
                    k = Some(i - 1);
                    break;
                }
            }
        }
        let k = if let Some(k) = k {
            k
        } else {
            self.h[N - 1] = x;
            N - 1
        };

        // 2) Increment positions of markers above k
        for i in (k + 1)..=(N - 1) {
            self.npos[i] += 1.0;
        }

        // 3) Update desired positions
        self.update_desired_positions();

        // 4) Adjust interior markers i = 1..N - 1
        for i in 1..(N - 1) {
            loop {
                let di = self.ndes[i] - self.npos[i];
                if di >= 1.0 && (self.npos[i + 1] - self.npos[i]) > 1.0 {
                    self.adjust_marker(i, 1.0);
                } else if di <= -1.0 && (self.npos[i - 1] - self.npos[i]) < -1.0 {
                    self.adjust_marker(i, -1.0);
                } else {
                    break;
                }
            }
        }

        Ok(())
    }

    pub fn estimate(self) -> f64 {
        if self.n == 0 {
            return f64::NAN;
        }
        if !self.initialized {
            // Not enough data to initialize P square; compute direct sample quantile
            return sample_quantile(&self.init_buf, self.q);
        }
        self.h[N / 2]
    }

    fn update_desired_positions(&mut self) {
        // ndes[i] = 1 + p[i] * (n - 1)
        let nf = self.n as f64;
        for i in 0..N {
            self.ndes[i] = 1.0 + self.p[i] * (nf - 1.0);
        }
    }

    fn adjust_marker(&mut self, i: usize, dsign: f64) {
        // Try parabolic prediction
        let n_im1 = self.npos[i - 1];
        let n_i = self.npos[i];
        let n_ip1 = self.npos[i + 1];
        let h_im1 = self.h[i - 1];
        let h_i = self.h[i];
        let h_ip1 = self.h[i + 1];

        let denom = n_ip1 - n_im1;
        let mut h_par = h_i;
        if denom != 0.0 {
            let a = (n_i - n_im1 + dsign) / (n_ip1 - n_i) * (h_ip1 - h_i);
            let b = (n_ip1 - n_i - dsign) / (n_i - n_im1) * (h_i - h_im1);
            h_par = h_i + (dsign / denom) * (a + b);
        }

        // If parabolic result is within neighbors, use it; otherwise linear
        let h_new = if h_par > h_im1 && h_par < h_ip1 && h_par.is_finite() {
            h_par
        } else {
            // Linear step toward neighbor indicated by dsign
            if dsign > 0.0 {
                h_i + (h_ip1 - h_i) / (n_ip1 - n_i)
            } else {
                h_i + (h_im1 - h_i) / (n_im1 - n_i)
            }
        };

        self.h[i] = h_new;
        self.npos[i] += dsign;
    }
}

/// Simple linear-interpolated sample quantile
fn sample_quantile(xs: &[f64], q: f64) -> f64 {
    let n = xs.len();
    if n == 0 {
        return f64::NAN;
    }
    if n == 1 {
        return xs[0];
    }
    let mut v = xs.to_owned();
    v.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());

    let k = q * (n as f64 - 1.0);
    let lo = k.floor() as usize;
    let hi = k.ceil() as usize;
    if lo == hi {
        v[lo]
    } else {
        let frac = k - lo as f64;
        v[lo] + frac * (v[hi] - v[lo])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn true_quantile(data: &Vec<f64>, q: f64) -> f64 {
        let mut v = data.clone();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = v.len();
        let k = q * (n as f64 - 1.0);
        let lo = k.floor() as usize;
        let hi = k.ceil() as usize;
        if lo == hi {
            v[lo]
        } else {
            let frac = k - lo as f64;
            v[lo] + frac * (v[hi] - v[lo])
        }
    }

    #[test]
    fn test_random_stable() {
        // Simple stability check
        use rand::SeedableRng;
        use rand::rngs::StdRng;
        use rand::Rng;

        let mut tdigest = P2Quantile::<9>::new(0.99).unwrap();

        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(10_000);
        for _ in 0..10_000 {
            let value = rng.random::<f64>();
            data.push(value);
            tdigest.push(value).unwrap();
        }

        let true_p95 = true_quantile(&data, 0.99);

        let p95 = tdigest.estimate();
        assert_eq!(p95, true_p95);
    }

    #[test]
    fn test_random_normal() {
        // Simple stability check
        use rand::SeedableRng;
        use rand::rngs::StdRng;
        use rand::Rng;
        use rand_distr::StandardNormal;

        let q = 1.0 - (1.0 - 0.9544) / 2.0;

        let mut tdigest = P2Quantile::<7>::new(q).unwrap();

        // mean 2, standard deviation 3
        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(100_000);
        for _ in 0..100_000 {
            let value = rng.sample(StandardNormal);
            data.push(value);
            tdigest.push(value).unwrap();
        }

        let true_p95 = true_quantile(&data, q);

        let p95 = tdigest.estimate();
        assert_eq!(p95, true_p95);
    }

    #[test]
    fn test_random_poisson() {
        // Simple stability check
        use rand::SeedableRng;
        use rand::rngs::StdRng;
        use rand::Rng;
        use rand_distr::Poisson;

        let q = 0.99;

        let mut tdigest = P2Quantile::<9>::new(q).unwrap();

        // mean 2, standard deviation 3
        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(100_000);
        for _ in 0..100_000 {
            let value = rng.sample(Poisson::new(2.0).unwrap()) as f64;
            data.push(value);
            tdigest.push(value).unwrap();
        }

        let true_p95 = true_quantile(&data, q);

        let p95 = tdigest.estimate();
        assert_eq!(p95, true_p95);
    }

    #[test]
    fn test_random_student() {
        // Simple stability check
        use rand::SeedableRng;
        use rand::rngs::StdRng;
        use rand::Rng;
        use rand_distr::StudentT;

        let q = 0.99;

        let mut tdigest = P2Quantile::<9>::new(q).unwrap();

        // mean 2, standard deviation 3
        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(100_000);
        for _ in 0..100_000 {
            let value = rng.sample(StudentT::new(1.0).unwrap()) as f64;
            data.push(value);
            tdigest.push(value).unwrap();
        }

        let true_p95 = true_quantile(&data, q);

        let p95 = tdigest.estimate();
        assert_eq!(p95, true_p95);
    }
}
