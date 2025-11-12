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
pub struct P2Quantile<const N: usize = 9> {
    q: f64,
    init_buf: Vec<f64>,
    n: usize,
    markers: [Marker; N],
    initialized: bool,
}

#[derive(Clone)]
struct Marker {
    h: f64,
    npos: f64,
    ndes: f64,
    p: f64,
}

impl Marker {
    fn adjust(&mut self, prev: Marker, next: Marker, dsign: f64) {
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
        let p = Self::generate_grid_quantiles(q);
        Ok(Self {
            q,
            init_buf: Vec::with_capacity(N),
            n: 0,
            markers: (0..N).map(|i| Marker {
                h: f64::NAN,
                npos: 0.0,
                ndes: 0.0,
                p: p[i],
            }).collect::<Vec<_>>().try_into().map_err(|_| {
                EncodingError::EncodingError("Cannot convert vec into array".to_string())
            })?,
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
                self.init_buf.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
                for i in 0..N {
                    self.markers[i].h = self.init_buf[i];
                }
                for i in 0..N {
                    self.markers[i].npos = (i + 1) as f64;
                }
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
        self.update_desired_positions();

        // 4) Adjust interior markers i = 1..N - 2
        for i in 1..(N - 1) {
            loop {
                let di = self.markers[i].ndes - self.markers[i].npos;
                if di >= 1.0 && (self.markers[i + 1].npos - self.markers[i].npos) > 1.0 {
                    self.markers[i].adjust(
                        self.markers[i - 1].clone(),
                        self.markers[i + 1].clone(),
                        1.0,
                    );
                } else if di <= -1.0 && (self.markers[i - 1].npos - self.markers[i].npos) < -1.0 {
                    self.markers[i].adjust(
                        self.markers[i - 1].clone(),
                        self.markers[i + 1].clone(),
                        -1.0,
                    );
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
        // `N / 2` marker tracks the target quantile
        self.markers[N / 2].h
    }

    fn update_desired_positions(&mut self) {
        for i in 0..N {
            self.markers[i].update_desired_position(self.n);
        }
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
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};

        let mut tdigest = P2Quantile::<7>::new(0.99).unwrap();

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
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};
        use rand_distr::StandardNormal;

        let q = 1.0 - (1.0 - 0.9544) / 2.0;

        let mut tdigest = P2Quantile::<7>::new(q).unwrap();

        // mean 2, standard deviation 3
        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(10_000);
        for _ in 0..10_000 {
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
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};
        use rand_distr::Poisson;

        let q = 0.99;

        let mut tdigest = P2Quantile::<7>::new(q).unwrap();

        // mean 2, standard deviation 3
        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(10_000);
        for _ in 0..10_000 {
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
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};
        use rand_distr::StudentT;

        let q = 0.99;

        let mut tdigest = P2Quantile::<7>::new(q).unwrap();

        // mean 2, standard deviation 3
        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(10_000);
        for _ in 0..10_000 {
            let value = rng.sample(StudentT::new(2.0).unwrap()) as f64;
            data.push(value);
            tdigest.push(value).unwrap();
        }

        let true_p95 = true_quantile(&data, q);

        let p95 = tdigest.estimate();
        assert_eq!(p95, true_p95);
    }
}
