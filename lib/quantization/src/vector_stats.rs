use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorStats {
    pub elements_stats: Vec<VectorElementStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorElementStats {
    pub min: f32,
    pub max: f32,
    pub mean: f32,
    pub stddev: f32,
}

impl Default for VectorElementStats {
    fn default() -> Self {
        VectorElementStats {
            min: f32::MAX,
            max: f32::MIN,
            mean: 0.0,
            stddev: 0.0,
        }
    }
}

/// Streaming Welford accumulator. Use [`VectorStatsBuilder::add`] for each
/// sample, then [`VectorStatsBuilder::build`] to finalize.
pub struct VectorStatsBuilder {
    stats: VectorStats,
    means: Vec<f64>,
    m2: Vec<f64>,
    count: u64,
}

impl VectorStatsBuilder {
    pub fn new(dim: usize) -> Self {
        VectorStatsBuilder {
            stats: VectorStats {
                elements_stats: vec![VectorElementStats::default(); dim],
            },
            means: vec![0.0f64; dim],
            m2: vec![0.0f64; dim],
            count: 0,
        }
    }

    pub fn add<T: Copy>(&mut self, vector: &[T])
    where
        f64: From<T>,
    {
        // `assert_eq!` rather than `debug_assert_eq!` — a length mismatch
        // here would silently zip-truncate in release builds, partially
        // updating the Welford aggregates and corrupting them without ever
        // surfacing the error.
        assert_eq!(
            vector.len(),
            self.stats.elements_stats.len(),
            "Vector length does not match the expected dimension"
        );
        self.count += 1;
        let count_f64 = self.count as f64;

        for (((&value, element_stats), mean), m2) in vector
            .iter()
            .zip(self.stats.elements_stats.iter_mut())
            .zip(self.means.iter_mut())
            .zip(self.m2.iter_mut())
        {
            let value_f64 = f64::from(value);
            let value_f32 = value_f64 as f32;
            if value_f32 < element_stats.min {
                element_stats.min = value_f32;
            }
            if value_f32 > element_stats.max {
                element_stats.max = value_f32;
            }

            let delta = value_f64 - *mean;
            *mean += delta / count_f64;
            *m2 += delta * (value_f64 - *mean);
        }
    }

    pub fn build(mut self) -> VectorStats {
        let count = self.count;
        for ((element_stats, means), m2) in self
            .stats
            .elements_stats
            .iter_mut()
            .zip(self.means.iter())
            .zip(self.m2.iter())
        {
            element_stats.stddev = if count > 1 {
                (*m2 / (count - 1) as f64).sqrt() as f32
            } else {
                0.0
            };
            element_stats.mean = *means as f32;
        }
        self.stats
    }
}

impl VectorStats {
    pub fn build<T>(data: impl Iterator<Item = impl AsRef<[T]>>, dim: usize) -> Self
    where
        T: Copy,
        f64: From<T>,
    {
        let mut builder = VectorStatsBuilder::new(dim);
        for vector in data {
            builder.add(vector.as_ref());
        }
        builder.build()
    }
}
