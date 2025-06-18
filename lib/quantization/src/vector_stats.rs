use serde::{Deserialize, Serialize};

use crate::VectorParameters;

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

impl VectorStats {
    pub fn build<'a>(
        data: impl Iterator<Item = impl AsRef<[f32]> + 'a>,
        vector_params: &VectorParameters,
    ) -> Self {
        // The Welford's Algorithm.
        let mut stats = VectorStats {
            elements_stats: vec![VectorElementStats::default(); vector_params.dim],
        };

        // For internal calculations use higher precision.
        let mut m2 = vec![0.0f64; vector_params.dim];
        let mut means = vec![0.0f64; vector_params.dim];

        let mut count = 0;
        for vector in data {
            let vector = vector.as_ref();
            count += 1;

            debug_assert_eq!(
                vector.len(),
                vector_params.dim,
                "Vector length does not match the expected dimension"
            );

            for (((&value, element_stats), mean), m2) in vector
                .iter()
                .zip(stats.elements_stats.iter_mut())
                .zip(means.iter_mut())
                .zip(m2.iter_mut())
            {
                element_stats.min = if value < element_stats.min {
                    value
                } else {
                    element_stats.min
                };
                element_stats.max = if value > element_stats.max {
                    value
                } else {
                    element_stats.max
                };

                let delta = f64::from(value) - *mean;
                *mean += delta / count as f64;
                *m2 += delta * (f64::from(value) - *mean);
            }
        }

        debug_assert_eq!(
            count, vector_params.count,
            "Count of vectors processed does not match the expected count in vector parameters"
        );
        for ((element_stats, means), m2) in stats
            .elements_stats
            .iter_mut()
            .zip(means.iter())
            .zip(m2.iter())
        {
            element_stats.stddev = if count > 1 {
                (*m2 / (count - 1) as f64).sqrt() as f32
            } else {
                0.0
            };
            element_stats.mean = *means as f32;
        }

        stats
    }
}
