use serde::{Deserialize, Serialize};

use crate::VectorParameters;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorStats {
    pub elements_stats: Vec<VectorElementStats>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
        data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
        vector_params: &VectorParameters,
    ) -> Self {
        // The Welford's Algorithm
        let mut stats = VectorStats {
            elements_stats: vec![VectorElementStats::default(); vector_params.dim],
        };
        let mut m2 = vec![0.0; vector_params.dim];

        for (i, vector) in data.enumerate() {
            let vector = vector.as_ref();
            let count = i + 1;
            for ((&value, element_stats), m2) in vector
                .iter()
                .zip(stats.elements_stats.iter_mut())
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

                let delta = value - element_stats.mean;
                element_stats.mean += delta / count as f32;
                *m2 += delta * (value - element_stats.mean);
            }
        }

        for (element_stats, m2) in stats.elements_stats.iter_mut().zip(m2.iter()) {
            element_stats.stddev = if vector_params.count > 1 {
                (*m2 / (vector_params.count - 1) as f32).sqrt()
            } else {
                0.0
            };
        }

        stats
    }
}
