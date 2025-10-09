use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::VectorParameters;
use crate::vector_stats::VectorStats;

#[derive(Serialize, Deserialize)]
pub struct Shifter {
    vector_stats: Option<VectorStats>,
}

impl Shifter {
    pub fn new(
        data: impl Iterator<Item = impl AsRef<[f32]>> + Clone,
        vector_params: &VectorParameters,
        debug_path: Option<&Path>,
    ) -> Self {
        if data.clone().next().is_none() {
            return Self { vector_stats: None };
        }

        if let Some(debug_path) = debug_path {
            std::fs::create_dir_all(debug_path).ok();
        }

        let vector_stats = VectorStats::build(data, vector_params);
        Self {
            vector_stats: Some(vector_stats),
        }
    }

    pub fn shift(&self, vector: &mut [f32]) -> f32 {
        if let Some(vector_stats) = &self.vector_stats {
            for (v, stats) in vector.iter_mut().zip(vector_stats.elements_stats.iter()) {
                *v -= stats.mean;
            }
            vector_stats
                .elements_stats
                .iter()
                .zip(vector.iter())
                .map(|(s, &v)| v * s.mean)
                .sum()
        } else {
            0.0
        }
    }
}
