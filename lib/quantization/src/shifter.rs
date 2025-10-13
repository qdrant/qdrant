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
        let skip = std::env::var("SKIP_SHIFTING")
            .unwrap_or_default()
            .trim()
            .parse()
            .unwrap_or(0)
            == 1;

        if skip {
            log::info!("Skipping shifting as per environment variable");
        }

        if data.clone().next().is_none() || skip {
            return Self { vector_stats: None };
        }

        //let debug_path = debug_path.map(|p| p.join(format!("orig_histograms")));
        if let Some(debug_path) = &debug_path {
            std::fs::create_dir_all(debug_path).ok();
            //    for dim in 0..vector_params.dim {
            //        let numbers = data
            //            .clone()
            //            .map(|v| v.as_ref()[dim])
            //            .collect::<Vec<f32>>();
            //        crate::rotation::plot_histogram(&numbers, &debug_path.join(format!("orig_histogram_{dim}.png")), None).unwrap();
            //    }
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
