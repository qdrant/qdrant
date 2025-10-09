use std::path::Path;

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::VectorParameters;
use crate::vector_stats::{VectorElementStats, VectorStats};

pub const ROTATION_STEPS: usize = 1;

#[derive(Serialize, Deserialize)]
pub struct Rotation {
    simple_rotations: Vec<SimpleRotation>,
}

#[derive(Serialize, Deserialize)]
pub struct SimpleRotation {
    permutation: Vec<usize>,
    pair_rotations: usize,
}

impl Rotation {
    pub fn new(
        data: impl Iterator<Item = impl AsRef<[f32]>> + Clone,
        vector_params: &VectorParameters,
        debug_path: Option<&Path>,
    ) -> Self {
        if data.clone().next().is_none() {
            return Self {
                simple_rotations: vec![],
            };
        }
        if let Some(debug_path) = debug_path {
            std::fs::create_dir_all(debug_path).ok();
        }

        let steps: usize = std::env::var("ROTATION_STEPS")
            .unwrap_or_default()
            .trim()
            .parse()
            .unwrap_or(ROTATION_STEPS);

        let mut simple_rotations: Vec<SimpleRotation> = vec![];
        for step in 0..steps {
            let data = data.clone().map(|v| {
                let mut vector = v.as_ref().to_vec();
                for simple_rotation in &simple_rotations {
                    simple_rotation.rotate(&mut vector);
                }
                vector
            });
            let debug_path = debug_path.map(|p| p.join(format!("rotation_{step}")));
            let rotation = SimpleRotation::new(
                data.clone(),
                vector_params,
                debug_path.as_ref().map(|p| p.as_ref()),
            );
            simple_rotations.push(rotation);
        }

        Self { simple_rotations }
    }

    pub fn rotate(&self, vector: &mut [f32]) {
        for simple_rotation in &self.simple_rotations {
            simple_rotation.rotate(vector);
        }
    }
}

impl SimpleRotation {
    fn new(
        data: impl Iterator<Item = impl AsRef<[f32]>>,
        vector_params: &VectorParameters,
        debug_path: Option<&Path>,
    ) -> Self {
        if let Some(debug_path) = debug_path {
            std::fs::create_dir_all(debug_path).ok();
        }

        let vector_stats = VectorStats::build(data, vector_params);
        let mut indices: Vec<usize> = (0..vector_params.dim).collect();
        indices.sort_by_key(|&i| OrderedFloat(param(&vector_stats.elements_stats[i])));

        let mut permutation: Vec<usize> = Default::default();
        for i in 0..(vector_params.dim / 2) {
            permutation.push(indices[i]);
            permutation.push(indices[vector_params.dim - 1 - i]);
        }
        if vector_params.dim % 2 == 1 {
            permutation.push(indices[vector_params.dim / 2]);
        }

        let pair_rotations = permutation
            .chunks_exact(2)
            .map(|pair| (pair[0], pair[1]))
            .map(|(i, j)| {
                let stats_i = &vector_stats.elements_stats[i];
                let stats_j = &vector_stats.elements_stats[j];
                let param_i = param(stats_i);
                let param_j = param(stats_j);
                param_i < param_j * 1.2
            })
            .take_while(|&x| x)
            .count();

        println!(
            "Pair rotations: {pair_rotations} / {}",
            vector_params.dim / 2
        );

        Self {
            permutation,
            pair_rotations,
        }
    }

    fn rotate(&self, vector: &mut [f32]) {
        // apply permutation
        let permuted = vector.to_owned();

        for (i, &p) in self.permutation.iter().enumerate() {
            vector[i] = permuted[p];
        }

        // apply 2x2 rotations
        for v in vector.chunks_exact_mut(2).take(self.pair_rotations) {
            let a = v[0] as f64;
            let b = v[1] as f64;
            let new_a = 0.707106 * a - 0.707106 * b;
            let new_b = 0.707106 * a + 0.707106 * b;
            v[0] = new_a as f32;
            v[1] = new_b as f32;
        }
    }
}

fn param(s: &VectorElementStats) -> f32 {
    s.stddev
}
