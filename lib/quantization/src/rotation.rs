use std::error::Error;
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
        log::info!("Rotation steps: {steps}");

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
        data: impl Iterator<Item = impl AsRef<[f32]>> + Clone,
        vector_params: &VectorParameters,
        debug_path: Option<&Path>,
    ) -> Self {
        if let Some(debug_path) = debug_path {
            std::fs::create_dir_all(debug_path).ok();
            //            for dim in 0..vector_params.dim {
            //                let numbers = data
            //                    .clone()
            //                    .map(|v| v.as_ref()[dim])
            //                    .collect::<Vec<f32>>();
            //                plot_histogram(&numbers, &debug_path.join(format!("histogram_{dim}.png")), None).unwrap();
            //            }
        }

        let rotation_bound: f32 = std::env::var("ROTATION_BOUND")
            .unwrap_or_default()
            .trim()
            .parse()
            .unwrap_or(0.8);

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
                //                println!(
                //                    "Pair ({i}, {j}): stddevs = ({:.4}, {:.4}), params = ({:.4}, {:.4}), ratio = {:.4}",
                //                    stats_i.stddev,
                //                    stats_j.stddev,
                //                    param_i,
                //                    param_j,
                //                    param_i.abs() / param_j.abs()
                //                );
                param_i.abs() / param_j.abs() < rotation_bound
            })
            .take_while(|&x| x)
            .count();

        log::info!(
            "Pair rotations: {pair_rotations} / {} with bound {rotation_bound}",
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

pub fn plot_histogram(
    data: &[f32],
    path: &Path,
    maybe_bins: Option<usize>,
) -> Result<(), Box<dyn Error>> {
    use plotters::prelude::*;

    let dump = std::env::var("DUMP_IMAGES").unwrap_or_default();
    if dump != "1" && dump.to_lowercase() != "true" {
        return Ok(());
    }

    let width_px: u32 = std::env::var("WIDTH_PX")
        .unwrap_or_default()
        .trim()
        .parse()
        .unwrap_or(512);

    let height_px: u32 = std::env::var("HEIGHT_PX")
        .unwrap_or_default()
        .trim()
        .parse()
        .unwrap_or(512);

    if data.is_empty() {
        return Err("Empty data array".into());
    }

    let (min_v_f32, max_v_f32) = data
        .iter()
        .fold((f32::INFINITY, f32::NEG_INFINITY), |(mn, mx), &v| {
            (mn.min(v), mx.max(v))
        });

    let (min_v, max_v) = if (max_v_f32 - min_v_f32).abs() < f32::EPSILON {
        let delta = 0.5_f32.max(max_v_f32.abs() * 0.1);
        ((min_v_f32 - delta) as f64, (max_v_f32 + delta) as f64)
    } else {
        (min_v_f32 as f64, max_v_f32 as f64)
    };

    let n = data.len();
    let bins = maybe_bins.unwrap_or_else(|| (n as f64).sqrt().round().max(5.0) as usize);

    let range = max_v - min_v;
    let bin_w = range / bins as f64;

    let safe_bin_w = if bin_w > 0.0 { bin_w } else { 1.0 };

    let mut counts = vec![0usize; bins];
    for &v in data {
        let mut idx = ((v as f64 - min_v) / safe_bin_w).floor() as isize;
        if idx < 0 {
            idx = 0;
        } else if idx as usize >= bins {
            idx = bins as isize - 1;
        }
        counts[idx as usize] += 1;
    }

    let max_count = counts.iter().copied().max().unwrap_or(1);

    let root = BitMapBackend::new(path, (width_px, height_px)).into_drawing_area();
    root.fill(&WHITE)?;

    let mut chart = ChartBuilder::on(&root)
        .margin(20)
        .caption("Histogram", ("sans-serif", 24))
        .x_label_area_size(40)
        .y_label_area_size(50)
        .build_cartesian_2d(min_v..max_v, 0..(max_count + (max_count / 10).max(1)))?;

    chart
        .configure_mesh()
        .x_desc("Value")
        .y_desc("Count")
        .disable_mesh()
        .x_labels(10)
        .y_labels(10)
        .label_style(("sans-serif", 14))
        .draw()?;

    for (i, &c) in counts.iter().enumerate() {
        let x0 = min_v + i as f64 * safe_bin_w;
        let x1 = x0 + safe_bin_w;
        let y0 = 0;
        let y1 = c;

        chart
            .draw_series(std::iter::once(Rectangle::new(
                [(x0, y0), (x1, y1)],
                BLUE.mix(0.7).filled(),
            )))?
            .label("bin")
            .legend(|(x, y)| Rectangle::new([(x, y - 5), (x + 10, y + 5)], BLUE.filled()));
    }

    chart
        .configure_series_labels()
        .border_style(&BLACK)
        .background_style(&WHITE.mix(0.8))
        .draw()?;

    root.present()?;
    Ok(())
}
