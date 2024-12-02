use std::sync::atomic::{AtomicBool, Ordering};

use rand::Rng;
use rayon::prelude::*;
use rayon::ThreadPool;

use crate::EncodingError;

pub fn kmeans(
    data: &[f32],
    centroids_count: usize,
    dim: usize,
    max_iterations: usize,
    max_threads: usize,
    accuracy: f32,
    stopped: &AtomicBool,
) -> Result<Vec<f32>, EncodingError> {
    let pool = rayon::ThreadPoolBuilder::new()
        .thread_name(|idx| format!("kmeans-{idx}"))
        .num_threads(max_threads)
        .build()
        .map_err(|e| {
            EncodingError::EncodingError(format!("Failed PQ encoding while thread pool init: {e}"))
        })?;

    // initial centroids positions are some vectors from data
    let mut centroids = data[0..centroids_count * dim].to_vec();
    let mut centroid_indexes = vec![0u32; data.len() / dim];

    for _ in 0..max_iterations {
        if stopped.load(Ordering::Relaxed) {
            return Err(EncodingError::Stopped);
        }

        update_indexes(&pool, data, &mut centroid_indexes, &centroids);
        if update_centroids(
            &pool,
            data,
            &centroid_indexes,
            &mut centroids,
            max_threads,
            accuracy,
        ) {
            break;
        }
    }
    update_indexes(&pool, data, &mut centroid_indexes, &centroids);
    Ok(centroids)
}

fn update_centroids(
    pool: &ThreadPool,
    data: &[f32],
    centroid_indexes: &[u32],
    centroids: &mut [f32],
    max_threads: usize,
    accuracy: f32,
) -> bool {
    struct CentroidsCounter {
        counter: Vec<usize>,
        acc: Vec<f64>,
    }

    let dim = data.len() / centroid_indexes.len();
    let centroids_count = centroids.len() / dim;

    let mut counters = (0..max_threads)
        .map(|_| CentroidsCounter {
            counter: vec![0usize; centroids_count],
            acc: vec![0.0_f64; centroids.len()],
        })
        .collect::<Vec<_>>();

    pool.install(|| {
        counters
            .par_iter_mut()
            .enumerate()
            .for_each(|(i, counter)| {
                let chunk_size = centroid_indexes.len() / max_threads;
                let vector_data_range = if i + 1 == max_threads {
                    chunk_size * i..centroid_indexes.len()
                } else {
                    chunk_size * i..chunk_size * (i + 1)
                };

                for i in vector_data_range {
                    let vector_data = &data[dim * i..dim * (i + 1)];
                    let centroid_index = centroid_indexes[i] as usize;
                    counter.counter[centroid_index] += 1;
                    let centroid_data =
                        &mut counter.acc[dim * centroid_index..dim * (centroid_index + 1)];
                    for (c, v) in centroid_data.iter_mut().zip(vector_data.iter()) {
                        *c += f64::from(*v);
                    }
                }
            })
    });

    let mut counter = CentroidsCounter {
        counter: vec![0usize; centroids_count],
        acc: vec![0.0_f64; centroids.len()],
    };
    for c in counters {
        for (dst, src) in counter.counter.iter_mut().zip(c.counter.iter()) {
            *dst += src;
        }
        for (dst, src) in counter.acc.iter_mut().zip(c.acc.iter()) {
            *dst += src;
        }
    }

    for (centroid_index, centroid_data) in counter.acc.chunks_exact_mut(dim).enumerate() {
        if counter.counter[centroid_index] == 0 {
            // the cluster is empty, so we take random vector as centroid
            let data_index = rand::thread_rng().gen_range(0..centroid_indexes.len());
            let vector = &data[dim * data_index..dim * (data_index + 1)];
            centroid_data
                .iter_mut()
                .zip(vector.iter())
                .for_each(|(c, v)| *c = f64::from(*v));
        } else {
            let count = counter.counter[centroid_index] as f64;
            centroid_data.iter_mut().for_each(|c| *c /= count);
        }
    }

    let diff = centroids
        .iter_mut()
        .zip(counter.acc.iter())
        .map(|(c, c_acc)| {
            let c_acc = *c_acc as f32;
            let value = (*c - c_acc).abs();
            *c = c_acc;
            value
        })
        .sum::<f32>();
    diff < accuracy
}

fn update_indexes(
    pool: &ThreadPool,
    data: &[f32],
    centroid_indexes: &mut [u32],
    centroids: &[f32],
) {
    let dim = data.len() / centroid_indexes.len();
    pool.install(|| {
        centroid_indexes
            .par_iter_mut()
            .enumerate()
            .for_each(|(i, c)| {
                let vector_data = &data[dim * i..dim * (i + 1)];
                let mut min_distance = f32::MAX;
                let mut min_centroid_index = 0;
                for (centroid_index, centroid_data) in centroids.chunks_exact(dim).enumerate() {
                    let distance = vector_data
                        .iter()
                        .zip(centroid_data.iter())
                        .map(|(a, b)| (a - b).powi(2))
                        .sum();
                    if distance < min_distance {
                        min_distance = distance;
                        min_centroid_index = centroid_index;
                    }
                }
                *c = min_centroid_index as u32;
            })
    });
}
