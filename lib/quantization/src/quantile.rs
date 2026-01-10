use std::sync::atomic::{AtomicBool, Ordering};

use permutation_iterator::Permutor;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::EncodingError;
use crate::p_square::P2Quantile;

pub const QUANTILE_SAMPLE_SIZE: usize = 100_000;
pub const P2_SAMPLE_SIZE: usize = 5_000;
pub const P2_MARKERS: usize = 7;

pub(crate) fn find_min_max_from_iter<'a>(
    iter: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
) -> (f32, f32) {
    iter.fold((f32::MAX, f32::MIN), |(mut min, mut max), vector| {
        for &value in vector.as_ref() {
            if value < min {
                min = value;
            }
            if value > max {
                max = value;
            }
        }
        (min, max)
    })
}

pub(crate) fn find_quantile_interval<'a>(
    vector_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
    dim: usize,
    count: usize,
    quantile: f32,
    stopped: &AtomicBool,
) -> Result<Option<(f32, f32)>, EncodingError> {
    if count < 127 || quantile >= 1.0 {
        return Ok(None);
    }

    let mut data_slice =
        take_random_vectors(vector_data, dim, count, QUANTILE_SAMPLE_SIZE / dim, stopped)?;
    let selected_vector_count = data_slice.len() / dim;

    let data_slice_len = data_slice.len();
    if data_slice_len < 4 {
        return Ok(None);
    }

    let cut_index = std::cmp::min(
        (data_slice_len - 1) / 2,
        (selected_vector_count as f32 * (1.0 - quantile) / 2.0) as usize,
    );
    let cut_index = std::cmp::max(cut_index, 1);
    let comparator = |a: &f32, b: &f32| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal);
    let (selected_values, _, _) =
        data_slice.select_nth_unstable_by(data_slice_len - cut_index, comparator);
    let (_, _, selected_values) = selected_values.select_nth_unstable_by(cut_index, comparator);

    if selected_values.len() < 2 {
        return Ok(None);
    }

    let selected_values = [selected_values];
    Ok(Some(find_min_max_from_iter(
        selected_values.iter().map(|v| &v[..]),
    )))
}

pub fn find_interval_per_coordinate<'a>(
    vector_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
    dim: usize,
    count: usize,
    quantile: f32,
    num_threads: usize,
    stopped: &AtomicBool,
) -> Result<Vec<(f32, f32)>, EncodingError> {
    debug_assert!(quantile > 0.5 && quantile <= 1.0);

    // In case of max quantile, return min-max per dimension
    if quantile >= 1.0 {
        return find_min_max_interval_per_coordinate(vector_data, dim, count, stopped);
    }

    // Otherwise, use P Square algorithm to estimate quantile intervals
    find_interval_per_coordinate_p2(vector_data, dim, count, quantile, num_threads, stopped)
}

fn find_min_max_interval_per_coordinate<'a>(
    vector_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
    dim: usize,
    count: usize,
    stopped: &AtomicBool,
) -> Result<Vec<(f32, f32)>, EncodingError> {
    let mut result = Vec::with_capacity(dim);
    for _ in 0..dim {
        result.push((f32::MAX, f32::MIN));
    }

    let selected_vectors = take_random_vectors(vector_data, dim, count, P2_SAMPLE_SIZE, stopped)?;

    for vector in selected_vectors.chunks_exact(dim) {
        for ((min, max), &value) in result.iter_mut().zip(vector.iter()) {
            *min = min.min(value);
            *max = max.max(value);
        }
    }

    for min_max in result.iter_mut() {
        if min_max.0 == f32::MAX || min_max.1 == f32::MIN {
            *min_max = (0.0, 0.0);
        }
    }

    Ok(result)
}

fn find_interval_per_coordinate_p2<'a>(
    vector_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
    dim: usize,
    count: usize,
    quantile: f32,
    num_threads: usize,
    stopped: &AtomicBool,
) -> Result<Vec<(f32, f32)>, EncodingError> {
    let selected_vectors = take_random_vectors(vector_data, dim, count, P2_SAMPLE_SIZE, stopped)?;

    let pool = rayon::ThreadPoolBuilder::new()
        .thread_name(|idx| format!("p-square-{idx}"))
        .num_threads(num_threads.max(1))
        .build()
        .map_err(|e| {
            EncodingError::EncodingError(format!(
                "Failed P Square estimation while thread pool init: {e}"
            ))
        })?;

    // Process each dimension in parallel
    pool.install(|| {
        (0..dim)
            .into_par_iter()
            .map(|d| -> Result<(f32, f32), EncodingError> {
                // Because quantile parameter (like 95%) is defined for both tails,
                // we need to divide by 2.0 to get the correct min and max quantiles.
                let min_quantile = (1.0 - f64::from(quantile)) / 2.0;
                let mut min = P2Quantile::<P2_MARKERS>::new(min_quantile)?;

                let max_quantile = 1.0 - min_quantile;
                let mut max = P2Quantile::<P2_MARKERS>::new(max_quantile)?;

                for vector in selected_vectors.chunks_exact(dim) {
                    if stopped.load(Ordering::Relaxed) {
                        return Err(EncodingError::Stopped);
                    }

                    let value = f64::from(vector[d]);
                    min.push(value);
                    max.push(value);
                }

                Ok((min.estimate() as f32, max.estimate() as f32))
            })
            .collect()
    })
}

// Take random vectors from the input iterator using `Permutor`.
// To reduce allocations count, all selected vectors are flattened into a single Vec<f32>.
fn take_random_vectors<'a>(
    vector_data: impl Iterator<Item = impl AsRef<[f32]> + 'a>,
    dim: usize,
    count: usize,
    sample_size: usize,
    stopped: &AtomicBool,
) -> Result<Vec<f32>, EncodingError> {
    let slice_size = std::cmp::min(count, sample_size);
    let permutor = Permutor::new(count as u64);
    let mut selected_vectors: Vec<usize> = permutor.map(|i| i as usize).take(slice_size).collect();
    selected_vectors.sort_unstable();

    let mut data_slice = Vec::with_capacity(slice_size * dim);
    let mut selected_index: usize = 0;
    for (vector_index, vector_data) in vector_data.into_iter().enumerate() {
        if stopped.load(Ordering::Relaxed) {
            return Err(EncodingError::Stopped);
        }

        if vector_index == selected_vectors[selected_index] {
            data_slice.extend_from_slice(vector_data.as_ref());
            selected_index += 1;
            if selected_index == slice_size {
                break;
            }
        }
    }

    Ok(data_slice)
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(1.0)]
    #[case(0.95)]
    fn test_vectors_quantile_interval(#[case] quantile: f32) {
        const COUNT: usize = 5_000;
        const DIM: usize = 4;

        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(COUNT);
        for _ in 0..COUNT {
            let mut vector = Vec::with_capacity(DIM);
            for _ in 0..DIM {
                vector.push(rng.random::<f32>());
            }
            data.push(vector);
        }

        let per_coordinate = find_interval_per_coordinate(
            data.iter(),
            DIM,
            COUNT,
            quantile,
            2,
            &AtomicBool::new(false),
        )
        .unwrap();

        let acc = 0.05;
        let min_result = (1.0 - quantile).abs() / 2.0;
        let max_result = 1.0 - min_result;
        for (min, max) in per_coordinate {
            assert!(
                ((min - min_result).abs() < acc),
                "Min value is out of expected range"
            );
            assert!(
                ((max - max_result).abs() < acc),
                "Max value is out of expected range"
            );
        }
    }
}
