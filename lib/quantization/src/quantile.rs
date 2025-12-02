use std::sync::atomic::{AtomicBool, Ordering};

use permutation_iterator::Permutor;

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

    let (slice_size, selected_vectors) =
        take_random_vectors(vector_data, count, QUANTILE_SAMPLE_SIZE);

    let mut data_slice = Vec::with_capacity(slice_size * dim);
    for vector_data in selected_vectors {
        if stopped.load(Ordering::Relaxed) {
            return Err(EncodingError::Stopped);
        }

        data_slice.extend_from_slice(vector_data.as_ref());
    }

    let data_slice_len = data_slice.len();
    if data_slice_len < 4 {
        return Ok(None);
    }

    let cut_index = std::cmp::min(
        (data_slice_len - 1) / 2,
        (slice_size as f32 * (1.0 - quantile) / 2.0) as usize,
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

#[allow(dead_code)]
pub(crate) fn find_interval_per_coordinate<'a>(
    vector_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
    dim: usize,
    count: usize,
    quantile: f32,
    stopped: &AtomicBool,
) -> Result<Vec<(f32, f32)>, EncodingError> {
    // In case of max quantile, return min-max per dimension
    if quantile == 1.0 {
        return find_min_max_interval_per_coordinate(vector_data, dim, count, stopped);
    }

    // Single thread case
    find_interval_per_coordinate_single_thread(vector_data, dim, quantile, stopped)
}

fn find_min_max_interval_per_coordinate<'a>(
    vector_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
    dim: usize,
    count: usize,
    stopped: &AtomicBool,
) -> Result<Vec<(f32, f32)>, EncodingError> {
    let mut result = Vec::with_capacity(dim);
    for _ in 0..dim {
        result.push((f32::MIN, f32::MAX));
    }

    let (_, selected_vectors) = take_random_vectors(vector_data, count, P2_SAMPLE_SIZE);

    for vector in selected_vectors {
        if stopped.load(Ordering::Relaxed) {
            return Err(EncodingError::Stopped);
        }

        let vector = vector.as_ref();
        debug_assert_eq!(
            vector.len(),
            dim,
            "Vector length does not match the expected dimension"
        );

        for (i, min_max) in result.iter_mut().enumerate() {
            if vector[i] < min_max.0 {
                min_max.0 = vector[i];
            }
            if vector[i] > min_max.1 {
                min_max.1 = vector[i];
            }
        }
    }

    for min_max in result.iter_mut() {
        if min_max.0 == f32::MAX || min_max.1 == f32::MIN {
            *min_max = (0.0, 0.0);
        }
    }

    Ok(result)
}

fn find_interval_per_coordinate_single_thread<'a>(
    vector_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
    dim: usize,
    quantile: f32,
    stopped: &AtomicBool,
) -> Result<Vec<(f32, f32)>, EncodingError> {
    let mut p_squares: Vec<(P2Quantile<P2_MARKERS>, P2Quantile<P2_MARKERS>)> = (0..dim)
        .map(|_| {
            (
                P2Quantile::<P2_MARKERS>::new(f64::from(quantile)).unwrap(),
                P2Quantile::<P2_MARKERS>::new(1.0 - f64::from(quantile)).unwrap(),
            )
        })
        .collect();

    let (_, selected_vectors) = take_random_vectors(vector_data, usize::MAX, P2_SAMPLE_SIZE);

    for vector in selected_vectors {
        if stopped.load(Ordering::Relaxed) {
            return Err(EncodingError::Stopped);
        }

        let vector = vector.as_ref();
        debug_assert_eq!(
            vector.len(),
            dim,
            "Vector length does not match the expected dimension"
        );

        for d in 0..dim {
            p_squares[d].0.push(f64::from(vector[d]));
            p_squares[d].1.push(f64::from(vector[d]));
        }
    }
    let mut result = Vec::with_capacity(dim);
    for (p_square_min, p_square_max) in p_squares {
        result.push((
            p_square_min.estimate() as f32,
            p_square_max.estimate() as f32,
        ));
    }
    Ok(result)
}

fn take_random_vectors<'a>(
    vector_data: impl Iterator<Item = impl AsRef<[f32]> + 'a>,
    count: usize,
    sample_size: usize,
) -> (usize, impl Iterator<Item = impl AsRef<[f32]> + 'a>) {
    let slice_size = std::cmp::min(count, sample_size);
    let permutor = Permutor::new(count as u64);
    let mut selected_vectors: Vec<usize> = permutor.map(|i| i as usize).take(slice_size).collect();
    selected_vectors.sort_unstable();

    (
        slice_size,
        vector_data
            .enumerate()
            .filter_map(move |(vector_index, vector)| {
                if selected_vectors.binary_search(&vector_index).is_ok() {
                    Some(vector)
                } else {
                    None
                }
            }),
    )
}
