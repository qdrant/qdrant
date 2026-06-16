use std::sync::atomic::{AtomicBool, Ordering};

use permutation_iterator::Permutor;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefMutIterator, ParallelIterator};

use crate::EncodingError;
use crate::p_square::P2Quantile;

pub const SAMPLE_SIZE: usize = 5_000;
/// P-square marker count. Original Jain & Chlamtac (1985) algorithm uses
/// 5; we run with the extended variant at 7 because Bits4 anchors on
/// `p = Φ(2.733) ≈ 0.997` and 5 markers don't track that tail accurately
/// enough on small samples (`n ≤ ~10³` failed `recall_skewed_data` on
/// Bits4 at N=5). The cost of the extra two markers is paid only on each
/// `push`, and on Bits4's deep-tail target the higher marker count is
/// what keeps the estimator stable at the chosen `sample_size`.
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

    let selected_vectors = take_random_vectors(vector_data, count, SAMPLE_SIZE, stopped)?;
    let selected_vectors_count = selected_vectors.len();
    let mut data_slice: Vec<f32> = Vec::with_capacity(selected_vectors_count * dim);
    for vector in &selected_vectors {
        if stopped.load(Ordering::Relaxed) {
            return Err(EncodingError::Stopped);
        }

        data_slice.extend_from_slice(vector.as_ref());
    }

    let data_slice_len = data_slice.len();
    if data_slice_len < 4 {
        return Ok(None);
    }

    let cut_index = std::cmp::min(
        (data_slice_len - 1) / 2,
        (selected_vectors_count as f32 * (1.0 - quantile) / 2.0) as usize,
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

/// Quantile estimation via vector-level uniform random sampling, with
/// a fused per-vector preprocess.
///
/// Picks `sample_size` random indices in `[0, count)` (uniform without
/// replacement, sorted ascending), then drives the input iterator
/// forward with `iter.nth(skip)` to land exactly on each chosen index.
/// For mmap-backed `Range::map(get_dense)` style iterators — the common
/// case in segment storage — `nth(k)` advances the underlying `Range`
/// counter in O(1) and **does not invoke the closure for skipped
/// indices**. So we pay one `get_dense` (and one Hadamard rotation, and
/// one P-square push per coord) only for the `R = sample_size` vectors
/// we actually sampled, not for all `count` of them.
///
/// **Why sampling is sound here.** The variance of the per-coord
/// quantile estimator at probability `p` over `R` independent samples
/// is `p(1-p) / (R · f(F⁻¹(p))²)`. It depends on `R` only — reading
/// every one of `count` vectors gives no extra accuracy beyond an
/// `R`-sized i.i.d. sample, just at much higher I/O and compute cost.
///
/// **Pipeline shape.** Streaming, batched into `BATCH_SIZE` vectors at
/// a time:
///
/// ```text
///   1. Sort R random indices.
///   2. For each batch of up to BATCH_SIZE indices:
///        a. Sequential read+preprocess: iter.nth(skip) → preprocess
///           writes padded_dim f64s into row b of `batch_scratch`.
///        b. Parallel coord-chunk push: each rayon work item handles a
///           contiguous slice of coords and pushes all batch values for
///           those coords into both per-coord P-square estimators.
///   3. Finalize: pair up the per-coord estimators and read out
///      `(p_lo, p_hi)`.
/// ```
///
/// No reservoir, no intermediate `Vec<Vec<f32>>`. The only working
/// buffer is `BATCH_SIZE × padded_dim × f64` (~192 KB at
/// padded_dim=1536) plus `2 × padded_dim × P2Quantile` (~200 KB) — both
/// independent of `R`.
///
/// `preprocess(raw, scratch)` must write exactly `padded_dim` finite
/// values into `scratch`. It runs on the main thread, so it does not
/// need `Send`/`Sync`.
///
/// `sample_size` controls how many random vectors are actually read
/// and processed. The caller picks this based on how extreme the
/// target quantile is — see `TQBits::sample_size` for the per-bits
/// table used in TQ+.
#[allow(clippy::too_many_arguments)]
pub fn find_quantile_interval_per_coordinate_with_preprocess<'a, F>(
    data: impl Iterator<Item = impl AsRef<[f32]> + 'a>,
    raw_dim: usize,
    padded_dim: usize,
    count: usize,
    quantile: f32,
    num_threads: usize,
    sample_size: usize,
    preprocess: F,
    stopped: &AtomicBool,
) -> Result<Vec<(f32, f32)>, EncodingError>
where
    F: Fn(&[f32], &mut [f64]) + Send + Sync + 'a,
{
    debug_assert!(quantile > 0.5 && quantile < 1.0);
    debug_assert!(raw_dim > 0 && padded_dim > 0);
    debug_assert!(sample_size > 0);

    // Empty-input fast path: no quantile to estimate. Match the legacy
    // contract of returning a `padded_dim`-wide vector of zero pairs so
    // downstream `shift = 0`, `scale = 1` and EC is identity.
    if count == 0 {
        return Ok(vec![(0.0, 0.0); padded_dim]);
    }

    let min_quantile = (1.0 - f64::from(quantile)) / 2.0;
    let max_quantile = 1.0 - min_quantile;

    // Random sample of `R` indices in `[0, count)`, sorted ascending so
    // the iterator can step through them with monotonic `nth(skip)`
    // calls. `Permutor` produces a deterministic permutation per
    // `count` — same input collection produces the same sample, which
    // is what we want for reproducible recall across runs.
    let actual_sample = sample_size.min(count);
    let mut indices: Vec<usize> = Permutor::new(count as u64)
        .map(|i| i as usize)
        .take(actual_sample)
        .collect();
    indices.sort_unstable();

    // One pair of P-square estimators per coord. The interleaved
    // `(min, max)` layout lets us iterate them as a single contiguous
    // slice during the per-vector parallel push.
    //
    // Memory: `padded_dim × 2 × sizeof(P2Quantile)`, ~200 KB at
    // padded_dim=1536 — independent of `sample_size`. The previous
    // implementation kept an intermediate `Vec<f32>` of
    // `sample_size × max(raw_dim, padded_dim)` (≈12 MB at 2k samples,
    // up to ≈50 MB at 8k); streaming push removes that buffer entirely.
    let mut estimators: Vec<(P2Quantile<P2_MARKERS>, P2Quantile<P2_MARKERS>)> = (0..padded_dim)
        .map(|_| {
            Ok::<_, EncodingError>((
                P2Quantile::<P2_MARKERS>::new(min_quantile)?,
                P2Quantile::<P2_MARKERS>::new(max_quantile)?,
            ))
        })
        .collect::<Result<_, _>>()?;

    let pool = rayon::ThreadPoolBuilder::new()
        .thread_name(|idx| format!("tq-prepass-{idx}"))
        .num_threads(num_threads.max(1))
        .build()
        .map_err(|e| {
            EncodingError::EncodingError(format!(
                "Failed quantile pre-pass while thread pool init: {e}"
            ))
        })?;

    // Streaming pass with small batches. Per batch:
    //   1. Read + preprocess `BATCH_SIZE` vectors sequentially into
    //      `batch_scratch[BATCH_SIZE * padded_dim]` (row-major: row `b`
    //      holds the rotated coords of the `b`-th vector in this batch).
    //   2. Parallel coord-chunk push: each rayon work item handles a
    //      contiguous slice of coords and pushes all `BATCH_SIZE` values
    //      for those coords across both estimators.
    //
    // Why batching: the previous "push one vector at a time" form spent
    // most time on rayon dispatch — `par_iter_mut().for_each(...)` invoked
    // once per vector ran 24 work-units × 2k vectors = 50k dispatch events
    // on Bits1, ~200k on Bits4. Batching at B=16 amortizes that cost over
    // the batch (3.1k dispatches on Bits1, 12.5k on Bits4), with minimal
    // memory impact: `batch_scratch` is `BATCH_SIZE × padded_dim × f64`
    // ≈ 192 KB at padded_dim=1536, fits comfortably in L2.
    //
    // The strided column read inside the push closure
    // (`batch_scratch[b * padded_dim + d]` across `b`) is non-contiguous,
    // but each coord's column footprint is `BATCH_SIZE × 8 B` = 128 B, so
    // the worker keeps the relevant cache lines hot for the full chunk.
    const BATCH_SIZE: usize = 16;
    const PUSH_MIN_CHUNK: usize = 64;
    let mut batch_scratch = vec![0.0f64; BATCH_SIZE * padded_dim];

    let mut data = data;
    let mut cursor = 0usize;
    let mut idx_iter = indices.iter().copied();
    loop {
        // Fill batch sequentially.
        let mut filled = 0usize;
        while filled < BATCH_SIZE {
            let Some(idx) = idx_iter.next() else { break };
            if stopped.load(Ordering::Relaxed) {
                return Err(EncodingError::Stopped);
            }
            debug_assert!(idx >= cursor);
            let skip = idx - cursor;
            let v = data.nth(skip).ok_or_else(|| {
                EncodingError::EncodingError(format!(
                    "input iterator exhausted at sampled index {idx} (count={count})",
                ))
            })?;
            let v_ref = v.as_ref();
            // A short vector would leave part of `row` untouched when
            // `preprocess` only writes to the first `v_ref.len()` slots,
            // and the leftover f64s from the previous batch would land
            // in the P-square estimators. Bail out with a real error.
            if v_ref.len() != raw_dim {
                return Err(EncodingError::EncodingError(format!(
                    "input vector dim mismatch at sampled index {idx}: expected {raw_dim}, got {}",
                    v_ref.len(),
                )));
            }
            let row = &mut batch_scratch[filled * padded_dim..(filled + 1) * padded_dim];
            preprocess(v_ref, row);
            cursor = idx + 1;
            filled += 1;
        }
        if filled == 0 {
            break;
        }

        // Parallel coord-chunk push for the just-filled batch.
        let batch_view = &batch_scratch[..filled * padded_dim];
        pool.install(|| {
            estimators
                .par_iter_mut()
                .enumerate()
                .with_min_len(PUSH_MIN_CHUNK)
                .for_each(|(d, (min_q, max_q))| {
                    for b in 0..filled {
                        let v = batch_view[b * padded_dim + d];
                        min_q.push(v);
                        max_q.push(v);
                    }
                });
        });
    }

    let intervals: Vec<(f32, f32)> = estimators
        .into_iter()
        .map(|(min_q, max_q)| (min_q.estimate() as f32, max_q.estimate() as f32))
        .collect();

    Ok(intervals)
}

// Take random vectors from the input iterator using `Permutor`.
fn take_random_vectors<'a>(
    vector_data: impl Iterator<Item = impl AsRef<[f32]> + 'a>,
    count: usize,
    sample_size: usize,
    stopped: &AtomicBool,
) -> Result<Vec<impl AsRef<[f32]> + 'a>, EncodingError> {
    let slice_size = std::cmp::min(count, sample_size);
    let permutor = Permutor::new(count as u64);
    let mut selected_vectors: Vec<usize> = permutor.map(|i| i as usize).take(slice_size).collect();
    selected_vectors.sort_unstable();

    let mut data_slice = Vec::with_capacity(slice_size);
    let mut selected_index: usize = 0;
    for (vector_index, vector_data) in vector_data.into_iter().enumerate() {
        if stopped.load(Ordering::Relaxed) {
            return Err(EncodingError::Stopped);
        }

        if vector_index == selected_vectors[selected_index] {
            data_slice.push(vector_data);
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
    use rand::{RngExt, SeedableRng};
    use rstest::rstest;

    use super::*;

    /// Per-coord quantile recovery on uniform `[0, 1)` data.
    ///
    /// The empirical quantile of `Uniform(0, 1)` at probability `p` is
    /// `p`, so the symmetric interval at `q` is `((1-q)/2, 1-(1-q)/2)`.
    /// Identity preprocess (f32 → f64 widening) lets us drive the same
    /// pipeline TQ+ uses, without rotation.
    #[rstest]
    #[case(0.95, 2)]
    #[case(0.95, 4)]
    fn test_quantile_interval_per_coord(#[case] quantile: f32, #[case] num_threads: usize) {
        const COUNT: usize = 5_000;
        const DIM: usize = 4;
        const SAMPLE: usize = 2_048;

        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(COUNT);
        for _ in 0..COUNT {
            let mut vector = Vec::with_capacity(DIM);
            for _ in 0..DIM {
                vector.push(rng.random::<f32>());
            }
            data.push(vector);
        }

        let per_coordinate = find_quantile_interval_per_coordinate_with_preprocess(
            data.iter(),
            DIM,
            DIM,
            COUNT,
            quantile,
            num_threads,
            SAMPLE,
            |raw, scratch| {
                for (s, &v) in scratch.iter_mut().zip(raw.iter()) {
                    *s = f64::from(v);
                }
            },
            &AtomicBool::new(false),
        )
        .unwrap();

        let acc = 0.05;
        let min_result = (1.0 - quantile) / 2.0;
        let max_result = 1.0 - min_result;
        for (min, max) in per_coordinate {
            assert!(
                (min - min_result).abs() < acc,
                "Min value is out of expected range: got {min}, expected ~{min_result}"
            );
            assert!(
                (max - max_result).abs() < acc,
                "Max value is out of expected range: got {max}, expected ~{max_result}"
            );
        }
    }
}
