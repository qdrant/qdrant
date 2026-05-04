use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use permutation_iterator::Permutor;
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator,
    IntoParallelRefMutIterator, ParallelIterator,
};

use crate::EncodingError;
use crate::p_square::P2Quantile;

pub const SAMPLE_SIZE: usize = 5_000;
/// P-square marker count. Original Jain & Chlamtac (1985) algorithm uses
/// 5; we run with the extended variant at 7 because Bits4 anchors on
/// `p=Φ(2.733)≈0.997` and 5 markers don't track that tail accurately
/// enough on small samples (`n ≤ ~10³` failed `recall_skewed_data` on
/// Bits4 at N=5). The cost of the extra two markers is paid only in the
/// finalize phase (P-square run over `RESERVOIR_SIZE` values per coord),
/// which is already parallel across coords.
pub const P2_MARKERS: usize = 7;
/// How many input vectors to buffer before parallel-processing one chunk
/// across the per-coordinate reservoirs. Sized to keep the transposed
/// buffer (`dim × CHUNK_SIZE × f64`) cache-friendly while still giving
/// each worker thread enough reservoir pushes per chunk to amortize the
/// rayon dispatch overhead.
const P2_CHUNK_SIZE: usize = 4_096;
/// Default per-coordinate reservoir size for the legacy
/// `find_interval_per_coordinate*` APIs. The fused
/// `find_quantile_interval_per_coordinate_with_preprocess` takes the size
/// as an explicit argument so the caller can scale it to the actual
/// quantile being estimated. At 8192 the variance of the quantile
/// estimator at p=0.997 is ≈2.5% relative for N(0, 1)-like data —
/// matching or beating P-square (N=7 markers) on the extreme tail,
/// while keeping per-push cost ≈5 ns vs P-square's ≈50 ns.
const DEFAULT_RESERVOIR_SIZE: usize = 8_192;

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

pub fn find_interval_per_coordinate<'a>(
    vector_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone + Send + 'a,
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
/// one reservoir slot write) only for the `R = sample_size` vectors we
/// actually sampled, not for all `count` of them.
///
/// **Why sampling is sound here.** The variance of the per-coord
/// quantile estimator at probability `p` over `R` independent samples
/// is `p(1-p) / (R · f(F⁻¹(p))²)`. It depends on `R` only — reading
/// every one of `count` vectors and feeding them into a length-R
/// reservoir gives the same `R`-sample variance, just at much higher
/// I/O and compute cost. Vector-level uniform sampling produces an
/// equivalent estimator at a fraction of the work.
///
/// **Pipeline shape.**
///
/// ```text
///   1. Sort R random indices.
///   2. Sequential read via iter.nth(skip):
///        for each sampled vector → copy raw_dim f32s into a slot of
///        a contiguous `sample_buf`.
///   3. Parallel rotation + narrow:
///        sample_buf.par_chunks_mut(slot_size).for_each_init(...) runs
///        `preprocess(slot[..raw_dim], scratch)` on each slot and
///        narrows the f64 result back into the same slot's first
///        `padded_dim` f32s.
///   4. Parallel P-square per coord:
///        for each `d` in 0..padded_dim, run min/max P-square over the
///        column `sample_buf[k * slot_size + d]` for `k = 0..R`.
/// ```
///
/// No producer/consumer threading, no channels, no reservoirs. Single
/// `Vec<f32>` of size `R × slot_size`; one `Vec<f64>` per rayon worker
/// of size `padded_dim` for rotation scratch.
///
/// `preprocess(raw, scratch)` must write exactly `padded_dim` finite
/// values into `scratch`. Concurrency: `preprocess` runs on rayon
/// workers, so it must be `Send + Sync`.
///
/// `sample_size` controls how many random vectors are actually read
/// and processed. The caller picks this based on how extreme the
/// target quantile is — see `TQBits::reservoir_size` for the per-bits
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
        let started = Instant::now();
        log::info!(
            "find_quantile_interval_per_coordinate_with_preprocess: raw_dim={raw_dim} padded_dim={padded_dim} count=0 sampled=0 quantile={quantile} threads={} total={:.3?}",
            num_threads.max(1),
            started.elapsed(),
        );
        return Ok(vec![(0.0, 0.0); padded_dim]);
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .thread_name(|idx| format!("reservoir-{idx}"))
        .num_threads(num_threads.max(1))
        .build()
        .map_err(|e| {
            EncodingError::EncodingError(format!(
                "Failed reservoir estimation while thread pool init: {e}"
            ))
        })?;

    let started = Instant::now();
    log::debug!(
        "find_quantile_interval_per_coordinate_with_preprocess: starting (raw_dim={raw_dim}, padded_dim={padded_dim}, count={count}, quantile={quantile}, threads={}, sample={sample_size})",
        num_threads.max(1),
    );

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

    // Single scratch reused for every sampled vector — caller's
    // `preprocess` writes `padded_dim` finite f64s into it.
    let mut scratch = vec![0.0f64; padded_dim];

    // Streaming pass: read → preprocess → push, vector by vector.
    //
    // - Read+preprocess stay sequential on the main thread. The
    //   per-vector preprocess (Hadamard rotate + length rescale +
    //   narrow) is a few µs; serializing it costs less than the rayon
    //   overhead of dispatching one preprocess per vector.
    // - The push step is parallel across coord chunks. P-square push
    //   touches N=7 markers per call — at padded_dim×2 estimators per
    //   vector we'd otherwise be single-thread-bound on the tail of
    //   the run.
    //
    // `with_min_len(PUSH_MIN_CHUNK)` keeps each rayon work item large
    // enough that dispatch overhead amortizes; without it the default
    // splitter creates O(padded_dim) work units per vector.
    const PUSH_MIN_CHUNK: usize = 64;

    let mut data = data;
    let mut cursor = 0usize;
    for &idx in &indices {
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
        debug_assert_eq!(v_ref.len(), raw_dim, "input vector dim mismatch");
        preprocess(v_ref, &mut scratch);
        cursor = idx + 1;

        pool.install(|| {
            estimators
                .par_iter_mut()
                .zip(scratch.par_iter())
                .with_min_len(PUSH_MIN_CHUNK)
                .for_each(|((min_q, max_q), &x)| {
                    min_q.push(x);
                    max_q.push(x);
                });
        });
    }

    let intervals: Vec<(f32, f32)> = estimators
        .into_iter()
        .map(|(min_q, max_q)| (min_q.estimate() as f32, max_q.estimate() as f32))
        .collect();

    let elapsed = started.elapsed();
    log::info!(
        "find_quantile_interval_per_coordinate_with_preprocess: raw_dim={raw_dim} padded_dim={padded_dim} count={count} sampled={actual_sample} quantile={quantile} threads={} total={:.3?}",
        num_threads.max(1),
        elapsed,
    );

    Ok(intervals)
}

fn find_min_max_interval_per_coordinate<'a>(
    vector_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
    dim: usize,
    count: usize,
    stopped: &AtomicBool,
) -> Result<Vec<(f32, f32)>, EncodingError> {
    let mut result = vec![(f32::MAX, f32::MIN); dim];

    let selected_vectors = take_random_vectors(vector_data, count, SAMPLE_SIZE, stopped)?;

    for vector in selected_vectors {
        if stopped.load(Ordering::Relaxed) {
            return Err(EncodingError::Stopped);
        }

        for ((min, max), &value) in result.iter_mut().zip(vector.as_ref().iter()) {
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
    vector_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Send + 'a,
    dim: usize,
    count: usize,
    quantile: f32,
    num_threads: usize,
    stopped: &AtomicBool,
) -> Result<Vec<(f32, f32)>, EncodingError> {
    let pool = rayon::ThreadPoolBuilder::new()
        .thread_name(|idx| format!("reservoir-{idx}"))
        .num_threads(num_threads.max(1))
        .build()
        .map_err(|e| {
            EncodingError::EncodingError(format!(
                "Failed reservoir estimation while thread pool init: {e}"
            ))
        })?;

    let started = Instant::now();
    log::debug!(
        "find_interval_per_coordinate_p2: starting (dim={dim}, count={count}, quantile={quantile}, threads={}, reservoir={DEFAULT_RESERVOIR_SIZE})",
        num_threads.max(1),
    );

    let min_quantile = (1.0 - f64::from(quantile)) / 2.0;
    let max_quantile = 1.0 - min_quantile;

    // Per-coord reservoirs. RNG seeds derived deterministically from
    // a fixed master seed plus coord index — reproducible across runs
    // and independent across coords.
    let mut reservoirs: Vec<CoordReservoir> = (0..dim)
        .map(|d| {
            CoordReservoir::new(
                DEFAULT_RESERVOIR_SIZE,
                RESERVOIR_SEED.wrapping_add(d as u64),
            )
        })
        .collect();

    // Producer/consumer pipeline so disk reads + upstream rotation
    // overlap with reservoir flush:
    //
    //   producer thread        : pulls `vector_data` (which itself
    //                            does Hadamard rotation in batches),
    //                            packs into `Vec<Vec<f32>>` chunks of
    //                            `P2_CHUNK_SIZE`, hands off via
    //                            `sync_channel(1)`.
    //   consumer (main thread) : drains the channel, runs `flush_chunk`
    //                            on the rayon pool — parallel push into
    //                            per-coord reservoirs.
    //
    // `sync_channel(1)` gives one chunk in flight + one being built,
    // which is enough to hide the consumer's flush latency behind the
    // producer's disk-read+rotation latency (the producer is the slower
    // stage on real workloads). `std::thread::scope` lets the producer
    // borrow `&AtomicBool stopped` without forcing a 'static bound.
    let total_seen = std::thread::scope(|s| -> Result<usize, EncodingError> {
        let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<Vec<f32>>>(1);

        let producer = s.spawn(move || -> Result<usize, EncodingError> {
            let mut chunk: Vec<Vec<f32>> = Vec::with_capacity(P2_CHUNK_SIZE);
            let mut total = 0usize;
            for vector in vector_data {
                if stopped.load(Ordering::Relaxed) {
                    return Err(EncodingError::Stopped);
                }
                let v = vector.as_ref();
                debug_assert_eq!(v.len(), dim, "vector length must equal dim");
                chunk.push(v.to_vec());
                total += 1;
                if chunk.len() == P2_CHUNK_SIZE {
                    if tx.send(std::mem::take(&mut chunk)).is_err() {
                        // Consumer dropped its end early — bail.
                        return Ok(total);
                    }
                    chunk = Vec::with_capacity(P2_CHUNK_SIZE);
                }
            }
            if !chunk.is_empty() {
                let _ = tx.send(chunk);
            }
            Ok(total)
        });

        // Consumer: drain channel until producer drops `tx`.
        while let Ok(chunk) = rx.recv() {
            flush_chunk(&pool, &chunk, &mut reservoirs);
        }

        producer.join().map_err(|_| {
            EncodingError::EncodingError("p-square producer thread panicked".to_string())
        })?
    })?;

    let stream_done = started.elapsed();

    // Final pass: parallel P-square per coord over the captured
    // reservoir samples. `O(R · N_markers)` per coord, parallel.
    let intervals: Vec<(f32, f32)> = pool.install(|| {
        reservoirs
            .into_par_iter()
            .map(|res| res.into_p2_quantiles(min_quantile, max_quantile))
            .collect::<Result<Vec<_>, _>>()
    })?;

    let elapsed = started.elapsed();
    log::info!(
        "find_interval_per_coordinate_p2: dim={dim} count={count} seen={total_seen} quantile={quantile} threads={} stream={:.3?} total={:.3?}",
        num_threads.max(1),
        stream_done,
        elapsed,
    );

    Ok(intervals)
}

/// Master seed for per-coord reservoir RNGs. Constant so runs are
/// reproducible; XOR'd with the coord index to decorrelate coords.
const RESERVOIR_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

/// Push the current vector-major chunk into the per-coordinate reservoirs
/// in parallel across coordinates. Each worker iterates `raw_chunk` for
/// its assigned coord index, reading column `d` at stride `dim`. The
/// reservoir is updated sequentially within one coord (Algorithm R needs
/// a stable per-coord `seen` counter), but coords run independently.
fn flush_chunk(
    pool: &rayon::ThreadPool,
    raw_chunk: &[Vec<f32>],
    reservoirs: &mut [CoordReservoir],
) {
    pool.install(|| {
        reservoirs.par_iter_mut().enumerate().for_each(|(d, res)| {
            for v in raw_chunk {
                // SAFETY: `dim` is identical for every vector
                // (debug_assert in the caller) and the reservoir
                // count equals `dim`, so `v[d]` is always in range.
                res.push(f64::from(v[d]));
            }
        });
    });
}

/// Vitter's Algorithm R reservoir sampler over `f64` values, plus an
/// `into_quantiles` finalizer that returns two empirical quantiles from
/// the captured sample. Each push is `O(1)` in memory and time (one
/// uniform RNG draw, one branch, one possible store) — vs the ~50 ns of
/// branch-heavy marker-adjustment work P-square does per push.
///
/// The single reservoir serves both `min_quantile` and `max_quantile`
/// because they're computed from the same i.i.d. sample of the coord's
/// distribution; sorting once + indexing twice beats running two
/// independent estimators.
struct CoordReservoir {
    /// Sampled values. Capacity is fixed at construction; `samples.len()
    /// < capacity` only during the initial fill phase.
    samples: Vec<f64>,
    /// Total number of values pushed (across all chunks). Drives Algorithm
    /// R's replacement probability `R / (seen + 1)`.
    seen: usize,
    /// Per-coord PRNG. `SmallRng` (xoshiro256++) costs ~3 ns per draw and
    /// has 32 bytes of state — light enough to keep one per coord.
    rng: SmallRng,
}

impl CoordReservoir {
    fn new(capacity: usize, seed: u64) -> Self {
        Self {
            samples: Vec::with_capacity(capacity),
            seen: 0,
            rng: SmallRng::seed_from_u64(seed),
        }
    }

    /// Push one value through Algorithm R.
    ///
    /// Phase 1 (`seen < capacity`): direct fill — the first `R` values
    /// always land in the reservoir.
    ///
    /// Phase 2 (`seen >= capacity`): pick `j` uniform in `[0, seen]`; if
    /// `j < capacity`, write to `samples[j]`, otherwise discard. This
    /// preserves the invariant that every value seen so far has equal
    /// probability `R / (seen + 1)` of being in the reservoir.
    #[inline]
    fn push(&mut self, value: f64) {
        // Skip non-finite values to match P-square's behavior — quantile
        // estimation on NaN/Inf isn't defined, and a single Inf would
        // poison the post-sort order statistic.
        if !value.is_finite() {
            return;
        }
        let cap = self.samples.capacity();
        if self.samples.len() < cap {
            self.samples.push(value);
        } else {
            let j = self.rng.random_range(0..=self.seen);
            if j < cap {
                self.samples[j] = value;
            }
        }
        self.seen += 1;
    }

    /// Drive two `P2Quantile` estimators over the captured sample, returning
    /// the pair `(p_lo, p_hi)` quantiles as `f32`. Returns `(0.0, 0.0)` on
    /// empty input.
    ///
    /// Why P-square over the reservoir instead of the empirical order
    /// statistic: reservoir's `samples` is at most `R` values regardless of
    /// stream length `N`, but the quantile estimator should still match
    /// what plain P-square would have produced on the full stream. P-square
    /// is **consistent** — it converges to the true quantile from a uniform
    /// sample of the distribution, which is exactly what reservoir gives
    /// us. And P-square's marker interpolation smooths over noisy tail
    /// order statistics (the failure mode for raw `sorted[k]` at extreme
    /// quantiles when `R` is moderate).
    ///
    /// When `N < R`, `samples` holds the full stream in input order — so
    /// this path is **bit-identical** to the previous all-stream P-square,
    /// preserving recall on small datasets where empirical quantile would
    /// be jittery.
    fn into_p2_quantiles(self, p_lo: f64, p_hi: f64) -> Result<(f32, f32), EncodingError> {
        if self.samples.is_empty() {
            return Ok((0.0, 0.0));
        }
        let mut min_q = P2Quantile::<P2_MARKERS>::new(p_lo)?;
        let mut max_q = P2Quantile::<P2_MARKERS>::new(p_hi)?;
        for &v in &self.samples {
            min_q.push(v);
            max_q.push(v);
        }
        Ok((min_q.estimate() as f32, max_q.estimate() as f32))
    }
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
