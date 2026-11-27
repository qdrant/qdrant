//! fp32 dense batch scoring from a storage much larger than the caches: how memory
//! access order changes the cost of one check.
//!
//! Vectors sit 4 bytes past a 64-byte boundary, like `ImmutableDenseVectorData`
//! (`[4-byte header][vector_0][vector_1]...`). Each batch is 64 random ids, the size
//! of one HNSW neighbor read (`VECTOR_READ_BATCH_SIZE`), scored against one query.
//!
//! Variants:
//! - `base`: today's path, `CosineMetric::similarity` one vector after another.
//! - `helper`: the quantized storages' prefetch (`prefetch_windows`), as in #10973.
//! - `next1`, `next2`: hint the first lines of the vector 1 or 2 ahead, nothing else.
//! - `multi2`, `multi4`: score 2 or 4 vectors per loop with the query loaded once.
//! - `multi4next`: `multi4`, hinting the next group's first lines.
//! - `real4`: the shipped `CosineMetric::similarity_4`, four vectors per call.
//!
//! Configured by environment, so one variant can run under `perf stat`:
//! `DIMS` (default "128,1024,3072"), `GIB` (storage size, default 1),
//! `VARIANTS` (default all), `REPS` (default 5), `BATCHES` (per rep, default 20000),
//! `THREADS` (default "1": comma-separated thread counts; each thread scores its own
//! batches against the shared storage, and the total checks/s is reported, to see
//! where memory bandwidth runs out; restrict cores with `taskset`).
//!
//! ```text
//! cargo bench -p segment --bench dense_batch_scoring
//! VARIANTS=multi4 DIMS=1024 perf stat -e l1d_cache_refill,l2d_cache_refill \
//!     target/release/deps/dense_batch_scoring-<hash>
//! ```

use std::hint::black_box;
use std::io::Write;
use std::time::Instant;

use common::prefetch::{prefetch_slice, prefetch_slice_l2, prefetch_windows};
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};
use segment::spaces::metric::Metric;
use segment::spaces::simple::CosineMetric;

const BATCH: usize = 64;
const HEADER: usize = 4;
const ALL_VARIANTS: &[&str] = &[
    "base",
    "helper",
    "next1",
    "next2",
    "multi2",
    "multi4",
    "multi4next",
    "real4",
];

/// `count` normalized vectors of `dim` floats, each starting `HEADER` bytes past a
/// 64-byte boundary of the buffer, as in the on-disk layout.
struct Storage {
    bytes: Vec<u8>,
    dim: usize,
    count: usize,
}

impl Storage {
    fn new(dim: usize, gib: f64, rng: &mut SmallRng) -> Self {
        let vector_bytes = dim * size_of::<f32>();
        let count = ((gib * (1u64 << 30) as f64) as usize / vector_bytes).max(BATCH);
        // 64 spare bytes to place the data at a 64-byte boundary + HEADER
        let mut bytes = vec![0u8; count * vector_bytes + HEADER + 64];
        let start = bytes.as_ptr().align_offset(64) + HEADER;
        let mut vector = vec![0f32; dim];
        for i in 0..count {
            for x in vector.iter_mut() {
                *x = rng.random::<f32>() - 0.5;
            }
            let norm = vector
                .iter()
                .map(|x| x * x)
                .sum::<f32>()
                .sqrt()
                .max(f32::EPSILON);
            let at = start + i * vector_bytes;
            for (j, x) in vector.iter().enumerate() {
                bytes[at + j * 4..at + j * 4 + 4].copy_from_slice(&(x / norm).to_ne_bytes());
            }
        }
        // Never resized after this, so `start` stays valid
        bytes.truncate(start + count * vector_bytes);
        Self { bytes, dim, count }
    }

    fn start(&self) -> usize {
        self.bytes.as_ptr().align_offset(64) + HEADER
    }

    fn raw(&self, id: usize) -> &[u8] {
        let vector_bytes = self.dim * size_of::<f32>();
        let at = self.start() + id * vector_bytes;
        &self.bytes[at..at + vector_bytes]
    }

    fn vector(&self, id: usize) -> &[f32] {
        let raw = self.raw(id);
        // SAFETY: `raw` holds `dim` f32 values written by `new`. f32 needs 4-byte
        // alignment: 64-byte boundary + HEADER (4) + a multiple of 4 bytes.
        unsafe { std::slice::from_raw_parts(raw.as_ptr().cast::<f32>(), self.dim) }
    }
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// Dot products of `query` with `N` vectors in one pass, the query read once per block.
#[inline(always)]
fn dot_many<const N: usize>(query: &[f32], vectors: [&[f32]; N]) -> [f32; N] {
    #[cfg(target_arch = "aarch64")]
    if std::arch::is_aarch64_feature_detected!("neon") {
        // SAFETY: NEON is available; all slices have the query's length.
        return unsafe { neon::dot_many(query, vectors) };
    }
    #[cfg(target_arch = "x86_64")]
    if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
        // SAFETY: AVX2 and FMA are available; all slices have the query's length.
        return unsafe { avx::dot_many(query, vectors) };
    }
    vectors.map(|vector| dot(query, vector))
}

#[cfg(target_arch = "aarch64")]
mod neon {
    use std::arch::aarch64::*;

    /// 16 floats per step and 4 accumulators per vector, like `dot_similarity_neon`.
    #[target_feature(enable = "neon")]
    pub unsafe fn dot_many<const N: usize>(query: &[f32], vectors: [&[f32]; N]) -> [f32; N] {
        unsafe {
            let n = query.len();
            let m = n - n % 16;
            let q = query.as_ptr();
            let mut acc = [[vdupq_n_f32(0.); 4]; N];
            let mut i = 0;
            while i < m {
                let q0 = vld1q_f32(q.add(i));
                let q1 = vld1q_f32(q.add(i + 4));
                let q2 = vld1q_f32(q.add(i + 8));
                let q3 = vld1q_f32(q.add(i + 12));
                for v in 0..N {
                    let p = vectors[v].as_ptr().add(i);
                    acc[v][0] = vfmaq_f32(acc[v][0], q0, vld1q_f32(p));
                    acc[v][1] = vfmaq_f32(acc[v][1], q1, vld1q_f32(p.add(4)));
                    acc[v][2] = vfmaq_f32(acc[v][2], q2, vld1q_f32(p.add(8)));
                    acc[v][3] = vfmaq_f32(acc[v][3], q3, vld1q_f32(p.add(12)));
                }
                i += 16;
            }
            let mut out = [0.0; N];
            for v in 0..N {
                let [a0, a1, a2, a3] = acc[v];
                out[v] = vaddvq_f32(a0) + vaddvq_f32(a1) + vaddvq_f32(a2) + vaddvq_f32(a3);
                for j in m..n {
                    out[v] += query[j] * vectors[v][j];
                }
            }
            out
        }
    }
}

#[cfg(target_arch = "x86_64")]
mod avx {
    use std::arch::x86_64::*;

    /// 16 floats per step and 2 accumulators per vector, so 4 vectors fit the 16 registers.
    #[target_feature(enable = "avx2,fma")]
    pub unsafe fn dot_many<const N: usize>(query: &[f32], vectors: [&[f32]; N]) -> [f32; N] {
        unsafe {
            let n = query.len();
            let m = n - n % 16;
            let q = query.as_ptr();
            let mut acc = [[_mm256_setzero_ps(); 2]; N];
            let mut i = 0;
            while i < m {
                let q0 = _mm256_loadu_ps(q.add(i));
                let q1 = _mm256_loadu_ps(q.add(i + 8));
                for v in 0..N {
                    let p = vectors[v].as_ptr().add(i);
                    acc[v][0] = _mm256_fmadd_ps(q0, _mm256_loadu_ps(p), acc[v][0]);
                    acc[v][1] = _mm256_fmadd_ps(q1, _mm256_loadu_ps(p.add(8)), acc[v][1]);
                }
                i += 16;
            }
            let mut out = [0.0; N];
            for v in 0..N {
                let sum = _mm256_add_ps(acc[v][0], acc[v][1]);
                let mut lanes = [0.0f32; 8];
                _mm256_storeu_ps(lanes.as_mut_ptr(), sum);
                out[v] = lanes.iter().sum();
                for j in m..n {
                    out[v] += query[j] * vectors[v][j];
                }
            }
            out
        }
    }
}

/// Scores one batch with `variant`, writing one score per id.
fn score_batch(variant: &str, storage: &Storage, query: &[f32], ids: &[usize], out: &mut [f32]) {
    match variant {
        "base" => {
            for (score, &id) in out.iter_mut().zip(ids) {
                *score = CosineMetric::similarity(query, storage.vector(id));
            }
        }
        "helper" => {
            let (near, far) = prefetch_windows(storage.dim * size_of::<f32>());
            for &id in ids.iter().take(far).skip(near) {
                prefetch_slice_l2(storage.raw(id));
            }
            for &id in ids.iter().take(near) {
                prefetch_slice(storage.raw(id));
            }
            for (index, &id) in ids.iter().enumerate() {
                if far > 0
                    && let Some(&upcoming) = ids.get(index + far)
                {
                    prefetch_slice_l2(storage.raw(upcoming));
                }
                if let Some(&upcoming) = ids.get(index + near) {
                    prefetch_slice(storage.raw(upcoming));
                }
                out[index] = CosineMetric::similarity(query, storage.vector(id));
            }
        }
        "next1" | "next2" => {
            let lead = if variant == "next1" { 1 } else { 2 };
            for &id in ids.iter().take(lead) {
                prefetch_slice(storage.raw(id));
            }
            for (index, &id) in ids.iter().enumerate() {
                if let Some(&upcoming) = ids.get(index + lead) {
                    prefetch_slice(storage.raw(upcoming));
                }
                out[index] = CosineMetric::similarity(query, storage.vector(id));
            }
        }
        "multi2" => score_groups::<2>(storage, query, ids, out, false),
        "multi4" => score_groups::<4>(storage, query, ids, out, false),
        "multi4next" => score_groups::<4>(storage, query, ids, out, true),
        "real4" => {
            let (groups, rest) = ids.as_chunks::<4>();
            let (outs, rest_out) = out.as_chunks_mut::<4>();
            for (group, scores) in groups.iter().zip(outs) {
                *scores = CosineMetric::similarity_4(query, group.map(|id| storage.vector(id)));
            }
            for (score, &id) in rest_out.iter_mut().zip(rest) {
                *score = CosineMetric::similarity(query, storage.vector(id));
            }
        }
        other => panic!("unknown variant {other}"),
    }
}

/// Scores `N` vectors per pass; `hint_next` hints the next group's first lines first.
fn score_groups<const N: usize>(
    storage: &Storage,
    query: &[f32],
    ids: &[usize],
    out: &mut [f32],
    hint_next: bool,
) {
    let (groups, _) = ids.as_chunks::<N>();
    let (outs, _) = out.as_chunks_mut::<N>();
    for (index, (group, scores)) in groups.iter().zip(outs).enumerate() {
        if hint_next && let Some(next) = groups.get(index + 1) {
            for &id in next {
                prefetch_slice(storage.raw(id));
            }
        }
        *scores = dot_many(query, group.map(|id| storage.vector(id)));
    }
    let done = ids.len() - ids.len() % N;
    for (score, &id) in out[done..].iter_mut().zip(&ids[done..]) {
        *score = CosineMetric::similarity(query, storage.vector(id));
    }
}

fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let dims: Vec<usize> = env("DIMS", "128,1024,3072".to_string())
        .split(',')
        .map(|dim| dim.trim().parse().expect("DIMS: comma-separated integers"))
        .collect();
    let gib: f64 = env("GIB", 1.0);
    let reps: usize = env("REPS", 5);
    let batches: usize = env("BATCHES", 20_000);
    let variants: Vec<String> = env("VARIANTS", ALL_VARIANTS.join(","))
        .split(',')
        .map(str::to_string)
        .collect();
    let thread_counts: Vec<usize> = env("THREADS", "1".to_string())
        .split(',')
        .map(|n| n.trim().parse().expect("THREADS: comma-separated integers"))
        .collect();
    let mut rng = SmallRng::seed_from_u64(42);
    let mut stdout = std::io::stdout();

    for dim in dims {
        let started = Instant::now();
        let storage = Storage::new(dim, gib, &mut rng);
        writeln!(
            stdout,
            "dim {dim}: {} vectors, {:.2} GiB, built in {:.1}s",
            storage.count,
            (storage.count * dim * 4) as f64 / (1u64 << 30) as f64,
            started.elapsed().as_secs_f64(),
        )
        .unwrap();
        stdout.flush().unwrap();

        let max_threads = *thread_counts.iter().max().unwrap();
        let queries: Vec<Vec<f32>> = (0..max_threads)
            .map(|_| storage.vector(rng.random_range(0..storage.count)).to_vec())
            .collect();
        let thread_ids: Vec<Vec<usize>> = (0..max_threads)
            .map(|_| {
                (0..batches * BATCH)
                    .map(|_| rng.random_range(0..storage.count))
                    .collect()
            })
            .collect();
        let (query, ids) = (&queries[0], &thread_ids[0]);
        let mut out = vec![0f32; BATCH];

        // Every variant must give the base scores
        let mut expected = vec![0f32; BATCH];
        score_batch("base", &storage, query, &ids[..BATCH], &mut expected);
        for variant in &variants {
            score_batch(variant, &storage, query, &ids[..BATCH], &mut out);
            for (got, want) in out.iter().zip(&expected) {
                assert!((got - want).abs() <= 1e-4, "{variant}: {got} != {want}");
            }
        }

        for &threads in &thread_counts {
            for variant in &variants {
                let mut per_check = Vec::with_capacity(reps);
                for rep in 0..reps {
                    let barrier = std::sync::Barrier::new(threads);
                    let started = Instant::now();
                    std::thread::scope(|scope| {
                        for t in 0..threads {
                            let (storage, barrier, variant) =
                                (&storage, &barrier, variant.as_str());
                            let (query, ids) = (&queries[t], &thread_ids[t]);
                            scope.spawn(move || {
                                barrier.wait();
                                let mut out = vec![0f32; BATCH];
                                let mut sink = 0.0;
                                for batch in ids.as_chunks::<BATCH>().0 {
                                    score_batch(variant, storage, query, batch, &mut out);
                                    sink += out[0];
                                }
                                black_box(sink);
                            });
                        }
                    });
                    let total = (threads * batches * BATCH) as f64;
                    // ns of one core per check: wall time x threads / checks
                    let ns = started.elapsed().as_nanos() as f64 * threads as f64 / total;
                    per_check.push(ns);
                    writeln!(
                        stdout,
                        "  dim {dim} t {threads:>3} {variant:<11} rep {rep}: {ns:7.1} core-ns/check, {:.1} M checks/s",
                        threads as f64 * 1e3 / ns,
                    )
                    .unwrap();
                    stdout.flush().unwrap();
                }
                per_check.sort_by(f64::total_cmp);
                let median = per_check[reps / 2];
                writeln!(
                    stdout,
                    "RESULT dim {dim} threads {threads} {variant:<11} median {median:7.1} core-ns/check, {:.2} M checks/s, {:.1} GB/s (min {:.1}, max {:.1}, reps {reps})",
                    threads as f64 * 1e3 / median,
                    threads as f64 * (dim * 4) as f64 / median,
                    per_check[0],
                    per_check[reps - 1],
                )
                .unwrap();
                stdout.flush().unwrap();
            }
        }
    }
}
