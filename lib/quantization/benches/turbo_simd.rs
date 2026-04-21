use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use quantization::turboquant::simd::{
    Query4bitSimd, score_4bit_internal, score_4bit_internal_scalar,
};
#[cfg(target_arch = "x86_64")]
use quantization::turboquant::simd::{
    score_4bit_internal_avx2, score_4bit_internal_avx512_vnni, score_4bit_internal_sse,
};
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use quantization::turboquant::simd::{score_4bit_internal_neon, score_4bit_internal_neon_sdot};
use rand::prelude::StdRng;
use rand::seq::SliceRandom;
use rand::{RngExt, SeedableRng};

const DIMS: &[usize] = &[128, 384, 768, 1024, 1536, 4096];

/// Pool size ≫ L2. Indices are shuffled so the hardware prefetcher can't stream
/// vectors into cache — each iteration pays a real DRAM fetch.
const POOL_BYTES: usize = 64 * 1024 * 1024;

struct VectorPool {
    buf: Vec<u8>,
    indices: Vec<u32>,
    /// Packed bytes per vector (= dim / 2, since two 4-bit codes share a byte).
    packed_bytes: usize,
}

impl VectorPool {
    fn new(dim: usize, seed: u64) -> Self {
        assert!(dim.is_multiple_of(2));
        let packed_bytes = dim / 2;
        let count = (POOL_BYTES / packed_bytes).max(1024);
        let mut rng = StdRng::seed_from_u64(seed);
        let buf: Vec<u8> = (0..count * packed_bytes)
            .map(|_| rng.random_range(0..=u8::MAX))
            .collect();
        let mut indices: Vec<u32> = (0..count as u32).collect();
        indices.shuffle(&mut rng);
        Self {
            buf,
            indices,
            packed_bytes,
        }
    }

    #[inline]
    fn vector(&self, cursor: usize) -> &[u8] {
        let idx = self.indices[cursor % self.indices.len()] as usize;
        &self.buf[idx * self.packed_bytes..(idx + 1) * self.packed_bytes]
    }
}

fn make_query(dim: usize) -> Vec<f32> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..dim).map(|_| rng.random_range(-1.0_f32..1.0)).collect()
}

fn bench_dotprod_cold(c: &mut Criterion) {
    let mut group = c.benchmark_group("query4bit_dotprod_cold");
    for &dim in DIMS {
        let q = make_query(dim);
        let query = Query4bitSimd::new(&q);
        let pool = VectorPool::new(dim, 7);

        group.throughput(Throughput::Elements(dim as u64));

        group.bench_with_input(BenchmarkId::new("scalar", dim), &dim, |b, _| {
            let mut cursor = 0usize;
            b.iter(|| {
                let v = pool.vector(cursor);
                cursor = cursor.wrapping_add(1);
                black_box(&query).dotprod_raw(black_box(v))
            });
        });

        // Full public path: `dotprod_raw_best` (best SIMD backend) + the
        // `sum_codebook_over_vector` bias correction + `as f32` reconstruction.
        // This is what a real caller pays.  Comparing against the best raw
        // backend for the current CPU shows the bias-correction overhead.
        group.bench_with_input(BenchmarkId::new("dotprod", dim), &dim, |b, _| {
            let mut cursor = 0usize;
            b.iter(|| {
                let v = pool.vector(cursor);
                cursor = cursor.wrapping_add(1);
                black_box(&query).dotprod(black_box(v))
            });
        });

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            group.bench_with_input(BenchmarkId::new("neon", dim), &dim, |b, _| {
                let mut cursor = 0usize;
                b.iter(|| {
                    let v = pool.vector(cursor);
                    cursor = cursor.wrapping_add(1);
                    unsafe { black_box(&query).dotprod_raw_neon(black_box(v)) }
                });
            });

            if std::arch::is_aarch64_feature_detected!("dotprod") {
                group.bench_with_input(BenchmarkId::new("neon_sdot", dim), &dim, |b, _| {
                    let mut cursor = 0usize;
                    b.iter(|| {
                        let v = pool.vector(cursor);
                        cursor = cursor.wrapping_add(1);
                        unsafe { black_box(&query).dotprod_raw_neon_sdot(black_box(v)) }
                    });
                });
            }
        }

        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("sse4.1") && std::is_x86_feature_detected!("ssse3") {
                group.bench_with_input(BenchmarkId::new("sse", dim), &dim, |b, _| {
                    let mut cursor = 0usize;
                    b.iter(|| {
                        let v = pool.vector(cursor);
                        cursor = cursor.wrapping_add(1);
                        unsafe { black_box(&query).dotprod_raw_sse(black_box(v)) }
                    });
                });
            }

            if std::is_x86_feature_detected!("avx2") {
                group.bench_with_input(BenchmarkId::new("avx2", dim), &dim, |b, _| {
                    let mut cursor = 0usize;
                    b.iter(|| {
                        let v = pool.vector(cursor);
                        cursor = cursor.wrapping_add(1);
                        unsafe { black_box(&query).dotprod_raw_avx2(black_box(v)) }
                    });
                });
            }

            if std::is_x86_feature_detected!("avx512f")
                && std::is_x86_feature_detected!("avx512bw")
                && std::is_x86_feature_detected!("avx512vnni")
            {
                group.bench_with_input(BenchmarkId::new("avx512_vnni", dim), &dim, |b, _| {
                    let mut cursor = 0usize;
                    b.iter(|| {
                        let v = pool.vector(cursor);
                        cursor = cursor.wrapping_add(1);
                        unsafe { black_box(&query).dotprod_raw_avx512_vnni(black_box(v)) }
                    });
                });
            }
        }
    }
    group.finish();
}

/// Benchmarks [`score_4bit_internal`] (both vectors already PQ-encoded, both
/// cold from DRAM).  Every iteration picks two *different* pool indices so
/// each call pays two independent random cache misses — this models the
/// HNSW-internal case where we score one PQ vector against another stored
/// PQ vector, not against a hot query.
fn bench_score_cold(c: &mut Criterion) {
    let mut group = c.benchmark_group("query4bit_score_cold");
    for &dim in DIMS {
        let pool = VectorPool::new(dim, 7);

        group.throughput(Throughput::Elements(dim as u64));

        group.bench_with_input(BenchmarkId::new("scalar", dim), &dim, |b, _| {
            let mut cursor = 0usize;
            b.iter(|| {
                let va = pool.vector(cursor);
                let vb = pool.vector(cursor + 1);
                cursor = cursor.wrapping_add(2);
                score_4bit_internal_scalar(black_box(va), black_box(vb))
            });
        });

        // Public dispatcher — picks best available SIMD at runtime.
        group.bench_with_input(BenchmarkId::new("dispatch", dim), &dim, |b, _| {
            let mut cursor = 0usize;
            b.iter(|| {
                let va = pool.vector(cursor);
                let vb = pool.vector(cursor + 1);
                cursor = cursor.wrapping_add(2);
                score_4bit_internal(black_box(va), black_box(vb))
            });
        });

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            group.bench_with_input(BenchmarkId::new("neon", dim), &dim, |b, _| {
                let mut cursor = 0usize;
                b.iter(|| {
                    let va = pool.vector(cursor);
                    let vb = pool.vector(cursor + 1);
                    cursor = cursor.wrapping_add(2);
                    unsafe { score_4bit_internal_neon(black_box(va), black_box(vb)) }
                });
            });

            if std::arch::is_aarch64_feature_detected!("dotprod") {
                group.bench_with_input(BenchmarkId::new("neon_sdot", dim), &dim, |b, _| {
                    let mut cursor = 0usize;
                    b.iter(|| {
                        let va = pool.vector(cursor);
                        let vb = pool.vector(cursor + 1);
                        cursor = cursor.wrapping_add(2);
                        unsafe { score_4bit_internal_neon_sdot(black_box(va), black_box(vb)) }
                    });
                });
            }
        }

        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("sse4.1") && std::is_x86_feature_detected!("ssse3") {
                group.bench_with_input(BenchmarkId::new("sse", dim), &dim, |b, _| {
                    let mut cursor = 0usize;
                    b.iter(|| {
                        let va = pool.vector(cursor);
                        let vb = pool.vector(cursor + 1);
                        cursor = cursor.wrapping_add(2);
                        unsafe { score_4bit_internal_sse(black_box(va), black_box(vb)) }
                    });
                });
            }

            if std::is_x86_feature_detected!("avx2") {
                group.bench_with_input(BenchmarkId::new("avx2", dim), &dim, |b, _| {
                    let mut cursor = 0usize;
                    b.iter(|| {
                        let va = pool.vector(cursor);
                        let vb = pool.vector(cursor + 1);
                        cursor = cursor.wrapping_add(2);
                        unsafe { score_4bit_internal_avx2(black_box(va), black_box(vb)) }
                    });
                });
            }

            if std::is_x86_feature_detected!("avx512f")
                && std::is_x86_feature_detected!("avx512bw")
                && std::is_x86_feature_detected!("avx512vnni")
            {
                group.bench_with_input(BenchmarkId::new("avx512_vnni", dim), &dim, |b, _| {
                    let mut cursor = 0usize;
                    b.iter(|| {
                        let va = pool.vector(cursor);
                        let vb = pool.vector(cursor + 1);
                        cursor = cursor.wrapping_add(2);
                        unsafe { score_4bit_internal_avx512_vnni(black_box(va), black_box(vb)) }
                    });
                });
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_dotprod_cold, bench_score_cold);
criterion_main!(benches);
