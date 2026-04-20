use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use quantization::turboquant::simd::Query4bitSimd;
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
        assert!(dim % 2 == 0);
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

fn make_query(dim: usize) -> ([f32; 16], Vec<f32>) {
    let mut rng = StdRng::seed_from_u64(42);
    let codebook: [f32; 16] = std::array::from_fn(|_| rng.random_range(-1.0_f32..1.0));
    let query: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0_f32..1.0)).collect();
    (codebook, query)
}

fn bench_dotprod_cold(c: &mut Criterion) {
    let mut group = c.benchmark_group("query4bit_dotprod_cold");
    for &dim in DIMS {
        let (cb, q) = make_query(dim);
        let query = Query4bitSimd::new(&q, &cb);
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

                group.bench_with_input(BenchmarkId::new("neon_sdot_x2", dim), &dim, |b, _| {
                    let mut cursor = 0usize;
                    b.iter(|| {
                        let v = pool.vector(cursor);
                        cursor = cursor.wrapping_add(1);
                        unsafe { black_box(&query).dotprod_raw_neon_sdot_x2(black_box(v)) }
                    });
                });

                group.bench_with_input(BenchmarkId::new("neon_sdot_x4", dim), &dim, |b, _| {
                    let mut cursor = 0usize;
                    b.iter(|| {
                        let v = pool.vector(cursor);
                        cursor = cursor.wrapping_add(1);
                        unsafe { black_box(&query).dotprod_raw_neon_sdot_x4(black_box(v)) }
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

            if std::is_x86_feature_detected!("avxvnni") && std::is_x86_feature_detected!("avx2") {
                group.bench_with_input(BenchmarkId::new("avx_vnni", dim), &dim, |b, _| {
                    let mut cursor = 0usize;
                    b.iter(|| {
                        let v = pool.vector(cursor);
                        cursor = cursor.wrapping_add(1);
                        unsafe { black_box(&query).dotprod_raw_avx_vnni(black_box(v)) }
                    });
                });

                group.bench_with_input(BenchmarkId::new("avx_vnni_x4", dim), &dim, |b, _| {
                    let mut cursor = 0usize;
                    b.iter(|| {
                        let v = pool.vector(cursor);
                        cursor = cursor.wrapping_add(1);
                        unsafe { black_box(&query).dotprod_raw_avx_vnni_x4(black_box(v)) }
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

                group.bench_with_input(BenchmarkId::new("avx512_vnni_x4", dim), &dim, |b, _| {
                    let mut cursor = 0usize;
                    b.iter(|| {
                        let v = pool.vector(cursor);
                        cursor = cursor.wrapping_add(1);
                        unsafe { black_box(&query).dotprod_raw_avx512_vnni_x4(black_box(v)) }
                    });
                });
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_dotprod_cold);
criterion_main!(benches);
