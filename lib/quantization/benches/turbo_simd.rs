use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use quantization::encoded_vectors_binary::BitsStoreType;
use quantization::turboquant::simd::{
    Query1bitSimd, Query2bitSimd, Query4bitSimd, score_1bit_internal, score_1bit_internal_scalar,
    score_2bit_internal, score_2bit_internal_scalar, score_4bit_internal,
    score_4bit_internal_scalar,
};
#[cfg(target_arch = "x86_64")]
use quantization::turboquant::simd::{
    score_1bit_internal_avx2, score_1bit_internal_avx512_vpopcntdq, score_1bit_internal_sse,
    score_2bit_internal_avx2, score_2bit_internal_avx512_vnni, score_2bit_internal_sse,
    score_4bit_internal_avx2, score_4bit_internal_avx512_vnni, score_4bit_internal_sse,
};
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use quantization::turboquant::simd::{
    score_1bit_internal_neon, score_2bit_internal_neon, score_2bit_internal_neon_sdot,
    score_4bit_internal_neon, score_4bit_internal_neon_sdot,
};
use rand::prelude::StdRng;
use rand::seq::SliceRandom;
use rand::{RngExt, SeedableRng};

/// Two "nicely aligned" dims — `128` (single SIMD block) and `1536` (large,
/// divisible by every chunk size we ship) — plus a per-bit-width "ugly" dim
/// that hits the worst-case tail for that pipeline:
///   • odd chunk count → SDOT / AVX2 / AVX-512 leftover branch fires.
///   • `dim % chunk_dim` at its maximum → scalar tail helper does the most work.
const DIMS_4BIT: &[usize] = &[128, 1534, 1536]; // 1534 = 95 chunks (odd) + 14-dim tail
const DIMS_2BIT: &[usize] = &[128, 1532, 1536]; // 1532 = 95 chunks (odd) + 12-dim tail
const DIMS_1BIT: &[usize] = &[128, 1528, 1536]; // 1528 = 11 blocks (odd) + 120-dim tail

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
    fn with_packed_bytes(packed_bytes: usize, seed: u64) -> Self {
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

    /// 4-bit PQ pool: two codes per byte → `dim / 2` packed bytes per vector.
    fn new_4bit(dim: usize, seed: u64) -> Self {
        assert!(dim.is_multiple_of(2));
        Self::with_packed_bytes(dim / 2, seed)
    }

    /// 2-bit PQ pool: four codes per byte → `dim / 4` packed bytes per vector.
    fn new_2bit(dim: usize, seed: u64) -> Self {
        assert!(dim.is_multiple_of(4));
        Self::with_packed_bytes(dim / 4, seed)
    }

    /// 1-bit PQ pool: 8 codes per byte → `dim / 8` packed bytes per vector.
    fn new_1bit(dim: usize, seed: u64) -> Self {
        assert!(dim.is_multiple_of(8));
        Self::with_packed_bytes(dim / 8, seed)
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
    for &dim in DIMS_4BIT {
        let q = make_query(dim);
        let query = Query4bitSimd::new(&q);
        let pool = VectorPool::new_4bit(dim, 7);

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
    for &dim in DIMS_4BIT {
        let pool = VectorPool::new_4bit(dim, 7);

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

/// Cold-cache benchmarks for [`score_1bit_internal`] — vector-vs-vector
/// XOR+popcount scoring.  Both operands are drawn from different shuffled
/// pool indices so each call pays two independent DRAM fetches.
fn bench_score_1bit_cold(c: &mut Criterion) {
    let mut group = c.benchmark_group("query1bit_score_cold");
    for &dim in DIMS_1BIT {
        let pool = VectorPool::new_1bit(dim, 7);

        group.throughput(Throughput::Elements(dim as u64));

        group.bench_with_input(BenchmarkId::new("scalar", dim), &dim, |b, _| {
            let mut cursor = 0usize;
            b.iter(|| {
                let va = pool.vector(cursor);
                let vb = pool.vector(cursor + 1);
                cursor = cursor.wrapping_add(2);
                score_1bit_internal_scalar(black_box(va), black_box(vb))
            });
        });

        group.bench_with_input(BenchmarkId::new("dispatch", dim), &dim, |b, _| {
            let mut cursor = 0usize;
            b.iter(|| {
                let va = pool.vector(cursor);
                let vb = pool.vector(cursor + 1);
                cursor = cursor.wrapping_add(2);
                score_1bit_internal(black_box(va), black_box(vb))
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
                    unsafe { score_1bit_internal_neon(black_box(va), black_box(vb)) }
                });
            });
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
                        unsafe { score_1bit_internal_sse(black_box(va), black_box(vb)) }
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
                        unsafe { score_1bit_internal_avx2(black_box(va), black_box(vb)) }
                    });
                });
            }

            if std::is_x86_feature_detected!("avx512f")
                && std::is_x86_feature_detected!("avx512vpopcntdq")
            {
                group.bench_with_input(BenchmarkId::new("avx512_vpopcntdq", dim), &dim, |b, _| {
                    let mut cursor = 0usize;
                    b.iter(|| {
                        let va = pool.vector(cursor);
                        let vb = pool.vector(cursor + 1);
                        cursor = cursor.wrapping_add(2);
                        unsafe {
                            score_1bit_internal_avx512_vpopcntdq(black_box(va), black_box(vb))
                        }
                    });
                });
            }
        }
    }
    group.finish();
}

/// Query-against-data benchmarks: a single hot query is scored against cold
/// 1-bit PQ data vectors.  Mirrors the HNSW scoring pattern.
///
/// Compares our `Query1bitSimd<{8,12,16}>` (signed bit-plane transpose +
/// AND-popcount) against the existing BQ `Scalar8bits` path
/// (`BitsStoreType::xor_popcnt_scalar` with `bits_count=8`) — BQ stays at
/// 8 bits (its only supported scalar width) and serves as the baseline.
/// 12/16 rows show the linear cost of widening the query.
fn bench_query1bit_vs_bq_hot(c: &mut Criterion) {
    let mut group = c.benchmark_group("query1bit_vs_bq_scalar8bits");
    let mut rng_seed = StdRng::seed_from_u64(42);
    for &dim in DIMS_1BIT {
        let pool = VectorPool::new_1bit(dim, 7);
        let query_floats: Vec<f32> = (0..dim)
            .map(|_| rng_seed.random_range(-1.0_f32..1.0))
            .collect();

        let q_our_8 = Query1bitSimd::<8>::new(&query_floats);
        let q_our_12 = Query1bitSimd::<12>::new(&query_floats);
        let q_our_16 = Query1bitSimd::<16>::new(&query_floats);
        let q_bq = encode_bq_scalar8bits(&query_floats);

        group.throughput(Throughput::Elements(dim as u64));

        group.bench_with_input(BenchmarkId::new("query1bit_8bit", dim), &dim, |b, _| {
            let mut cursor = 0usize;
            b.iter(|| {
                let v = pool.vector(cursor);
                cursor = cursor.wrapping_add(1);
                q_our_8.dotprod(black_box(v))
            });
        });

        group.bench_with_input(BenchmarkId::new("query1bit_12bit", dim), &dim, |b, _| {
            let mut cursor = 0usize;
            b.iter(|| {
                let v = pool.vector(cursor);
                cursor = cursor.wrapping_add(1);
                q_our_12.dotprod(black_box(v))
            });
        });

        group.bench_with_input(BenchmarkId::new("query1bit_16bit", dim), &dim, |b, _| {
            let mut cursor = 0usize;
            b.iter(|| {
                let v = pool.vector(cursor);
                cursor = cursor.wrapping_add(1);
                q_our_16.dotprod(black_box(v))
            });
        });

        group.bench_with_input(BenchmarkId::new("bq_scalar8bits", dim), &dim, |b, _| {
            let mut cursor = 0usize;
            b.iter(|| {
                let v = pool.vector(cursor);
                cursor = cursor.wrapping_add(1);
                <u8 as BitsStoreType>::xor_popcnt_scalar(black_box(v), black_box(&q_bq), 8)
            });
        });
    }
    group.finish();
}

/// Minimal reproduction of the BQ `Scalar8bits` encoding (u8 store,
/// `bits_count = 8`): per group of 8 consecutive query dims, emit 8
/// consecutive bytes — one per bit-plane — containing those 8 dims' bits.
/// See `_encode_scalar_query_vector` in `encoded_vectors_binary.rs` for the
/// canonical version.
fn encode_bq_scalar8bits(query: &[f32]) -> Vec<u8> {
    assert!(query.len().is_multiple_of(8));
    let max_abs = query
        .iter()
        .map(|x| x.abs())
        .fold(0.0_f32, f32::max)
        .max(f32::EPSILON);
    let min = -max_abs;
    let delta = 2.0 * max_abs / 255.0;

    let mut encoded = vec![0_u8; query.len()];
    for (chunk_idx, chunk) in query.chunks(8).enumerate() {
        for (shift, &value) in chunk.iter().enumerate() {
            let q = ((value - min) / delta).round().clamp(0.0, 255.0) as usize;
            for b in 0..8 {
                let bit = ((q >> b) & 1) as u8;
                encoded[8 * chunk_idx + b] |= bit << shift;
            }
        }
    }
    encoded
}

/// Cold-cache query-vs-vector dotprod for 2-bit PQ.  Mirrors
/// [`bench_dotprod_cold`] for 4-bit — a hot `Query2bitSimd` against cold
/// data vectors drawn from a shuffled 64 MB pool.
fn bench_dotprod_2bit_cold(c: &mut Criterion) {
    let mut group = c.benchmark_group("query2bit_dotprod_cold");
    for &dim in DIMS_2BIT {
        let q = make_query(dim);
        let query = Query2bitSimd::new(&q);
        let pool = VectorPool::new_2bit(dim, 7);

        group.throughput(Throughput::Elements(dim as u64));

        group.bench_with_input(BenchmarkId::new("scalar", dim), &dim, |b, _| {
            let mut cursor = 0usize;
            b.iter(|| {
                let v = pool.vector(cursor);
                cursor = cursor.wrapping_add(1);
                black_box(&query).dotprod_raw(black_box(v))
            });
        });

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

            if std::arch::is_aarch64_feature_detected!("dotprod") && dim.is_multiple_of(32) {
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

/// Cold-cache vector-vs-vector score for 2-bit PQ.  Mirrors
/// [`bench_score_cold`] for 4-bit.
fn bench_score_2bit_cold(c: &mut Criterion) {
    let mut group = c.benchmark_group("query2bit_score_cold");
    for &dim in DIMS_2BIT {
        let pool = VectorPool::new_2bit(dim, 7);

        group.throughput(Throughput::Elements(dim as u64));

        group.bench_with_input(BenchmarkId::new("scalar", dim), &dim, |b, _| {
            let mut cursor = 0usize;
            b.iter(|| {
                let va = pool.vector(cursor);
                let vb = pool.vector(cursor + 1);
                cursor = cursor.wrapping_add(2);
                score_2bit_internal_scalar(black_box(va), black_box(vb))
            });
        });

        group.bench_with_input(BenchmarkId::new("dispatch", dim), &dim, |b, _| {
            let mut cursor = 0usize;
            b.iter(|| {
                let va = pool.vector(cursor);
                let vb = pool.vector(cursor + 1);
                cursor = cursor.wrapping_add(2);
                score_2bit_internal(black_box(va), black_box(vb))
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
                    unsafe { score_2bit_internal_neon(black_box(va), black_box(vb)) }
                });
            });

            if std::arch::is_aarch64_feature_detected!("dotprod") {
                group.bench_with_input(BenchmarkId::new("neon_sdot", dim), &dim, |b, _| {
                    let mut cursor = 0usize;
                    b.iter(|| {
                        let va = pool.vector(cursor);
                        let vb = pool.vector(cursor + 1);
                        cursor = cursor.wrapping_add(2);
                        unsafe { score_2bit_internal_neon_sdot(black_box(va), black_box(vb)) }
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
                        unsafe { score_2bit_internal_sse(black_box(va), black_box(vb)) }
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
                        unsafe { score_2bit_internal_avx2(black_box(va), black_box(vb)) }
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
                        unsafe { score_2bit_internal_avx512_vnni(black_box(va), black_box(vb)) }
                    });
                });
            }
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_dotprod_cold,
    bench_score_cold,
    bench_dotprod_2bit_cold,
    bench_score_2bit_cold,
    bench_score_1bit_cold,
    bench_query1bit_vs_bq_hot,
);
criterion_main!(benches);
