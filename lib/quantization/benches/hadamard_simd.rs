//! Compares the scalar Walsh–Hadamard Transform
//! (`quantization::turboquant::rotation::in_place_walsh_hadamard_transform`)
//! against the SIMD variants in `quantization::turboquant::simd::hadamard`:
//! AVX2 (`wht_avx2`, `wht_avx2_radix16_4x`) on x86_64 and NEON (`wht_neon`,
//! `wht_neon_radix16_4x`) on aarch64. Bit-equality between SIMD and scalar is
//! covered by unit tests in `simd::hadamard::tests` — this bench only measures
//! throughput.

use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use quantization::turboquant::rotation::in_place_walsh_hadamard_transform;
#[cfg(target_arch = "aarch64")]
use quantization::turboquant::simd::hadamard::simd_arm::{wht_neon, wht_neon_radix16_4x};
#[cfg(target_arch = "x86_64")]
use quantization::turboquant::simd::hadamard::simd_x86::{wht_avx2, wht_avx2_radix16_4x};
use rand::prelude::StdRng;
use rand::{RngExt, SeedableRng};

const SIZES: &[usize] = &[64, 128, 256, 512, 1024, 2048, 4096, 8192];

fn make_input(n: usize) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(n as u64);
    (0..n).map(|_| rng.random_range(-1.0f64..1.0)).collect()
}

fn bench_wht_scalar(c: &mut Criterion) {
    let mut group = c.benchmark_group("wht_scalar");
    for &n in SIZES {
        let input = make_input(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched(
                || input.clone(),
                |mut data| in_place_walsh_hadamard_transform(black_box(&mut data)),
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

#[cfg(target_arch = "x86_64")]
fn bench_wht_avx2(c: &mut Criterion) {
    if !std::is_x86_feature_detected!("avx2") {
        eprintln!("skipping wht_avx2: CPU does not support AVX2");
        return;
    }

    let mut group = c.benchmark_group("wht_avx2");
    for &n in SIZES {
        let input = make_input(n);
        group.bench_with_input(BenchmarkId::new("baseline", n), &n, |b, _| {
            b.iter_batched(
                || input.clone(),
                |mut data| unsafe { wht_avx2(black_box(&mut data)) },
                BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("radix16_4x", n), &n, |b, _| {
            b.iter_batched(
                || input.clone(),
                |mut data| unsafe { wht_avx2_radix16_4x(black_box(&mut data)) },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

#[cfg(not(target_arch = "x86_64"))]
fn bench_wht_avx2(_c: &mut Criterion) {
    eprintln!("skipping wht_avx2: not on x86_64");
}

#[cfg(target_arch = "aarch64")]
fn bench_wht_neon(c: &mut Criterion) {
    if !std::arch::is_aarch64_feature_detected!("neon") {
        eprintln!("skipping wht_neon: CPU does not support NEON");
        return;
    }

    let mut group = c.benchmark_group("wht_neon");
    for &n in SIZES {
        let input = make_input(n);
        group.bench_with_input(BenchmarkId::new("baseline", n), &n, |b, _| {
            b.iter_batched(
                || input.clone(),
                |mut data| unsafe { wht_neon(black_box(&mut data)) },
                BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("radix16_4x", n), &n, |b, _| {
            b.iter_batched(
                || input.clone(),
                |mut data| unsafe { wht_neon_radix16_4x(black_box(&mut data)) },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

#[cfg(not(target_arch = "aarch64"))]
fn bench_wht_neon(_c: &mut Criterion) {
    eprintln!("skipping wht_neon: not on aarch64");
}

criterion_group!(benches, bench_wht_scalar, bench_wht_avx2, bench_wht_neon);
criterion_main!(benches);
