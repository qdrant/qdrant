#[cfg(not(target_os = "windows"))]
mod prof;

use criterion::{criterion_group, criterion_main, Criterion};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use segment::data_types::vectors::VectorElementTypeByte;
use segment::spaces::metric::Metric;
#[cfg(target_arch = "x86_64")]
use segment::spaces::metric_uint::avx2::avx_cosine::avx_cosine_similarity_bytes;
#[cfg(target_arch = "x86_64")]
use segment::spaces::metric_uint::avx2::avx_dot::avx_dot_similarity_bytes;
#[cfg(target_arch = "x86_64")]
use segment::spaces::metric_uint::avx2::avx_euclid::avx_euclid_similarity_bytes;
#[cfg(target_arch = "x86_64")]
use segment::spaces::metric_uint::avx2::avx_manhattan::avx_manhattan_similarity_bytes;
#[cfg(target_arch = "aarch64")]
use segment::spaces::metric_uint::neon::neon_cosine::neon_cosine_similarity_bytes;
#[cfg(target_arch = "aarch64")]
use segment::spaces::metric_uint::neon::neon_dot::neon_dot_similarity_bytes;
#[cfg(target_arch = "aarch64")]
use segment::spaces::metric_uint::neon::neon_euclid::neon_euclid_similarity_bytes;
#[cfg(target_arch = "aarch64")]
use segment::spaces::metric_uint::neon::neon_simple_manhattan::neon_manhattan_similarity_bytes;
use segment::spaces::metric_uint::simple_cosine::cosine_similarity_bytes;
use segment::spaces::metric_uint::simple_dot::dot_similarity_bytes;
use segment::spaces::metric_uint::simple_euclid::euclid_similarity_bytes;
use segment::spaces::metric_uint::simple_manhattan::manhattan_similarity_bytes;
#[cfg(target_arch = "x86_64")]
use segment::spaces::metric_uint::sse2::sse_cosine::sse_cosine_similarity_bytes;
#[cfg(target_arch = "x86_64")]
use segment::spaces::metric_uint::sse2::sse_dot::sse_dot_similarity_bytes;
#[cfg(target_arch = "x86_64")]
use segment::spaces::metric_uint::sse2::sse_euclid::sse_euclid_similarity_bytes;
#[cfg(target_arch = "x86_64")]
use segment::spaces::metric_uint::sse2::sse_manhattan::sse_manhattan_similarity_bytes;
use segment::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};

const DIM: usize = 1024;
const COUNT: usize = 100_000;

fn byte_metrics_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("byte-metrics-bench-group");

    let mut rng = StdRng::seed_from_u64(42);

    let random_vectors_1: Vec<Vec<u8>> = (0..COUNT)
        .map(|_| (0..DIM).map(|_| rng.gen_range(0..255)).collect())
        .collect();
    let random_vectors_2: Vec<Vec<u8>> = (0..COUNT)
        .map(|_| (0..DIM).map(|_| rng.gen_range(0..255)).collect())
        .collect();

    group.bench_function("byte-dot", |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % COUNT;
            <DotProductMetric as Metric<VectorElementTypeByte>>::similarity(
                &random_vectors_1[i],
                &random_vectors_2[i],
            )
        });
    });

    group.bench_function("byte-dot-no-simd", |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % COUNT;
            dot_similarity_bytes(&random_vectors_1[i], &random_vectors_2[i])
        });
    });

    #[cfg(target_arch = "x86_64")]
    group.bench_function("byte-dot-avx", |b| {
        let mut i = 0;
        b.iter(|| unsafe {
            i = (i + 1) % COUNT;
            avx_dot_similarity_bytes(&random_vectors_1[i], &random_vectors_2[i])
        });
    });

    #[cfg(target_arch = "x86_64")]
    group.bench_function("byte-dot-sse", |b| {
        let mut i = 0;
        b.iter(|| unsafe {
            i = (i + 1) % COUNT;
            sse_dot_similarity_bytes(&random_vectors_1[i], &random_vectors_2[i])
        });
    });

    #[cfg(target_arch = "aarch64")]
    group.bench_function("byte-dot-neon", |b| {
        let mut i = 0;
        b.iter(|| unsafe {
            i = (i + 1) % COUNT;
            neon_dot_similarity_bytes(&random_vectors_1[i], &random_vectors_2[i])
        });
    });

    group.bench_function("byte-cosine", |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % COUNT;
            <CosineMetric as Metric<VectorElementTypeByte>>::similarity(
                &random_vectors_1[i],
                &random_vectors_2[i],
            )
        });
    });

    group.bench_function("byte-cosine-no-simd", |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % COUNT;
            cosine_similarity_bytes(&random_vectors_1[i], &random_vectors_2[i])
        });
    });

    #[cfg(target_arch = "x86_64")]
    group.bench_function("byte-cosine-avx", |b| {
        let mut i = 0;
        b.iter(|| unsafe {
            i = (i + 1) % COUNT;
            avx_cosine_similarity_bytes(&random_vectors_1[i], &random_vectors_2[i])
        });
    });

    #[cfg(target_arch = "x86_64")]
    group.bench_function("byte-cosine-sse", |b| {
        let mut i = 0;
        b.iter(|| unsafe {
            i = (i + 1) % COUNT;
            sse_cosine_similarity_bytes(&random_vectors_1[i], &random_vectors_2[i])
        });
    });

    #[cfg(target_arch = "aarch64")]
    group.bench_function("byte-cosine-neon", |b| {
        let mut i = 0;
        b.iter(|| unsafe {
            i = (i + 1) % COUNT;
            neon_cosine_similarity_bytes(&random_vectors_1[i], &random_vectors_2[i])
        });
    });

    group.bench_function("byte-euclid", |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % COUNT;
            <EuclidMetric as Metric<VectorElementTypeByte>>::similarity(
                &random_vectors_1[i],
                &random_vectors_2[i],
            )
        });
    });

    group.bench_function("byte-euclid-no-simd", |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % COUNT;
            euclid_similarity_bytes(&random_vectors_1[i], &random_vectors_2[i])
        });
    });

    #[cfg(target_arch = "x86_64")]
    group.bench_function("byte-euclid-avx", |b| {
        let mut i = 0;
        b.iter(|| unsafe {
            i = (i + 1) % COUNT;
            avx_euclid_similarity_bytes(&random_vectors_1[i], &random_vectors_2[i])
        });
    });

    #[cfg(target_arch = "x86_64")]
    group.bench_function("byte-euclid-sse", |b| {
        let mut i = 0;
        b.iter(|| unsafe {
            i = (i + 1) % COUNT;
            sse_euclid_similarity_bytes(&random_vectors_1[i], &random_vectors_2[i])
        });
    });

    #[cfg(target_arch = "aarch64")]
    group.bench_function("byte-euclid-neon", |b| {
        let mut i = 0;
        b.iter(|| unsafe {
            i = (i + 1) % COUNT;
            neon_euclid_similarity_bytes(&random_vectors_1[i], &random_vectors_2[i])
        });
    });

    group.bench_function("byte-manhattan", |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % COUNT;
            <ManhattanMetric as Metric<VectorElementTypeByte>>::similarity(
                &random_vectors_1[i],
                &random_vectors_2[i],
            )
        });
    });

    group.bench_function("byte-manhattan-no-simd", |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % COUNT;
            manhattan_similarity_bytes(&random_vectors_1[i], &random_vectors_2[i])
        });
    });

    #[cfg(target_arch = "x86_64")]
    group.bench_function("byte-manhattan-avx", |b| {
        let mut i = 0;
        b.iter(|| unsafe {
            i = (i + 1) % COUNT;
            avx_manhattan_similarity_bytes(&random_vectors_1[i], &random_vectors_2[i])
        });
    });

    #[cfg(target_arch = "x86_64")]
    group.bench_function("byte-manhattan-sse", |b| {
        let mut i = 0;
        b.iter(|| unsafe {
            i = (i + 1) % COUNT;
            sse_manhattan_similarity_bytes(&random_vectors_1[i], &random_vectors_2[i])
        });
    });

    #[cfg(target_arch = "aarch64")]
    group.bench_function("byte-manhattan-neon", |b| {
        let mut i = 0;
        b.iter(|| unsafe {
            i = (i + 1) % COUNT;
            neon_manhattan_similarity_bytes(&random_vectors_1[i], &random_vectors_2[i])
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = byte_metrics_bench
}

criterion_main!(benches);
