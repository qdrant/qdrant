mod prof;

use criterion::{criterion_group, criterion_main, Criterion};
use segment::spaces::metric::Metric;
use segment::spaces::simple::{EuclidMetric, DotProductMetric};
use segment::fixtures::index_fixtures::random_vector;

fn euclid_metric(c: &mut Criterion) {
    let mut group = c.benchmark_group("euclid-metric-group");

    let mut rng = rand::thread_rng();
    let data_1k_1 = random_vector(&mut rng, 1013);
    let data_1k_2 = random_vector(&mut rng, 1013);

    group.bench_function("euclid-metric-1k", |b| {
        b.iter(|| {
            let metric = EuclidMetric {};
            metric.similarity(&data_1k_1, &data_1k_2);
        });
    });

    let data_10k_1 = random_vector(&mut rng, 10013);
    let data_10k_2 = random_vector(&mut rng, 10013);

    group.bench_function("euclid-metric-10k", |b| {
        b.iter(|| {
            let metric = EuclidMetric {};
            metric.similarity(&data_10k_1, &data_10k_2);
        });
    });
}

fn dot_product_metric(c: &mut Criterion) {
    let mut group = c.benchmark_group("dot-product-metric-group");

    let mut rng = rand::thread_rng();
    let data_1k_1 = random_vector(&mut rng, 1013);
    let data_1k_2 = random_vector(&mut rng, 1013);

    group.bench_function("dot-product-metric-1k", |b| {
        b.iter(|| {
            let metric = DotProductMetric {};
            metric.similarity(&data_1k_1, &data_1k_2);
        });
    });

    let data_10k_1 = random_vector(&mut rng, 10013);
    let data_10k_2 = random_vector(&mut rng, 10013);

    group.bench_function("dot-product-metric-10k", |b| {
        b.iter(|| {
            let metric = DotProductMetric {};
            metric.similarity(&data_10k_1, &data_10k_2);
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = euclid_metric, dot_product_metric
}

criterion_main!(benches);
