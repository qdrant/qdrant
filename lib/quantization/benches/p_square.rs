use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use quantization::p_square::P2Quantile;

fn p_square(c: &mut Criterion) {
    let mut group = c.benchmark_group("p_square");

    let count = 10_000;
    let data = (0..count)
        .map(|_| rand::random::<f64>())
        .collect::<Vec<f64>>();
    let quantile = 0.99;

    group.bench_function("p_square_5", |b| {
        b.iter(|| {
            let mut p2 = P2Quantile::<5>::new(quantile).unwrap();
            for &x in &data {
                p2.push(x);
            }
            black_box(p2.estimate());
        });
    });

    group.bench_function("p_square_7", |b| {
        b.iter(|| {
            let mut p2 = P2Quantile::<7>::new(quantile).unwrap();
            for &x in &data {
                p2.push(x);
            }
            black_box(p2.estimate());
        });
    });

    group.bench_function("p_square_9", |b| {
        b.iter(|| {
            let mut p2 = P2Quantile::<9>::new(quantile).unwrap();
            for &x in &data {
                p2.push(x);
            }
            black_box(p2.estimate());
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = p_square,
}

criterion_main!(benches);
