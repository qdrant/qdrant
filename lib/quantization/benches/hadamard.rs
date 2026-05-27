use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use quantization::turboquant::rotation::HadamardRotation;

const DIMS: &[usize] = &[128, 384, 768, 1024, 1536, 4096];

fn bench_apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("hadamard_apply");

    for &dim in DIMS {
        let rot = HadamardRotation::new(dim);
        let input: Vec<f64> = (0..dim).map(|i| (i as f64) * 0.1).collect();

        group.bench_with_input(BenchmarkId::from_parameter(dim), &dim, |b, _| {
            b.iter_batched(
                || input.clone(),
                |mut data| rot.apply(black_box(&mut data)),
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_apply_inverse(c: &mut Criterion) {
    let mut group = c.benchmark_group("hadamard_apply_inverse");

    for &dim in DIMS {
        let rot = HadamardRotation::new(dim);
        let input: Vec<f64> = (0..dim).map(|i| (i as f64) * 0.1).collect();
        let mut rotated = input.clone();
        rot.apply(&mut rotated);

        group.bench_with_input(BenchmarkId::from_parameter(dim), &dim, |b, _| {
            b.iter_batched(
                || rotated.clone(),
                |mut data| rot.apply_inverse(black_box(&mut data)),
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}
criterion_group!(benches, bench_apply, bench_apply_inverse,);
criterion_main!(benches);
