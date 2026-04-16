use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use quantization::turboquant::rotation::{
    HadamardRotation, compute_chunk_sizes, in_place_walsh_hadamard_transform,
};

const DIMS: &[usize] = &[128, 384, 768, 1024, 1536, 4096];

fn bench_wht(c: &mut Criterion) {
    let mut group = c.benchmark_group("walsh_hadamard_transform");

    for &dim in DIMS {
        let chunk_size = *compute_chunk_sizes(dim).first().unwrap();
        let mut data: Vec<f64> = (0..chunk_size).map(|i| i as f64 * 0.01).collect();

        group.bench_with_input(BenchmarkId::from_parameter(dim), &dim, |b, _| {
            b.iter(|| {
                in_place_walsh_hadamard_transform(black_box(&mut data));
            });
        });
    }

    group.finish();
}

fn bench_apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("hadamard_apply");

    for &dim in DIMS {
        let rot = HadamardRotation::new(dim);
        let input: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.1).collect();
        let mut buf = vec![0.0f64; dim];

        group.bench_with_input(BenchmarkId::from_parameter(dim), &dim, |b, _| {
            b.iter(|| black_box(rot.apply(black_box(&input), &mut buf)));
        });
    }

    group.finish();
}

fn bench_apply_inverse(c: &mut Criterion) {
    let mut group = c.benchmark_group("hadamard_apply_inverse");

    for &dim in DIMS {
        let rot = HadamardRotation::new(dim);
        let input: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.1).collect();
        let mut buf = vec![0.0f64; dim];
        let rotated = rot.apply(&input, &mut buf);

        group.bench_with_input(BenchmarkId::from_parameter(dim), &dim, |b, _| {
            b.iter(|| black_box(rot.apply_inverse(black_box(&rotated), &mut buf)));
        });
    }

    group.finish();
}
criterion_group!(benches, bench_wht, bench_apply, bench_apply_inverse,);
criterion_main!(benches);
