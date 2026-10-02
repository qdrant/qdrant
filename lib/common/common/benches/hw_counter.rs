use std::hint::black_box;

use common::counter::AmbientContext;
use common::counter::hw::HwMetric;
use criterion::{Criterion, criterion_group, criterion_main};

fn bench_hw_counter(c: &mut Criterion) {
    c.bench_function("Hw scope", |b| {
        let acc = AmbientContext::new();
        b.iter(|| acc.measure(|| HwMetric::Cpu.bump(black_box(1))));
    });

    c.bench_function("AmbientContext::new", |b| {
        b.iter(|| {
            let _ = AmbientContext::new();
        });
    });
}

criterion_group!(hw_counter, bench_hw_counter);
criterion_main!(hw_counter);
