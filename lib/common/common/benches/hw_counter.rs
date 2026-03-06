use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use criterion::{Criterion, criterion_group, criterion_main};

fn bench_hw_counter(c: &mut Criterion) {
    c.bench_function("Disposable Hw Cell", |b| {
        b.iter(|| {
            let _ = HardwareCounterCell::new();
        });
    });

    c.bench_function("Disposable Hw Acc", |b| {
        b.iter(|| {
            let _ = HwMeasurementAcc::new();
        });
    });
}

criterion_group!(hw_counter, bench_hw_counter);
criterion_main!(hw_counter);
