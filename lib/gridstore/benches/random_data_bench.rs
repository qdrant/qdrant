use common::counter::hardware_counter::HardwareCounterCell;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use gridstore::fixtures::{empty_storage, random_payload};

/// sized similarly to the real dataset for a fair comparison
const PAYLOAD_COUNT: u32 = 100_000;

pub fn random_data_bench(c: &mut Criterion) {
    let (_dir, mut storage) = empty_storage();
    let mut rng = rand::rng();
    c.bench_function("write random payload", |b| {
        let hw_counter = HardwareCounterCell::new();
        let hw_counter_ref = hw_counter.ref_payload_io_write_counter();
        b.iter_batched_ref(
            || random_payload(&mut rng, 2),
            |payload| {
                for i in 0..PAYLOAD_COUNT {
                    storage.put_value(i, payload, hw_counter_ref).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    c.bench_function("read random payload", |b| {
        let hw_counter = HardwareCounterCell::new();
        b.iter(|| {
            for i in 0..PAYLOAD_COUNT {
                let res = storage.get_value(i, &hw_counter);
                assert!(res.is_some());
            }
        });
    });
}

criterion_group!(benches, random_data_bench);
criterion_main!(benches);
