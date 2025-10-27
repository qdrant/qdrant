use std::time::{Duration, Instant};

use common::counter::hardware_counter::HardwareCounterCell;
use criterion::{Criterion, criterion_group, criterion_main};
use gridstore::fixtures::{empty_storage, random_payload};
use rand::Rng;

pub fn flush_bench(c: &mut Criterion) {
    let prepopulation_size = 10_000;

    // Test sequential updates' flushing performance
    for unflushed_updates in [100, 1_000, prepopulation_size].iter() {
        let bench_name = format!("flush after {unflushed_updates} sequential writes");

        c.bench_function(&bench_name, |b| {
            // Setup: Create a storage with a specified number of records
            let (_dir, mut storage) = empty_storage();
            let mut rng = rand::rng();
            let hw_counter = HardwareCounterCell::new();
            let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

            // Pre-populate storage with sequential random data
            for i in 0..prepopulation_size {
                let payload = random_payload(&mut rng, 1); // Small payload to speed up setup
                storage.put_value(i, &payload, hw_counter_ref).unwrap();
            }

            b.iter_custom(|iters| {
                let mut total_elapsed = Duration::ZERO;
                for _ in 0..iters {
                    // apply sequential ids
                    for i in 0..*unflushed_updates {
                        let payload = random_payload(&mut rng, 1);
                        storage.put_value(i, &payload, hw_counter_ref).unwrap();
                    }

                    // Benchmark the flush operation after accumulating updates
                    let instant = Instant::now();

                    storage.flush().unwrap();

                    total_elapsed += instant.elapsed();
                }
                total_elapsed
            });
        });
    }

    // Test random updates' flushing performance
    for unflushed_updates in [100, 1_000, prepopulation_size].iter() {
        let bench_name = format!("flush after {unflushed_updates} random writes");

        c.bench_function(&bench_name, |b| {
            // Setup: Create a storage with a specified number of records
            let (_dir, mut storage) = empty_storage();
            let mut rng = rand::rng();
            let hw_counter = HardwareCounterCell::new();
            let hw_counter_ref = hw_counter.ref_payload_io_write_counter();

            // Pre-populate storage with random data
            for i in 0..prepopulation_size {
                let payload = random_payload(&mut rng, 1); // Small payload to speed up setup
                storage.put_value(i, &payload, hw_counter_ref).unwrap();
            }

            b.iter_custom(|iters| {
                let mut total_elapsed = Duration::ZERO;
                for _ in 0..iters {
                    // Apply random updates
                    for _ in 0..*unflushed_updates {
                        let id = rng.random_range(0..prepopulation_size);
                        let payload = random_payload(&mut rng, 1);
                        storage.put_value(id, &payload, hw_counter_ref).unwrap();
                    }

                    // Benchmark the flush operation after accumulating updates
                    let instant = Instant::now();

                    storage.flush().unwrap();

                    total_elapsed += instant.elapsed();
                }
                total_elapsed
            });
        });
    }
}

criterion_group!(benches, flush_bench);
criterion_main!(benches);
