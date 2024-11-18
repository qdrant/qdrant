use blob_store::fixtures::{empty_storage, random_payload};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

/// sized similarly to the real dataset for a fair comparison
const PAYLOAD_COUNT: u32 = 100_000;

pub fn random_data_bench(c: &mut Criterion) {
    let (_dir, mut storage) = empty_storage();
    let mut rng = rand::thread_rng();
    c.bench_function("write random payload", |b| {
        b.iter_batched_ref(
            || random_payload(&mut rng, 2),
            |payload| {
                for i in 0..PAYLOAD_COUNT {
                    storage.put_value(i, payload).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    c.bench_function("read random payload", |b| {
        b.iter(|| {
            for i in 0..PAYLOAD_COUNT {
                let res = storage.get_value(i);
                assert!(res.is_some());
            }
        });
    });
}

criterion_group!(benches, random_data_bench);
criterion_main!(benches);
