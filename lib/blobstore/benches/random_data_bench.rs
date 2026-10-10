use blobstore::fixtures::{empty_storage, random_payload};
use common::ambient;
use common::ambient::hw::HwMetric;
use common::generic_consts::Random;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use rand::rngs::SmallRng;

/// sized similarly to the real dataset for a fair comparison
const PAYLOAD_COUNT: u32 = 100_000;

pub fn random_data_bench(c: &mut Criterion) {
    let (_dir, mut storage) = empty_storage();
    let mut rng = rand::make_rng::<SmallRng>();
    c.bench_function("write random payload", |b| {
        let _scope = ambient::test_guard();
        b.iter_batched_ref(
            || random_payload(&mut rng, 2),
            |payload| {
                for i in 0..PAYLOAD_COUNT {
                    storage
                        .put_value(i, payload, HwMetric::PayloadIoWrite)
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    c.bench_function("read random payload", |b| {
        let _scope = ambient::test_guard();
        b.iter(|| {
            for i in 0..PAYLOAD_COUNT {
                let res = storage.get_value::<Random>(i).unwrap();
                assert!(res.is_some());
            }
        });
    });
}

criterion_group!(benches, random_data_bench);
criterion_main!(benches);
