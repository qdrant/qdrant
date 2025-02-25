use std::time::Instant;

use common::types::PointOffsetType;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::Rng;
use segment::id_tracker::IdTracker;
use segment::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use segment::types::ExtendedPointId;

fn benchmark(c: &mut Criterion) {
    c.bench_function("idtracker", |b| {
        b.iter_custom(|i| {
            let mut id_tracker = InMemoryIdTracker::new();
            let mut rand = rand::rng();

            let ids: Vec<i32> = (0..i).map(|_| rand.random_range(0..100_000)).collect();

            let start = Instant::now();

            for external in 0..i {
                id_tracker
                    .set_link(
                        ExtendedPointId::NumId(external),
                        ids[external as usize] as PointOffsetType,
                    )
                    .unwrap();
            }

            start.elapsed()
        })
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
