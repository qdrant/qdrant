#[cfg(not(target_os = "windows"))]
mod prof;

use collection::hash_ring::Inner;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::Rng;

fn hash_ring_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash-ring-bench");

    let mut ring_raw = Inner::raw();
    let mut ring_fair = Inner::fair(100);

    // add 10 shards to ring
    for i in 0..10 {
        ring_raw.add(i);
        ring_fair.add(i);
    }

    let mut rnd = rand::thread_rng();

    group.bench_function("hash-ring-fair", |b| {
        b.iter(|| {
            let point = rnd.gen_range(0..100000);
            let _shard = ring_fair.get(&point);
        })
    });

    group.bench_function("hash-ring-raw", |b| {
        b.iter(|| {
            let point = rnd.gen_range(0..100000);
            let _shard = ring_raw.get(&point);
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = hash_ring_bench,
}

criterion_main!(benches);
