use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::top_k::TopK;
use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};

/// Pushed points, and how many of them the structure keeps.
const CASES: [(usize, usize); 9] = [
    (32, 10),
    (64, 10),
    (128, 10),
    (50_000, 10),
    (50_000, 32),
    (50_000, 64),
    (50_000, 100),
    (50_000, 1000),
    (50_000, 10_000),
];

fn bench_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("top_of");

    for (size, limit) in CASES {
        let points = random_points(size);
        let case = format!("{size}/top={limit}");
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("fpq_push", &case), &points, |b, points| {
            b.iter(|| {
                let mut pq = FixedLengthPriorityQueue::new(limit);
                for &point in points {
                    pq.push(point);
                }
                pq.into_sorted_vec()
            });
        });

        group.bench_with_input(BenchmarkId::new("top_k", &case), &points, |b, points| {
            b.iter(|| {
                let mut top = TopK::new(limit);
                for &point in points {
                    top.push(point);
                }
                top.into_vec()
            });
        });
    }

    group.finish();
}

fn random_points(size: usize) -> Vec<ScoredPointOffset> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size)
        .map(|idx| ScoredPointOffset {
            idx: idx as PointOffsetType,
            score: rng.random::<ScoreType>(),
        })
        .collect()
}

criterion_group!(benches, bench_push);
criterion_main!(benches);
