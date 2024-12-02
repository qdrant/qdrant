use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use criterion::{criterion_group, criterion_main, Criterion};
use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
use quantization::encoded_vectors_pq::EncodedVectorsPQ;
use rand::Rng;

fn encode_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode");

    let vectors_count = 100_000;
    let vector_dim = 1024;
    let mut rng = rand::thread_rng();
    let mut list: Vec<f32> = Vec::new();
    for _ in 0..vectors_count {
        let vector: Vec<f32> = (0..vector_dim).map(|_| rng.gen()).collect();
        list.extend_from_slice(&vector);
    }

    let pq_encoded = EncodedVectorsPQ::encode(
        (0..vectors_count).map(|i| &list[i * vector_dim..(i + 1) * vector_dim]),
        Vec::<u8>::new(),
        &VectorParameters {
            dim: vector_dim,
            count: vectors_count,
            distance_type: DistanceType::Dot,
            invert: false,
        },
        2,
        2,
        &AtomicBool::new(false),
    )
    .unwrap();

    let query: Vec<f32> = (0..vector_dim).map(|_| rng.gen()).collect();
    let encoded_query = pq_encoded.encode_query(&query);

    let mut total = 0.0;

    let hardware_counter = HardwareCounterCell::new();

    group.bench_function("score random access pq", |b| {
        b.iter(|| {
            let random_idx = rand::random::<u32>() % vectors_count as u32;
            total += pq_encoded.score_point(&encoded_query, random_idx, &hardware_counter);
        });
    });

    hardware_counter.discard_results();

    println!("total: {total}");
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = encode_bench
}

criterion_main!(benches);
