use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use criterion::{criterion_group, criterion_main, Criterion};
use permutation_iterator::Permutor;
use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
use quantization::encoded_vectors_binary::EncodedVectorsBin;
use rand::{Rng, SeedableRng};

fn generate_number(rng: &mut rand::rngs::StdRng) -> f32 {
    let n = f32::signum(rng.gen_range(-1.0..1.0));
    if n == 0.0 {
        1.0
    } else {
        n
    }
}

fn generate_vector(dim: usize, rng: &mut rand::rngs::StdRng) -> Vec<f32> {
    (0..dim).map(|_| generate_number(rng)).collect()
}

fn binary_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode");

    let vectors_count = 100_000;
    let vector_dim = 1024;
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let mut vectors: Vec<Vec<f32>> = (0..vectors_count)
        .map(|_| generate_vector(vector_dim, &mut rng))
        .collect();
    for _ in 0..vectors_count {
        let vector: Vec<f32> = (0..vector_dim).map(|_| rng.gen()).collect();
        vectors.push(vector);
    }

    let encoded_u128 = EncodedVectorsBin::<u128, _>::encode(
        vectors.iter(),
        Vec::<u8>::new(),
        &VectorParameters {
            dim: vector_dim,
            count: vectors_count,
            distance_type: DistanceType::Dot,
            invert: false,
        },
        &AtomicBool::new(false),
    )
    .unwrap();

    let query = generate_vector(vector_dim, &mut rng);
    let encoded_query = encoded_u128.encode_query(&query);

    let hardware_counter = HardwareCounterCell::new();

    group.bench_function("score binary linear access u128", |b| {
        b.iter(|| {
            let mut _s = 0.0;
            for i in 0..vectors_count as u32 {
                _s = encoded_u128.score_point(&encoded_query, i, &hardware_counter);
            }
        });
    });

    let permutor = Permutor::new(vectors_count as u64);
    let permutation: Vec<u32> = permutor.map(|i| i as u32).collect();

    group.bench_function("score binary random access u128", |b| {
        b.iter(|| {
            let mut _s = 0.0;
            for &i in &permutation {
                _s = encoded_u128.score_point(&encoded_query, i, &hardware_counter);
            }
        });
    });

    let encoded_u8 = EncodedVectorsBin::<u8, _>::encode(
        vectors.iter(),
        Vec::<u8>::new(),
        &VectorParameters {
            dim: vector_dim,
            count: vectors_count,
            distance_type: DistanceType::Dot,
            invert: false,
        },
        &AtomicBool::new(false),
    )
    .unwrap();

    let query = generate_vector(vector_dim, &mut rng);
    let encoded_query = encoded_u8.encode_query(&query);

    group.bench_function("score binary linear access u8", |b| {
        b.iter(|| {
            let mut _s = 0.0;
            for i in 0..vectors_count as u32 {
                _s = encoded_u8.score_point(&encoded_query, i, &hardware_counter);
            }
        });
    });

    let permutor = Permutor::new(vectors_count as u64);
    let permutation: Vec<u32> = permutor.map(|i| i as u32).collect();

    group.bench_function("score binary random access u8", |b| {
        b.iter(|| {
            let mut _s = 0.0;
            for &i in &permutation {
                _s = encoded_u8.score_point(&encoded_query, i, &hardware_counter);
            }
        });
    });

    hardware_counter.discard_results();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = binary_bench
}

criterion_main!(benches);
