use std::hint::black_box;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use criterion::{Criterion, criterion_group, criterion_main};
use permutation_iterator::Permutor;
use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
use quantization::encoded_vectors_binary::{
    EncodedQueryBQ, EncodedVectorsBin, Encoding, QueryEncoding,
};
use rand::{Rng, SeedableRng};

fn generate_number(rng: &mut rand::rngs::StdRng) -> f32 {
    let n = f32::signum(rng.random_range(-1.0..1.0));
    if n == 0.0 { 1.0 } else { n }
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
        let vector: Vec<f32> = (0..vector_dim).map(|_| rng.random()).collect();
        vectors.push(vector);
    }

    let encoded_u128 = EncodedVectorsBin::<u128, _>::encode(
        vectors.iter(),
        Vec::<u8>::new(),
        &VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        },
        Encoding::OneBit,
        QueryEncoding::SameAsStorage,
        &AtomicBool::new(false),
    )
    .unwrap();

    let query = generate_vector(vector_dim, &mut rng);
    let encoded_query = encoded_u128.encode_query(&query);

    let hardware_counter = HardwareCounterCell::new();

    group.bench_function("score binary linear access u128", |b| {
        b.iter(|| {
            for i in 0..vectors_count as u32 {
                let s = encoded_u128.score_point(&encoded_query, i, &hardware_counter);
                black_box(s);
            }
        });
    });

    let permutor = Permutor::new(vectors_count as u64);
    let permutation: Vec<u32> = permutor.map(|i| i as u32).collect();

    group.bench_function("score binary random access u128", |b| {
        b.iter(|| {
            for &i in &permutation {
                let s = encoded_u128.score_point(&encoded_query, i, &hardware_counter);
                black_box(s);
            }
        });
    });

    let encoded_u8 = EncodedVectorsBin::<u8, _>::encode(
        vectors.iter(),
        Vec::<u8>::new(),
        &VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        },
        Encoding::OneBit,
        QueryEncoding::SameAsStorage,
        &AtomicBool::new(false),
    )
    .unwrap();

    let query = generate_vector(vector_dim, &mut rng);
    let encoded_query = encoded_u8.encode_query(&query);

    group.bench_function("score binary linear access u8", |b| {
        b.iter(|| {
            for i in 0..vectors_count as u32 {
                let s = encoded_u8.score_point(&encoded_query, i, &hardware_counter);
                black_box(s);
            }
        });
    });

    let permutor = Permutor::new(vectors_count as u64);
    let permutation: Vec<u32> = permutor.map(|i| i as u32).collect();

    group.bench_function("score binary random access u8", |b| {
        b.iter(|| {
            for &i in &permutation {
                let s = encoded_u8.score_point(&encoded_query, i, &hardware_counter);
                black_box(s);
            }
        });
    });
}

fn binary_scalar_query_bench_impl(c: &mut Criterion) {
    let mut group = c.benchmark_group("binary_u128_scalar_query");

    let vectors_count = 100_000;
    let vector_dim = 1024;
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let mut vectors: Vec<Vec<f32>> = (0..vectors_count)
        .map(|_| generate_vector(vector_dim, &mut rng))
        .collect();
    for _ in 0..vectors_count {
        let vector: Vec<f32> = (0..vector_dim).map(|_| rng.random()).collect();
        vectors.push(vector);
    }

    let encoded_u128 = EncodedVectorsBin::<u128, _>::encode(
        vectors.iter(),
        Vec::<u8>::new(),
        &VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        },
        Encoding::OneBit,
        QueryEncoding::Scalar8bits,
        &AtomicBool::new(false),
    )
    .unwrap();

    let query = generate_vector(vector_dim, &mut rng);
    let encoded_query = encoded_u128.encode_query(&query);

    let hardware_counter = HardwareCounterCell::new();
    let permutor = Permutor::new(vectors_count as u64);
    let permutation: Vec<u32> = permutor.map(|i| i as u32).collect();

    group.bench_function("binary u128 scalar query", |b| {
        b.iter(|| {
            for &i in &permutation {
                let s = encoded_u128.score_point(&encoded_query, i, &hardware_counter);
                black_box(s);
            }
        });
    });

    let native_scorer = |query: &[u128], vector: &[u128]| {
        let mut result = 0;
        for (&b1, b2_chunk) in vector.iter().zip(query.chunks_exact(8)) {
            for (i, &b2) in b2_chunk.iter().enumerate() {
                result += (b1 ^ b2).count_ones() << i;
            }
        }
        result as usize
    };

    let query = match &encoded_query {
        EncodedQueryBQ::Scalar8bits(encoded) => &encoded.encoded_vector,
        _ => panic!("Expected Scalar8bits"),
    };

    group.bench_function("binary u128 scalar query native", |b| {
        b.iter(|| {
            for &i in &permutation {
                let vector = encoded_u128.get_quantized_vector(i);
                let vector = bytemuck::cast_slice::<u8, u128>(vector);
                let s = native_scorer(query, vector);
                black_box(s);
            }
        });
    });

    let encoded_u8 = EncodedVectorsBin::<u8, _>::encode(
        vectors.iter(),
        Vec::<u8>::new(),
        &VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        },
        Encoding::OneBit,
        QueryEncoding::Scalar8bits,
        &AtomicBool::new(false),
    )
    .unwrap();

    let query = generate_vector(vector_dim, &mut rng);
    let encoded_query = encoded_u8.encode_query(&query);

    group.bench_function("binary u8 scalar query", |b| {
        b.iter(|| {
            for &i in &permutation {
                let s = encoded_u8.score_point(&encoded_query, i, &hardware_counter);
                black_box(s);
            }
        });
    });

    let native_scorer = |query: &[u8], vector: &[u8]| {
        let mut result = 0;
        for (&b1, b2_chunk) in vector.iter().zip(query.chunks_exact(8)) {
            for (i, &b2) in b2_chunk.iter().enumerate() {
                result += (b1 ^ b2).count_ones() << i;
            }
        }
        result as usize
    };

    let query = match &encoded_query {
        EncodedQueryBQ::Scalar8bits(encoded) => &encoded.encoded_vector,
        _ => panic!("Expected Scalar8bits"),
    };

    group.bench_function("binary u8 scalar query native", |b| {
        b.iter(|| {
            for &i in &permutation {
                let vector = encoded_u8.get_quantized_vector(i);
                let s = native_scorer(query, vector);
                black_box(s);
            }
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = binary_bench, binary_scalar_query_bench_impl
}

criterion_main!(benches);
