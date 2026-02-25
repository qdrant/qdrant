use std::sync::atomic::AtomicBool;

use criterion::{Criterion, criterion_group, criterion_main};
use permutation_iterator::Permutor;
use quantization::encoded_storage::{TestEncodedStorage, TestEncodedStorageBuilder};
use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
use quantization::encoded_vectors_u8::{EncodedVectorsU8, ScalarQuantizationMethod};
use rand::RngExt;

fn encode_dot_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode dot");

    let vectors_count = 100_000;
    let vector_dim = 1024;
    let mut rng = rand::rng();
    let mut list: Vec<f32> = Vec::new();
    for _ in 0..vectors_count {
        let vector: Vec<f32> = (0..vector_dim).map(|_| rng.random()).collect();
        list.extend_from_slice(&vector);
    }

    let vector_parameters = VectorParameters {
        dim: vector_dim,
        deprecated_count: None,
        distance_type: DistanceType::Dot,
        invert: false,
    };
    let quantized_vector_size =
        EncodedVectorsU8::<TestEncodedStorage>::get_quantized_vector_size(&vector_parameters);
    let i8_encoded = EncodedVectorsU8::encode(
        (0..vectors_count).map(|i| &list[i * vector_dim..(i + 1) * vector_dim]),
        TestEncodedStorageBuilder::new(None, quantized_vector_size),
        &vector_parameters,
        vectors_count,
        None,
        ScalarQuantizationMethod::Int8,
        None,
        &AtomicBool::new(false),
    )
    .unwrap();

    let query: Vec<f32> = (0..vector_dim).map(|_| rng.random()).collect();
    let encoded_query = i8_encoded.encode_query(&query);

    #[cfg(target_arch = "x86_64")]
    group.bench_function("score all u8 avx", |b| {
        b.iter(|| {
            let mut _s = 0.0;
            for i in 0..vectors_count as u32 {
                let quantized_vector = i8_encoded.get_quantized_vector(i);
                _s = i8_encoded.score_point_avx(&encoded_query, quantized_vector);
            }
        });
    });

    #[cfg(target_arch = "x86_64")]
    group.bench_function("score all u8 sse", |b| {
        b.iter(|| {
            let mut _s = 0.0;
            for i in 0..vectors_count as u32 {
                let quantized_vector = i8_encoded.get_quantized_vector(i);
                _s = i8_encoded.score_point_sse(&encoded_query, quantized_vector);
            }
        });
    });

    #[cfg(target_arch = "aarch64")]
    group.bench_function("score all u8 neon", |b| {
        b.iter(|| {
            let mut _s = 0.0;
            for i in 0..vectors_count as u32 {
                let quantized_vector = i8_encoded.get_quantized_vector(i);
                _s = i8_encoded.score_point_neon(&encoded_query, quantized_vector);
            }
        });
    });

    let permutor = Permutor::new(vectors_count as u64);
    let permutation: Vec<u32> = permutor.map(|i| i as u32).collect();

    #[cfg(target_arch = "x86_64")]
    group.bench_function("score random access u8 avx", |b| {
        b.iter(|| {
            let mut _s = 0.0;
            for &i in &permutation {
                let quantized_vector = i8_encoded.get_quantized_vector(i);
                _s = i8_encoded.score_point_avx(&encoded_query, quantized_vector);
            }
        });
    });

    #[cfg(target_arch = "x86_64")]
    group.bench_function("score random access u8 sse", |b| {
        let mut _s = 0.0;
        b.iter(|| {
            for &i in &permutation {
                let quantized_vector = i8_encoded.get_quantized_vector(i);
                _s = i8_encoded.score_point_sse(&encoded_query, quantized_vector);
            }
        });
    });

    #[cfg(target_arch = "aarch64")]
    group.bench_function("score random access u8 neon", |b| {
        let mut _s = 0.0;
        b.iter(|| {
            for &i in &permutation {
                let quantized_vector = i8_encoded.get_quantized_vector(i);
                _s = i8_encoded.score_point_neon(&encoded_query, quantized_vector);
            }
        });
    });
}

fn encode_l1_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode l1");

    let vectors_count = 100_000;
    let vector_dim = 1024;
    let mut rng = rand::rng();
    let mut list: Vec<f32> = Vec::new();
    for _ in 0..vectors_count {
        let vector: Vec<f32> = (0..vector_dim).map(|_| rng.random()).collect();
        list.extend_from_slice(&vector);
    }

    let vector_parameters = VectorParameters {
        dim: vector_dim,
        deprecated_count: None,
        distance_type: DistanceType::L1,
        invert: true,
    };
    let quantized_vector_size =
        EncodedVectorsU8::<TestEncodedStorage>::get_quantized_vector_size(&vector_parameters);
    let i8_encoded = EncodedVectorsU8::encode(
        (0..vectors_count).map(|i| &list[i * vector_dim..(i + 1) * vector_dim]),
        TestEncodedStorageBuilder::new(None, quantized_vector_size),
        &vector_parameters,
        vectors_count,
        None,
        ScalarQuantizationMethod::Int8,
        None,
        &AtomicBool::new(false),
    )
    .unwrap();

    let query: Vec<f32> = (0..vector_dim).map(|_| rng.random()).collect();
    let encoded_query = i8_encoded.encode_query(&query);

    #[cfg(target_arch = "x86_64")]
    group.bench_function("score all u8 avx", |b| {
        b.iter(|| {
            let mut _s = 0.0;
            for i in 0..vectors_count as u32 {
                let quantized_vector = i8_encoded.get_quantized_vector(i);
                _s = i8_encoded.score_point_avx(&encoded_query, quantized_vector);
            }
        });
    });

    #[cfg(target_arch = "x86_64")]
    group.bench_function("score all u8 sse", |b| {
        b.iter(|| {
            let mut _s = 0.0;
            for i in 0..vectors_count as u32 {
                let quantized_vector = i8_encoded.get_quantized_vector(i);
                _s = i8_encoded.score_point_sse(&encoded_query, quantized_vector);
            }
        });
    });

    #[cfg(target_arch = "aarch64")]
    group.bench_function("score all u8 neon", |b| {
        b.iter(|| {
            let mut _s = 0.0;
            for i in 0..vectors_count as u32 {
                let quantized_vector = i8_encoded.get_quantized_vector(i);
                _s = i8_encoded.score_point_neon(&encoded_query, quantized_vector);
            }
        });
    });

    let permutor = Permutor::new(vectors_count as u64);
    let permutation: Vec<u32> = permutor.map(|i| i as u32).collect();

    #[cfg(target_arch = "x86_64")]
    group.bench_function("score random access u8 avx", |b| {
        b.iter(|| {
            let mut _s = 0.0;
            for &i in &permutation {
                let quantized_vector = i8_encoded.get_quantized_vector(i);
                _s = i8_encoded.score_point_avx(&encoded_query, quantized_vector);
            }
        });
    });

    #[cfg(target_arch = "x86_64")]
    group.bench_function("score random access u8 sse", |b| {
        let mut _s = 0.0;
        b.iter(|| {
            for &i in &permutation {
                let quantized_vector = i8_encoded.get_quantized_vector(i);
                _s = i8_encoded.score_point_sse(&encoded_query, quantized_vector);
            }
        });
    });

    #[cfg(target_arch = "aarch64")]
    group.bench_function("score random access u8 neon", |b| {
        let mut _s = 0.0;
        b.iter(|| {
            for &i in &permutation {
                let quantized_vector = i8_encoded.get_quantized_vector(i);
                _s = i8_encoded.score_point_neon(&encoded_query, quantized_vector);
            }
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = encode_dot_bench, encode_l1_bench
}

criterion_main!(benches);
