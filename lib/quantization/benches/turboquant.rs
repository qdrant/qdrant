use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use quantization::DistanceType;
use quantization::encoded_vectors::VectorParameters;
use quantization::turboquant::quantization::TurboQuantizer;
use quantization::turboquant::{Metadata, TQBits, TQMode};
use rand::prelude::StdRng;
use rand::{RngExt, SeedableRng};

const DIMS: &[usize] = &[128, 384, 768, 1024, 1536, 4096];

fn make_tq(dim: usize, bits: TQBits) -> TurboQuantizer {
    let metadata = Metadata {
        vector_parameters: VectorParameters {
            dim,
            distance_type: DistanceType::Dot,
            invert: false,
            deprecated_count: None,
        },
        bits,
        mode: TQMode::Normal,
    };
    TurboQuantizer::new_from_metadata(&metadata)
}

fn random_vector(dim: usize, rng: &mut StdRng) -> Vec<f32> {
    (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect()
}

fn bench_quantize(c: &mut Criterion) {
    let bit_widths: &[(TQBits, &str)] = &[
        (TQBits::Bits1, "1bit"),
        (TQBits::Bits2, "2bit"),
        (TQBits::Bits4, "4bit"),
    ];

    for &(bits, bits_name) in bit_widths {
        let mut group = c.benchmark_group(format!("turboquant_{bits_name}"));

        for &dim in DIMS {
            let tq = make_tq(dim, bits);
            let mut rng = StdRng::seed_from_u64(42);

            group.bench_with_input(BenchmarkId::from_parameter(dim), &dim, |b, _| {
                b.iter_batched(
                    || (random_vector(dim, &mut rng), vec![0.0f64; dim]),
                    |(vec, mut buf)| tq.quantize(black_box(&vec), &mut buf),
                    criterion::BatchSize::SmallInput,
                );
            });
        }

        group.finish();
    }
}

fn bench_dot(c: &mut Criterion) {
    let bit_widths: &[(TQBits, &str)] = &[
        (TQBits::Bits1, "1bit"),
        (TQBits::Bits2, "2bit"),
        (TQBits::Bits4, "4bit"),
    ];

    for &(bits, bits_name) in bit_widths {
        let mut group = c.benchmark_group(format!("turboquant_dot_{bits_name}"));

        for &dim in DIMS {
            let tq = make_tq(dim, bits);
            let mut rng = StdRng::seed_from_u64(42);

            // Pre-quantize a vector so the bench measures only the dot path.
            let mut buf = vec![0.0f64; dim];
            let vec = random_vector(dim, &mut rng);
            let packed = tq.quantize(&vec, &mut buf);

            group.bench_with_input(BenchmarkId::from_parameter(dim), &dim, |b, _| {
                b.iter_batched(
                    || random_vector(dim, &mut rng),
                    |query| {
                        tq.score_precomputed(
                            black_box(&tq.precompute_query(&query)),
                            black_box(&packed),
                        )
                    },
                    criterion::BatchSize::SmallInput,
                );
            });
        }

        group.finish();
    }
}

fn bench_dot_precomputed(c: &mut Criterion) {
    let bit_widths: &[(TQBits, &str)] = &[
        (TQBits::Bits1, "1bit"),
        (TQBits::Bits2, "2bit"),
        (TQBits::Bits4, "4bit"),
    ];

    for &(bits, bits_name) in bit_widths {
        let mut group = c.benchmark_group(format!("turboquant_dot_precomputed_{bits_name}"));

        for &dim in DIMS {
            let tq = make_tq(dim, bits);
            let mut rng = StdRng::seed_from_u64(42);

            // Pre-quantize a vector so the bench measures only the dot path.
            let mut buf = vec![0.0f64; dim];
            let vec = random_vector(dim, &mut rng);
            let packed = tq.quantize(&vec, &mut buf);

            group.bench_with_input(BenchmarkId::from_parameter(dim), &dim, |b, _| {
                b.iter_batched(
                    || tq.precompute_query(&random_vector(dim, &mut rng)),
                    |query| tq.score_precomputed(black_box(&query), black_box(&packed)),
                    criterion::BatchSize::SmallInput,
                );
            });
        }

        group.finish();
    }
}

criterion_group!(benches, bench_quantize, bench_dot, bench_dot_precomputed);
criterion_main!(benches);
