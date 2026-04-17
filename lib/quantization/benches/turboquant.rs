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
                    || random_vector(dim, &mut rng),
                    |vec| tq.quantize(black_box(&vec)),
                    criterion::BatchSize::SmallInput,
                );
            });
        }

        group.finish();
    }
}

criterion_group!(benches, bench_quantize);
criterion_main!(benches);
