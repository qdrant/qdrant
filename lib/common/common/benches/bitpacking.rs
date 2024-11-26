use std::hint::black_box;

use common::bitpacking::{BitReader, BitWriter};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use rand::rngs::StdRng;
use rand::{Rng as _, SeedableRng as _};

pub fn bench_bitpacking(c: &mut Criterion) {
    let mut group = c.benchmark_group("bitpacking");

    let mut rng = StdRng::seed_from_u64(42);
    let data8 = (0..64_000_000).map(|_| rng.gen()).collect::<Vec<u8>>();
    let data32 = (0..4_000_000).map(|_| rng.gen()).collect::<Vec<u32>>();

    let mut rng = StdRng::seed_from_u64(42);
    group.bench_function("read", |b| {
        b.iter_batched(
            || {
                let bits = rng.gen_range(1..=32);
                let bytes = rng.gen_range(0..=16);
                let start = rng.gen_range(0..data8.len() - bytes);
                (bits, &data8[start..start + bytes])
            },
            |(bits, data)| {
                let mut r = BitReader::new(data);
                r.set_bits(bits);
                for _ in 0..(data.len() * u8::BITS as usize / bits as usize) {
                    black_box(r.read());
                }
            },
            BatchSize::SmallInput,
        )
    });

    let mut rng = StdRng::seed_from_u64(42);
    let mut out = Vec::new();
    group.bench_function("write", |b| {
        b.iter_batched(
            || {
                let bits = rng.gen_range(1..=32);
                let values = rng.gen_range(0..=16);
                let start = rng.gen_range(0..data32.len() - values);
                (bits, &data32[start..start + values])
            },
            |(bits, data)| {
                let mut w = BitWriter::new(&mut out);
                for &x in data {
                    w.write(x, bits);
                }
                w.finish();
                black_box(&mut out);
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_bitpacking
}

criterion_main!(benches);
