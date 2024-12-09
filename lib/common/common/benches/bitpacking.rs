use std::hint::black_box;

use common::bitpacking::{BitReader, BitWriter};
use common::bitpacking_links::{for_each_packed_link, pack_links};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use itertools::Itertools as _;
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
                out.clear();
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

pub fn bench_bitpacking_links(c: &mut Criterion) {
    let mut group = c.benchmark_group("bitpacking_links");

    let mut rng = StdRng::seed_from_u64(42);
    let mut links = Vec::new();
    let mut pos = vec![(0, 0, 0)];
    while links.len() <= 64_000_000 {
        let bits_per_unsorted = rng.gen_range(7..=32);
        let sorted_count = rng.gen_range(0..100);
        let unsorted_count = rng.gen_range(0..100);
        if 1 << bits_per_unsorted < sorted_count + unsorted_count {
            continue;
        }

        pack_links(
            &mut links,
            std::iter::repeat_with(|| rng.gen_range(0..1u64 << bits_per_unsorted) as u32)
                .unique()
                .take(sorted_count + unsorted_count)
                .collect(),
            bits_per_unsorted,
            sorted_count,
        );
        pos.push((links.len(), bits_per_unsorted, sorted_count));
    }

    let mut rng = StdRng::seed_from_u64(42);
    group.bench_function("read", |b| {
        b.iter_batched(
            || {
                let idx = rng.gen_range(1..pos.len());
                (&links[pos[idx - 1].0..pos[idx].0], pos[idx].1, pos[idx].2)
            },
            |(links, bits_per_unsorted, sorted_count)| {
                for_each_packed_link(links, bits_per_unsorted, sorted_count, |x| {
                    black_box(x);
                });
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_bitpacking, bench_bitpacking_links,
}

criterion_main!(benches);
