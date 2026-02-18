use std::hint::black_box;

use common::atomic_bitvec::prelude::BitVec;
use criterion::{Criterion, criterion_group, criterion_main};
use gridstore::bitmask::Bitmask;
use gridstore::config::DEFAULT_REGION_SIZE_BLOCKS;
use rand::Rng;

pub fn bench_bitmask_ops(c: &mut Criterion) {
    let distr = rand::distr::StandardUniform;
    let rng = rand::rng();
    let random_bitvec = rng
        .sample_iter::<bool, _>(distr)
        .take(1000 * DEFAULT_REGION_SIZE_BLOCKS)
        .collect::<BitVec>();

    // It could be just random_bitvec.windows(DEFAULT_REGION_SIZE_BLOCKS).cycle(), but `bitvec`'s `Window` has
    // incorrectly restrictive `Clone` bounds.
    let mut bitslice_iter = (0..(random_bitvec.len() - DEFAULT_REGION_SIZE_BLOCKS))
        .map(|start| &random_bitvec[start..(start + DEFAULT_REGION_SIZE_BLOCKS)]);

    c.bench_function("calculate_gaps", |b| {
        b.iter(|| {
            let bitslice = bitslice_iter.next().unwrap();
            Bitmask::calculate_gaps(black_box(bitslice), DEFAULT_REGION_SIZE_BLOCKS)
        })
    });

    c.bench_function("find_available_blocks_in_slice", |b| {
        let mut rng = rand::rng();
        b.iter(|| {
            let bitslice = bitslice_iter.next().unwrap();
            let num_blocks = rng.random_range(1..10);
            Bitmask::find_available_blocks_in_slice(black_box(bitslice), num_blocks, |_| (0, 0))
        })
    });
}

criterion_group!(benches, bench_bitmask_ops);
criterion_main!(benches);
