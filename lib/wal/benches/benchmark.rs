use crc::{CRC_32_ISCSI, Crc};
use criterion::{Criterion, criterion_group, criterion_main};
use rand::RngCore;
use std::hint::black_box;

pub const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

pub fn criterion_benchmark_4k(c: &mut Criterion) {
    let mut buffer = [0u8; 8192];
    let mut rng = rand::rng();
    rng.fill_bytes(&mut buffer);

    let mut group = c.benchmark_group("8k");
    group.throughput(criterion::Throughput::Bytes(8192));
    group.bench_function("crc", |b| {
        b.iter(|| {
            let mut digest = CASTAGNOLI.digest();
            digest.update(&buffer);
            black_box(digest.finalize());
        })
    });

    group.bench_function("crc32c", |b| {
        b.iter(|| {
            black_box(crc32c::crc32c(&buffer));
        })
    });
}

pub fn criterion_benchmark_1024k(c: &mut Criterion) {
    let mut buffer = [0u8; 1048576];
    let mut rng = rand::rng();
    rng.fill_bytes(&mut buffer);

    let mut group = c.benchmark_group("1M");
    group.throughput(criterion::Throughput::Bytes(1048576));
    group.bench_function("crc", |b| {
        b.iter(|| {
            let mut digest = CASTAGNOLI.digest();
            digest.update(&buffer);
            black_box(digest.finalize());
        })
    });

    group.bench_function("crc32c", |b| {
        b.iter(|| {
            black_box(crc32c::crc32c(&buffer));
        })
    });
}

criterion_group!(benches, criterion_benchmark_4k, criterion_benchmark_1024k);
criterion_main!(benches);
