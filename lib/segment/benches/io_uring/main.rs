mod overengineered;
mod simple;

use std::io::{self, Write as _};
use std::mem;

use criterion::*;
use rand::prelude::*;
use segment::spaces::metric::Metric as _;
use segment::spaces::simple::CosineMetric;

criterion_main!(benches);
criterion_group!(benches, io_uring);

const VECTORS: usize = 1024;
const VECTOR_SIZE_ITEMS: usize = 128;

const DISK_PARALLELISM: usize = 16;

pub fn io_uring(c: &mut Criterion) {
    let mut group = c.benchmark_group("io_uring");

    let mut input = tempfile::tempfile().unwrap();

    let mut values = StdRng::seed_from_u64(6942)
        .sample_iter::<f32, _>(rand::distributions::Uniform::new_inclusive(-1., 1.));

    {
        let mut writer = io::BufWriter::new(&mut input);

        for value in (&mut values).take(VECTORS * VECTOR_SIZE_ITEMS) {
            writer.write_all(&value.to_le_bytes()).unwrap();
        }
    }

    let vector_size_bytes = VECTOR_SIZE_ITEMS * mem::size_of::<f32>();
    let this = values.take(VECTOR_SIZE_ITEMS).collect::<Vec<_>>();

    let file = input.try_clone().unwrap();
    group.bench_function("overengineered", |b| {
        let mut reader = overengineered::UringReader::new(
            file.try_clone().unwrap(),
            0,
            vector_size_bytes,
            DISK_PARALLELISM,
        )
        .unwrap();

        b.iter(|| {
            reader
                .read_stream(0..(VECTORS as u32), |_, _, other| {
                    CosineMetric::similarity(&this, other);
                })
                .unwrap();
        })
    });

    let file = input.try_clone().unwrap();
    group.bench_function("simple", |b| {
        let mut reader = simple::UringReader::new(
            file.try_clone().unwrap(),
            vector_size_bytes,
            0,
            DISK_PARALLELISM,
        )
        .unwrap();

        b.iter(|| {
            reader
                .read_stream(0..(VECTORS as u32), |_, _, other| {
                    CosineMetric::similarity(&this, other);
                })
                .unwrap();
        })
    });

    group.finish();
}
