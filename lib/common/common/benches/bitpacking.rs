use std::hint::black_box;
use std::io::Write;
use std::path::Path;

use common::bitpacking::{BitReader, BitWriter};
use common::bitpacking_links::{iterate_packed_links, pack_links};
use common::bitpacking_ordered;
use common::mmap::AdviceSetting;
#[cfg(target_os = "linux")]
use common::universal_io::IoUringFs;
use common::universal_io::{MmapFs, OpenOptions, Populate, UniversalReadFs};
use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, Criterion, criterion_group, criterion_main};
use itertools::Itertools as _;
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng as _};
use tempfile::NamedTempFile;
use zerocopy::IntoBytes;

pub fn bench_bitpacking(c: &mut Criterion) {
    let mut group = c.benchmark_group("bitpacking");

    let mut rng = SmallRng::seed_from_u64(42);
    let data8 = (0..64_000_000).map(|_| rng.random()).collect::<Vec<u8>>();
    let data32 = (0..4_000_000).map(|_| rng.random()).collect::<Vec<u32>>();

    let mut rng = SmallRng::seed_from_u64(42);
    group.bench_function("read", |b| {
        b.iter_batched(
            || {
                let bits = rng.random_range(1..=32);
                let bytes = rng.random_range(0..=16);
                let start = rng.random_range(0..data8.len() - bytes);
                (bits, &data8[start..start + bytes])
            },
            |(bits, data)| {
                let mut r = BitReader::new(data);
                r.set_bits(bits);
                for _ in 0..(data.len() * u8::BITS as usize / bits as usize) {
                    black_box(r.read::<u32>());
                }
            },
            BatchSize::SmallInput,
        )
    });

    let mut rng = SmallRng::seed_from_u64(42);
    let mut out = Vec::new();
    group.bench_function("write", |b| {
        b.iter_batched(
            || {
                let bits = rng.random_range(1..=32);
                let values = rng.random_range(0..=16);
                let start = rng.random_range(0..data32.len() - values);
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

    let mut rng = SmallRng::seed_from_u64(42);
    let mut links = Vec::new();
    let mut pos = vec![(0, 0, 0)];
    while links.len() <= 64_000_000 {
        let bits_per_unsorted = rng.random_range(7..=32);
        let sorted_count = rng.random_range(0..100);
        let unsorted_count = rng.random_range(0..100);
        if 1 << bits_per_unsorted < sorted_count + unsorted_count {
            continue;
        }

        pack_links(
            &mut links,
            &mut std::iter::repeat_with(|| rng.random_range(0..1u64 << bits_per_unsorted) as u32)
                .unique()
                .take(sorted_count + unsorted_count)
                .collect_vec(),
            bits_per_unsorted,
            sorted_count,
        );
        pos.push((links.len(), bits_per_unsorted, sorted_count));
    }

    let mut rng = SmallRng::seed_from_u64(42);
    group.bench_function("read", |b| {
        b.iter_batched(
            || {
                let idx = rng.random_range(1..pos.len());
                (&links[pos[idx - 1].0..pos[idx].0], pos[idx].1, pos[idx].2)
            },
            |(links, bits_per_unsorted, sorted_count)| {
                iterate_packed_links(links, bits_per_unsorted, sorted_count).for_each(|x| {
                    black_box(x);
                });
            },
            BatchSize::SmallInput,
        )
    });
}

pub fn bench_bitpacking_ordered(c: &mut Criterion) {
    let mut group = c.benchmark_group("bitpacking_ordered");

    let values =
        bitpacking_ordered::gen_test_sequence(&mut SmallRng::seed_from_u64(42), 32, 1 << 22);

    group.sample_size(10);
    group.bench_function("compress", |b| {
        b.iter(|| bitpacking_ordered::compress(&values))
    });
    // Recreate the group to reset the sample size.
    drop(group);
    let mut group = c.benchmark_group("bitpacking_ordered");

    let (compressed, params) = bitpacking_ordered::compress(&values);
    let reader = params.validate().unwrap();
    let slice_reader = reader.slice_reader(&compressed).unwrap();
    println!(
        "Original size: {:.1} MB, compressed size: {:.1} MB, {:?}",
        values.as_bytes().len() as f64 / 1e6,
        compressed.len() as f64 / 1e6,
        params,
    );

    let mut rng = SmallRng::seed_from_u64(42);
    group.bench_function("get_raw", |b| {
        b.iter_batched(
            || rng.random_range(0..values.len()),
            |i| {
                black_box(compressed[i]);
            },
            BatchSize::SmallInput,
        )
    });

    let mut rng = SmallRng::seed_from_u64(42);
    let mut out = [(0, 0); MAX_BATCH_SIZE];
    group.bench_function("batch/slice_reader", |b| {
        b.iter_batched(
            || random_batch(&mut rng, values.len()),
            |indices| {
                for (&index, out) in indices.iter().zip(&mut out) {
                    *out = slice_reader.read_pair(index).unwrap();
                }
                black_box(&mut out);
            },
            BatchSize::SmallInput,
        )
    });

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(&compressed).unwrap();

    bench_uio_batch(&mut group, "batch/uio_mmap", &MmapFs, file.path(), reader);
    #[cfg(target_os = "linux")]
    bench_uio_batch(
        &mut group,
        "batch/uio_io_uring",
        &IoUringFs,
        file.path(),
        reader,
    );
}

const MAX_BATCH_SIZE: usize = 32;

fn random_batch(rng: &mut SmallRng, len: usize) -> Vec<usize> {
    let batch_size = rng.random_range(1..=MAX_BATCH_SIZE);
    (0..batch_size)
        .map(|_| rng.random_range(0..len - 1))
        .collect()
}

fn bench_uio_batch<Fs: UniversalReadFs>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    name: &str,
    fs: &Fs,
    path: &Path,
    reader: bitpacking_ordered::Reader,
) {
    let options = OpenOptions {
        writeable: false,
        need_sequential: false,
        populate: Populate::Blocking,
        advice: AdviceSetting::Global,
    };
    let storage = fs.open(path, options, Default::default()).unwrap();

    let len = reader.decompressed_len();
    let mut rng = SmallRng::seed_from_u64(42);
    let mut out = [(0, 0); MAX_BATCH_SIZE];
    group.bench_function(name, |b| {
        b.iter_batched(
            || random_batch(&mut rng, len),
            |indices| {
                let out = &mut out[..indices.len()];
                for result in reader.read_pairs_iter(&storage, 0, &indices).unwrap() {
                    let (position, pair) = result.unwrap();
                    out[position] = pair;
                }
                black_box(out);
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_bitpacking, bench_bitpacking_links, bench_bitpacking_ordered,
}

criterion_main!(benches);
