use std::array;
use std::hint::black_box;
use std::io::{BufWriter, Write};

use common::generic_consts::*;
use common::universal_io::mmap_universal::MmapUniversal as MmapFileWrapper;
use common::universal_io::*;
use criterion::*;

criterion_main!(universal_mmap_read);

criterion_group! {
    name = universal_mmap_read;
    config = Criterion::default();
    targets = bench_universal_mmap_read,
}

const FILE_LENGTH: usize = 1_000_000;

fn bench_universal_mmap_read(c: &mut Criterion) {
    let mut file = tempfile::NamedTempFile::new().expect("temporary file created");

    {
        let mut writer = BufWriter::new(file.by_ref());

        for _ in 0..(FILE_LENGTH / 1000) {
            let floats: [f32; 1000] = array::from_fn(|_| rand::random_range(0.0..1.0));

            writer
                .write_all(bytemuck::cast_slice(&floats))
                .expect("random data written to file");
        }
    }

    let options = OpenOptions {
        writeable: false,
        need_sequential: true,
        disk_parallel: None,
        populate: Some(true),
        advice: None,
    };

    let mut group = c.benchmark_group("universal_mmap_read");

    let mmap = MmapUniversal::<f32>::open(file.path(), options).expect("MmapUniversal<f32> opened");
    group.bench_function("MmapUniversal<f32> overhead", |b| {
        b.iter(|| bench::<Sequential>(&mmap, overhead))
    });
    group.bench_function("MmapUniversal<f32> access", |b| {
        b.iter(|| bench::<Sequential>(&mmap, access))
    });

    let mmap = MmapUniversal::<u8>::open(file.path(), options).expect("MmapUniversal<u8> opened");
    group.bench_function("MmapUniversal<u8> overhead", |b| {
        b.iter(|| bench::<Sequential>(&mmap, overhead))
    });
    group.bench_function("MmapUniversal<u8> access", |b| {
        b.iter(|| bench::<Sequential>(&mmap, access))
    });

    let mmap: MmapFile = UniversalRead::<f32>::open(file.path(), options).expect("MmapFile opened");
    group.bench_function("MmapFile overhead", |b| {
        b.iter(|| bench::<Sequential>(&mmap, overhead))
    });
    group.bench_function("MmapFile access", |b| {
        b.iter(|| bench::<Sequential>(&mmap, access))
    });

    let mmap =
        MmapFileWrapper::<f32>::open(file.path(), options).expect("MmapFileWrapper<f32> opened");
    group.bench_function("MmapFileWrapper<f32> overhead", |b| {
        b.iter(|| bench::<Sequential>(&mmap, overhead))
    });
    group.bench_function("MmapFileWrapper<f32> access", |b| {
        b.iter(|| bench::<Sequential>(&mmap, access))
    });

    group.finish();
}

#[inline(always)]
fn bench<P: AccessPattern>(mmap: &impl Mmap, callback: fn(&[f32])) {
    let ranges = (0..FILE_LENGTH / 100).map(|iter| ReadRange {
        byte_offset: (iter * 100) as _,
        length: 100,
    });

    mmap.read_f32_batch::<P>(ranges, move |_, vec| Ok(callback(vec)))
        .expect("data read succesfully");
}

#[inline(always)]
fn overhead(floats: &[f32]) {
    black_box(floats);
}

#[inline(always)]
fn access(floats: &[f32]) {
    let sum: f32 = floats.into_iter().sum();
    black_box(sum);
}

trait Mmap {
    fn name() -> &'static str;

    fn read_f32_batch<P: AccessPattern>(
        &self,
        ranges: impl IntoIterator<Item = ReadRange>,
        callback: impl FnMut(usize, &[f32]) -> Result<()>,
    ) -> Result<()>;
}

impl Mmap for MmapUniversal<f32> {
    fn name() -> &'static str {
        "MmapUniversal<f32>"
    }

    #[inline(always)]
    fn read_f32_batch<P: AccessPattern>(
        &self,
        ranges: impl IntoIterator<Item = ReadRange>,
        callback: impl FnMut(usize, &[f32]) -> Result<()>,
    ) -> Result<()> {
        // reads `&[f32]` directly from mmap slice without additional checks, should be fastest
        self.read_batch::<P>(ranges, callback)
    }
}

impl Mmap for MmapUniversal<u8> {
    fn name() -> &'static str {
        "MmapUniversal<u8>"
    }

    #[inline(always)]
    fn read_f32_batch<P: AccessPattern>(
        &self,
        ranges: impl IntoIterator<Item = ReadRange>,
        mut callback: impl FnMut(usize, &[f32]) -> Result<()>,
    ) -> Result<()> {
        let ranges = ranges.into_iter().map(|range| {
            let ReadRange {
                byte_offset,
                length: length_f32,
            } = range;

            ReadRange {
                byte_offset,
                length: length_f32 * size_of::<f32>() as u64,
            }
        });

        // makes `bytemuck::cast_slice` call and remaps `ranges`, should be slower
        self.read_batch::<P>(ranges, move |idx, bytes| {
            callback(idx, bytemuck::cast_slice(bytes))
        })
    }
}

impl Mmap for MmapFile {
    fn name() -> &'static str {
        "MmapFile"
    }

    #[inline(always)]
    fn read_f32_batch<P: AccessPattern>(
        &self,
        ranges: impl IntoIterator<Item = ReadRange>,
        callback: impl FnMut(usize, &[f32]) -> Result<()>,
    ) -> Result<()> {
        // makes `bytemuck::cast_slice` call and additional `Arc` access, should be slower
        self.read_batch::<P>(ranges, callback)
    }
}

impl Mmap for MmapFileWrapper<f32> {
    fn name() -> &'static str {
        "MmapFileWrapper<f32>"
    }

    #[inline(always)]
    fn read_f32_batch<P: AccessPattern>(
        &self,
        ranges: impl IntoIterator<Item = ReadRange>,
        callback: impl FnMut(usize, &[f32]) -> Result<()>,
    ) -> Result<()> {
        // makes `bytemuck::cast_slice` call and additional `Arc` access, should be slower
        self.read_batch::<P>(ranges, callback)
    }
}
