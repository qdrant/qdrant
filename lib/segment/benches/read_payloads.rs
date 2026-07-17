use std::hint::black_box;
use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::{AccessPattern, Random, Sequential};
use common::types::PointOffsetType;
#[cfg(target_os = "linux")]
use common::universal_io::IoUringFile;
use common::universal_io::{MmapFile, UniversalWrite};
use criterion::{Criterion, criterion_group, criterion_main};
use rand::prelude::{SliceRandom, SmallRng};
use rand::{RngExt, SeedableRng};
use segment::payload_json;
use segment::payload_storage::payload_storage_impl::{PayloadStorageImpl, storage_dir};
use segment::payload_storage::{PayloadStorage, PayloadStorageRead};
use segment::types::Payload;
use tempfile::{Builder, TempDir};

const RNG_SEED: u64 = 42;
const POINT_COUNT: usize = 10_000;
const READ_BATCH_SIZE: usize = 1024;
const TAGS_COUNT: usize = 4;

criterion_main!(benches);

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench
}

fn bench(c: &mut Criterion) {
    let sequential_keys: Vec<_> = (0..READ_BATCH_SIZE as PointOffsetType).collect();

    let mut random_keys: Vec<_> = (0..POINT_COUNT as PointOffsetType).collect();
    random_keys.shuffle(&mut SmallRng::seed_from_u64(RNG_SEED));
    random_keys.truncate(READ_BATCH_SIZE);

    let mut group = c.benchmark_group("read-payloads");

    // Shared dir to reuse the same storage setup
    let tmpdir = tmpdir();

    {
        group.bench_function("mmap/sequential", |b| {
            let storage = storage::<MmapFile>(tmpdir.path());
            b.iter(|| read_payloads::<Sequential, _>(&storage, &sequential_keys));
        });

        group.bench_function("mmap/random", |b| {
            let storage = storage::<MmapFile>(tmpdir.path());
            b.iter(|| read_payloads::<Random, _>(&storage, &random_keys));
        });
    }

    #[cfg(target_os = "linux")]
    {
        group.bench_function("io-uring/sequential", |b| {
            let storage = storage::<IoUringFile>(tmpdir.path());
            b.iter(|| read_payloads::<Sequential, _>(&storage, &sequential_keys));
        });

        group.bench_function("io-uring/random", |b| {
            let storage = storage::<IoUringFile>(tmpdir.path());
            b.iter(|| read_payloads::<Random, _>(&storage, &random_keys));
        });
    }

    group.finish();
}

fn tmpdir() -> TempDir {
    Builder::new()
        .prefix("read-payloads-bench")
        .tempdir()
        .expect("tempdir created")
}

fn storage<S>(path: &Path) -> PayloadStorageImpl<S>
where
    S: UniversalWrite + 'static,
    S::Fs: Default,
{
    let storage_exists = storage_dir(path).exists();
    let mut storage = PayloadStorageImpl::open_or_create(path.to_path_buf(), false)
        .expect("payload storage opened");

    if !storage_exists {
        let mut rng = SmallRng::seed_from_u64(RNG_SEED);
        let hw_counter = HardwareCounterCell::disposable();

        for point_id in 0..POINT_COUNT as PointOffsetType {
            let payload = random_payload(&mut rng, point_id);

            storage
                .overwrite(point_id, &payload, &hw_counter)
                .expect("payload inserted");
        }

        storage.flusher()().expect("storage flushed");
    }

    storage.populate().expect("storage populated");
    storage
}

fn random_payload(rng: &mut SmallRng, point_id: PointOffsetType) -> Payload {
    let tags: Vec<_> = (0..TAGS_COUNT)
        .map(|_| format!("tag-{}", rng.random_range(0..64)))
        .collect();

    payload_json! {
        "point_id": point_id,
        "price": rng.random_range(0..1_000_000),
        "rating": rng.random::<f64>(),
        "active": rng.random_bool(0.8),
        "category": format!("category-{}", rng.random_range(0..32)),
        "tags": tags,
    }
}

fn read_payloads<P, S>(storage: &S, point_offsets: &[PointOffsetType]) -> usize
where
    P: AccessPattern,
    S: PayloadStorageRead,
{
    let point_offsets = point_offsets.iter().map(|&point_offset| ((), point_offset));
    let hw_counter = HardwareCounterCell::disposable();

    let mut fields_read = 0;
    storage
        .read_payloads::<P, _>(
            point_offsets,
            |_, payload| {
                fields_read += payload.len();
                Ok(())
            },
            &hw_counter,
        )
        .expect("payloads read");

    black_box(fields_read)
}
