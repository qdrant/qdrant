use std::cell::LazyCell;
use std::hint::black_box;
use std::ops::Deref as _;
use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::{AccessPattern, Random, Sequential};
use common::types::PointOffsetType;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::prelude::{SliceRandom, StdRng};
use rand::{RngExt, SeedableRng};
use segment::payload_json;
use segment::payload_storage::mmap_payload_storage::MmapPayloadStorage;
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
    random_keys.shuffle(&mut StdRng::seed_from_u64(RNG_SEED));
    random_keys.truncate(READ_BATCH_SIZE);

    let mut group = c.benchmark_group("read-payloads");

    {
        let tmpdir = tmpdir();
        let storage = LazyCell::new(|| storage(tmpdir.path()));

        group.bench_function("mmap/sequential", |b| {
            b.iter(|| read_payloads::<Sequential, _>(storage.deref(), &sequential_keys));
        });

        group.bench_function("mmap/random", |b| {
            b.iter(|| read_payloads::<Random, _>(storage.deref(), &random_keys));
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

fn storage(path: &Path) -> MmapPayloadStorage {
    let mut storage = MmapPayloadStorage::open_or_create(path.to_path_buf(), true)
        .expect("mmap payload storage opened");

    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    let hw_counter = HardwareCounterCell::disposable();

    for point_id in 0..POINT_COUNT as PointOffsetType {
        let payload = random_payload(&mut rng, point_id);

        storage
            .overwrite(point_id, &payload, &hw_counter)
            .expect("payload inserted");
    }

    storage.flusher()().expect("storage flushed");
    storage.populate().expect("storage populated");

    storage
}

fn random_payload(rng: &mut StdRng, point_id: PointOffsetType) -> Payload {
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
