use common::mmap_hashmap::{MmapHashMap, gen_ident, gen_map};
use criterion::{Criterion, criterion_group, criterion_main};
use rand::SeedableRng;
use rand::rngs::StdRng;

fn bench_mmap_hashmap(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let map = gen_map(&mut rng, gen_ident, 100_000);

    let tmpdir = tempfile::Builder::new().tempdir().unwrap();
    let mmap_path = tmpdir.path().join("data");
    let mut keys = map.keys().cloned().collect::<Vec<_>>();
    keys.sort_unstable();

    MmapHashMap::<str, u32>::create(
        &mmap_path,
        map.iter().map(|(k, v)| (k.as_str(), v.iter().copied())),
    )
    .unwrap();

    let mmap = MmapHashMap::<str, u32>::open(&mmap_path, true).unwrap();

    let mut it = keys.iter().cycle();
    c.bench_function("get", |b| {
        b.iter(|| mmap.get(it.next().unwrap()).iter().copied().max())
    });

    drop(tmpdir);
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_mmap_hashmap,
}

criterion_main!(benches);
