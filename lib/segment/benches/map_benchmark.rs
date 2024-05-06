#[cfg(not(target_os = "windows"))]
mod prof;

use std::collections::{BTreeMap, HashMap};
use std::iter;
use std::sync::atomic::AtomicBool;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use segment::common::operation_error::check_process_stopped;
use segment::data_types::tiny_map::TinyMap;
use segment::fixtures::index_fixtures::random_vector;
use segment::vector_storage::dense::dynamic_mmap_flags::DynamicMmapFlags;
use tempfile::tempdir;

const DIM: usize = 100;

fn small_map_obj(c: &mut Criterion) {
    let mut group = c.benchmark_group("small-map-obj-group");

    let mut rng = StdRng::seed_from_u64(42);
    let default_key = "vector".to_string();
    let default_key_2 = "vector1".to_string();
    let default_key_3 = "vector2".to_string();
    let random_vector = random_vector(&mut rng, DIM);

    group.bench_function("hash-map", |b| {
        b.iter(|| {
            let mut map = HashMap::new();
            map.insert(default_key.clone(), random_vector.clone());
            map.insert(default_key_2.clone(), random_vector.clone());
            map.insert(default_key_3.clone(), random_vector.clone());
            let _ = map.get(&default_key_3);
        });
    });

    group.bench_function("btree-map", |b| {
        b.iter(|| {
            let mut map = BTreeMap::new();
            map.insert(default_key.clone(), random_vector.clone());
            map.insert(default_key_2.clone(), random_vector.clone());
            map.insert(default_key_3.clone(), random_vector.clone());
            let _ = map.get(&default_key_3);
        });
    });

    #[allow(clippy::vec_init_then_push)]
    group.bench_function("vec-map", |b| {
        b.iter(|| {
            let mut map = Vec::with_capacity(3);
            map.push((default_key.clone(), random_vector.clone()));
            map.push((default_key_2.clone(), random_vector.clone()));
            map.push((default_key_3.clone(), random_vector.clone()));
            let _ = map.iter().find(|(k, _)| k == &default_key_3);
        });
    });

    group.bench_function("tiny-map", |b| {
        b.iter(|| {
            let mut map = TinyMap::new();
            map.insert(default_key.clone(), random_vector.clone());
            map.insert(default_key_2.clone(), random_vector.clone());
            map.insert(default_key_3.clone(), random_vector.clone());
            let _ = map.get(&default_key_3);
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = small_map_obj
}

criterion_main!(benches);
