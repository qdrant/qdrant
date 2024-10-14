#[cfg(not(target_os = "windows"))]
mod prof;

use std::collections::{BTreeMap, HashMap};

use criterion::{criterion_group, criterion_main, Criterion};
use rand::Rng;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
enum EnumIdTagged {
    Num(u64),
    Uuid(Uuid),
}

#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
#[serde(untagged)]
enum EnumId {
    Num(u64),
    Uuid(Uuid),
}

#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
struct StructId {
    id: Option<u64>,
    uuid: Option<Uuid>,
}

fn id_serialization_speed(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialization-group");
    let mut rng = rand::thread_rng();

    group.bench_function("u64", |b| {
        b.iter(|| {
            let key: u64 = rng.gen_range(0..100000000);
            bincode::serialize(&key).unwrap();
        });
    });

    group.bench_function("u128", |b| {
        b.iter(|| {
            let key: u64 = rng.gen_range(0..100000000);
            let new_key = u128::from(key);
            bincode::serialize(&new_key).unwrap();
        });
    });

    group.bench_function("struct-u64", |b| {
        b.iter(|| {
            let key: u64 = rng.gen_range(0..100000000);
            let new_key = StructId {
                id: Some(key),
                uuid: None,
            };
            bincode::serialize(&new_key).unwrap();
        });
    });

    group.bench_function("struct-uuid", |b| {
        b.iter(|| {
            let key: u64 = rng.gen_range(0..100000000);
            let new_key = StructId {
                id: None,
                uuid: Some(Uuid::from_u128(u128::from(key))),
            };
            bincode::serialize(&new_key).unwrap();
        });
    });

    group.bench_function("enum-u64", |b| {
        b.iter(|| {
            let key: u64 = rng.gen_range(0..100000000);
            let new_key = EnumIdTagged::Num(key);
            bincode::serialize(&new_key).unwrap();
        });
    });

    group.bench_function("enum-uuid", |b| {
        b.iter(|| {
            let key: u64 = rng.gen_range(0..100000000);
            let new_key = EnumIdTagged::Uuid(Uuid::from_u128(u128::from(key)));
            bincode::serialize(&new_key).unwrap();
        });
    });

    group.bench_function("struct-cbor-u128", |b| {
        b.iter(|| {
            let key: u64 = rng.gen_range(0..100000000);
            let new_key = u128::from(key);
            serde_cbor::to_vec(&new_key).unwrap();
        });
    });

    group.bench_function("struct-cbor-u64", |b| {
        b.iter(|| {
            let key: u64 = rng.gen_range(0..100000000);
            let new_key = StructId {
                id: Some(key),
                uuid: None,
            };
            serde_cbor::to_vec(&new_key).unwrap();
        });
    });

    group.bench_function("struct-cbor-uuid", |b| {
        b.iter(|| {
            let key: u64 = rng.gen_range(0..100000000);
            let new_key = StructId {
                id: None,
                uuid: Some(Uuid::from_u128(u128::from(key))),
            };
            serde_cbor::to_vec(&new_key).unwrap();
        });
    });

    group.bench_function("enum-cbor-u64", |b| {
        b.iter(|| {
            let key: u64 = rng.gen_range(0..100000000);
            let new_key = EnumId::Num(key);
            serde_cbor::to_vec(&new_key).unwrap();
        });
    });

    group.bench_function("enum-cbor-uuid", |b| {
        b.iter(|| {
            let key: u64 = rng.gen_range(0..100000000);
            let new_key = EnumId::Uuid(Uuid::from_u128(u128::from(key)));
            serde_cbor::to_vec(&new_key).unwrap();
        });
    });
}

fn u128_hash_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash-search-group");

    let mut data: HashMap<u128, bool> = HashMap::default();
    let key: u128 = 123;
    let val = true;

    group.bench_function("u128", |b| {
        b.iter(|| {
            data.insert(key, val);
            data.get(&key);
        });
    });
}

fn enum_hash_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash-search-group");

    let mut data: BTreeMap<EnumId, bool> = BTreeMap::default();
    let key: EnumId = EnumId::Num(123);
    let val = true;

    group.bench_function("enum-u64", |b| {
        b.iter(|| {
            data.insert(key, val);
            data.get(&key);
        });
    });

    let key: EnumId = EnumId::Uuid(Uuid::from_u128(123));
    let val = true;

    group.bench_function("enum-uuid", |b| {
        b.iter(|| {
            data.insert(key, val);
            data.get(&key);
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = id_serialization_speed, u128_hash_search, enum_hash_search
}

criterion_main!(benches);
