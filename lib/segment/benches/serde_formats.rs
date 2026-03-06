#[cfg(not(target_os = "windows"))]
mod prof;

use criterion::{Criterion, criterion_group, criterion_main};
use itertools::Itertools;
use segment::payload_json;
use segment::types::Payload;

fn serde_formats_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("serde-formats-group");

    let payloads = (0..1000)
        .map(|x| {
            let payload = payload_json! {"val": format!("val_{x}")};
            payload
        })
        .collect_vec();

    let cbor_bytes = payloads
        .iter()
        .map(|p| serde_cbor::to_vec(p).unwrap())
        .collect_vec();

    let rmp_bytes = payloads
        .iter()
        .map(|p| rmp_serde::to_vec(p).unwrap())
        .collect_vec();

    group.bench_function("serde-serialize-cbor", |b| {
        b.iter(|| {
            for payload in &payloads {
                let vec = serde_cbor::to_vec(payload);
                vec.unwrap();
            }
        });
    });

    group.bench_function("serde-deserialize-cbor", |b| {
        b.iter(|| {
            for bytes in &cbor_bytes {
                let _payload: Payload = serde_cbor::from_slice(bytes).unwrap();
            }
        });
    });

    group.bench_function("serde-serialize-rmp", |b| {
        b.iter(|| {
            for payload in &payloads {
                let vec = rmp_serde::to_vec(payload);
                vec.unwrap();
            }
        });
    });

    group.bench_function("serde-deserialize-rmp", |b| {
        b.iter(|| {
            for bytes in &rmp_bytes {
                let _payload: Payload = rmp_serde::from_slice(bytes).unwrap();
            }
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = serde_formats_bench
}

criterion_main!(benches);
