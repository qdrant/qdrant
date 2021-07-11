mod prof;

use criterion::{criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use segment::types::{PayloadInterface, PayloadInterfaceStrict, PayloadVariant};

fn serde_formats_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("serde-formats-group");

    let payloads = (0..1000)
        .map(|x| {
            PayloadInterface::Payload(PayloadInterfaceStrict::Keyword(PayloadVariant::Value(
                format!("val_{}", x),
            )))
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
            for payload in payloads.iter() {
                let vec = serde_cbor::to_vec(payload);
                vec.unwrap();
            }
        });
    });

    group.bench_function("serde-deserialize-cbor", |b| {
        b.iter(|| {
            for bytes in cbor_bytes.iter() {
                let _payload: PayloadInterface = serde_cbor::from_slice(bytes).unwrap();
            }
        });
    });

    group.bench_function("serde-serialize-rmp", |b| {
        b.iter(|| {
            for payload in payloads.iter() {
                let vec = rmp_serde::to_vec(payload);
                vec.unwrap();
            }
        });
    });

    group.bench_function("serde-deserialize-rmp", |b| {
        b.iter(|| {
            for bytes in rmp_bytes.iter() {
                let _payload: PayloadInterface = rmp_serde::from_read_ref(bytes).unwrap();
            }
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = serde_formats_bench
}

criterion_main!(benches);
