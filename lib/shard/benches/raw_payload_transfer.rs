//! What `transfer_raw_payloads` actually costs and saves, per point.
//!
//! Both arms transfer raw points; they differ only in how the payload travels, which
//! is exactly what the flag switches:
//!
//! - `blob`: the payload stays the stored byte blob from the sending node's storage
//!   into the receiving node's WAL, and is parsed once when the point is applied.
//! - `value_tree`: the sender parses the blob and builds a protobuf value tree, the
//!   receiver turns that tree back into a payload. What happens with the flag off.
//!
//! Run with:
//!     cargo bench -p shard --bench raw_payload_transfer
//!
//! Sizes are printed once at startup, since criterion only reports time.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use prost::Message as _;
use segment::types::{Payload, PointIdType, RawPayload};
use shard::operations::point_ops::{PointOperations, PointStructRawPersisted, RawVectorsPersisted};

/// Points per measured batch, in the order of a transfer batch.
const BATCH: usize = 100;

/// Payload widths to measure, in `field_count`. Payload size drives the whole
/// comparison, so a narrow and a wide payload are measured separately.
const WIDTHS: [usize; 3] = [1, 10, 100];

/// A payload shaped like a real one — strings, numbers, a nested object and an array
/// — rather than a flat map of a single key.
fn payload(fields: usize) -> Payload {
    let mut map = serde_json::Map::new();
    for i in 0..fields {
        map.insert(
            format!("field_{i}"),
            serde_json::json!(format!("value number {i} with some descriptive text")),
        );
        map.insert(format!("count_{i}"), serde_json::json!(i));
    }
    map.insert(
        "nested".to_string(),
        serde_json::json!({"city": "Berlin", "geo": {"lat": 52.52, "lon": 13.405}}),
    );
    map.insert("tags".to_string(), serde_json::json!(["a", "b", "c"]));
    serde_json::from_value(serde_json::Value::Object(map)).unwrap()
}

/// A batch as a raw read hands it out: vectors and payload both still stored bytes.
fn batch_as_read(fields: usize) -> Vec<PointStructRawPersisted> {
    let blob = serde_json::to_vec(&payload(fields)).unwrap();
    let vector: Vec<u8> = [1.0f32, 2.0, 3.0, 4.0]
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect();

    (0..BATCH)
        .map(|i| PointStructRawPersisted {
            id: PointIdType::from(i as u64),
            vectors: RawVectorsPersisted::from(vec![("dense".to_string(), vector.clone())]),
            payload: None,
            payload_raw: Some(RawPayload::from_storage_bytes(blob.clone())),
        })
        .collect()
}

/// The same batch with every blob already parsed, i.e. what the sender holds with the
/// flag off.
fn batch_decoded(fields: usize) -> Vec<PointStructRawPersisted> {
    let mut points = batch_as_read(fields);
    for point in &mut points {
        point.decode_payload_raw().unwrap();
    }
    points
}

/// Sender: turn a batch into the wire messages.
fn to_wire(points: &[PointStructRawPersisted]) -> Vec<api::grpc::qdrant::PointStructRaw> {
    points
        .iter()
        .cloned()
        .map(api::grpc::qdrant::PointStructRaw::from)
        .collect()
}

/// Receiver: turn wire messages back into points ready to apply, which for a blob
/// includes the one parse the whole design leans on.
fn from_wire(wire: &[api::grpc::qdrant::PointStructRaw]) -> Vec<PointStructRawPersisted> {
    wire.iter()
        .cloned()
        .map(|point| {
            let mut point = PointStructRawPersisted::try_from(point).unwrap();
            point.decode_payload_raw().unwrap();
            point
        })
        .collect()
}

/// What the WAL write serializes.
fn wal_bytes(points: Vec<PointStructRawPersisted>) -> Vec<u8> {
    serde_cbor::to_vec(&PointOperations::UpsertPointsRaw(points)).unwrap()
}

fn report_sizes() {
    eprintln!(
        "\n{:>7}  {:>10}  {:>12}  {:>12}",
        "fields", "arm", "wire bytes", "WAL bytes"
    );
    for fields in WIDTHS {
        for (arm, points) in [
            ("blob", batch_as_read(fields)),
            ("value_tree", batch_decoded(fields)),
        ] {
            let wire: usize = to_wire(&points)
                .iter()
                .map(api::grpc::qdrant::PointStructRaw::encoded_len)
                .sum();
            let wal = wal_bytes(points).len();
            eprintln!("{fields:>7}  {arm:>10}  {wire:>12}  {wal:>12}");
        }
    }
    eprintln!();
}

fn raw_payload_bench(c: &mut Criterion) {
    report_sizes();

    let mut sender = c.benchmark_group("raw-payload-sender");
    for fields in WIDTHS {
        sender.throughput(Throughput::Elements(BATCH as u64));

        let as_read = batch_as_read(fields);
        sender.bench_with_input(BenchmarkId::new("blob", fields), &as_read, |b, points| {
            b.iter(|| black_box(to_wire(points)));
        });

        // With the flag off the sender also pays the parse, so it starts from the
        // batch as read too.
        sender.bench_with_input(
            BenchmarkId::new("value_tree", fields),
            &as_read,
            |b, points| {
                b.iter(|| {
                    let mut points = points.clone();
                    for point in &mut points {
                        point.decode_payload_raw().unwrap();
                    }
                    black_box(to_wire(&points))
                });
            },
        );
    }
    sender.finish();

    let mut receiver = c.benchmark_group("raw-payload-receiver");
    for fields in WIDTHS {
        receiver.throughput(Throughput::Elements(BATCH as u64));

        let blob_wire = to_wire(&batch_as_read(fields));
        receiver.bench_with_input(BenchmarkId::new("blob", fields), &blob_wire, |b, wire| {
            b.iter(|| black_box(from_wire(wire)));
        });

        let tree_wire = to_wire(&batch_decoded(fields));
        receiver.bench_with_input(
            BenchmarkId::new("value_tree", fields),
            &tree_wire,
            |b, wire| {
                b.iter(|| black_box(from_wire(wire)));
            },
        );
    }
    receiver.finish();

    let mut wal = c.benchmark_group("raw-payload-wal-encode");
    for fields in WIDTHS {
        wal.throughput(Throughput::Elements(BATCH as u64));

        let as_read = batch_as_read(fields);
        wal.bench_with_input(BenchmarkId::new("blob", fields), &as_read, |b, points| {
            b.iter(|| black_box(wal_bytes(points.clone())));
        });

        let decoded = batch_decoded(fields);
        wal.bench_with_input(
            BenchmarkId::new("value_tree", fields),
            &decoded,
            |b, points| {
                b.iter(|| black_box(wal_bytes(points.clone())));
            },
        );
    }
    wal.finish();
}

criterion_group!(benches, raw_payload_bench);
criterion_main!(benches);
