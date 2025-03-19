use common::counter::hardware_counter::HardwareCounterCell;
use criterion::{Criterion, criterion_group, criterion_main};
use segment::data_types::vectors::only_default_vector;
use segment::entry::entry_point::SegmentEntry;
use segment::json_path::JsonPath;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{Distance, Payload, PayloadFieldSchema, PayloadSchemaType};
use serde_json::{Map, Value};
use tempfile::Builder;

pub fn criterion_benchmark(c: &mut Criterion) {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let dim = 400;

    let mut segment = build_simple_segment(dir.path(), dim, Distance::Dot).unwrap();

    let vector = vec![0.1f32; 400];

    let mut payload: Map<String, Value> = Map::default();

    for i in 0..3 {
        let key = format!("key{i}");
        payload.insert(key.clone(), "value".to_string().into());
        segment
            .create_field_index(
                100,
                &JsonPath::new(&key),
                Some(&PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword)),
                &HardwareCounterCell::new(),
            )
            .unwrap();
    }
    let payload = Payload::from(payload);

    let hw_counter = HardwareCounterCell::new();

    for id in 0..100000u64 {
        segment
            .upsert_point(100, id.into(), only_default_vector(&vector), &hw_counter)
            .unwrap();
        segment
            .set_payload(100, id.into(), &payload, &None, &hw_counter)
            .unwrap();
    }

    c.bench_function("segment-info", |b| {
        b.iter(|| {
            let _ = segment.info();
        })
    });

    c.bench_function("segment-size-info", |b| {
        b.iter(|| {
            let _ = segment.size_info();
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
