use std::collections::HashMap;

use criterion::{criterion_group, criterion_main, Criterion};
use segment::data_types::vectors::{only_default_vector, DEFAULT_VECTOR_NAME};
use segment::entry::entry_point::SegmentEntry;
use segment::json_path::JsonPath;
use segment::segment_constructor::build_segment;
use segment::types::{
    Distance, Indexes, Payload, PayloadFieldSchema, PayloadSchemaType, SegmentConfig,
    VectorDataConfig, VectorStorageType,
};
use serde_json::{Map, Value};
use tempfile::Builder;

pub fn criterion_benchmark(c: &mut Criterion) {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let dim = 400;
    let config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: dim,
                distance: Distance::Dot,
                storage_type: VectorStorageType::Memory,
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config: None,
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };
    let mut segment = build_segment(dir.path(), &config, true).unwrap();

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
            )
            .unwrap();
    }
    let payload = Payload::from(payload);

    for id in 0..100000u64 {
        segment
            .upsert_point(100, id.into(), only_default_vector(&vector))
            .unwrap();
        segment
            .set_payload(100, id.into(), &payload, &None)
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
