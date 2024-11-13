use std::fs::File;

use blob_store::fixtures::{empty_storage, Payload, HM_FIELDS};
use criterion::{criterion_group, criterion_main, Criterion};
use serde_json::Value;

pub fn real_data_data_bench(c: &mut Criterion) {
    let (_dir, mut storage) = empty_storage();
    let csv_path = dataset::Dataset::HMArticles
        .download()
        .expect("download should succeed");

    let expected_point_count = 105_542;

    c.bench_function("write real payload", |b| {
        b.iter(|| {
            let csv_file = File::open(csv_path.clone()).expect("file should open");
            let mut rdr = csv::Reader::from_reader(csv_file);
            let mut point_offset = 0;
            for result in rdr.records() {
                let record = result.unwrap();
                let mut payload = Payload::default();
                for (i, &field) in HM_FIELDS.iter().enumerate() {
                    payload.0.insert(
                        field.to_string(),
                        Value::String(record.get(i).unwrap().to_string()),
                    );
                }
                storage.put_value(point_offset, &payload).unwrap();
                point_offset += 1;
            }
            assert_eq!(point_offset, expected_point_count);
        });
    });

    c.bench_function("read real payload", |b| {
        b.iter(|| {
            for i in 0..expected_point_count {
                let res = storage.get_value(i).unwrap();
                assert!(res.0.contains_key("article_id"));
            }
        });
    });
}

criterion_group!(benches, real_data_data_bench);
criterion_main!(benches);
