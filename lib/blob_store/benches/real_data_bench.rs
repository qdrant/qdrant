use std::fs::File;
use std::path::Path;

use blob_store::fixtures::{empty_storage, Payload, HM_FIELDS};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use serde_json::Value;

/// Insert CSV data into the storage
fn insert_csv_data(
    storage: &mut blob_store::BlobStore<Payload>,
    csv_path: &Path,
    expected_point_count: u32,
) {
    let csv_file = File::open(csv_path).expect("file should open");
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
}

/// Recursively compute the size of a directory in megabytes
fn compute_folder_size_mb<P: AsRef<Path>>(path: P) -> u64 {
    let mut size = 0;
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let metadata = entry.metadata().unwrap();

        if metadata.is_dir() {
            size += compute_folder_size_mb(entry.path());
        } else {
            size += metadata.len();
        }
    }
    (size as f32 / 1_000_000.0).ceil() as u64
}

pub fn real_data_data_bench(c: &mut Criterion) {
    let (dir, mut storage) = empty_storage();
    let csv_path = dataset::Dataset::HMArticles
        .download()
        .expect("download should succeed");

    // check source file size
    let file_size_bytes = std::fs::metadata(csv_path.clone())
        .expect("file should exist")
        .len();
    assert_eq!(file_size_bytes, 36_127_865); // 36MB

    // the CSV file has 105_542 rows
    let expected_point_count = 105_542;

    // insert data once
    insert_csv_data(&mut storage, &csv_path, expected_point_count);

    // flush to get a consistent bitmask
    storage.flush().unwrap();

    // sanity check of storage size
    let storage_size = storage.get_storage_size_bytes();
    assert_eq!(storage_size, 54_034_048); // 54MB

    // check storage folder size
    let file_size_mb = compute_folder_size_mb(dir.path());
    assert_eq!(file_size_mb, 70); // 70MB (includes metadata)

    c.bench_function("compute storage size", |b| {
        b.iter(|| black_box(storage.get_storage_size_bytes()));
    });

    c.bench_function("read real payload", |b| {
        b.iter(|| {
            for i in 0..expected_point_count {
                let res = storage.get_value(i).unwrap();
                assert!(res.0.contains_key("article_id"));
            }
        });
    });

    // disclaimer: updating values creates a lot of pages due to copy-on-write
    c.bench_function("upsert real payload", |b| {
        b.iter(|| insert_csv_data(&mut storage, &csv_path, expected_point_count));
    });

    let inflated_storage_size = storage.get_storage_size_bytes();
    assert_eq!(inflated_storage_size, 5_619_540_992); // 5.6GB!

    c.bench_function("compute storage size (large storage)", |b| {
        b.iter(|| black_box(storage.get_storage_size_bytes()));
    });

    c.bench_function("read real payload (large storage)", |b| {
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
