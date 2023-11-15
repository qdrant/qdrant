#[cfg(not(target_os = "windows"))]
mod prof;

use std::fs::File;
use std::io::BufReader;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::types::PointOffsetType;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::rngs::StdRng;
use rand::SeedableRng;
use segment::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
use segment::fixtures::payload_context_fixture::FixtureIdTracker;
use segment::index::sparse_index::sparse_index_config::SparseIndexConfig;
use segment::index::sparse_index::sparse_vector_index::SparseVectorIndex;
use segment::index::struct_payload_index::StructPayloadIndex;
use segment::index::VectorIndex;
use segment::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use segment::types::Distance;
use segment::vector_storage::simple_sparse_vector_storage::open_simple_sparse_vector_storage;
use segment::vector_storage::{VectorStorage, VectorStorageEnum};
use serde_json::{Deserializer, Value};
use sparse::common::sparse_vector::SparseVector;
use sparse::common::sparse_vector_fixture::random_sparse_vector;
use sparse::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use tempfile::Builder;

// warning: the data properties needs to be described up front as it is not loaded at once in memory for analysis
const SPLADE_DATA_PATH: &str = "../../../sparse-vectors-experiments/data/sparse-vectors.jsonl";
const NUM_VECTORS: usize = 34880;
const MAX_SPARSE_DIM: usize = 30265;
const TOP: usize = 100;

fn sparse_vector_index_splade_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("sparse-vector-splade-group");

    let stopped = AtomicBool::new(false);
    let mut rnd = StdRng::seed_from_u64(42);

    let payload_dir = Builder::new().prefix("payload_dir").tempdir().unwrap();
    let storage_dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let index_dir = Builder::new().prefix("index_dir").tempdir().unwrap();

    // setup
    let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(NUM_VECTORS)));
    let payload_storage = InMemoryPayloadStorage::default();
    let wrapped_payload_storage = Arc::new(AtomicRefCell::new(payload_storage.into()));
    let payload_index = StructPayloadIndex::open(
        wrapped_payload_storage,
        id_tracker.clone(),
        payload_dir.path(),
        true,
    )
    .unwrap();
    let wrapped_payload_index = Arc::new(AtomicRefCell::new(payload_index));

    let db = open_db(storage_dir.path(), &[DB_VECTOR_CF]).unwrap();
    let vector_storage =
        open_simple_sparse_vector_storage(db, DB_VECTOR_CF, Distance::Dot).unwrap();
    let mut borrowed_storage = vector_storage.borrow_mut();

    // check file size
    let f = File::open(SPLADE_DATA_PATH).unwrap();
    let data_len = f.metadata().unwrap().len();
    drop(f);
    println!("Data size: {} mb", data_len / 1024 / 1024);
    println!("{} vectors", NUM_VECTORS);
    println!("{} max dimensions", MAX_SPARSE_DIM);

    println!("Loading data...");

    // load in storage
    let now = std::time::Instant::now();
    load_splade_embeddings(SPLADE_DATA_PATH, &mut borrowed_storage);
    println!("Data loaded in {} ms", now.elapsed().as_millis());
    drop(borrowed_storage);

    // save index config to disk
    let index_config = SparseIndexConfig::new(10_000);

    // build once to reuse in mmap conversion benchmark
    let mut sparse_vector_index: SparseVectorIndex<InvertedIndexRam> = SparseVectorIndex::open(
        index_config,
        id_tracker,
        vector_storage.clone(),
        wrapped_payload_index,
        index_dir.path(),
    )
    .unwrap();

    sparse_vector_index.build_index(&stopped).unwrap();

    let sparse_vector = random_sparse_vector(&mut rnd, MAX_SPARSE_DIM);
    println!(
        "query sparse vector size = {:#?}",
        sparse_vector.values.len()
    );
    println!("TOP = {:#?}", TOP);
    let query_vector = sparse_vector.into();

    // intent: bench `search` without filter
    group.bench_function("inverted-index", |b| {
        b.iter(|| {
            let results = sparse_vector_index
                .search(&[&query_vector], None, TOP, None, &stopped)
                .unwrap();

            assert_eq!(results[0].len(), TOP);
        })
    });

    group.finish();
}

pub fn load_splade_embeddings(path: &str, storage: &mut VectorStorageEnum) {
    let f = File::open(path).unwrap();
    let reader = BufReader::new(f);
    // steam jsonl values
    let stream = Deserializer::from_reader(reader).into_iter::<Value>();

    let mut idx = 0;

    for value in stream {
        let value = value.expect("Unable to parse JSON");
        match value {
            Value::Object(map) => {
                let keys_count = map.len();
                let mut indices = Vec::with_capacity(keys_count);
                let mut values = Vec::with_capacity(keys_count);
                for (key, value) in map {
                    indices.push(key.parse::<u32>().unwrap());
                    values.push(value.as_f64().unwrap() as f32);
                }

                // sort indices and values
                let mut indexed_values: Vec<(u32, f32)> = indices
                    .iter()
                    .zip(values.iter())
                    .map(|(&i, &v)| (i, v))
                    .collect();

                // Sort the vector of tuples by indices
                indexed_values.sort_by_key(|&(i, _)| i);

                // Update the indices and values vectors based on the sorted tuples
                indices = indexed_values.iter().map(|&(i, _)| i).collect();
                values = indexed_values.iter().map(|&(_, v)| v).collect();

                let vec = &SparseVector::new(indices, values).unwrap();
                storage
                    .insert_vector(idx as PointOffsetType, vec.into())
                    .unwrap();
                idx += 1;
            }
            _ => panic!("Unexpected value"),
        }
    }
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = sparse_vector_index_splade_benchmark
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = sparse_vector_index_build_benchmark,
}

criterion_main!(benches);
