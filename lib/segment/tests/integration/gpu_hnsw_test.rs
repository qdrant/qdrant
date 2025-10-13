use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::FeatureFlags;
use common::types::TelemetryDetail;
use parking_lot::Mutex;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::{random_int_payload, random_vector};
use segment::index::hnsw_index::gpu::gpu_devices_manager::LockedGpuDevice;
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::index::hnsw_index::num_rayon_threads;
use segment::index::{PayloadIndex, VectorIndex};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment_constructor::VectorIndexBuildArgs;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, HnswGlobalConfig, PayloadSchemaType,
    Range, SearchParams, SeqNumberType,
};
use tempfile::Builder;

/// Captured logs from env_logger. It's used to check that indexing was performed using GPU correctly.
/// We cannot just check `Ok` because it's possible that GPU fails and index will be built on CPU without errors.
pub struct CapturedLogs {
    strings: Arc<Mutex<Vec<String>>>,
}

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Ok(buf_str) = std::str::from_utf8(buf) {
            let mut strings = self.strings.lock();
            strings.push(buf_str.to_string());
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn test_gpu_filterable_hnsw() {
    let captured_logs = Arc::new(Mutex::new(Vec::new()));
    let _env_logger = env_logger::builder()
        .is_test(true)
        .target(env_logger::Target::Pipe(Box::new(CapturedLogs {
            strings: captured_logs.clone(),
        })))
        .filter_level(log::LevelFilter::Trace)
        .try_init();

    let stopped = AtomicBool::new(false);
    let max_failures = 5;
    let dim = 8;
    let m = 8;
    let num_vectors: u64 = 10_000;
    let ef = 32;
    let ef_construct = 16;
    let distance = Distance::Cosine;
    let full_scan_threshold = 32; // KB
    let num_payload_values = 2;

    let mut rng = StdRng::seed_from_u64(42);

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();

    let int_key = "int";

    let hw_counter = HardwareCounterCell::new();

    let mut segment = build_simple_segment(dir.path(), dim, distance).unwrap();
    for n in 0..num_vectors {
        let idx = n.into();
        let vector = random_vector(&mut rng, dim);

        let int_payload = random_int_payload(&mut rng, num_payload_values..=num_payload_values);
        let payload = payload_json! {int_key: int_payload};

        segment
            .upsert_point(
                n as SeqNumberType,
                idx,
                only_default_vector(&vector),
                &hw_counter,
            )
            .unwrap();
        segment
            .set_full_payload(n as SeqNumberType, idx, &payload, &hw_counter)
            .unwrap();
    }

    let payload_index_ptr = segment.payload_index.clone();

    let hnsw_config = HnswConfig {
        m,
        ef_construct,
        full_scan_threshold,
        max_indexing_threads: 2,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
    };

    let vector_storage = &segment.vector_data[DEFAULT_VECTOR_NAME].vector_storage;
    let quantized_vectors = &segment.vector_data[DEFAULT_VECTOR_NAME].quantized_vectors;

    payload_index_ptr
        .borrow_mut()
        .set_indexed(
            &JsonPath::new(int_key),
            PayloadSchemaType::Integer,
            &hw_counter,
        )
        .unwrap();

    let permit_cpu_count = num_rayon_threads(hnsw_config.max_indexing_threads);
    let permit = Arc::new(ResourcePermit::dummy(permit_cpu_count as u32));

    let instance = gpu::GPU_TEST_INSTANCE.clone();
    let device =
        Mutex::new(gpu::Device::new(instance.clone(), &instance.physical_devices()[0]).unwrap());
    let locked_device = LockedGpuDevice::new(device.lock());

    let hnsw_index = HNSWIndex::build(
        HnswIndexOpenArgs {
            path: hnsw_dir.path(),
            id_tracker: segment.id_tracker.clone(),
            vector_storage: vector_storage.clone(),
            quantized_vectors: quantized_vectors.clone(),
            payload_index: payload_index_ptr.clone(),
            hnsw_config,
        },
        VectorIndexBuildArgs {
            permit,
            old_indices: &[],
            gpu_device: Some(&locked_device), // enable GPU
            rng: &mut rng,
            stopped: &stopped,
            hnsw_global_config: &HnswGlobalConfig::default(),
            feature_flags: FeatureFlags::default(),
        },
    )
    .unwrap();

    let top = 3;
    let mut hits = 0;
    let attempts = 100;
    for i in 0..attempts {
        let query = random_vector(&mut rng, dim).into();

        let range_size = 40;
        let left_range = rng.random_range(0..400);
        let right_range = left_range + range_size;

        let filter = Filter::new_must(Condition::Field(FieldCondition::new_range(
            JsonPath::new(int_key),
            Range {
                lt: None,
                gt: None,
                gte: Some(f64::from(left_range)),
                lte: Some(f64::from(right_range)),
            },
        )));

        let filter_query = Some(&filter);

        let index_result = hnsw_index
            .search(
                &[&query],
                filter_query,
                top,
                Some(&SearchParams {
                    hnsw_ef: Some(ef),
                    ..Default::default()
                }),
                &Default::default(),
            )
            .unwrap();

        // check that search was performed using HNSW index
        assert_eq!(
            hnsw_index
                .get_telemetry_data(TelemetryDetail::default())
                .filtered_large_cardinality
                .count,
            i + 1
        );

        let plain_result = segment.vector_data[DEFAULT_VECTOR_NAME]
            .vector_index
            .borrow()
            .search(&[&query], filter_query, top, None, &Default::default())
            .unwrap();

        if plain_result == index_result {
            hits += 1;
        }
    }
    assert!(
        attempts - hits <= max_failures,
        "hits: {hits} of {attempts}"
    ); // Not more than X% failures
    eprintln!("hits = {hits:#?} out of {attempts}");

    // Check from logs that GPU was used correctly.
    let logs = captured_logs.lock().clone();
    const UPLOAD_VECTORS_PATTERN: &str = "Upload vector data";
    const UPLOAD_LINKS_PATTERN: &str = "Upload links on level 0";
    // Check that vectors was uploaded to GPU only one time.
    assert_eq!(
        logs.iter()
            .filter(|s| s.contains(UPLOAD_VECTORS_PATTERN))
            .count(),
        1
    );
    // Check that indexing was called more than one time.
    let gpu_indexes_count = logs
        .iter()
        .filter(|s| s.contains(UPLOAD_LINKS_PATTERN))
        .count();
    assert!(gpu_indexes_count > 1);
}
