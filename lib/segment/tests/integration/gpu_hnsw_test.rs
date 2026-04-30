use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::FeatureFlags;
use common::progress_tracker::ProgressTracker;
use common::types::ScoredPointOffset;
use ordered_float::OrderedFloat;
use parking_lot::Mutex;
use rand::prelude::StdRng;
use rand::{RngExt, SeedableRng};
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, QueryVector, only_default_vector};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::{random_int_payload, random_vector};
use segment::index::hnsw_index::get_num_indexing_threads;
use segment::index::hnsw_index::gpu::gpu_devices_manager::LockedGpuDevice;
use segment::index::hnsw_index::gpu::set_gpu_groups_count;
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::index::{PayloadIndex, VectorIndexRead};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::VectorIndexBuildArgs;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, HnswGlobalConfig, PayloadSchemaType,
    Range, SearchParams, SeqNumberType,
};
use tempfile::Builder;

fn build_hnsw(
    path: &Path,
    segment: &Segment,
    hnsw_config: HnswConfig,
    gpu_device: Option<&LockedGpuDevice>,
    stopped: &AtomicBool,
) -> HNSWIndex {
    let permit_cpu_count = get_num_indexing_threads(hnsw_config.max_indexing_threads);
    let permit = Arc::new(ResourcePermit::dummy(permit_cpu_count as u32));
    let mut build_rng = StdRng::seed_from_u64(100);

    let vector_storage = &segment.vector_data[DEFAULT_VECTOR_NAME].vector_storage;
    let quantized_vectors = &segment.vector_data[DEFAULT_VECTOR_NAME].quantized_vectors;

    HNSWIndex::build(
        HnswIndexOpenArgs {
            path,
            id_tracker: segment.id_tracker.clone(),
            vector_storage: vector_storage.clone(),
            quantized_vectors: quantized_vectors.clone(),
            payload_index: segment.payload_index.clone(),
            hnsw_config,
        },
        VectorIndexBuildArgs {
            permit,
            old_indices: &[],
            gpu_device,
            rng: &mut build_rng,
            stopped,
            hnsw_global_config: &HnswGlobalConfig::default(),
            feature_flags: FeatureFlags::default(),
            progress: ProgressTracker::new_for_test(),
        },
    )
    .unwrap()
}

fn measure_accuracy(
    hnsw_index: &HNSWIndex,
    queries: &[(QueryVector, Filter)],
    ground_truth: &[Vec<Vec<ScoredPointOffset>>],
    ef: usize,
    top: usize,
) -> usize {
    let search_params = SearchParams {
        hnsw_ef: Some(ef),
        ..Default::default()
    };

    queries
        .iter()
        .zip(ground_truth)
        .filter(|((query, filter), truth)| {
            let result = hnsw_index
                .search(
                    &[query],
                    Some(filter),
                    top,
                    Some(&search_params),
                    &Default::default(),
                )
                .unwrap();
            // Compare by point IDs only, ignoring score differences and ordering
            let result_ids: HashSet<_> = result[0].iter().map(|s| s.idx).collect();
            let truth_ids: HashSet<_> = truth[0].iter().map(|s| s.idx).collect();
            result_ids == truth_ids
        })
        .count()
}

fn create_test_segment(
    dir: &Path,
    dim: usize,
    distance: Distance,
    num_vectors: u64,
    int_key: &str,
    num_payload_values: usize,
    with_payload_index: bool,
) -> Segment {
    let mut rng = StdRng::seed_from_u64(42);
    let hw_counter = HardwareCounterCell::new();

    let mut segment = build_simple_segment(dir, dim, distance).unwrap();
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

    if with_payload_index {
        segment
            .payload_index
            .borrow_mut()
            .set_indexed(
                &JsonPath::new(int_key),
                PayloadSchemaType::Integer,
                &hw_counter,
            )
            .unwrap();
    }

    segment
}

#[test]
fn test_gpu_filterable_hnsw() {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        .try_init();

    let stopped = AtomicBool::new(false);
    let dim = 8;
    let m = 8;
    let num_vectors: u64 = 10_000;
    let ef = 32;
    let ef_construct = 16;
    let distance = Distance::Cosine;
    let full_scan_threshold = 32; // KB
    let num_payload_values = 2;
    let int_key = "int";
    let gpu_groups = 64; // limit gpu parallelism for small data

    let hnsw_config = HnswConfig {
        m,
        ef_construct,
        full_scan_threshold,
        max_indexing_threads: 2,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
    };

    // Pre-generate queries
    let top = 3;
    let attempts = 100;
    let mut query_rng = StdRng::seed_from_u64(12345);
    let queries: Vec<(QueryVector, Filter)> = (0..attempts)
        .map(|_| {
            let query: QueryVector = random_vector(&mut query_rng, dim).into();
            let range_size = 40;
            let left_range = query_rng.random_range(0..400);
            let right_range = left_range + range_size;
            let filter = Filter::new_must(Condition::Field(FieldCondition::new_range(
                JsonPath::new(int_key),
                Range {
                    lt: None,
                    gt: None,
                    gte: Some(OrderedFloat::from(f64::from(left_range))),
                    lte: Some(OrderedFloat::from(f64::from(right_range))),
                },
            )));
            (query, filter)
        })
        .collect();

    // GPU device setup (shared across GPU builds)
    let gpu_instance = gpu::GPU_TEST_INSTANCE.clone();
    let gpu_device = Mutex::new(
        gpu::Device::new(gpu_instance.clone(), &gpu_instance.physical_devices()[0]).unwrap(),
    );

    set_gpu_groups_count(Some(gpu_groups));

    // == Build without payload index (fresh segment) ==

    let seg_dir_no_idx = Builder::new().prefix("seg_no_idx").tempdir().unwrap();
    let segment_no_idx = create_test_segment(
        seg_dir_no_idx.path(),
        dim,
        distance,
        num_vectors,
        int_key,
        num_payload_values,
        false,
    );

    // Compute ground truth using exact (plain) search
    let ground_truth: Vec<_> = queries
        .iter()
        .map(|(query, filter)| {
            segment_no_idx.vector_data[DEFAULT_VECTOR_NAME]
                .vector_index
                .borrow()
                .search(&[query], Some(filter), top, None, &Default::default())
                .unwrap()
        })
        .collect();

    let hnsw_dir_cpu_no_idx = Builder::new().prefix("hnsw_cpu_no_idx").tempdir().unwrap();
    let hnsw_cpu_no_idx = build_hnsw(
        hnsw_dir_cpu_no_idx.path(),
        &segment_no_idx,
        hnsw_config,
        None,
        &stopped,
    );

    let hnsw_dir_gpu_no_idx = Builder::new().prefix("hnsw_gpu_no_idx").tempdir().unwrap();
    let hnsw_gpu_no_idx = {
        let locked = LockedGpuDevice::new(gpu_device.lock());
        build_hnsw(
            hnsw_dir_gpu_no_idx.path(),
            &segment_no_idx,
            hnsw_config,
            Some(&locked),
            &stopped,
        )
    };

    // == Build with payload index (fresh segment) ==

    let seg_dir_idx = Builder::new().prefix("seg_idx").tempdir().unwrap();
    let segment_idx = create_test_segment(
        seg_dir_idx.path(),
        dim,
        distance,
        num_vectors,
        int_key,
        num_payload_values,
        true,
    );

    let hnsw_dir_cpu_idx = Builder::new().prefix("hnsw_cpu_idx").tempdir().unwrap();
    let hnsw_cpu_idx = build_hnsw(
        hnsw_dir_cpu_idx.path(),
        &segment_idx,
        hnsw_config,
        None,
        &stopped,
    );

    let hnsw_dir_gpu_idx = Builder::new().prefix("hnsw_gpu_idx").tempdir().unwrap();
    let hnsw_gpu_idx = {
        let locked = LockedGpuDevice::new(gpu_device.lock());
        build_hnsw(
            hnsw_dir_gpu_idx.path(),
            &segment_idx,
            hnsw_config,
            Some(&locked),
            &stopped,
        )
    };

    // == Measure accuracies ==

    let hits_cpu_no_idx = measure_accuracy(&hnsw_cpu_no_idx, &queries, &ground_truth, ef, top);
    let hits_cpu_idx = measure_accuracy(&hnsw_cpu_idx, &queries, &ground_truth, ef, top);
    let hits_gpu_no_idx = measure_accuracy(&hnsw_gpu_no_idx, &queries, &ground_truth, ef, top);
    let hits_gpu_idx = measure_accuracy(&hnsw_gpu_idx, &queries, &ground_truth, ef, top);

    eprintln!("CPU without payload index: {hits_cpu_no_idx}/{attempts}");
    eprintln!("CPU with payload index:    {hits_cpu_idx}/{attempts}");
    eprintln!("GPU without payload index: {hits_gpu_no_idx}/{attempts}");
    eprintln!("GPU with payload index:    {hits_gpu_idx}/{attempts}");

    let min_diff = 5;
    assert!(
        hits_cpu_idx >= hits_cpu_no_idx + min_diff,
        "Payload index should improve accuracy: cpu_idx={hits_cpu_idx} < cpu_no_idx={hits_cpu_no_idx} + {min_diff}"
    );

    assert!(
        hits_gpu_idx >= hits_gpu_no_idx + min_diff,
        "Payload index should improve accuracy: gpu_idx={hits_gpu_idx} < gpu_no_idx={hits_gpu_no_idx} + {min_diff}"
    );

    // GPU accuracy should be roughly the same as CPU
    let max_diff = 5;
    let diff_no_idx = (hits_cpu_no_idx as i64 - hits_gpu_no_idx as i64).unsigned_abs();
    let diff_idx = (hits_cpu_idx as i64 - hits_gpu_idx as i64).unsigned_abs();

    assert!(
        diff_no_idx <= max_diff,
        "GPU accuracy should be close to CPU (no index): cpu={hits_cpu_no_idx}, gpu={hits_gpu_no_idx}, diff={diff_no_idx}"
    );
    assert!(
        diff_idx <= max_diff,
        "GPU accuracy should be close to CPU (with index): cpu={hits_cpu_idx}, gpu={hits_gpu_idx}, diff={diff_idx}"
    );

    // Reset global GPU groups state to avoid leaking to other tests
    set_gpu_groups_count(None);
}
