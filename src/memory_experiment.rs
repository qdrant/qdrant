use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use parking_lot::RwLock;
#[cfg(all(
    not(target_env = "msvc"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
use tikv_jemallocator::Jemalloc;

use {collection, segment, tempfile};
use collection::collection_manager::holders::segment_holder::SegmentHolder;
use collection::collection_manager::optimizers::segment_optimizer::{
    OptimizerThresholds, SegmentOptimizer,
};
use collection::collection_manager::optimizers::vacuum_optimizer::VacuumOptimizer;
use collection::config::CollectionParams;
use collection::operations::types::VectorsConfig;
use collection::operations::vector_params_builder::VectorParamsBuilder;
use common::cpu::CpuPermit;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::index_fixtures::random_vector;
use segment::segment::Segment;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    Distance, HnswConfig, QuantizationConfig, ScalarQuantization, ScalarQuantizationConfig,
};

const DIM: usize = 128;

fn random_segment(path: &Path, num_points: usize, dim: usize) -> Segment {
    let distance = Distance::Dot;

    let mut rnd_gen = rand::thread_rng();

    let mut segment = build_simple_segment(path, dim, distance).unwrap();

    for point_id in 0..num_points {
        let vector = random_vector(&mut rnd_gen, dim);

        segment
            .upsert_point(
                100,
                (point_id as u64).into(),
                NamedVectors::from_ref(DEFAULT_VECTOR_NAME, vector.as_slice().into()),
            )
            .unwrap();
    }

    segment
}

#[cfg(all(
    not(target_env = "msvc"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn run_optimizer(segments: Vec<Segment>, start_time: std::time::Instant) {
    let mut holder = SegmentHolder::default();

    for segment in segments {
        holder.add_new(segment);
    }

    let locked_segment_holder = Arc::new(RwLock::new(holder));

    let temp_dir = tempfile::Builder::new()
        .prefix("segment_temp_dir")
        .tempdir()
        .unwrap();

    let dir = tempfile::Builder::new()
        .prefix("segment_dir_2")
        .tempdir()
        .unwrap();

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Multi(BTreeMap::from([(
            "".into(),
            VectorParamsBuilder::new(DIM as _, Distance::Dot)
                .with_on_disk(true)
                .build(),
        )])),
        ..CollectionParams::empty()
    };

    let hnsw_config = HnswConfig {
        m: 2,
        ef_construct: 100,
        full_scan_threshold: 10, // Force to build HNSW links for payload
        ..HnswConfig::default()
    };

    let quantization_config = Some(QuantizationConfig::Scalar(ScalarQuantization {
        scalar: ScalarQuantizationConfig {
            r#type: Default::default(),
            quantile: None,
            always_ram: Some(true),
        },
    }));

    let quantization_config = None;

    let optimizer = VacuumOptimizer::new(
        0.1,
        100,
        OptimizerThresholds {
            max_segment_size_kb: 10000000,
            memmap_threshold_kb: 10000000,
            indexing_threshold_kb: 100,
        },
        dir.path().to_owned(),
        temp_dir.path().to_owned(),
        collection_params,
        hnsw_config,
        quantization_config,
    );

    let stopped = AtomicBool::new(false);

    for i in 0..5 {
        let all_segment_ids: Vec<_> = locked_segment_holder.read().segment_ids();

        let cpu_permit = CpuPermit::dummy(4);

        optimizer
            .optimize(
                locked_segment_holder.clone(),
                all_segment_ids,
                cpu_permit,
                &stopped,
            )
            .unwrap();

        println!("[{}] Iteration {}", start_time.elapsed().as_millis(), i);
    }
}

fn main() {
    // This is a simple routine, which is dedicated to make sure memory is not leaking during
    // load and offload of the segments.

    let start_time = std::time::Instant::now();

    println!("[{}] Start", start_time.elapsed().as_millis());

    std::thread::sleep(std::time::Duration::from_millis(500));

    let dir = tempfile::Builder::new()
        .prefix("segment_dir")
        .tempdir()
        .unwrap();

    let num_vectors = 10000;

    let segment = random_segment(dir.path(), num_vectors, DIM);
    let empty_segment = random_segment(dir.path(), 0, DIM);

    println!("[{}] Segment created", start_time.elapsed().as_millis());
    std::thread::sleep(std::time::Duration::from_millis(500));

    run_optimizer(vec![segment, empty_segment], start_time.clone());

    // drop(segment);
    // drop(empty_segment);

    println!("[{}] Segment dropped", start_time.elapsed().as_millis());
    std::thread::sleep(std::time::Duration::from_millis(500));
}
