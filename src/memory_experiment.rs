use std::collections::BTreeMap;
use std::env;
use std::hint::black_box;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use collection::collection_manager::holders::segment_holder::SegmentHolder;
use collection::collection_manager::optimizers::segment_optimizer::{
    OptimizerThresholds, SegmentOptimizer,
};
use collection::collection_manager::optimizers::vacuum_optimizer::VacuumOptimizer;
use collection::config::CollectionParams;
use collection::operations::types::VectorsConfig;
use collection::operations::vector_params_builder::VectorParamsBuilder;
use common::cpu::CpuPermit;
use parking_lot::RwLock;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::index_fixtures::random_vector;
use segment::fixtures::payload_fixtures::generate_diverse_payload;
use segment::segment::Segment;
use segment::segment_constructor::load_segment;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{Distance, HnswConfig};
use tempfile::Builder;
#[cfg(all(
    not(target_env = "msvc"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
use tikv_jemallocator::Jemalloc;
use {collection, segment};

fn random_segment(path: &Path, num_points: usize, dim: usize) -> Segment {
    let distance = Distance::Dot;

    let mut rnd_gen = rand::thread_rng();

    let mut segment = build_simple_segment(path, dim, distance).unwrap();

    for point_id in 0..num_points {
        let vector = random_vector(&mut rnd_gen, dim);
        let payload = generate_diverse_payload(&mut rnd_gen);

        segment
            .upsert_point(
                100,
                (point_id as u64).into(),
                NamedVectors::from_ref(DEFAULT_VECTOR_NAME, vector.as_slice().into()),
            )
            .unwrap();
        segment
            .set_payload(100, (point_id as u64).into(), &payload, &None)
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

fn main() {
    // This is a simple routine, which is dedicated to make sure memory is not leaking during
    // load and offload of the segments.
    let mem = segment::utils::mem::Mem::new();

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let dim = 128;
    let num_vectors = 1000;

    let mut holder = SegmentHolder::default();

    let stopped = AtomicBool::new(false);

    let segment = random_segment(dir.path(), num_vectors, dim);

    holder.add_new(segment);

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Multi(BTreeMap::from([(
            "".into(),
            VectorParamsBuilder::new(dim as _, Distance::Dot).build(),
        )])),
        ..CollectionParams::empty()
    };

    let hnsw_config = HnswConfig {
        m: 16,
        ef_construct: 100,
        full_scan_threshold: 10, // Force to build HNSW links for payload
        ..HnswConfig::default()
    };

    let query_config = None;

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
        query_config,
    );

    let locked_segment_holder = Arc::new(RwLock::new(holder));

    for i in 0..100 {
        let all_segment_ids: Vec<_> = holder.iter().map(|(id, _)| *id).collect();

        let cpu_permit = CpuPermit::dummy(4);

        optimizer.optimize(
            locked_segment_holder.clone(),
            all_segment_ids,
            cpu_permit,
            &stopped,
        ).unwrap();

        println!("Iteration: {}", i);
    }
}
