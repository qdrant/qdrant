use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::FeatureFlags;
use itertools::Itertools as _;
use rand::rngs::StdRng;
use rand::seq::SliceRandom as _;
use rand::{Rng, SeedableRng as _};
use segment::data_types::vectors::{
    DEFAULT_VECTOR_NAME, QueryVector, VectorElementType, only_default_vector,
};
use segment::entry::SegmentEntry as _;
use segment::fixtures::index_fixtures::random_vector;
use segment::index::VectorIndexEnum;
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::index::hnsw_index::num_rayon_threads;
use segment::segment::Segment;
use segment::segment_constructor::VectorIndexBuildArgs;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{Distance, ExtendedPointId, HnswConfig, HnswGlobalConfig, SeqNumberType};
use tap::Tap as _;
use tempfile::Builder;

use crate::hnsw_quantized_search_test::check_matches;

const NUM_POINTS: usize = 5_000;
const ITERATIONS: usize = 10;

const DIM: usize = 8;
const M: usize = 16;
const EF_CONSTRUCT: usize = 64;
const DISTANCE: Distance = Distance::Cosine;

#[test]
fn hnsw_incremental_build() {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        .try_init();

    let mut rng = StdRng::seed_from_u64(42);

    let dir = Builder::new()
        .prefix("hnsw_incremental_build")
        .tempdir()
        .unwrap();

    let ids = std::iter::repeat_with(|| ExtendedPointId::NumId(rng.random()))
        .unique()
        .take(NUM_POINTS)
        .collect_vec();
    let vectors = std::iter::repeat_with(|| random_vector(&mut rng, DIM))
        .take(NUM_POINTS)
        .collect_vec();

    let vector_refs = vectors.iter().map(|v| v.as_slice()).collect_vec();

    let num_queries = 10;
    let query_vectors: Vec<QueryVector> = (0..num_queries)
        .map(|_| random_vector(&mut rng, DIM).into())
        .collect();

    let mut last_index = None;
    for i in 1..=ITERATIONS {
        log::info!(
            "Building segment with {:.0}% of data",
            i as f64 / ITERATIONS as f64 * 100.0
        );

        let num_points = i * NUM_POINTS / ITERATIONS;
        let segment = make_segment(
            &mut rng,
            &dir.path().join(format!("segment_{i}")),
            &ids[0..num_points],
            &vector_refs[0..num_points],
        );

        let index_path = dir.path().join(format!("hnsw_{i}"));
        let old_indices = last_index
            .as_ref()
            .map_or(vec![], |idx| vec![Arc::clone(idx)]);

        let index = build_hnsw_index(&mut rng, &index_path, &segment, &old_indices);

        let ef = 64;
        let top = 10;
        check_matches(&query_vectors, &segment, &index, None, ef, top);

        last_index = Some(Arc::new(AtomicRefCell::new(VectorIndexEnum::Hnsw(index))));
    }
}

fn make_segment(
    rng: &mut StdRng,
    path: &Path,
    ids: &[ExtendedPointId],
    vectors: &[&[VectorElementType]],
) -> Segment {
    let mut sequence = (0..ids.len()).collect_vec();
    sequence.shuffle(rng);

    let hw_counter = HardwareCounterCell::new();

    let mut segment = build_simple_segment(path, DIM, DISTANCE).unwrap();
    for n in sequence {
        let vector = only_default_vector(vectors[n]);
        segment
            .upsert_point(n as SeqNumberType, ids[n], vector, &hw_counter)
            .unwrap();
    }

    segment
}

fn build_hnsw_index<R: Rng + ?Sized>(
    rng: &mut R,
    path: &Path,
    segment: &Segment,
    old_indices: &[Arc<AtomicRefCell<VectorIndexEnum>>],
) -> HNSWIndex {
    log::info!("Building HNSW index for {:?}", path.file_name().unwrap());

    let hnsw_config = HnswConfig {
        m: M,
        ef_construct: EF_CONSTRUCT,
        full_scan_threshold: 1,
        max_indexing_threads: 0,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
    };

    let permit_cpu_count = num_rayon_threads(hnsw_config.max_indexing_threads);
    let permit = Arc::new(ResourcePermit::dummy(permit_cpu_count as u32));

    HNSWIndex::build(
        HnswIndexOpenArgs {
            path,
            id_tracker: segment.id_tracker.clone(),
            vector_storage: segment.vector_data[DEFAULT_VECTOR_NAME]
                .vector_storage
                .clone(),
            quantized_vectors: Default::default(),
            payload_index: Arc::clone(&segment.payload_index),
            hnsw_config,
        },
        VectorIndexBuildArgs {
            permit,
            old_indices,
            gpu_device: None,
            rng,
            stopped: &AtomicBool::new(false),
            hnsw_global_config: &HnswGlobalConfig::default(),
            feature_flags: FeatureFlags::default().tap_mut(|flags| {
                flags.incremental_hnsw_building = true;
            }),
        },
    )
    .unwrap()
}
