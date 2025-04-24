use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use ahash::{HashSet, HashSetExt as _};
use atomic_refcell::AtomicRefCell;
use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::FeatureFlags;
use rand::rngs::StdRng;
use rand::seq::{IndexedRandom, SliceRandom as _};
use rand::{Rng as _, SeedableRng as _};
use rstest::rstest;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::{
    DEFAULT_VECTOR_NAME, QueryVector, VectorElementType, only_default_vector,
};
use segment::entry::SegmentEntry as _;
use segment::fixtures::index_fixtures::random_vector;
use segment::index::VectorIndexEnum;
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexBuildStats, HnswIndexOpenArgs};
use segment::index::hnsw_index::num_rayon_threads;
use segment::segment::Segment;
use segment::segment_constructor::VectorIndexBuildArgs;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{Distance, HnswConfig};
use tap::Tap as _;
use tempfile::Builder;

use crate::hnsw_quantized_search_test::check_matches;

const DIM: usize = 8;
const M: usize = 16;
const EF_CONSTRUCT: usize = 64;
const DISTANCE: Distance = Distance::Cosine;

struct Point {
    id: u64,
    vector: Option<Vec<VectorElementType>>,
}

#[rstest]
#[case::default(false, 10)]
#[case::with_hard_deletions(true, 3)]
fn hnsw_incremental_build(#[case] do_deletions: bool, #[case] iterations: usize) {
    use rand::seq::IndexedMutRandom as _;

    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        .try_init();
    let hw_counter = HardwareCounterCell::new();

    let mut rnd = StdRng::seed_from_u64(42);

    let dir = Builder::new()
        .prefix("hnsw_incremental_build")
        .tempdir()
        .unwrap();

    let mut unique_ids = HashSet::<u64>::new();

    let mut points = Vec::<Point>::new();

    let num_queries = 10;
    let query_vectors: Vec<QueryVector> = (0..num_queries)
        .map(|_| random_vector(&mut rnd, DIM).into())
        .collect();

    let mut op_id = 0;
    let mut last_index = None;
    let mut last_indexed_points = 0;
    for i in 0..iterations {
        log::info!("Iteration {i}/{iterations}");

        //
        // Phase 1.
        // Decide what points/vectors a new segment will have.
        //

        if i > 0 {
            if do_deletions {
                for _ in 0..10 {
                    // Hard-delete a point.
                    loop {
                        let idx = rnd.random_range(0..points.len());
                        if points[idx].vector.is_some() {
                            points.swap_remove(idx);
                            break;
                        }
                    }
                }
            }

            for _ in 0..20 {
                // Add a vector to a point.
                loop {
                    let point = points.choose_mut(&mut rnd).unwrap();
                    if point.vector.is_none() {
                        point.vector = Some(random_vector(&mut rnd, DIM));
                        break;
                    }
                }
            }
        }

        // Add points with vectors.
        for _ in 0..500 {
            points.push(Point {
                id: unique_id(&mut rnd, &mut unique_ids),
                vector: Some(random_vector(&mut rnd, DIM)),
            });
        }

        // Add points without vectors.
        for _ in 0..50 {
            points.push(Point {
                id: unique_id(&mut rnd, &mut unique_ids),
                vector: None,
            });
        }

        // Shuffle to stress-test old_to_new/new_to_old mappings.
        points.shuffle(&mut rnd);

        //
        // Phase 2:
        // Build segment and index.
        //

        let segment_path = dir.path().join(format!("segment_{i}"));
        let mut segment = build_simple_segment(&segment_path, DIM, DISTANCE).unwrap();

        let mut indexed_points = 0;
        for point in &points {
            let vectors = match point.vector.as_ref() {
                Some(vector) => {
                    indexed_points += 1;
                    only_default_vector(vector)
                }
                None => NamedVectors::default(),
            };
            segment
                .upsert_point(op_id, point.id.into(), vectors, &hw_counter)
                .unwrap();
            op_id += 1;
        }

        let index_path = dir.path().join(format!("hnsw_{i}"));
        let old_indices = last_index
            .as_ref()
            .map_or(vec![], |idx| vec![Arc::clone(idx)]);

        let (index, stats) = build_hnsw_index(&index_path, &segment, &old_indices);
        if do_deletions {
            // Not supported yet, so should be 0.
            assert_eq!(stats.reused_points, 0);
        } else {
            assert_eq!(stats.reused_points, last_indexed_points);
        }
        last_indexed_points = indexed_points;

        let ef = 32;
        let top = 10;
        check_matches(&query_vectors, &segment, &index, None, ef, top);

        //
        // Phase 3.
        //
        // Soft-delete vectors and points from the indexed segment.
        // They will re-appear in the new segment on the next iteration.
        //

        points.choose_multiple(&mut rnd, 100).for_each(|point| {
            segment
                .delete_vector(op_id, point.id.into(), DEFAULT_VECTOR_NAME)
                .unwrap();
            op_id += 1;
        });

        points.choose_multiple(&mut rnd, 100).for_each(|point| {
            segment
                .delete_point(op_id, point.id.into(), &hw_counter)
                .unwrap();
            op_id += 1;
        });

        last_index = Some(Arc::new(AtomicRefCell::new(VectorIndexEnum::Hnsw(index))));
    }
}

fn unique_id(rnd: &mut StdRng, unique_ids: &mut HashSet<u64>) -> u64 {
    loop {
        let id = rnd.random_range(0..u64::MAX);
        if unique_ids.insert(id) {
            return id;
        }
    }
}

fn build_hnsw_index(
    path: &Path,
    segment: &Segment,
    old_indices: &[Arc<AtomicRefCell<VectorIndexEnum>>],
) -> (HNSWIndex, HnswIndexBuildStats) {
    log::info!("Building HNSW index for {:?}", path.file_name().unwrap());

    let hnsw_config = HnswConfig {
        m: M,
        ef_construct: EF_CONSTRUCT,
        full_scan_threshold: 1,
        max_indexing_threads: 0,
        on_disk: Some(false),
        payload_m: None,
    };

    let permit_cpu_count = num_rayon_threads(hnsw_config.max_indexing_threads);
    let permit = Arc::new(ResourcePermit::dummy(permit_cpu_count as u32));

    HNSWIndex::build_with_stats(
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
            stopped: &AtomicBool::new(false),
            feature_flags: FeatureFlags::default().tap_mut(|flags| {
                flags.incremental_hnsw_building = true;
            }),
        },
    )
    .unwrap()
}
