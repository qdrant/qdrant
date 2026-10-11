use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use common::ambient;
use common::budget::ResourcePermit;
use common::flags::FeatureFlags;
use common::progress_tracker::ProgressTracker;
use common::types::PointOffsetType;
use rand::rngs::StdRng;
use rand::seq::SliceRandom as _;
use rand::{Rng, RngExt as _, SeedableRng as _};
use tap::Tap as _;
use tempfile::Builder;

use crate::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
use crate::entry::entry_point::SegmentEntry;
use crate::fixtures::index_fixtures::random_vector;
use crate::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use crate::index::{PayloadIndex as _, VectorIndexEnum};
use crate::json_path::JsonPath;
use crate::payload_json;
use crate::segment::Segment;
use crate::segment_constructor::VectorIndexBuildArgs;
use crate::segment_constructor::simple_segment_constructor::build_simple_segment;
use crate::types::{Distance, HnswConfig, HnswGlobalConfig, PayloadSchemaType, SeqNumberType};

const DIM: usize = 16;
const POINTS: usize = 6_000;
const VALUES: usize = 3;
const HEALS: usize = 6;

/// Heal a graph of payload links only (m = 0) again and again, replacing a quarter of the
/// points each time. A seeded point keeps its old links and replaces those to removed points;
/// the replacements must be linked back, as in a graph built by search, or each heal leaves
/// more one-way links and filtered search gets a little worse every time.
///
/// After six heals, the share of two-way links was 0.82 of the links healed this way, 0.77
/// without the backlinks, 0.75 replacing each removed point with its closest link, and 0.88 in
/// a fresh build.
#[test]
fn test_heal_payload_links_repeatedly() {
    let mut rng = StdRng::seed_from_u64(42);
    let dir = Builder::new()
        .prefix("heal_payload_links")
        .tempdir()
        .unwrap();

    let total = POINTS + HEALS * POINTS / 4;
    let vectors: Vec<Vec<f32>> = std::iter::repeat_with(|| random_vector(&mut rng, DIM))
        .take(total)
        .collect();
    let values: Vec<usize> = (0..total).map(|_| rng.random_range(0..VALUES)).collect();

    let mut rows: Vec<usize> = (0..POINTS).collect();
    let mut next_row = POINTS;
    let segment = make_segment(&dir.path().join("segment_0"), &rows, &vectors, &values);
    let mut index = build_index(&mut rng, &dir.path().join("hnsw_0"), &segment, &[]);
    for heal in 1..=HEALS {
        rows.shuffle(&mut rng);
        rows.truncate(POINTS * 3 / 4);
        rows.extend(next_row..next_row + POINTS / 4);
        next_row += POINTS / 4;
        rows.sort_unstable();

        let segment = make_segment(
            &dir.path().join(format!("segment_{heal}")),
            &rows,
            &vectors,
            &values,
        );
        let old_indices = [Arc::new(AtomicRefCell::new(VectorIndexEnum::Hnsw(index)))];
        index = build_index(
            &mut rng,
            &dir.path().join(format!("hnsw_{heal}")),
            &segment,
            &old_indices,
        );
        if heal == HEALS {
            let fresh = build_index(&mut rng, &dir.path().join("fresh"), &segment, &[]);
            let healed = two_way_share(&index);
            let fresh = two_way_share(&fresh);
            println!("two-way links: healed {healed:.3}, fresh {fresh:.3}");
            assert!(
                healed >= 0.9 * fresh,
                "healed graph has {healed:.3} two-way links, fresh {fresh:.3}",
            );
        }
    }
}

/// Share of the level 0 links whose target links back.
fn two_way_share(index: &HNSWIndex) -> f64 {
    let links = &index.graph().links;
    let mut total = 0usize;
    let mut two_way = 0usize;
    for point in 0..links.num_points() as PointOffsetType {
        for link in links.links(point, 0) {
            total += 1;
            if links.links(link, 0).any(|back| back == point) {
                two_way += 1;
            }
        }
    }
    two_way as f64 / total as f64
}

fn make_segment(path: &Path, rows: &[usize], vectors: &[Vec<f32>], values: &[usize]) -> Segment {
    let _scope = ambient::test_guard();
    let mut segment = build_simple_segment(path, DIM, Distance::Dot).unwrap();
    for &row in rows {
        let op_num = row as SeqNumberType;
        let id = (row as u64).into();
        segment
            .upsert_point(op_num, id, only_default_vector(&vectors[row]))
            .unwrap();
        let payload = payload_json! {"value": format!("v{}", values[row])};
        segment.set_full_payload(op_num, id, &payload).unwrap();
    }
    segment
        .payload_index
        .borrow_mut()
        .set_indexed(&JsonPath::new("value"), PayloadSchemaType::Keyword)
        .unwrap();
    segment
}

fn build_index<R: Rng + ?Sized>(
    rng: &mut R,
    path: &Path,
    segment: &Segment,
    old_indices: &[Arc<AtomicRefCell<VectorIndexEnum>>],
) -> HNSWIndex {
    // Each value has ~2000 points, above this threshold (256 points), so it gets payload links.
    let full_scan_threshold_kb = 256 * DIM * size_of::<f32>() / 1024;
    let hnsw_config = HnswConfig {
        m: 0,
        ef_construct: 64,
        full_scan_threshold: full_scan_threshold_kb,
        max_indexing_threads: 2,
        payload_m: Some(16),
        ..Default::default()
    };
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
            permit: Arc::new(ResourcePermit::dummy(2)),
            old_indices,
            gpu_device: None,
            rng,
            stopped: &AtomicBool::new(false),
            hnsw_global_config: &HnswGlobalConfig::default(),
            feature_flags: FeatureFlags::default().tap_mut(|flags| {
                flags.incremental_hnsw_building = true;
            }),
            inline_vectors: false,
            progress: ProgressTracker::new_for_test(),
        },
    )
    .unwrap()
}
