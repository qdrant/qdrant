use std::collections::{HashMap, HashSet};
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
use rstest::rstest;
use tap::Tap as _;
use tempfile::Builder;

use crate::data_types::index::IntegerIndexParams;
use crate::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
use crate::entry::entry_point::SegmentEntry;
use crate::fixtures::index_fixtures::random_vector;
use crate::id_tracker::IdTrackerRead as _;
use crate::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use crate::index::{PayloadIndex as _, VectorIndexEnum};
use crate::json_path::JsonPath;
use crate::payload_json;
use crate::segment::Segment;
use crate::segment_constructor::VectorIndexBuildArgs;
use crate::segment_constructor::simple_segment_constructor::build_simple_segment;
use crate::types::{
    Distance, ExtendedPointId, HnswConfig, HnswGlobalConfig, PayloadFieldSchema,
    PayloadSchemaParams, PayloadSchemaType, SeqNumberType,
};

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
    let mut index = build_index(&mut rng, 0, &dir.path().join("hnsw_0"), &segment, &[]);
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
            0,
            &dir.path().join(format!("hnsw_{heal}")),
            &segment,
            &old_indices,
        );
        if heal == HEALS {
            let fresh = build_index(&mut rng, 0, &dir.path().join("fresh"), &segment, &[]);
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

const OLD_INTEGER_VALUES: usize = 8;
const POINTS_PER_INTEGER_VALUE: usize = 600;
const NEW_INTEGER_VALUE_POINTS: usize = 350;

/// Heal an integer field indexed with `lookup` after a new value reached the threshold. The old
/// index has no block for that value, but the healed graph must give it one like a fresh build
/// does: filtered search on the value only walks links between its points, and the main graph
/// alone gives each of them about one such link. The points of the old build must still start
/// from their old links.
#[rstest]
fn test_heal_new_integer_value(#[values(0, 16)] m: usize, #[values(false, true)] range: bool) {
    let mut rng = StdRng::seed_from_u64(42);
    let dir = Builder::new()
        .prefix("heal_new_integer_value")
        .tempdir()
        .unwrap();

    let old_points = OLD_INTEGER_VALUES * POINTS_PER_INTEGER_VALUE;
    let total = old_points + NEW_INTEGER_VALUE_POINTS;
    let vectors: Vec<Vec<f32>> = std::iter::repeat_with(|| random_vector(&mut rng, DIM))
        .take(total)
        .collect();
    // The old rows hold values 0..8, the new rows value 8.
    let values: Vec<usize> = (0..total)
        .map(|row| {
            if row < old_points {
                row % OLD_INTEGER_VALUES
            } else {
                OLD_INTEGER_VALUES
            }
        })
        .collect();

    // Insert the old points in another order, so that the old graph differs from a fresh build
    // and seeding shows: the healed graph keeps the old links, a fresh one builds others.
    let mut old_rows: Vec<usize> = (0..old_points).collect();
    old_rows.shuffle(&mut rng);
    let old_segment = make_integer_segment(
        &dir.path().join("old_segment"),
        &old_rows,
        &vectors,
        &values,
        range,
    );
    let old_index = build_index(&mut rng, m, &dir.path().join("old_hnsw"), &old_segment, &[]);
    let old_links = links_by_row(&old_index, &old_segment);
    let old_indices = [Arc::new(AtomicRefCell::new(VectorIndexEnum::Hnsw(
        old_index,
    )))];

    let rows: Vec<usize> = (0..total).collect();
    let segment =
        make_integer_segment(&dir.path().join("segment"), &rows, &vectors, &values, range);
    let healed = build_index(
        &mut rng,
        m,
        &dir.path().join("healed"),
        &segment,
        &old_indices,
    );
    let fresh = build_index(&mut rng, m, &dir.path().join("fresh"), &segment, &[]);
    let healed_links = links_by_row(&healed, &segment);
    let fresh_links = links_by_row(&fresh, &segment);

    let new_rows = old_points..total;
    let healed_degree = same_value_degree(&healed_links, new_rows.clone());
    let fresh_degree = same_value_degree(&fresh_links, new_rows);
    let healed_kept = kept_share(&old_links, &healed_links);
    let fresh_kept = kept_share(&old_links, &fresh_links);
    println!(
        "m={m} range={range}: links among new value points per point: healed \
         {healed_degree:.2}, fresh {fresh_degree:.2}; old links kept: healed {healed_kept:.3}, \
         fresh {fresh_kept:.3}"
    );
    assert!(
        healed_degree >= fresh_degree / 2.0,
        "new value points have {healed_degree:.2} links to each other healed, \
         {fresh_degree:.2} fresh",
    );
    assert!(
        healed_kept >= fresh_kept + 0.1,
        "healed graph kept {healed_kept:.3} of the old links, fresh {fresh_kept:.3}",
    );
}

/// Level 0 links of every point of `segment`, as rows, by row.
fn links_by_row(index: &HNSWIndex, segment: &Segment) -> HashMap<usize, HashSet<usize>> {
    let id_tracker = segment.id_tracker.borrow();
    let row_of = |point: PointOffsetType| match id_tracker.external_id(point) {
        Some(ExtendedPointId::NumId(row)) => row as usize,
        id => panic!("unexpected id {id:?} of point {point}"),
    };
    let links = &index.graph().links;
    (0..id_tracker.total_point_count() as PointOffsetType)
        .map(|point| (row_of(point), links.links(point, 0).map(row_of).collect()))
        .collect()
}

/// Average number of links from each of `rows` to the others.
fn same_value_degree(links: &HashMap<usize, HashSet<usize>>, rows: std::ops::Range<usize>) -> f64 {
    let count = rows.len();
    let total: usize = rows
        .clone()
        .map(|row| {
            links[&row]
                .iter()
                .filter(|link| rows.contains(link))
                .count()
        })
        .sum();
    total as f64 / count as f64
}

/// Share of the links in `old` that `new` has too.
fn kept_share(old: &HashMap<usize, HashSet<usize>>, new: &HashMap<usize, HashSet<usize>>) -> f64 {
    let total: usize = old.values().map(HashSet::len).sum();
    let kept: usize = old
        .iter()
        .map(|(row, links)| links.intersection(&new[row]).count())
        .sum();
    kept as f64 / total as f64
}

fn make_integer_segment(
    path: &Path,
    rows: &[usize],
    vectors: &[Vec<f32>],
    values: &[usize],
    range: bool,
) -> Segment {
    let _scope = ambient::test_guard();
    let mut segment = build_simple_segment(path, DIM, Distance::Dot).unwrap();
    for &row in rows {
        let op_num = row as SeqNumberType;
        let id = (row as u64).into();
        segment
            .upsert_point(op_num, id, only_default_vector(&vectors[row]))
            .unwrap();
        let payload = payload_json! {"value": values[row]};
        segment.set_full_payload(op_num, id, &payload).unwrap();
    }
    let schema =
        PayloadFieldSchema::FieldParams(PayloadSchemaParams::Integer(IntegerIndexParams {
            lookup: Some(true),
            range: Some(range),
            ..Default::default()
        }));
    segment
        .payload_index
        .borrow_mut()
        .set_indexed(&JsonPath::new("value"), schema)
        .unwrap();
    segment
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
    m: usize,
    path: &Path,
    segment: &Segment,
    old_indices: &[Arc<AtomicRefCell<VectorIndexEnum>>],
) -> HNSWIndex {
    // Each value has at least 350 points, above this threshold (256 points), so it gets
    // payload links.
    let full_scan_threshold_kb = 256 * DIM * size_of::<f32>() / 1024;
    let hnsw_config = HnswConfig {
        m,
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
