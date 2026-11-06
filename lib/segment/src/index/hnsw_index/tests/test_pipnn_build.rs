//! Builds through `HNSWIndex::build` with PiPNN selected for the main graph.

// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use common::ambient;
use common::budget::ResourcePermit;
use common::flags::FeatureFlags;
use common::progress_tracker::{ProgressTree, new_progress_tracker};
use common::types::PointOffsetType;
use rand::SeedableRng as _;
use rand::rngs::StdRng;
use rand::seq::SliceRandom as _;
use rstest::rstest;
use tempfile::Builder;

use crate::data_types::vectors::{DEFAULT_VECTOR_NAME, QueryVector, only_default_vector};
use crate::entry::entry_point::{NonAppendableSegmentEntry as _, SegmentEntry};
use crate::fixtures::index_fixtures::random_vector;
use crate::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs, with_pipnn_min_points};
use crate::index::{PayloadIndex as _, VectorIndexEnum, VectorIndexRead as _};
use crate::json_path::JsonPath;
use crate::payload_json;
use crate::segment::Segment;
use crate::segment_constructor::VectorIndexBuildArgs;
use crate::segment_constructor::simple_segment_constructor::build_simple_segment;
use crate::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, HnswGlobalConfig, Match,
    PayloadSchemaType, SearchParams, SeqNumberType, ValueVariants,
};

const DIM: usize = 16;
const POINTS: usize = 10_000;
const M: usize = 16;

/// PiPNN counts at least this many units per point of level 0, HNSW one.
const PIPNN_UNITS_PER_POINT: u64 = 100;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Method {
    Pipnn,
    Hnsw,
}

/// PiPNN's minimum size for a build that selects PiPNN for any main graph.
const ANY_SIZE: usize = 0;
/// PiPNN's minimum size for a build that keeps HNSW.
const NEVER: usize = usize::MAX;

fn make_segment(path: &Path, distance: Distance, seed: u64) -> Segment {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut segment = build_simple_segment(path, DIM, distance).unwrap();
    for n in 0..POINTS as u64 {
        let vector = random_vector(&mut rng, DIM);
        segment
            .upsert_point(n as SeqNumberType, n.into(), only_default_vector(&vector))
            .unwrap();
    }
    segment
}

/// Builds the index of `segment` with PiPNN's minimum size at `min_points`, on `threads`
/// threads, and returns it with the progress of its main graph.
fn build_index(
    segment: &Segment,
    path: &Path,
    min_points: usize,
    threads: u32,
    seed: u64,
) -> (HNSWIndex, ProgressTree) {
    let (index, progress) = build_index_from(segment, path, min_points, threads, seed, &[]);
    let main_graph = find_progress(&progress, "main_graph")
        .expect("main graph progress")
        .clone();
    (index, main_graph)
}

/// As [`build_index`], healing one of `old_indices` if one qualifies, and with the progress
/// of the whole build.
fn build_index_from(
    segment: &Segment,
    path: &Path,
    min_points: usize,
    threads: u32,
    seed: u64,
    old_indices: &[Arc<AtomicRefCell<VectorIndexEnum>>],
) -> (HNSWIndex, ProgressTree) {
    let hnsw_config = HnswConfig {
        memory: None,
        m: M,
        ef_construct: 100,
        // In KB: about 160 vectors of dimension 16, so that searches use the graph
        full_scan_threshold: 10,
        max_indexing_threads: threads as usize,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
    };
    let (progress_view, progress) = new_progress_tracker();
    let mut rng = StdRng::seed_from_u64(seed);
    let index = with_pipnn_min_points(min_points, || {
        HNSWIndex::build(
            HnswIndexOpenArgs {
                path,
                id_tracker: segment.id_tracker.clone(),
                vector_storage: segment.vector_data[DEFAULT_VECTOR_NAME]
                    .vector_storage
                    .clone(),
                quantized_vectors: Default::default(),
                payload_index: segment.payload_index.clone(),
                hnsw_config,
            },
            VectorIndexBuildArgs {
                permit: Arc::new(ResourcePermit::dummy(threads)),
                old_indices,
                gpu_device: None,
                rng: &mut rng,
                stopped: &AtomicBool::new(false),
                hnsw_global_config: &HnswGlobalConfig::default(),
                feature_flags: FeatureFlags {
                    incremental_hnsw_building: true,
                    ..FeatureFlags::default()
                },
                inline_vectors: false,
                progress,
            },
        )
        .unwrap()
    });
    (index, progress_view.snapshot("index"))
}

fn find_progress<'a>(tree: &'a ProgressTree, name: &str) -> Option<&'a ProgressTree> {
    if tree.name == name {
        return Some(tree);
    }
    tree.children
        .iter()
        .find_map(|child| find_progress(child, name))
}

/// Which method built the main graph, from its progress: PiPNN counts work units, HNSW points.
fn method_of(main_graph: &ProgressTree, points: usize) -> Method {
    let total = main_graph.total.expect("main graph total");
    assert_eq!(main_graph.done, Some(total), "main graph ends at its total");
    if total >= PIPNN_UNITS_PER_POINT * points as u64 {
        Method::Pipnn
    } else {
        assert_eq!(total, points as u64);
        Method::Hnsw
    }
}

/// Share of the exact top 10 that a search at ef 64 finds.
fn recall(index: &HNSWIndex, seed: u64) -> f64 {
    filtered_recall(index, None, seed)
}

/// Share of the exact top 10 within `filter` that a search at ef 64 finds.
fn filtered_recall(index: &HNSWIndex, filter: Option<&Filter>, seed: u64) -> f64 {
    let mut rng = StdRng::seed_from_u64(seed);
    let queries = 100;
    let top = 10;
    let mut found = 0;
    for _ in 0..queries {
        let query = QueryVector::Nearest(random_vector(&mut rng, DIM).into());
        let search = |params: SearchParams| {
            index
                .search(&[&query], filter, top, Some(&params), &Default::default())
                .unwrap()
                .remove(0)
                .into_iter()
                .map(|scored| scored.idx)
                .collect::<HashSet<_>>()
        };
        let exact = search(SearchParams {
            exact: true,
            ..Default::default()
        });
        let graph = search(SearchParams {
            hnsw_ef: Some(64),
            ..Default::default()
        });
        found += exact.intersection(&graph).count();
    }
    found as f64 / (queries * top) as f64
}

#[rstest]
fn builds_and_searches(
    #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
) {
    let _scope = ambient::test_guard();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment = make_segment(dir.path(), distance, 42);

    let pipnn_dir = Builder::new().prefix("pipnn_dir").tempdir().unwrap();
    let (pipnn, main_graph) = build_index(&segment, pipnn_dir.path(), ANY_SIZE, 4, 1);
    assert_eq!(method_of(&main_graph, POINTS), Method::Pipnn);
    for point in 0..POINTS as PointOffsetType {
        assert!(
            pipnn.graph().links.links(point, 0).next().is_some(),
            "point {point} has no level 0 links",
        );
    }

    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();
    let (hnsw, main_graph) = build_index(&segment, hnsw_dir.path(), NEVER, 4, 1);
    assert_eq!(method_of(&main_graph, POINTS), Method::Hnsw);

    let pipnn_recall = recall(&pipnn, 7);
    let hnsw_recall = recall(&hnsw, 7);
    assert!(
        pipnn_recall >= hnsw_recall - 0.05,
        "PiPNN recall {pipnn_recall}, HNSW {hnsw_recall}",
    );
}

/// Below its minimum size, PiPNN refuses and HNSW builds the main graph.
#[test]
fn small_segments_stay_on_hnsw() {
    let _scope = ambient::test_guard();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment = make_segment(dir.path(), Distance::Cosine, 42);
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();
    let (_, main_graph) = build_index(&segment, hnsw_dir.path(), POINTS + 1, 4, 1);
    assert_eq!(method_of(&main_graph, POINTS), Method::Hnsw);
}

/// The same seed builds the same graph on any number of threads, entry points included.
#[test]
fn deterministic_across_threads() {
    let _scope = ambient::test_guard();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment = make_segment(dir.path(), Distance::Cosine, 42);

    let graphs: Vec<_> = [1, 4]
        .into_iter()
        .map(|threads| {
            let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();
            let (index, main_graph) = build_index(&segment, hnsw_dir.path(), ANY_SIZE, threads, 3);
            assert_eq!(method_of(&main_graph, POINTS), Method::Pipnn);
            graph_links(&index)
        })
        .collect();
    assert!(
        graphs[0] == graphs[1],
        "the graph depends on the thread count"
    );
}

/// Links of every point on every level, and the main entry point.
fn graph_links(
    index: &HNSWIndex,
) -> (
    Vec<Vec<Vec<PointOffsetType>>>,
    Option<crate::index::hnsw_index::entry_points::EntryPoint>,
) {
    let graph = index.graph();
    let links = (0..graph.links.num_points() as PointOffsetType)
        .map(|point| {
            (0..=graph.links.point_level(point))
                .map(|level| graph.links.links(point, level).collect())
                .collect()
        })
        .collect();
    (links, graph.entry_points.get_entry_point(|_| true))
}

/// Deleted points and points without a vector get no links and aren't linked to.
#[test]
fn deleted_points_unlinked() {
    let _scope = ambient::test_guard();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = make_segment(dir.path(), Distance::Cosine, 42);

    let mut rng = StdRng::seed_from_u64(5);
    let mut ids: Vec<u64> = (0..POINTS as u64).collect();
    ids.shuffle(&mut rng);
    let (deleted, rest) = ids.split_at(POINTS / 10);
    let without_vector = &rest[..POINTS / 20];
    let removed: HashSet<PointOffsetType> = deleted
        .iter()
        .chain(without_vector)
        .map(|&id| segment.get_internal_id(id.into()).unwrap())
        .collect();
    let op = POINTS as SeqNumberType;
    for &id in deleted {
        segment.delete_point(op, id.into()).unwrap();
    }
    for &id in without_vector {
        segment
            .delete_vector(op, id.into(), DEFAULT_VECTOR_NAME)
            .unwrap();
    }

    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();
    let (index, main_graph) = build_index(&segment, hnsw_dir.path(), ANY_SIZE, 4, 1);
    let live = POINTS - removed.len();
    assert_eq!(method_of(&main_graph, live), Method::Pipnn);

    let graph = index.graph();
    for point in 0..graph.links.num_points() as PointOffsetType {
        for level in 0..=graph.links.point_level(point) {
            let links: Vec<_> = graph.links.links(point, level).collect();
            if removed.contains(&point) {
                assert!(links.is_empty(), "removed point {point} has links");
            }
            for link in links {
                assert!(!removed.contains(&link), "{point} links to removed {link}");
            }
        }
    }
}

/// A PiPNN graph is healed like any other: after deleting a fifth of the points, the next
/// build heals it instead of building a new one, and keeps its recall.
#[test]
fn heals_a_pipnn_graph() {
    let _scope = ambient::test_guard();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = make_segment(dir.path(), Distance::Cosine, 42);

    let pipnn_dir = Builder::new().prefix("pipnn_dir").tempdir().unwrap();
    let (pipnn, main_graph) = build_index(&segment, pipnn_dir.path(), ANY_SIZE, 4, 1);
    assert_eq!(method_of(&main_graph, POINTS), Method::Pipnn);
    let pipnn_recall = recall(&pipnn, 7);

    for id in (0..POINTS as u64).step_by(5) {
        segment
            .delete_point(POINTS as SeqNumberType, id.into())
            .unwrap();
    }
    let old_indices = [Arc::new(AtomicRefCell::new(VectorIndexEnum::Hnsw(pipnn)))];
    let healed_dir = Builder::new().prefix("healed_dir").tempdir().unwrap();
    // PiPNN would apply, but healing wins
    let (healed, progress) =
        build_index_from(&segment, healed_dir.path(), ANY_SIZE, 4, 1, &old_indices);
    let migrate = find_progress(&progress, "migrate").expect("migrate progress");
    assert!(
        migrate.started_at.is_some(),
        "the old graph wasn't healed: {progress:?}"
    );

    let healed_recall = recall(&healed, 7);
    assert!(
        healed_recall >= pipnn_recall - 0.05,
        "healed recall {healed_recall}, PiPNN {pipnn_recall}",
    );
}

/// Payload blocks over a PiPNN main graph: blocks of a keyword and an integer field are built
/// with HNSW, their phases end at their totals, and filtered search keeps its recall.
#[test]
fn payload_blocks_over_pipnn() {
    let _scope = ambient::test_guard();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = make_segment(dir.path(), Distance::Cosine, 42);
    // 8 colors of about 1,250 points and 4 sizes of 2,500, all above the full scan threshold
    for n in 0..POINTS as u64 {
        let payload = payload_json! {"color": format!("c{}", n % 8), "size": (n % 4) as i64};
        segment
            .set_full_payload(n as SeqNumberType, n.into(), &payload)
            .unwrap();
    }
    for (field, schema) in [
        ("color", PayloadSchemaType::Keyword),
        ("size", PayloadSchemaType::Integer),
    ] {
        segment
            .payload_index
            .borrow_mut()
            .set_indexed(&JsonPath::new(field), schema)
            .unwrap();
    }
    let filters = [
        Filter::new_must(Condition::Field(FieldCondition::new_match(
            JsonPath::new("color"),
            Match::from("c3".to_string()),
        ))),
        Filter::new_must(Condition::Field(FieldCondition::new_match(
            JsonPath::new("size"),
            Match::from(ValueVariants::Integer(1)),
        ))),
    ];

    let mut filtered_recalls = Vec::new();
    for min_points in [ANY_SIZE, NEVER] {
        let index_dir = Builder::new().prefix("index_dir").tempdir().unwrap();
        let (index, progress) = build_index_from(&segment, index_dir.path(), min_points, 4, 1, &[]);
        let main_graph = find_progress(&progress, "main_graph").unwrap();
        let method = method_of(main_graph, POINTS);
        let additional_links = find_progress(&progress, "additional_links").unwrap();
        assert_eq!(additional_links.children.len(), 2, "{additional_links:?}");
        // Without a total: blocks are only counted once they are built
        for field in &additional_links.children {
            assert!(field.finished_at.is_some(), "{field:?}");
        }
        let linked: Vec<_> = additional_links
            .children
            .iter()
            .map(|field| (field.name.clone(), field.done))
            .collect();
        let recalls: Vec<f64> = filters
            .iter()
            .map(|filter| filtered_recall(&index, Some(filter), 11))
            .collect();
        println!("{method:?}: filtered recall {recalls:?}, points linked per field {linked:?}");
        filtered_recalls.push(recalls);
    }
    for (pipnn, hnsw) in std::iter::zip(&filtered_recalls[0], &filtered_recalls[1]) {
        assert!(
            *pipnn >= hnsw - 0.05,
            "filtered recall: PiPNN {pipnn}, HNSW {hnsw}"
        );
    }
}
