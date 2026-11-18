//! Exact duplicate vectors in a graph built by PiPNN: every copy has to be reachable from the
//! entry point on level 0, and a search for the vector has to find every copy.

// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

use std::collections::{HashSet, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::budget::ResourcePermit;
use common::flags::FeatureFlags;
use common::progress_tracker::new_progress_tracker;
use common::types::PointOffsetType;
use rand::SeedableRng as _;
use rand::rngs::StdRng;
use rand::seq::SliceRandom as _;
use rstest::rstest;
use tempfile::Builder;

use crate::data_types::vectors::{DEFAULT_VECTOR_NAME, QueryVector, only_default_vector};
use crate::entry::entry_point::SegmentEntry as _;
use crate::fixtures::index_fixtures::random_vector;
use crate::index::VectorIndexRead as _;
use crate::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs, with_pipnn_min_points};
use crate::segment::Segment;
use crate::segment_constructor::VectorIndexBuildArgs;
use crate::segment_constructor::simple_segment_constructor::build_simple_segment;
use crate::types::{Distance, HnswConfig, HnswGlobalConfig, SearchParams, SeqNumberType};

const DIM: usize = 16;
/// Distinct random points besides the groups.
const RANDOM_POINTS: usize = 10_000;
/// Copies per group of identical vectors: up to far more than HNSW's lists hold.
const GROUPS: &[usize] = &[2, 5, 21, 33, 64, 200, 1000];

/// The vector of a group and the internal ids of its copies.
type Group = (Vec<f32>, Vec<PointOffsetType>);

/// A segment of random points and [`GROUPS`], with the copies at random positions among the
/// ids. Returns each group's vector and the internal ids of its copies.
fn make_segment(path: &Path, distance: Distance, seed: u64) -> (Segment, Vec<Group>) {
    let mut rng = StdRng::seed_from_u64(seed);
    let group_vectors: Vec<Vec<f32>> = GROUPS
        .iter()
        .map(|_| random_vector(&mut rng, DIM))
        .collect();
    // `None` for a random point, else the group
    let mut slots: Vec<Option<usize>> = vec![None; RANDOM_POINTS];
    for (group, &size) in GROUPS.iter().enumerate() {
        slots.extend(std::iter::repeat_n(Some(group), size));
    }
    slots.shuffle(&mut rng);

    let mut segment = build_simple_segment(path, DIM, distance).unwrap();
    let mut external_ids = vec![Vec::new(); GROUPS.len()];
    for (n, slot) in slots.into_iter().enumerate() {
        let vector = match slot {
            Some(group) => {
                external_ids[group].push(n as u64);
                group_vectors[group].clone()
            }
            None => random_vector(&mut rng, DIM),
        };
        segment
            .upsert_point(
                n as SeqNumberType,
                (n as u64).into(),
                only_default_vector(&vector),
            )
            .unwrap();
    }
    let groups = std::iter::zip(group_vectors, external_ids)
        .map(|(vector, ids)| {
            let internal = ids
                .into_iter()
                .map(|id| segment.get_internal_id(id.into()).unwrap())
                .collect();
            (vector, internal)
        })
        .collect();
    (segment, groups)
}

/// The index of `segment`, its main graph built by PiPNN.
fn build_pipnn_index(segment: &Segment, path: &Path, seed: u64) -> HNSWIndex {
    let hnsw_config = HnswConfig {
        memory: None,
        m: 16,
        ef_construct: 100,
        full_scan_threshold: 10,
        max_indexing_threads: 4,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
    };
    let vector_data = &segment.vector_data[DEFAULT_VECTOR_NAME];
    let (_progress_view, progress) = new_progress_tracker();
    let mut rng = StdRng::seed_from_u64(seed);
    with_pipnn_min_points(0, || {
        HNSWIndex::build(
            HnswIndexOpenArgs {
                path,
                id_tracker: segment.id_tracker.clone(),
                vector_storage: vector_data.vector_storage.clone(),
                quantized_vectors: vector_data.quantized_vectors.clone(),
                payload_index: segment.payload_index.clone(),
                hnsw_config,
            },
            VectorIndexBuildArgs {
                permit: Arc::new(ResourcePermit::dummy(4)),
                old_indices: &[],
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
    })
}

/// Points reachable on level 0 from the main entry point.
fn reachable(index: &HNSWIndex) -> Vec<bool> {
    let graph = index.graph();
    let mut seen = vec![false; graph.links.num_points()];
    let entry = graph.entry_points.get_entry_point(|_| true).unwrap();
    let mut queue = VecDeque::from([entry.point_id]);
    seen[entry.point_id as usize] = true;
    while let Some(point) = queue.pop_front() {
        for link in graph.links.links(point, 0) {
            if !std::mem::replace(&mut seen[link as usize], true) {
                queue.push_back(link);
            }
        }
    }
    seen
}

/// Copies of a group that a search for its vector finds, with `top` the group size and `ef` at
/// least that.
fn found_by_search(index: &HNSWIndex, vector: &[f32], copies: &[PointOffsetType]) -> usize {
    let top = copies.len();
    let query = QueryVector::Nearest(vector.to_vec().into());
    let params = SearchParams {
        hnsw_ef: Some(top.max(64)),
        ..Default::default()
    };
    let found: HashSet<_> = index
        .search(&[&query], None, top, Some(&params), &Default::default())
        .unwrap()
        .remove(0)
        .into_iter()
        .map(|scored| scored.idx)
        .collect();
    copies.iter().filter(|copy| found.contains(copy)).count()
}

/// PiPNN reaches every point from the entry point, and a search for a group's vector finds every
/// copy, also in groups of hundreds of copies, which HNSW only partly reaches.
#[rstest]
fn pipnn_finds_every_copy(#[values(Distance::Cosine, Distance::Euclid)] distance: Distance) {
    let _scope = ambient::test_guard();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let (segment, groups) = make_segment(dir.path(), distance, 42);
    let index_dir = Builder::new().prefix("index_dir").tempdir().unwrap();
    let index = build_pipnn_index(&segment, index_dir.path(), 1);
    let seen = reachable(&index);
    assert!(seen.iter().all(|&seen| seen), "unreachable points");
    for (vector, copies) in &groups {
        let found = found_by_search(&index, vector, copies);
        assert_eq!(found, copies.len(), "copies found of {}", copies.len());
    }
}
