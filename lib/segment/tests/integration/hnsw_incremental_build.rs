// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use common::ambient;
use common::budget::ResourcePermit;
use common::flags::FeatureFlags;
use common::progress_tracker::ProgressTracker;
use itertools::Itertools as _;
use ordered_float::OrderedFloat;
use rand::rngs::StdRng;
use rand::seq::SliceRandom as _;
use rand::{Rng, RngExt, SeedableRng as _};
use rstest::rstest;
use segment::data_types::vectors::{
    DEFAULT_VECTOR_NAME, QueryVector, VectorElementType, only_default_vector,
};
use segment::entry::SegmentEntry as _;
use segment::fixtures::index_fixtures::random_vector;
use segment::index::hnsw_index::get_num_indexing_threads;
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::index::{PayloadIndex as _, VectorIndexEnum, VectorIndexRead as _};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::VectorIndexBuildArgs;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    Condition, Distance, ExtendedPointId, FieldCondition, Filter, HnswConfig, HnswGlobalConfig,
    PayloadSchemaType, Range, SearchParams, SeqNumberType,
};
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

    let _scope = ambient::test_guard();

    let mut segment = build_simple_segment(path, DIM, DISTANCE).unwrap();
    for n in sequence {
        let vector = only_default_vector(vectors[n]);
        segment
            .upsert_point(n as SeqNumberType, ids[n], vector)
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
        memory: None,
        m: M,
        ef_construct: EF_CONSTRUCT,
        full_scan_threshold: 1,
        max_indexing_threads: 0,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
    };

    let permit_cpu_count = get_num_indexing_threads(hnsw_config.max_indexing_threads);
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
            inline_vectors: false,
            progress: ProgressTracker::new_for_test(),
        },
    )
    .unwrap()
}

const PAYLOAD_KEY: &str = "value";
const PAYLOAD_VALUES: usize = 10;

/// How the payload value is stored and indexed.
#[derive(Clone, Copy, Debug)]
enum PayloadKind {
    /// Keyword `v{n}`, one payload block per value.
    Keyword,
    /// Float in `n..n + 1`, payload blocks are histogram ranges.
    Float,
}
const PAYLOAD_POINTS: usize = 8_000;
const PAYLOAD_DIM: usize = 16;

/// Heal an index with payload links, with and without a main graph. Filtered search on the
/// healed graph must be as good as on a graph built from scratch, both when every payload block
/// point can start from its old links (same points) and when points were removed and added.
#[rstest]
fn hnsw_incremental_build_payload_links(
    #[values(M, 0)] m: usize,
    #[values(PayloadKind::Keyword, PayloadKind::Float)] kind: PayloadKind,
) {
    let mut rng = StdRng::seed_from_u64(42);
    let dir = Builder::new()
        .prefix("hnsw_incremental_build_payload_links")
        .tempdir()
        .unwrap();

    let ids = std::iter::repeat_with(|| ExtendedPointId::NumId(rng.random()))
        .unique()
        .take(PAYLOAD_POINTS)
        .collect_vec();
    let vectors = std::iter::repeat_with(|| random_vector(&mut rng, PAYLOAD_DIM))
        .take(PAYLOAD_POINTS)
        .collect_vec();
    let values = std::iter::repeat_with(|| rng.random::<f64>() * PAYLOAD_VALUES as f64)
        .take(PAYLOAD_POINTS)
        .collect_vec();
    let queries: Vec<QueryVector> =
        std::iter::repeat_with(|| random_vector(&mut rng, PAYLOAD_DIM).into())
            .take(20)
            .collect();
    let rows = |rows: Vec<usize>| {
        let ids = rows.iter().map(|&i| ids[i]).collect_vec();
        let vectors = rows.iter().map(|&i| vectors[i].as_slice()).collect_vec();
        let values = rows.iter().map(|&i| values[i]).collect_vec();
        (rows, ids, vectors, values)
    };

    let old_rows = (0..PAYLOAD_POINTS * 9 / 10).collect_vec();
    let (rows_, ids_, vectors_, values_) = rows(old_rows.clone());
    let old_segment = make_payload_segment(
        kind,
        &dir.path().join("old_segment"),
        &rows_,
        &ids_,
        &vectors_,
        &values_,
    );
    let old_index =
        build_payload_hnsw_index(&mut rng, m, &dir.path().join("old_hnsw"), &old_segment, &[]);
    let old_indices = [Arc::new(AtomicRefCell::new(VectorIndexEnum::Hnsw(
        old_index,
    )))];

    // Every tenth old point removed, the last tenth of the rows added.
    let changed_rows = old_rows
        .iter()
        .copied()
        .filter(|i| i % 10 != 0)
        .chain(PAYLOAD_POINTS * 9 / 10..PAYLOAD_POINTS)
        .collect_vec();

    for (name, rows_) in [("same", old_rows), ("changed", changed_rows)] {
        let (rows_, ids_, vectors_, values_) = rows(rows_);
        let segment = make_payload_segment(
            kind,
            &dir.path().join(format!("{name}_segment")),
            &rows_,
            &ids_,
            &vectors_,
            &values_,
        );
        let healed = build_payload_hnsw_index(
            &mut rng,
            m,
            &dir.path().join(format!("{name}_healed")),
            &segment,
            &old_indices,
        );
        let fresh = build_payload_hnsw_index(
            &mut rng,
            m,
            &dir.path().join(format!("{name}_fresh")),
            &segment,
            &[],
        );
        let healed_recall = filtered_recall(kind, &queries, &segment, &healed);
        let fresh_recall = filtered_recall(kind, &queries, &segment, &fresh);
        println!(
            "m={m} {kind:?} {name}: healed recall {healed_recall:.4}, fresh recall {fresh_recall:.4}"
        );
        assert!(
            healed_recall >= fresh_recall - 0.02,
            "{name}: healed recall {healed_recall:.4} < fresh recall {fresh_recall:.4}",
        );
    }
}

/// Segment of `rows`, each upserted with its row number as the operation number, so a point
/// has the same version in every segment made from the same rows.
fn make_payload_segment(
    kind: PayloadKind,
    path: &Path,
    rows: &[usize],
    ids: &[ExtendedPointId],
    vectors: &[&[VectorElementType]],
    values: &[f64],
) -> Segment {
    let _scope = ambient::test_guard();

    let mut segment = build_simple_segment(path, PAYLOAD_DIM, DISTANCE).unwrap();
    for (n, &row) in rows.iter().enumerate() {
        let op_num = row as SeqNumberType;
        segment
            .upsert_point(op_num, ids[n], only_default_vector(vectors[n]))
            .unwrap();
        let payload = match kind {
            PayloadKind::Keyword => payload_json! {PAYLOAD_KEY: format!("v{}", values[n] as usize)},
            PayloadKind::Float => payload_json! {PAYLOAD_KEY: values[n]},
        };
        segment.set_full_payload(op_num, ids[n], &payload).unwrap();
    }
    segment
        .payload_index
        .borrow_mut()
        .set_indexed(
            &JsonPath::new(PAYLOAD_KEY),
            match kind {
                PayloadKind::Keyword => PayloadSchemaType::Keyword,
                PayloadKind::Float => PayloadSchemaType::Float,
            },
        )
        .unwrap();
    segment
}

fn build_payload_hnsw_index<R: Rng + ?Sized>(
    rng: &mut R,
    m: usize,
    path: &Path,
    segment: &Segment,
    old_indices: &[Arc<AtomicRefCell<VectorIndexEnum>>],
) -> HNSWIndex {
    // A payload value matches ~720-800 points, above this threshold (256 points), so filtered
    // searches use the graph, and its blocks get payload links.
    let full_scan_threshold_kb = 256 * PAYLOAD_DIM * size_of::<VectorElementType>() / 1024;
    let hnsw_config = HnswConfig {
        memory: None,
        m,
        ef_construct: EF_CONSTRUCT,
        full_scan_threshold: full_scan_threshold_kb,
        max_indexing_threads: 2,
        on_disk: Some(false),
        payload_m: Some(M),
        inline_storage: None,
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

/// Recall@10 of filtered searches for every payload value, against exact search.
fn filtered_recall(
    kind: PayloadKind,
    queries: &[QueryVector],
    segment: &Segment,
    index: &HNSWIndex,
) -> f64 {
    let _scope = ambient::test_guard();
    let top = 10;
    let params = SearchParams {
        hnsw_ef: Some(32),
        ..Default::default()
    };
    let mut found = 0;
    let mut total = 0;
    for value in 0..PAYLOAD_VALUES {
        let key = JsonPath::new(PAYLOAD_KEY);
        let condition = match kind {
            PayloadKind::Keyword => FieldCondition::new_match(key, format!("v{value}").into()),
            PayloadKind::Float => FieldCondition::new_range(
                key,
                Range {
                    gte: Some(OrderedFloat(value as f64)),
                    lt: Some(OrderedFloat(value as f64 + 1.0)),
                    gt: None,
                    lte: None,
                },
            ),
        };
        let filter = Filter::new_must(Condition::Field(condition));
        for query in queries {
            let exact = segment.vector_data[DEFAULT_VECTOR_NAME]
                .vector_index
                .borrow()
                .search(&[query], Some(&filter), top, None, &Default::default())
                .unwrap();
            let result = index
                .search(
                    &[query],
                    Some(&filter),
                    top,
                    Some(&params),
                    &Default::default(),
                )
                .unwrap();
            found += result[0]
                .iter()
                .filter(|p| exact[0].iter().any(|e| e.idx == p.idx))
                .count();
            total += exact[0].len();
        }
    }
    found as f64 / total as f64
}
