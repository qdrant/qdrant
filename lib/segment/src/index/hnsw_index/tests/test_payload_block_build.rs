// Config structs keep their deprecated placement fields until 2.0; a struct
// literal has to name them either way.
#![allow(deprecated)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize};

use atomic_refcell::AtomicRefCell;
use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::FeatureFlags;
use common::progress_tracker::ProgressTracker;
use common::types::PointOffsetType;
use parking_lot::Mutex;
use rand::SeedableRng;
use rand::prelude::StdRng;
use rstest::rstest;
use tempfile::Builder;

use crate::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
use crate::entry::entry_point::SegmentEntry;
use crate::fixtures::index_fixtures::random_vector;
use crate::id_tracker::IdTracker;
use crate::index::PayloadIndex;
use crate::index::hnsw_index::get_num_indexing_threads;
use crate::index::hnsw_index::hnsw::{HNSWIndex, HnswBuildDebugOptions, HnswIndexOpenArgs};
use crate::json_path::JsonPath;
use crate::payload_json;
use crate::segment::Segment;
use crate::segment_constructor::VectorIndexBuildArgs;
use crate::segment_constructor::simple_segment_constructor::build_simple_segment;
use crate::types::{
    Distance, HnswConfig, HnswGlobalConfig, PayloadSchemaType, QuantizationConfig, SeqNumberType,
    TurboQuantBitSize, TurboQuantQuantizationConfig, TurboQuantization,
};
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsStorageType,
};

const DIM: usize = 8;
const NUM_VECTORS: u64 = 2_000;
/// Distinct payload values. Every value covers 250 points of the fixture,
/// enough for the block it generates to clear `full_scan_threshold`.
const NUM_VALUES: u64 = 8;
const SEED: u64 = 42;

const KEYWORD_KEY: &str = "tenant";
const KEYWORD_TWIN_KEY: &str = "tenant_twin";
const INT_KEY: &str = "bucket";

#[derive(Clone, Copy, Debug)]
pub(super) enum FixtureField {
    Keyword,
    Integer,
    /// Two keyword fields holding the same values.
    TwinKeyword,
    /// Two keyword fields holding the same values, over vectors that cluster
    /// tightly per value. Each value's block is then already well connected in
    /// the main graph, which is what makes the connectivity shortcut skip it.
    ClusteredTwinKeyword,
    /// One keyword field whose values differ enough in cardinality to fall on
    /// both sides of a lowered large-block threshold.
    SkewedKeyword,
    /// One keyword field, with some points dropped from the ID tracker while
    /// their vectors stay live in vector storage. Those points leave both the
    /// main graph and the payload blocks, so blocks end up smaller than their
    /// payload cardinality.
    KeywordWithStalePoints,
}

/// How a [`FixtureField`] lays out its payload and vectors.
struct FixtureSpec {
    keys: &'static [(&'static str, PayloadSchemaType)],
    /// Points per value. Sums to `NUM_VECTORS`, and every entry has to stay
    /// above the block-generation threshold or it produces no block at all.
    value_sizes: Vec<u64>,
    /// Draw each value's points around a per-value centroid.
    clustered: bool,
    /// Drop every Nth point from the ID tracker after indexing.
    drop_every: Option<u64>,
}

impl FixtureField {
    fn spec(self) -> FixtureSpec {
        const KEYWORD: PayloadSchemaType = PayloadSchemaType::Keyword;
        let uniform = vec![NUM_VECTORS / NUM_VALUES; NUM_VALUES as usize];

        let (keys, value_sizes, clustered, drop_every): (&[_], _, _, _) = match self {
            // Keyword values produce one block each.
            FixtureField::Keyword => (&[(KEYWORD_KEY, KEYWORD)], uniform, false, None),
            // Integer values produce overlapping range blocks, as they do for
            // the numeric fields that dominate this stage in production.
            FixtureField::Integer => (
                &[(INT_KEY, PayloadSchemaType::Integer)],
                uniform,
                false,
                None,
            ),
            FixtureField::TwinKeyword => (
                &[(KEYWORD_KEY, KEYWORD), (KEYWORD_TWIN_KEY, KEYWORD)],
                uniform,
                false,
                None,
            ),
            FixtureField::ClusteredTwinKeyword => (
                &[(KEYWORD_KEY, KEYWORD), (KEYWORD_TWIN_KEY, KEYWORD)],
                uniform,
                true,
                None,
            ),
            // Sums to NUM_VECTORS, straddles a threshold of 256, and stays
            // under the percolation ceiling of `total / avg_links * 4`.
            FixtureField::SkewedKeyword => (
                &[(KEYWORD_KEY, KEYWORD)],
                vec![450, 400, 350, 300, 250, 150, 100],
                false,
                None,
            ),
            FixtureField::KeywordWithStalePoints => {
                (&[(KEYWORD_KEY, KEYWORD)], uniform, false, Some(9))
            }
        };

        debug_assert_eq!(value_sizes.iter().sum::<u64>(), NUM_VECTORS);
        FixtureSpec {
            keys,
            value_sizes,
            clustered,
            drop_every,
        }
    }

    fn is_keyword(self) -> bool {
        !matches!(self, FixtureField::Integer)
    }
}

/// Value assigned to each point, indexed by internal offset.
///
/// Points are upserted in id order into a fresh segment, so external ids and
/// internal offsets coincide and this doubles as the block membership map.
fn value_assignment(spec: &FixtureSpec) -> Vec<u64> {
    let mut values = Vec::with_capacity(NUM_VECTORS as usize);
    for (value, &size) in spec.value_sizes.iter().enumerate() {
        values.extend(std::iter::repeat_n(value as u64, size as usize));
    }
    values
}

/// The blocks a keyword fixture generates, as internal point offsets.
fn value_blocks(field: FixtureField) -> Vec<Vec<PointOffsetType>> {
    assert!(field.is_keyword(), "range blocks are not one per value");
    let spec = field.spec();
    let values = value_assignment(&spec);

    (0..spec.value_sizes.len() as u64)
        .map(|value| {
            values
                .iter()
                .enumerate()
                .filter(|&(_, &v)| v == value)
                .map(|(point, _)| point as PointOffsetType)
                .collect()
        })
        .collect()
}

/// Every link container of the final graph, per point and per level, plus the
/// entry point list. Compared verbatim, so any reordering shows up.
type GraphSnapshot = (Vec<Vec<Vec<PointOffsetType>>>, String);

/// A fixture segment with one or two indexed payload fields.
///
/// Only the single-field cases are comparable across builds. `indexed_fields`
/// hands back a `HashMap`, so which field is indexed first - and therefore the
/// order in which their links are merged - varies from build to build. Giving
/// two fields identical values does not rescue it either: each field's index
/// carries its own value map, so the two fields generate their blocks in
/// different orders and are not interchangeable.
///
/// The two-field cases are therefore only used by tests that look at a single
/// build, where they buy coverage of a multi-field segment and of the
/// connectivity shortcut, which only runs from the second field on.
fn build_fixture_segment(dir: &Path, field: FixtureField) -> Segment {
    let mut rng = StdRng::seed_from_u64(SEED);
    let hw_counter = HardwareCounterCell::new();

    let spec = field.spec();
    let values = value_assignment(&spec);

    // Tight enough that a value's points are each other's nearest neighbours,
    // so the main graph links them to one another and barely to anything else.
    const CLUSTER_NOISE: f32 = 0.02;
    let centroids: Vec<Vec<f32>> = (0..spec.value_sizes.len())
        .map(|_| random_vector(&mut rng, DIM))
        .collect();

    let mut segment = build_simple_segment(dir, DIM, Distance::Cosine).unwrap();
    for n in 0..NUM_VECTORS {
        let value = values[n as usize];
        let vector = if spec.clustered {
            let noise = random_vector(&mut rng, DIM);
            centroids[value as usize]
                .iter()
                .zip(&noise)
                .map(|(centroid, noise)| centroid + CLUSTER_NOISE * noise)
                .collect()
        } else {
            random_vector(&mut rng, DIM)
        };

        let payload = if field.is_keyword() {
            let mut payload = payload_json! {};
            for &(key, _) in spec.keys {
                payload
                    .0
                    .insert(key.to_owned(), format!("tenant-{value}").into());
            }
            payload
        } else {
            payload_json! {INT_KEY: value as i64}
        };

        segment
            .upsert_point(
                n as SeqNumberType,
                n.into(),
                only_default_vector(&vector),
                &hw_counter,
            )
            .unwrap();
        segment
            .set_full_payload(n as SeqNumberType, n.into(), &payload, &hw_counter)
            .unwrap();
    }

    for &(key, schema) in spec.keys {
        segment
            .payload_index
            .borrow_mut()
            .set_indexed(&JsonPath::new(key), schema, &hw_counter)
            .unwrap();
    }

    if let Some(drop_every) = spec.drop_every {
        // Drops the ID tracker mapping and nothing else: the vector stays live
        // in vector storage and the payload index keeps the point, so the point
        // still turns up as a block member while counting as deleted. That is
        // exactly the state `block_deleted_flags` exists to describe.
        //
        // `Segment::delete_point` would not do: it also clears the payload row,
        // which would take the point out of its block entirely.
        let mut id_tracker = segment.id_tracker.borrow_mut();
        for n in (0..NUM_VECTORS).step_by(drop_every as usize) {
            id_tracker.drop(n.into()).unwrap();
        }
    }

    segment
}

fn hnsw_config(threads: usize) -> HnswConfig {
    HnswConfig {
        m: 8,
        ef_construct: 16,
        // In KB. Vectors are 8 `f32`s, so this lands far below the per-value
        // cardinality of the fixture and every value produces a block.
        full_scan_threshold: 1,
        max_indexing_threads: threads,
        on_disk: Some(false),
        memory: None,
        payload_m: None,
        inline_storage: None,
    }
}

/// What the payload-block stage did during one build.
#[derive(Debug, Default)]
struct BlockStats {
    built: usize,
    skipped_by_connectivity: usize,
    large: usize,
    /// Blocks whose vectors were copied into a block-local scoring buffer.
    gathered: usize,
    /// Blocks handed over by the unified cross-field queue. Zero on the
    /// legacy path, which is what makes it a witness that the queue ran.
    via_queue: usize,
    /// Block index each block was filtered under, in call order.
    indices: Vec<usize>,
}

/// Non-default knobs for one build. `None` leaves the production default.
#[derive(Clone, Copy, Default)]
struct BuildOverrides {
    force_legacy_payload_blocks: bool,
    large_block_threshold: Option<usize>,
    /// Build over TurboQuant 4-bit quantized vectors instead of the raw dense
    /// storage - the production configuration of the block gather.
    quantize: bool,
}

/// Build an HNSW index over `segment` and snapshot the graph before it is
/// serialized.
fn build_and_snapshot(
    segment: &Segment,
    force_legacy_payload_blocks: bool,
    threads: usize,
) -> (GraphSnapshot, BlockStats) {
    build_and_snapshot_opts(
        segment,
        threads,
        BuildOverrides {
            force_legacy_payload_blocks,
            ..Default::default()
        },
    )
}

fn build_and_snapshot_with(
    segment: &Segment,
    force_legacy_payload_blocks: bool,
    threads: usize,
    large_block_threshold: Option<usize>,
) -> (GraphSnapshot, BlockStats) {
    build_and_snapshot_opts(
        segment,
        threads,
        BuildOverrides {
            force_legacy_payload_blocks,
            large_block_threshold,
            ..Default::default()
        },
    )
}

fn build_and_snapshot_opts(
    segment: &Segment,
    threads: usize,
    overrides: BuildOverrides,
) -> (GraphSnapshot, BlockStats) {
    let BuildOverrides {
        force_legacy_payload_blocks,
        large_block_threshold,
        quantize,
    } = overrides;

    let stopped = AtomicBool::new(false);
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(SEED);

    let quantized_vectors = if quantize {
        let config = QuantizationConfig::Turbo(TurboQuantization {
            turbo: TurboQuantQuantizationConfig {
                always_ram: Some(true),
                memory: None,
                bits: Some(TurboQuantBitSize::Bits4),
            },
        });
        let storage = segment.vector_data[DEFAULT_VECTOR_NAME]
            .vector_storage
            .borrow();
        let quantized = QuantizedVectors::create(
            &storage,
            &config,
            QuantizedVectorsStorageType::Immutable,
            hnsw_dir.path(),
            1,
            &stopped,
        )
        .unwrap();
        Arc::new(AtomicRefCell::new(Some(quantized)))
    } else {
        Default::default()
    };

    let snapshot = Mutex::new(None);
    let blocks_built = AtomicUsize::new(0);
    let blocks_skipped = AtomicUsize::new(0);
    let large_blocks = AtomicUsize::new(0);
    let blocks_gathered = AtomicUsize::new(0);
    let blocks_via_queue = AtomicUsize::new(0);
    let block_indices = Mutex::new(Vec::new());

    let hnsw_config = hnsw_config(threads);
    let permit_cpu_count = get_num_indexing_threads(hnsw_config.max_indexing_threads);
    let permit = Arc::new(ResourcePermit::dummy(permit_cpu_count as u32));

    HNSWIndex::build_with_debug_options(
        HnswIndexOpenArgs {
            path: hnsw_dir.path(),
            id_tracker: segment.id_tracker.clone(),
            vector_storage: segment.vector_data[DEFAULT_VECTOR_NAME]
                .vector_storage
                .clone(),
            quantized_vectors,
            payload_index: segment.payload_index.clone(),
            hnsw_config,
        },
        VectorIndexBuildArgs {
            permit,
            old_indices: &[],
            gpu_device: None,
            rng: &mut rng,
            stopped: &stopped,
            hnsw_global_config: &HnswGlobalConfig::default(),
            feature_flags: FeatureFlags::default(),
            progress: ProgressTracker::new_for_test(),
        },
        HnswBuildDebugOptions {
            force_legacy_payload_blocks,
            blocks_built: Some(&blocks_built),
            blocks_skipped_by_connectivity: Some(&blocks_skipped),
            large_blocks_built: Some(&large_blocks),
            blocks_gathered: Some(&blocks_gathered),
            blocks_via_queue: Some(&blocks_via_queue),
            block_indices: Some(&block_indices),
            large_block_threshold,
            inspect_builder: Some(&|builder| {
                *snapshot.lock() = Some((
                    builder.links_snapshot(),
                    format!("{:?}", *builder.get_entry_points()),
                ));
            }),
        },
    )
    .unwrap();

    let stats = BlockStats {
        built: blocks_built.into_inner(),
        skipped_by_connectivity: blocks_skipped.into_inner(),
        large: large_blocks.into_inner(),
        gathered: blocks_gathered.into_inner(),
        via_queue: blocks_via_queue.into_inner(),
        indices: block_indices.into_inner(),
    };
    (
        snapshot.into_inner().expect("builder was never inspected"),
        stats,
    )
}

/// The compact per-block builder must produce exactly the graph the legacy
/// segment-sized builder produces, link for link and in the same order.
///
/// The comparison covers everything the compact path does differently: blocks
/// are drained through the unified cross-field queue - serially in the order
/// the legacy path visits them - and each block is scored against a copy of
/// its own vectors. The copy holds the same encoded bytes and is scored
/// through the same `score_bytes` entry point, so the scores are bitwise equal
/// and the links must come out link for link and in the same order. These
/// cases run without quantized vectors, so what they pin down is the
/// unquantized dense path; the quantized path production uses is pinned the
/// same way by [`test_compact_payload_blocks_match_legacy_quantized`].
///
/// Single-field fixtures only: see [`build_fixture_segment`] for why anything
/// with two indexed fields cannot be compared across builds.
/// The `stale_points` case covers a segment carrying ID-tracker-deleted points
/// whose vectors are still live. It does *not* reach `block_deleted_flags`:
/// `iter_filtered_points` drops those points before they can become block
/// members, so the flags come out all-false either way. See that function for
/// why they are still derived.
#[rstest]
#[case::keyword(FixtureField::Keyword)]
#[case::integer(FixtureField::Integer)]
#[case::stale_points(FixtureField::KeywordWithStalePoints)]
fn test_compact_payload_blocks_match_legacy(#[case] field: FixtureField) {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment = build_fixture_segment(dir.path(), field);

    let (legacy, legacy_stats) = build_and_snapshot(&segment, true, 1);
    let (compact, compact_stats) = build_and_snapshot(&segment, false, 1);

    assert!(
        legacy_stats.built >= 5,
        "fixture is vacuous, only {} payload blocks were built",
        legacy_stats.built,
    );
    assert_eq!(legacy_stats.built, compact_stats.built);

    // Without these the comparison below would pass just as well against a
    // compact build that quietly skipped the queue or the vector copy, and
    // would then prove nothing about either.
    assert_eq!(legacy_stats.gathered, 0);
    assert_eq!(legacy_stats.via_queue, 0);
    assert_eq!(
        compact_stats.gathered, compact_stats.built,
        "gather did not engage: {compact_stats:?}",
    );
    assert_eq!(
        compact_stats.via_queue,
        compact_stats.indices.len(),
        "the compact build did not take every block off the queue: {compact_stats:?}",
    );

    // Both paths must number a field's blocks the same way, or they seed the
    // connectivity shortcut differently and can skip different blocks. Single
    // field here, so the numbering is simply generation order.
    let expected_indices: Vec<usize> = (0..legacy_stats.indices.len()).collect();
    assert_eq!(legacy_stats.indices, expected_indices);
    assert_eq!(compact_stats.indices, expected_indices);

    let (legacy_links, legacy_entry_points) = legacy;
    let (compact_links, compact_entry_points) = compact;

    assert_eq!(legacy_links.len(), compact_links.len());
    for (point_id, (legacy, compact)) in legacy_links.iter().zip(&compact_links).enumerate() {
        assert_eq!(
            legacy, compact,
            "links of point {point_id} differ between the legacy and compact block builders",
        );
    }
    assert_eq!(legacy_entry_points, compact_entry_points);
}

/// [`test_compact_payload_blocks_match_legacy`], on the quantized path
/// production actually uses (TurboQuant 4-bit). Here the block-local copy also
/// serves the link-selection heuristic's stored-vs-stored scoring
/// (`score_internal_bytes`): same bytes, same kernels, bitwise-equal scores,
/// so the graphs must still be byte-identical.
///
/// TQ is the only encoder whose symmetric kernel the copy can serve, so this
/// is the one build-level fixture where `score_internal_bytes` changes which
/// code runs rather than falling back - engagement of the bytes kernel itself
/// is pinned by the unit tests next to `BlockVectors`.
#[rstest]
#[case::keyword(FixtureField::Keyword)]
#[case::integer(FixtureField::Integer)]
fn test_compact_payload_blocks_match_legacy_quantized(#[case] field: FixtureField) {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment = build_fixture_segment(dir.path(), field);

    let arm = |force_legacy_payload_blocks| BuildOverrides {
        force_legacy_payload_blocks,
        quantize: true,
        ..Default::default()
    };

    let ((legacy_links, legacy_entries), legacy_stats) =
        build_and_snapshot_opts(&segment, 1, arm(true));
    let ((compact_links, compact_entries), compact_stats) =
        build_and_snapshot_opts(&segment, 1, arm(false));

    assert!(
        legacy_stats.built >= 5,
        "fixture is vacuous, only {} payload blocks were built",
        legacy_stats.built,
    );
    assert_eq!(legacy_stats.built, compact_stats.built);
    assert_eq!(legacy_stats.gathered, 0);
    // Without this the comparison below would pass on two identical
    // un-gathered builds and prove nothing.
    assert_eq!(
        compact_stats.gathered, compact_stats.built,
        "gather did not engage: {compact_stats:?}",
    );

    // And without this, `quantize: true` silently building over raw vectors
    // would prove the wrong path. Quantized scores differ from raw scores, so
    // an identical graph means quantization never engaged.
    let ((raw_links, _), _) = build_and_snapshot(&segment, false, 1);
    assert_ne!(
        raw_links, compact_links,
        "quantized build produced the raw build's graph; quantization did not engage",
    );

    for (point_id, (legacy, compact)) in legacy_links.iter().zip(&compact_links).enumerate() {
        assert_eq!(
            legacy, compact,
            "links of point {point_id} differ between the legacy and compact block builders",
        );
    }
    assert_eq!(legacy_entries, compact_entries);
}

/// With more than one field the unified queue is the point of the change, so the
/// numbering it hands blocks over under has to survive it.
///
/// Serially the queue is still fields in order and blocks in generation order -
/// the order the legacy path visits them - so every block is filtered under the
/// same index either way. That is the property under test, and it holds for both
/// cases. (The single-field equality tests above already compare the graphs
/// themselves; two-field fixtures cannot be, per [`build_fixture_segment`].)
///
/// The connectivity shortcut's *verdicts* are a different matter, and
/// `compare_verdicts` is what says so. `indexed_fields` hands back a `HashMap`,
/// so which twin field is visited first varies between the two builds this
/// compares - see [`build_fixture_segment`]. `check_connectivity` only applies
/// from the second field on, so a flipped order points the shortcut at the other
/// field, and any borderline verdict flips with it. The uniform case has
/// essentially nothing to skip so its counts match regardless; the clustered case
/// exists precisely to make the shortcut fire, so its counts are not comparable
/// across builds and only the numbering is checked. Pinning the field order would
/// mean fighting the `HashMap`, which is not what this test is for -
/// `test_connectivity_shortcut_skips_well_connected_blocks` covers the predicate
/// itself from a single build.
#[rstest]
#[case::twin_keyword(FixtureField::TwinKeyword, true)]
#[case::clustered_twin_keyword(FixtureField::ClusteredTwinKeyword, false)]
fn test_unified_block_queue_census_matches_legacy(
    #[case] field: FixtureField,
    #[case] compare_verdicts: bool,
) {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment = build_fixture_segment(dir.path(), field);

    let (_, legacy_stats) = build_and_snapshot(&segment, true, 1);
    let ((unified_links, _), unified_stats) = build_and_snapshot(&segment, false, 1);

    assert!(
        legacy_stats.built >= 5,
        "fixture is vacuous: {legacy_stats:?}",
    );
    if compare_verdicts {
        assert_eq!(
            legacy_stats.built, unified_stats.built,
            "a different set of blocks was built: {legacy_stats:?} vs {unified_stats:?}",
        );
        assert_eq!(
            legacy_stats.skipped_by_connectivity, unified_stats.skipped_by_connectivity,
            "the connectivity shortcut reached different verdicts: \
             {legacy_stats:?} vs {unified_stats:?}",
        );
    }
    assert_eq!(
        legacy_stats.indices, unified_stats.indices,
        "blocks were numbered differently: {legacy_stats:?} vs {unified_stats:?}",
    );

    // Without this the whole test would pass against a build that quietly fell
    // back to the legacy per-field path.
    assert_eq!(
        legacy_stats.via_queue, 0,
        "the legacy build went through the unified queue: {legacy_stats:?}",
    );
    assert_eq!(
        unified_stats.via_queue,
        unified_stats.indices.len(),
        "the unified build did not take every block off the queue: {unified_stats:?}",
    );

    assert_links_are_sane(&unified_links);
}

/// The compact path must leave the graph usable: no duplicate links, no links
/// to points outside the segment, no self-links.
#[rstest]
#[case::keyword(FixtureField::Keyword)]
#[case::integer(FixtureField::Integer)]
#[case::twin_keyword(FixtureField::TwinKeyword)]
#[case::stale_points(FixtureField::KeywordWithStalePoints)]
fn test_compact_payload_blocks_structural_invariants(#[case] field: FixtureField) {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment = build_fixture_segment(dir.path(), field);

    let ((links, _), stats) = build_and_snapshot(&segment, false, 1);
    assert!(stats.built >= 5);

    assert_links_are_sane(&links);
}

/// The connectivity shortcut has to actually skip blocks somewhere in the
/// suite, otherwise nothing pins down its predicate.
///
/// Clustered vectors are what make that happen: each value's points are one
/// another's nearest neighbours, so the main graph already connects them and a
/// block's own connectivity estimate lands above the whole-graph estimate the
/// threshold is drawn from. The count is totalled across fields because which
/// of the twin fields is visited first is not deterministic.
#[test]
fn test_connectivity_shortcut_skips_well_connected_blocks() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment = build_fixture_segment(dir.path(), FixtureField::ClusteredTwinKeyword);

    let ((links, _), stats) = build_and_snapshot(&segment, false, 1);

    assert!(
        stats.skipped_by_connectivity >= 4,
        "connectivity shortcut skipped only {} blocks, so its predicate is untested: {stats:?}",
        stats.skipped_by_connectivity,
    );
    assert!(
        stats.built >= 5,
        "fixture skipped everything, so nothing was built: {stats:?}",
    );
    assert_links_are_sane(&links);
}

/// Fraction of `points` that fall in the largest connected component of the
/// level-0 graph restricted to `points`.
fn largest_component_fraction(
    links: &[Vec<Vec<PointOffsetType>>],
    points: &[PointOffsetType],
) -> f64 {
    fn find(parent: &mut [usize], mut x: usize) -> usize {
        while parent[x] != x {
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        x
    }

    let local: HashMap<PointOffsetType, usize> = points.iter().copied().zip(0..).collect();
    let mut parent: Vec<usize> = (0..points.len()).collect();

    for (&global, &from) in &local {
        for &link in &links[global as usize][0] {
            if let Some(&to) = local.get(&link) {
                let (from, to) = (find(&mut parent, from), find(&mut parent, to));
                if from != to {
                    parent[from] = to;
                }
            }
        }
    }

    let mut sizes: HashMap<usize, usize> = HashMap::new();
    for point in 0..points.len() {
        *sizes.entry(find(&mut parent, point)).or_default() += 1;
    }
    sizes.values().copied().max().unwrap_or(0) as f64 / points.len() as f64
}

/// Building payload blocks concurrently must leave every block as connected as
/// the sequential build leaves it, and must not corrupt any link container.
///
/// The two builds are not compared link for link: the main graph is itself
/// built in parallel, so a multi-threaded build differs from a single-threaded
/// one before the payload stage even starts. `merge_block` is pinned down
/// exactly by `test_merge_block_concurrent_matches_sequential` instead.
///
/// The fixture's block cardinalities straddle `LARGE_BLOCK_THRESHOLD`, lowered
/// here from its production value, so the run exercises both halves of the
/// partition: large blocks handed the whole pool one at a time, small blocks
/// each running as one task of a parallel batch.
#[rstest]
#[case::uniform(FixtureField::Keyword, None)]
#[case::mixed_block_sizes(FixtureField::SkewedKeyword, Some(256))]
fn test_parallel_payload_blocks_stay_connected(
    #[case] field: FixtureField,
    #[case] large_block_threshold: Option<usize>,
) {
    const ITERATIONS: usize = 10;

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment = build_fixture_segment(dir.path(), field);
    let blocks = value_blocks(field);

    let ((sequential, _), sequential_stats) =
        build_and_snapshot_with(&segment, false, 1, large_block_threshold);
    assert!(
        sequential_stats.built >= 5,
        "fixture is vacuous, only {} payload blocks were built",
        sequential_stats.built,
    );

    let sequential_fractions: Vec<f64> = blocks
        .iter()
        .map(|points| largest_component_fraction(&sequential, points))
        .collect();
    assert!(
        sequential_fractions.iter().all(|&fraction| fraction > 0.9),
        "sequential build left blocks fragmented: {sequential_fractions:?}",
    );

    for iteration in 0..ITERATIONS {
        let ((parallel, _), parallel_stats) =
            build_and_snapshot_with(&segment, false, 4, large_block_threshold);
        assert_eq!(
            parallel_stats.built, sequential_stats.built,
            "iteration {iteration}: a different set of blocks was built",
        );

        if large_block_threshold.is_some() {
            assert!(
                parallel_stats.large > 0 && parallel_stats.large < parallel_stats.built,
                "iteration {iteration}: expected the run to use both sides of the block \
                 partition, got {parallel_stats:?}",
            );
        }

        assert_links_are_sane(&parallel);

        for (value, (points, sequential)) in blocks.iter().zip(&sequential_fractions).enumerate() {
            let fraction = largest_component_fraction(&parallel, points);
            assert!(
                fraction >= sequential - 0.01,
                "iteration {iteration}, block {value}: connectivity {fraction} below the \
                 sequential build's {sequential}",
            );
        }
    }
}

/// Draining every field's blocks through one parallel scope must leave the graph
/// intact and must still filter each block exactly once, under its own field's
/// numbering.
///
/// Unlike the serial case this is not a census-equality check. The connectivity
/// shortcut reads the graph as it stands when a block is filtered, and
/// interleaving the fields changes how much of an earlier field has merged by
/// then - so a block can be skipped by one schedule and built by another. That
/// was already true between blocks of one field on the parallel path; the
/// unified queue extends it across fields. What is pinned down here is what does
/// not move: every block is still filtered once, under the same index.
#[rstest]
#[case::twin_keyword(FixtureField::TwinKeyword)]
#[case::clustered_twin_keyword(FixtureField::ClusteredTwinKeyword)]
fn test_unified_block_queue_parallel_stays_sane(#[case] field: FixtureField) {
    const ITERATIONS: usize = 6;

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment = build_fixture_segment(dir.path(), field);

    let (_, serial_stats) = build_and_snapshot(&segment, false, 1);
    assert!(
        serial_stats.built >= 5,
        "fixture is vacuous: {serial_stats:?}"
    );

    let mut expected_indices = serial_stats.indices.clone();
    expected_indices.sort_unstable();

    for iteration in 0..ITERATIONS {
        let ((links, _), stats) = build_and_snapshot(&segment, false, 4);

        assert_links_are_sane(&links);

        let mut indices = stats.indices.clone();
        indices.sort_unstable();
        assert_eq!(
            indices, expected_indices,
            "iteration {iteration}: blocks were filtered a different number of times, \
             or under different indices: {stats:?}",
        );
        assert!(
            stats.built > 0,
            "iteration {iteration}: nothing was built: {stats:?}",
        );
    }
}

pub(super) fn assert_links_are_sane(links: &[Vec<Vec<PointOffsetType>>]) {
    for (point_id, levels) in links.iter().enumerate() {
        for (level, container) in levels.iter().enumerate() {
            let mut seen = container.clone();
            seen.sort_unstable();
            let before = seen.len();
            seen.dedup();
            assert_eq!(
                before,
                seen.len(),
                "point {point_id} level {level} has duplicate links",
            );
            assert!(
                container.iter().all(|&link| (link as usize) < links.len()),
                "point {point_id} level {level} links outside the segment",
            );
            assert!(
                container.iter().all(|&link| link as usize != point_id),
                "point {point_id} level {level} links to itself",
            );
        }
    }
}
