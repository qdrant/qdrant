//! Accuracy coverage for approximate faceting, checked against exact faceting on
//! a real [`LocalShard`] — the lowest level where both run on the same data
//! ([`LocalShard::exact_facet`] dedups points across segments; the approximate
//! path lives in `segment/src/segment/read_view/facet.rs`).
//!
//! The test sweeps a 2×3 matrix to hit every approximate plan, with filter
//! selectivity steering the terminal op (direct counts / filter-iter / facet-iter):
//!
//! | field                         | no filter | selective filter | broad filter |
//! |-------------------------------|-----------|------------------|--------------|
//! | low cardinality (`colour`)    | full      | full             | full         |
//! | high cardinality (`tag`, Zipf)| sampling  | sampling         | sampling     |
//!
//! Each case asserts the same invariant: the top-[`LIMIT`] hits of approximate
//! faceting equal those of exact faceting. The full strategy enumerates
//! exhaustively; the sampling strategy may drop rare values, but the Zipf skew
//! keeps the top-[`LIMIT`] values frequent enough to always be sampled (with
//! exact counts), so truncating to the top hits hides the incompleteness.
//!
//! The fixture also optimizes the insert into an immutable, indexed segment, then
//! deletes and overwrites a slice of points (see [`build_facet_fixture`]), so the
//! read paths must handle soft-deletes in immutable postings and points living in
//! two segments at different versions — the cross-segment case `exact_facet`
//! dedups and `approx_facet` sums visibly.

use std::sync::Arc;
use std::time::Duration;

use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::save_on_disk::SaveOnDisk;
use ordered_float::OrderedFloat;
use rand::SeedableRng;
use rand::distr::Distribution;
use rand::rngs::StdRng;
use rand_distr::Zipf;
use rstest::rstest;
use segment::data_types::facets::{FacetParams, FacetResponse};
use segment::data_types::vectors::VectorStructInternal;
use segment::json_path::JsonPath;
use segment::types::{Condition, FieldCondition, Filter, PayloadSchemaType, Range};
use tempfile::{Builder, TempDir};
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::operations::CollectionUpdateOperations;
use crate::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted,
};
use crate::operations::types::ShardStatus;
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::{ShardOperation, WaitUntil};
use crate::tests::fixtures::create_collection_config;
use crate::tests::payload::create_index;

/// Total points in the fixture.
const N_POINTS: usize = 5_000;
/// `seq <` this matches 5% of points — selective enough for the full strategy to
/// iterate the filter, and to drive the sampling strategy's selective branch.
const SELECTIVE: usize = N_POINTS / 20;
/// `seq <` this matches 90% of points — broad enough to walk the facet index.
const BROAD: usize = N_POINTS * 9 / 10;

/// `tag` vocabulary size. Zipf samples land in `1..=ZIPF_CARDINALITY`; combined
/// with the skew below this realises well over [`MIN_SAMPLE_TARGET`] unique
/// values, so the field engages the sampling strategy.
const ZIPF_CARDINALITY: usize = 2_000;
/// Zipf exponent — a realistic, moderately skewed facet distribution.
const ZIPF_SKEW: f64 = 0.7;
/// Fixed seed: the fixture must be deterministic.
const ZIPF_SEED: u64 = 0xFACE_F00D;

const COLOUR_KEY: &str = "colour";
const TAG_KEY: &str = "tag";
const SEQ_KEY: &str = "seq";

const COLOURS: [&str; 4] = ["red", "blue", "green", "yellow"];

/// New `colour` value introduced only by overwrites — never present at insert
/// time, so the facet index must surface a value it did not originally index.
const OVERWRITE_COLOUR: &str = "magenta";

/// One point in every `MUTATION_STRIDE` is deleted and a different one is
/// overwritten (disjoint residues). The stride is coprime with `COLOURS.len()`,
/// so both mutation sets spread evenly across colours.
const MUTATION_STRIDE: usize = 5;

/// Indexing threshold (KB of vector data) used to force optimization, derived to
/// sit midway between the full insert and the smaller overwrite batch: the
/// initial `N_POINTS` exceed it so they get indexed into an immutable segment,
/// while the later `N_POINTS / MUTATION_STRIDE` overwrites stay below it and
/// remain appendable — so no second optimization pass disturbs the cross-segment
/// state at query time. Tracks the fixture sizes instead of hard-coding a value
/// coupled to them. (Vectors are 4-dim `f32` = 16 bytes.)
const INDEXING_THRESHOLD_KB: usize = (N_POINTS + N_POINTS / MUTATION_STRIDE) * 16 / 2 / 1024;

const LIMIT: usize = 10;
const TIMEOUT: Duration = Duration::from_secs(60);

/// A built shard plus the temp dirs and runtime it borrows. The temp dirs are
/// kept alive for the shard's lifetime.
struct ShardFixture {
    _collection_dir: TempDir,
    _schema_dir: TempDir,
    shard: LocalShard,
    search_runtime: AdaptiveSearchHandle,
}

impl ShardFixture {
    /// Run the approximate facet path (the code under test).
    async fn approx(&self, key: &str, filter: Option<Filter>) -> FacetResponse {
        self.facet(key, filter, false).await
    }

    /// Run the exact facet path (the reference).
    async fn exact(&self, key: &str, filter: Option<Filter>) -> FacetResponse {
        self.facet(key, filter, true).await
    }

    async fn facet(&self, key: &str, filter: Option<Filter>, exact: bool) -> FacetResponse {
        let request = Arc::new(FacetParams {
            key: JsonPath::new(key),
            limit: LIMIT,
            filter,
            exact,
        });
        let runtime = &self.search_runtime;
        let hw = HwMeasurementAcc::new();
        let hits = if exact {
            self.shard.exact_facet(request, runtime, TIMEOUT, hw).await
        } else {
            self.shard.approx_facet(request, runtime, TIMEOUT, hw).await
        }
        .unwrap();
        let counts = hits.into_iter().map(|hit| (hit.value, hit.count)).collect();
        FacetResponse::top_hits(counts, LIMIT)
    }
}

/// `seq < n`: matches exactly the first `n` points (`seq` equals the point index).
fn seq_below(n: usize) -> Filter {
    Filter::new_must(Condition::Field(FieldCondition::new_range(
        JsonPath::new(SEQ_KEY),
        Range {
            lt: Some(OrderedFloat(n as f64)),
            ..Default::default()
        },
    )))
}

/// Build one point. `id` and `seq` both derive from the index `i`; `colour` and
/// `tag` populate the two facet fields.
fn make_point(i: usize, colour: &str, tag: u64) -> PointStructPersisted {
    PointStructPersisted {
        id: (i as u64 + 1).into(),
        vector: VectorStructInternal::from(vec![1.0, 2.0, 3.0, 4.0]).into(),
        payload: Some(
            serde_json::from_value(serde_json::json!({
                COLOUR_KEY: colour,
                TAG_KEY: format!("tag_{tag}"),
                SEQ_KEY: i as f64,
            }))
            .unwrap(),
        ),
    }
}

/// Issue `op` against the shard and wait for it to become visible.
async fn apply(shard: &LocalShard, op: CollectionUpdateOperations) {
    shard
        .update(op.into(), WaitUntil::Visible, None, HwMeasurementAcc::new())
        .await
        .unwrap();
}

/// Poll until the shard reaches a stable optimized state — `Green` with no proxy
/// segment in flight — so the segment layout is deterministic before querying.
async fn wait_for_optimization(shard: &LocalShard) {
    let start = std::time::Instant::now();
    loop {
        let (status, _) = shard.local_shard_status().await;
        let has_proxy = shard
            .segments()
            .read()
            .iter()
            .any(|(_, segment)| !segment.is_original());
        if status == ShardStatus::Green && !has_proxy {
            return;
        }
        assert!(
            start.elapsed() < TIMEOUT,
            "timed out waiting for optimization (status: {status:?}, has_proxy: {has_proxy})",
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Assert the shard holds at least one immutable (non-appendable) segment, i.e.
/// optimization actually produced the indexed segment the test means to cover.
fn assert_has_immutable_segment(shard: &LocalShard) {
    let segments = shard.segments();
    let has_immutable = segments
        .read()
        .iter()
        .any(|(_, segment)| !segment.get().read().is_appendable());
    assert!(
        has_immutable,
        "expected an immutable segment after optimization",
    );
}

/// Build a single-shard fixture of `N_POINTS` points. Each point carries:
/// * `colour` — keyword, `COLOURS.len()` uniques, low cardinality → full strategy.
/// * `tag` — keyword, Zipf-distributed over a large vocabulary, high cardinality
///   → sampling strategy.
/// * `seq` — float equal to the point index, for filters of exact selectivity.
async fn build_facet_fixture() -> ShardFixture {
    let collection_dir = Builder::new().prefix("facet_collection").tempdir().unwrap();
    let schema_dir = Builder::new().prefix("facet_schema").tempdir().unwrap();

    let mut config = create_collection_config();
    // Force the initial insert to be optimized into an immutable, indexed segment
    // so the facet paths run against immutable storage, not only appendable.
    config.optimizer_config.indexing_threshold = Some(INDEXING_THRESHOLD_KB);

    let update_runtime = Handle::current();
    let search_runtime = AdaptiveSearchHandle::current_for_tests();

    let schema_file = schema_dir.path().join("payload-schema.json");
    let payload_index_schema = Arc::new(SaveOnDisk::load_or_init_default(schema_file).unwrap());

    let shard = LocalShard::build(
        0,
        "test".to_string(),
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        Arc::new(Default::default()),
        payload_index_schema.clone(),
        update_runtime,
        search_runtime.clone(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    // Faceting requires a map index (keyword); `seq` is indexed too so range
    // filters resolve against an index. Create the indexes up front, before any
    // data, so the optimizer carries them into the immutable segment it builds.
    for (name, field_type) in [
        (COLOUR_KEY, PayloadSchemaType::Keyword),
        (TAG_KEY, PayloadSchemaType::Keyword),
        (SEQ_KEY, PayloadSchemaType::Float),
    ] {
        create_index(&shard, &payload_index_schema, name, field_type).await;
    }

    // Upsert all points in a single operation, drawing `tag` from a deterministic
    // Zipf distribution so the high-cardinality field has a realistic skew.
    let mut rng = StdRng::seed_from_u64(ZIPF_SEED);
    let zipf = Zipf::new(ZIPF_CARDINALITY as f64, ZIPF_SKEW).unwrap();
    let points: Vec<PointStructPersisted> = (0..N_POINTS)
        .map(|i| {
            let tag = zipf.sample(&mut rng) as u64;
            make_point(i, COLOURS[i % COLOURS.len()], tag)
        })
        .collect();
    apply(
        &shard,
        CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::from(points),
        )),
    )
    .await;

    // Optimize the insert into an immutable, indexed segment and confirm it
    // actually materialized before mutating.
    wait_for_optimization(&shard).await;
    assert_has_immutable_segment(&shard);

    // Mutate the optimized fixture so both facet paths must reconcile removals
    // and payload rewrites against immutable storage, not just a pristine load.
    // The two mutation sets are disjoint residues mod `MUTATION_STRIDE`.
    //
    // Since the points now live in an immutable segment, a delete becomes a
    // soft-delete recorded against it, and an overwrite soft-deletes the old copy
    // there while writing the new one to the appendable segment — so the same
    // point exists in two segments with different versions, the cross-segment
    // case `exact_facet` must dedup and `approx_facet` must sum visibly.

    // Delete one point in every `MUTATION_STRIDE`. Deleted points must vanish
    // from both counts (this exercises `DeferredBehavior::VisibleOnly`).
    apply(
        &shard,
        CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints {
            ids: (0..N_POINTS)
                .filter(|i| i % MUTATION_STRIDE == 0)
                .map(|i| (i as u64 + 1).into())
                .collect(),
        }),
    )
    .await;

    // Overwrite a different point in every `MUTATION_STRIDE` with a fresh
    // payload: the brand-new `OVERWRITE_COLOUR` and a freshly Zipf-sampled `tag`,
    // keeping `seq` so the range filters keep their selectivity. The facet index
    // must drop the stale values and adopt the new ones.
    let overwrites: Vec<PointStructPersisted> = (0..N_POINTS)
        .filter(|i| i % MUTATION_STRIDE == 1)
        .map(|i| {
            let tag = zipf.sample(&mut rng) as u64;
            make_point(i, OVERWRITE_COLOUR, tag)
        })
        .collect();
    apply(
        &shard,
        CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::from(overwrites),
        )),
    )
    .await;

    // The overwrite batch stays below `INDEXING_THRESHOLD_KB` and the soft-deletes
    // stay below the vacuum threshold, so nothing else should optimize; wait
    // anyway to pin down a deterministic segment layout before querying.
    wait_for_optimization(&shard).await;

    ShardFixture {
        _collection_dir: collection_dir,
        _schema_dir: schema_dir,
        shard,
        search_runtime,
    }
}

/// Sweep no-filter / selective / broad filtering for both a low- and a
/// high-cardinality field, comparing approximate faceting against exact.
#[tokio::test(flavor = "multi_thread")]
#[rstest]
#[case::low_card_no_filter(COLOUR_KEY, None)]
#[case::low_card_selective_filter(COLOUR_KEY, Some(seq_below(SELECTIVE)))]
#[case::low_card_broad_filter(COLOUR_KEY, Some(seq_below(BROAD)))]
#[case::high_card_no_filter(TAG_KEY, None)]
#[case::high_card_selective_filter(TAG_KEY, Some(seq_below(SELECTIVE)))]
#[case::high_card_broad_filter(TAG_KEY, Some(seq_below(BROAD)))]
async fn approx_facet_matches_exact_across_strategies(
    #[case] key: &str,
    #[case] filter: Option<Filter>,
) {
    let fixture = build_facet_fixture().await;
    let approx = fixture.approx(key, filter.clone()).await;
    let exact = fixture.exact(key, filter.clone()).await;
    assert_eq!(
        approx, exact,
        "approximate faceting must reproduce exact counts",
    );

    fixture.shard.stop_gracefully().await;
}
