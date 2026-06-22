//! Accuracy coverage for approximate faceting, comparing it against exact
//! faceting on a real [`LocalShard`].
//!
//! The approximate facet implementation lives in
//! `segment/src/segment/read_view/facet.rs`, but the *exact* counterpart is only
//! reachable one level up, at the shard: [`LocalShard::exact_facet`] enumerates
//! every value and counts each with a filtered read, deduplicating points across
//! segments. The shard is therefore the lowest level where both paths can be run
//! against the same data and compared.
//!
//! The test sweeps a 2×3 matrix to exercise every approximate plan:
//!
//! | field                         | no filter | selective filter | broad filter |
//! |-------------------------------|-----------|------------------|--------------|
//! | low cardinality (`colour`)    | full      | full             | full         |
//! | high cardinality (`tag`, Zipf)| sampling  | sampling         | sampling     |
//!
//! and within each strategy the filter selectivity steers the terminal op
//! (direct counts / filter-iter / facet-iter).
//!
//! Every case asserts one invariant: the top-[`LIMIT`] hits of approximate
//! faceting equal those of exact faceting. The full strategy (low cardinality)
//! enumerates exhaustively, so its entire result matches. The sampling strategy
//! (high cardinality) may omit rare values, but the Zipf skew keeps the heaviest
//! values — the only ones that can reach the top-[`LIMIT`] — over-represented
//! enough to be sampled on every run, and each value it surfaces carries an
//! exact count. Truncating both sides to their top hits therefore tolerates
//! sampling's incompleteness while still catching any miscount.
//!
//! The shard holds a single appendable segment, and the fixture deletes and
//! overwrites a slice of the points after the initial insert (see
//! [`build_facet_fixture`]). Even so, no point ever lands in two segments with
//! different versions: both mutations rewrite that one segment in place. The
//! per-segment-sum `approx_facet` performs therefore still equals the
//! cross-segment dedup `exact_facet` performs — now additionally proving both
//! paths honour deletions and payload overwrites.

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

/// Build a single-shard fixture of `N_POINTS` points. Each point carries:
/// * `colour` — keyword, `COLOURS.len()` uniques, low cardinality → full strategy.
/// * `tag` — keyword, Zipf-distributed over a large vocabulary, high cardinality
///   → sampling strategy.
/// * `seq` — float equal to the point index, for filters of exact selectivity.
async fn build_facet_fixture() -> ShardFixture {
    let collection_dir = Builder::new().prefix("facet_collection").tempdir().unwrap();
    let schema_dir = Builder::new().prefix("facet_schema").tempdir().unwrap();

    let config = create_collection_config();

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

    let upsert = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::from(points),
    ));
    shard
        .update(
            upsert.into(),
            WaitUntil::Visible,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .unwrap();

    // Faceting requires a map index (keyword); `seq` is indexed too so range
    // filters resolve against an index.
    for (name, field_type) in [
        (COLOUR_KEY, PayloadSchemaType::Keyword),
        (TAG_KEY, PayloadSchemaType::Keyword),
        (SEQ_KEY, PayloadSchemaType::Float),
    ] {
        create_index(&shard, &payload_index_schema, name, field_type).await;
    }

    // Mutate the fresh insert so both facet paths must reconcile removals and
    // payload rewrites, not just a pristine load. The two mutation sets are
    // disjoint residues mod `MUTATION_STRIDE`.

    // Delete one point in every `MUTATION_STRIDE`. Deleted points must vanish
    // from both counts (this exercises `DeferredBehavior::VisibleOnly`).
    let delete = CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints {
        ids: (0..N_POINTS)
            .filter(|i| i % MUTATION_STRIDE == 0)
            .map(|i| (i as u64 + 1).into())
            .collect(),
    });
    shard
        .update(
            delete.into(),
            WaitUntil::Visible,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .unwrap();

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
    let overwrite = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::from(overwrites),
    ));
    shard
        .update(
            overwrite.into(),
            WaitUntil::Visible,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .unwrap();

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
