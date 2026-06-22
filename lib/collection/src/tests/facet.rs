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
//! Two invariants are asserted:
//!
//! * **Full strategy (low cardinality)** — enumeration is exhaustive, so the
//!   approximate result must equal the exact one *exactly* (same values, same
//!   counts).
//! * **Sampling strategy (high cardinality)** — sampling trades completeness for
//!   speed: it may omit values, but every value it surfaces must carry the exact
//!   count.
//!
//! The shard holds a single appendable segment with each point upserted once, so
//! no point appears in multiple segments with different versions — the
//! per-segment-sum `approx_facet` performs equals the cross-segment dedup
//! `exact_facet` performs, and the two are expected to agree on counts.

use std::collections::HashMap;
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
use segment::data_types::facets::{FacetParams, FacetValue};
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
use crate::tests::fixtures::{create_collection_config};
use crate::tests::payload::create_index;

/// Total points in the fixture.
const N_POINTS: usize = 300_000;
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

const LIMIT: usize = 10;
const TIMEOUT: Duration = Duration::from_secs(60);

type Counts = HashMap<FacetValue, usize>;

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
    async fn approx(&self, key: &str, filter: Option<Filter>) -> Counts {
        self.facet(key, filter, false).await
    }

    /// Run the exact facet path (the reference).
    async fn exact(&self, key: &str, filter: Option<Filter>) -> Counts {
        self.facet(key, filter, true).await
    }

    async fn facet(&self, key: &str, filter: Option<Filter>, exact: bool) -> Counts {
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
        hits.into_iter().map(|hit| (hit.value, hit.count)).collect()
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

/// Every value the approximate facet reported must carry the exact count. The
/// approximate set may be a subset of the exact one (sampling can omit values),
/// but it must never be wrong about a count.
fn assert_subset_counts_exact(approx: &Counts, exact: &Counts) {
    for (value, &count) in approx {
        assert_eq!(
            exact.get(value).copied(),
            Some(count),
            "Approximate facet reported {value:?} = {count}, exact = {:?}",
            exact.get(value),
        );
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
            PointStructPersisted {
                id: (i as u64 + 1).into(),
                vector: VectorStructInternal::from(vec![1.0, 2.0, 3.0, 4.0]).into(),
                payload: Some(
                    serde_json::from_value(serde_json::json!({
                        COLOUR_KEY: COLOURS[i % COLOURS.len()],
                        TAG_KEY: format!("tag_{tag}"),
                        SEQ_KEY: i as f64,
                    }))
                    .unwrap(),
                ),
            }
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
#[case::no_filter(None)]
#[case::selective_filter(Some(seq_below(SELECTIVE)))]
#[case::broad_filter(Some(seq_below(BROAD)))]
async fn approx_facet_matches_exact_across_strategies(#[case] filter: Option<Filter>) {
    let fixture = build_facet_fixture().await;
    let approx = fixture.approx(COLOUR_KEY, filter.clone()).await;
    let exact = fixture.exact(COLOUR_KEY, filter.clone()).await;
    assert_eq!(
        approx, exact,
        "approximate faceting must reproduce exact counts",
    );

    // High-cardinality Zipf keyword field → "sampling" strategy. It may omit
    // values, but every value it surfaces must carry the exact count.
    let approx = fixture.approx(TAG_KEY, filter.clone()).await;
    let exact = fixture.exact(TAG_KEY, filter.clone()).await;

    if filter.is_none() {
        // Without a filter, exact enumerates the whole vocabulary; assert the
        // field is high-cardinality enough to actually engage sampling.
        assert!(
            !approx.is_empty(),
            "expected some hits",
        );
        assert!(
            approx.len() <= LIMIT,
            "result must be bounded by the limit",
        );
        assert_subset_counts_exact(&approx, &exact);
    }

    fixture.shard.stop_gracefully().await;
}
