//! Strict mode `max_update_by_filter_limit`: every shard gates the update it
//! resolved from a filter scan on its own point count, before writing anything.

use std::sync::Arc;

use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::save_on_disk::SaveOnDisk;
use common::types::DeferredBehavior;
use segment::data_types::vectors::VectorStructInternal;
use segment::payload_json;
use segment::types::{
    Condition, FieldCondition, Filter, Match, PointIdType, Slice, SliceCondition, StrictModeConfig,
    ValueVariants,
};
use shard::operations::CollectionUpdateOperations;
use shard::operations::payload_ops::{PayloadOps, SetPayloadOp};
use shard::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted,
};
use tempfile::{Builder, TempDir};
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::operations::types::CountRequestInternal;
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::{ShardOperation, WaitUntil};
use crate::tests::fixtures::create_collection_config;

/// Points 1..=POINT_COUNT all carry `color: "red"`, so the filter below scans
/// them all.
const POINT_COUNT: u64 = 6;
const LIMIT: usize = 2;

struct ShardFixture {
    shard: LocalShard,
    _collection_dir: TempDir,
    _payload_schema_dir: TempDir,
}

impl ShardFixture {
    async fn stop(self) {
        self.shard.stop_gracefully().await;
    }
}

fn red_filter() -> Filter {
    Filter::new_must(Condition::Field(FieldCondition::new_match(
        "color".try_into().unwrap(),
        Match::new_value(ValueVariants::String("red".into())),
    )))
}

fn point_ids() -> Vec<PointIdType> {
    (1..=POINT_COUNT).map(PointIdType::from).collect()
}

/// Build a shard holding [`POINT_COUNT`] red points, with strict mode enabled
/// and the given update-by-filter limit.
async fn shard_fixture(max_update_by_filter_limit: Option<usize>) -> ShardFixture {
    let collection_dir = Builder::new().prefix("update_by_filter").tempdir().unwrap();
    let payload_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
    let payload_index_schema = Arc::new(
        SaveOnDisk::load_or_init_default(payload_schema_dir.path().join("payload-schema.json"))
            .unwrap(),
    );

    let mut config = create_collection_config();
    config.strict_mode_config = Some(StrictModeConfig {
        enabled: Some(true),
        max_update_by_filter_limit,
        ..Default::default()
    });

    let shard = LocalShard::build(
        0,
        "test_update_by_filter_limit".to_string(),
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        Arc::new(Default::default()),
        payload_index_schema,
        Handle::current(),
        AdaptiveSearchHandle::current_for_tests(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    let points = (1..=POINT_COUNT)
        .map(|id| PointStructPersisted {
            id: id.into(),
            vector: VectorStructInternal::from(vec![1.0, 0.0, 0.5, 0.25]).into(),
            payload: Some(payload_json! {"color": "red"}),
        })
        .collect();
    let upsert = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(points),
    ));
    shard
        .update(
            upsert.into(),
            WaitUntil::Visible,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("failed to upsert points");

    ShardFixture {
        shard,
        _collection_dir: collection_dir,
        _payload_schema_dir: payload_schema_dir,
    }
}

/// Points currently matching `filter`.
async fn count(shard: &LocalShard, filter: Filter) -> usize {
    shard
        .count(
            Arc::new(CountRequestInternal {
                filter: Some(filter),
                exact: true,
            }),
            &AdaptiveSearchHandle::current_for_tests(),
            None,
            HwMeasurementAcc::new(),
            DeferredBehavior::VisibleOnly,
        )
        .await
        .expect("count failed")
        .count
}

fn delete_by_filter(filter: Filter) -> CollectionUpdateOperations {
    CollectionUpdateOperations::PointOperation(PointOperations::DeletePointsByFilter(filter))
}

fn set_payload_by_filter(
    filter: Option<Filter>,
    points: Option<Vec<PointIdType>>,
) -> CollectionUpdateOperations {
    CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(SetPayloadOp {
        payload: payload_json! {"visited": true},
        points,
        filter,
        key: None,
    }))
}

async fn update(
    shard: &LocalShard,
    operation: CollectionUpdateOperations,
) -> crate::operations::types::CollectionResult<()> {
    shard
        .update(
            operation.into(),
            WaitUntil::Visible,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .map(|_| ())
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_by_filter_over_the_limit_is_rejected() {
    let fixture = shard_fixture(Some(LIMIT)).await;
    let shard = &fixture.shard;

    let err = update(shard, delete_by_filter(red_filter()))
        .await
        .expect_err("scan over the limit must be rejected");
    let message = err.to_string();

    assert!(
        message.contains(&format!("matches {POINT_COUNT} points")),
        "error must report the exact match count: {message}",
    );
    assert!(
        message.contains(&format!("per-shard limit of {LIMIT}")),
        "error must report the limit: {message}",
    );
    // The nudge: 6 points at a limit of 2, with headroom for unbalanced slices.
    assert!(
        message.contains(r#"{"slice": {"total": 6, "index": 0}}"#),
        "error must point at the slice condition: {message}",
    );

    // Rejected before the WAL append, so nothing was deleted.
    assert_eq!(count(shard, red_filter()).await, POINT_COUNT as usize);

    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn scan_matching_exactly_the_limit_is_allowed() {
    let fixture = shard_fixture(Some(LIMIT)).await;
    let shard = &fixture.shard;

    // Narrow the scan to exactly `LIMIT` points with a `has_id` condition.
    let ids = point_ids().into_iter().take(LIMIT).collect::<Vec<_>>();
    let filter = red_filter().merge(&Filter::new_must(Condition::HasId(
        ids.iter().copied().collect::<ahash::AHashSet<_>>().into(),
    )));

    update(shard, delete_by_filter(filter))
        .await
        .expect("a scan matching exactly the limit must be allowed");

    assert_eq!(
        count(shard, red_filter()).await,
        POINT_COUNT as usize - LIMIT,
    );

    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn slice_condition_splits_a_rejected_update() {
    let fixture = shard_fixture(Some(LIMIT)).await;
    let shard = &fixture.shard;

    // The nudge in the error message: split the scan into `total` disjoint
    // slices. Slices are uniform over the id space but not exactly balanced,
    // so pick the smallest `total` that actually brings every slice under the
    // limit for these ids (deterministic: the slice hash is a stable API).
    let total = (1..=16u32)
        .find(|&total| {
            let total = total.try_into().unwrap();
            (0..u32::from(total)).all(|index| {
                let slice = Slice { total, index };
                point_ids().iter().filter(|id| slice.check(**id)).count() <= LIMIT
            })
        })
        .expect("some slicing must fit under the limit");
    assert!(
        total >= 3,
        "6 points at a limit of 2 need at least 3 slices"
    );

    for index in 0..total {
        let filter = red_filter().merge(&Filter::new_must(Condition::Slice(SliceCondition {
            slice: Slice {
                total: total.try_into().unwrap(),
                index,
            },
        })));
        update(shard, delete_by_filter(filter))
            .await
            .expect("each slice stays under the limit");
    }

    // The slices together covered every matching point.
    assert_eq!(count(shard, red_filter()).await, 0);

    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn set_payload_is_gated_only_when_it_scans() {
    let fixture = shard_fixture(Some(LIMIT)).await;
    let shard = &fixture.shard;

    update(shard, set_payload_by_filter(Some(red_filter()), None))
        .await
        .expect_err("set payload by filter over the limit must be rejected");

    // An explicit id list takes precedence over the filter on apply, so the
    // operation never scans and is not gated, however long the list is.
    update(
        shard,
        set_payload_by_filter(Some(red_filter()), Some(point_ids())),
    )
    .await
    .expect("set payload with an explicit id list must be allowed");

    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn limit_does_not_apply_to_id_lists() {
    let fixture = shard_fixture(Some(LIMIT)).await;
    let shard = &fixture.shard;

    // An id-based delete is not a scan, whatever its size.
    let delete_by_ids = CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints {
        ids: point_ids(),
    });
    update(shard, delete_by_ids)
        .await
        .expect("id-based deletes must not be gated");
    assert_eq!(count(shard, red_filter()).await, 0);

    fixture.stop().await;
}

/// A disposable measurement marks an operation as unmetered (it is also
/// swapped in for client updates routed to a resharding replica). It must not
/// switch the limit off.
#[tokio::test(flavor = "multi_thread")]
async fn unmetered_updates_are_still_gated() {
    let fixture = shard_fixture(Some(LIMIT)).await;
    let shard = &fixture.shard;

    shard
        .update(
            delete_by_filter(red_filter()).into(),
            WaitUntil::Visible,
            None,
            HwMeasurementAcc::disposable(),
        )
        .await
        .expect_err("a disposable measurement must not bypass the limit");
    assert_eq!(count(shard, red_filter()).await, POINT_COUNT as usize);

    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn no_limit_allows_any_scan() {
    let fixture = shard_fixture(None).await;
    let shard = &fixture.shard;

    update(shard, delete_by_filter(red_filter()))
        .await
        .expect("without a limit any scan is allowed");
    assert_eq!(count(shard, red_filter()).await, 0);

    fixture.stop().await;
}
