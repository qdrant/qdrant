use std::sync::Arc;
use std::time::Duration;

use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::{HwMeasurementAcc, HwSharedDrain};
use common::save_on_disk::SaveOnDisk;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng, rng};
use segment::data_types::vectors::{NamedQuery, VectorInternal, VectorStructInternal};
use shard::query::query_enum::QueryEnum;
use shard::search::CoreSearchRequestBatch;
use tempfile::Builder;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::operations::CollectionUpdateOperations;
use crate::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted,
};
use crate::operations::types::{CollectionError, CoreSearchRequest};
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::{ShardOperation, WaitUntil};
use crate::tests::fixtures::create_collection_config_with_dim;

#[tokio::test(flavor = "multi_thread")]
async fn test_hw_metrics_cancellation() {
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

    const DIM: usize = 2048;

    let mut config = create_collection_config_with_dim(DIM);
    config.optimizer_config.indexing_threshold = None;

    let collection_name = "test".to_string();

    let update_runtime = Handle::current();
    let current_runtime: AdaptiveSearchHandle = AdaptiveSearchHandle::current_for_tests();

    let payload_index_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
    let payload_index_schema_file = payload_index_schema_dir.path().join("payload-schema.json");
    let payload_index_schema =
        Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file).unwrap());

    let shard = LocalShard::build(
        0,
        collection_name.clone(),
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        Arc::new(Default::default()),
        payload_index_schema.clone(),
        update_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    let upsert_ops = make_random_points_upsert_op(50_000, DIM);
    shard
        .update(
            upsert_ops.into(),
            WaitUntil::Visible,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .unwrap();

    let mut rand = rng();
    let req = Arc::new(CoreSearchRequestBatch {
        searches: vec![CoreSearchRequest {
            query: QueryEnum::Nearest(NamedQuery {
                using: None,
                query: VectorInternal::from(rand_vector(DIM, &mut rand)),
            }),
            filter: None,
            params: None,
            limit: 1010,
            offset: 0,
            with_payload: None,
            with_vector: None,
            score_threshold: None,
        }],
    });

    // Warm up the blocking pool and measure how long a full search takes on this machine.
    // Fast CI runners (especially macos ARM) can finish the search well under a fixed 350ms
    // timeout; overloaded runners can miss a too-short timeout before spawn_blocking starts.
    // See https://github.com/qdrant/qdrant/pull/9233
    let warmup_started = std::time::Instant::now();
    shard
        .do_search(
            req.clone(),
            &current_runtime,
            Duration::from_secs(60),
            HwMeasurementAcc::new(),
        )
        .await
        .expect("warmup search should succeed");
    let baseline = warmup_started.elapsed();

    // Cancel roughly mid-flight. Keep a small floor so we still exercise cancellation even
    // when the baseline is tiny; isolation via nextest threads-required + warmup above should
    // make spawn_blocking prompt enough for short timeouts.
    let mut timeout = (baseline / 4).clamp(Duration::from_millis(15), Duration::from_millis(500));

    let mut cancelled_with_cpu = false;
    for _ in 0..12 {
        let outer_hw = Arc::new(HwSharedDrain::default());
        {
            let hw_counter = HwMeasurementAcc::new_with_metrics_drain(outer_hw.clone());
            let search_res = shard
                .do_search(req.clone(), &current_runtime, timeout, hw_counter)
                .await;

            match search_res {
                Err(CollectionError::Timeout { .. }) => {}
                Ok(_) => {
                    // Finished before timeout — tighten and retry.
                    timeout = (timeout / 2).max(Duration::from_millis(5));
                    continue;
                }
                Err(err) => panic!("unexpected search error: {err}"),
            }
        }

        // Cancellation and draining hardware counters is asynchronous on CI runners.
        let wait_timeout = Duration::from_secs(2);
        let poll_interval = Duration::from_millis(10);
        let wait_started = std::time::Instant::now();
        while outer_hw.get_cpu() == 0 && wait_started.elapsed() <= wait_timeout {
            tokio::time::sleep(poll_interval).await;
        }

        if outer_hw.get_cpu() > 0 {
            cancelled_with_cpu = true;
            break;
        }

        // Timed out before meaningful work started — give the search more time.
        timeout = (timeout * 2).min(baseline.saturating_mul(2).max(Duration::from_millis(50)));
    }

    assert!(
        cancelled_with_cpu,
        "failed to observe mid-flight search cancellation with non-zero CPU metrics \
         (baseline={baseline:?})",
    );
}

fn make_random_points_upsert_op(len: usize, dim: usize) -> CollectionUpdateOperations {
    let mut points = vec![];

    // ThreadRng is too slow for creating 40k vectors @ 2048 dimensions each.
    // SmallRng cuts total test duration in half (20s->10s).
    let mut rand = SmallRng::seed_from_u64(0xC0FFEE);

    for i in 0..len as u64 {
        let rand_vector = rand_vector(dim, &mut rand);
        points.push(PointStructPersisted {
            id: segment::types::ExtendedPointId::NumId(i),
            vector: VectorStructInternal::from(rand_vector).into(),
            payload: None,
        });
    }

    let op = PointInsertOperationsInternal::from(points);

    CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(op))
}

fn rand_vector(size: usize, rand: &mut impl Rng) -> Vec<f32> {
    (0..size).map(|_| rand.next_u32() as f32).collect()
}
