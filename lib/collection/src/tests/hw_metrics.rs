use std::sync::Arc;
use std::time::Duration;

use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::{HwMeasurementAcc, HwSharedDrain};
use common::save_on_disk::SaveOnDisk;
use rand::rngs::ThreadRng;
use rand::{RngCore, rng};
use segment::data_types::vectors::{NamedQuery, VectorInternal, VectorStructInternal};
use shard::query::query_enum::QueryEnum;
use shard::search::CoreSearchRequestBatch;
use tempfile::Builder;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::operations::CollectionUpdateOperations;
use crate::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted,
};
use crate::operations::types::{CollectionError, CoreSearchRequest};
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::ShardOperation;
use crate::tests::fixtures::create_collection_config_with_dim;

#[tokio::test(flavor = "multi_thread")]
async fn test_hw_metrics_cancellation() {
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

    let mut config = create_collection_config_with_dim(512);
    config.optimizer_config.indexing_threshold = None;

    let collection_name = "test".to_string();

    let current_runtime: Handle = Handle::current();

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
        current_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    let upsert_ops = make_random_points_upsert_op(10_000);
    shard
        .update(upsert_ops.into(), true, HwMeasurementAcc::new())
        .await
        .unwrap();

    let mut rand = rng();
    let req = CoreSearchRequestBatch {
        searches: vec![CoreSearchRequest {
            query: QueryEnum::Nearest(NamedQuery {
                using: None,
                query: VectorInternal::from(rand_vector(512, &mut rand)),
            }),
            filter: None,
            params: None,
            limit: 1010,
            offset: 0,
            with_payload: None,
            with_vector: None,
            score_threshold: None,
        }],
    };

    let outer_hw = HwSharedDrain::default();

    {
        let hw_counter = HwMeasurementAcc::new_with_metrics_drain(outer_hw.clone());
        let search_res = shard
            .do_search(
                Arc::new(req),
                &current_runtime,
                Some(Duration::from_millis(10)), // Very short duration to hit timeout before the search finishes
                hw_counter,
            )
            .await;

        // Ensure we triggered a timeout and the search didn't exit too early.
        assert!(matches!(
            search_res.unwrap_err(),
            CollectionError::Timeout { description: _ }
        ));

        // Wait until the cancellation is processed is finished
        std::thread::sleep(Duration::from_millis(50));
    }

    assert!(outer_hw.get_cpu() > 0);
}

fn make_random_points_upsert_op(len: usize) -> CollectionUpdateOperations {
    let mut points = vec![];

    let mut rand = rng();

    for i in 0..len as u64 {
        let rand_vector = rand_vector(512, &mut rand);
        points.push(PointStructPersisted {
            id: segment::types::ExtendedPointId::NumId(i),
            vector: VectorStructInternal::from(rand_vector).into(),
            payload: None,
        });
    }

    let op = PointInsertOperationsInternal::from(points);

    CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(op))
}

fn rand_vector(size: usize, rand: &mut ThreadRng) -> Vec<f32> {
    (0..size).map(|_| rand.next_u32() as f32).collect()
}
