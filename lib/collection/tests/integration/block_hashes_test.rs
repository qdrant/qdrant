use std::time::Duration;

use collection::collection::Collection;
use collection::operations::CollectionUpdateOperations;
use collection::operations::block_hashes::BlockHashesRequest;
use collection::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorStructPersisted,
    WriteOrdering,
};
use collection::operations::types::CollectionError;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::types::PointIdType;
use serde_json::json;

use crate::common::{TEST_OPTIMIZERS_CONFIG, collection_fixture};

fn point(id: PointIdType, value: Option<&str>) -> PointStructPersisted {
    PointStructPersisted {
        id,
        vector: VectorStructPersisted::Single(vec![0.0; 4]),
        payload: value
            .map(|v| serde_json::from_value(json!({"sync": {"fingerprint": v}})).unwrap()),
    }
}

async fn upsert(collection: &Collection, points: Vec<PointStructPersisted>) {
    collection
        .update_from_client_simple(
            CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                PointInsertOperationsInternal::PointsList(points),
            )),
            true,
            None,
            WriteOrdering::default(),
            HwMeasurementAcc::new(),
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn block_hashes_across_shards_and_segment_layouts() {
    let vectors: serde_json::Value =
        serde_json::from_str(include_str!("../../../../tests/fixtures/block_hashes.json")).unwrap();
    let request = BlockHashesRequest {
        payload_key: "sync.fingerprint".parse().unwrap(),
        block_count: 1,
        filter: None,
    };
    for (shards, segments) in [(1, 1), (1, 3), (3, 1), (3, 3)] {
        let dir = tempfile::tempdir().unwrap();
        let config = collection::optimizers_builder::OptimizersConfig {
            default_segment_number: segments,
            ..TEST_OPTIMIZERS_CONFIG.clone()
        };
        let collection = collection_fixture(dir.path(), shards, config).await;
        let scan = |timeout| {
            collection.block_hashes(
                request.clone(),
                None,
                None,
                timeout,
                HwMeasurementAcc::new(),
            )
        };
        let mut records = vectors["records"].as_array().unwrap().clone();
        if segments == 3 {
            records.reverse();
        }
        // Rewrite existing IDs at a later version; counts must remain logical.
        for version in 0..2 {
            let points = records
                .iter()
                .map(|record| {
                    point(
                        serde_json::from_value(record["id"].clone()).unwrap(),
                        Some(if version == 0 {
                            "stale"
                        } else {
                            record["value"].as_str().unwrap()
                        }),
                    )
                })
                .collect();
            upsert(&collection, points).await;
        }
        let expected = scan(None).await.unwrap();
        assert_eq!(expected.blocks[0].point_count, records.len() as u64);
        assert_eq!(
            expected.blocks[0].hash,
            vectors["one_block_hash"].as_str().unwrap()
        );

        // Cancel a pending audit, then verify another request can complete.
        let holder = collection.shards_holder();
        let guard = holder.write().await;
        let mut pending = Box::pin(scan(None));
        assert!(futures::poll!(pending.as_mut()).is_pending());
        drop(pending);
        drop(guard);
        assert_eq!(scan(None).await.unwrap(), expected);
        assert!(matches!(
            scan(Some(Duration::ZERO)).await,
            Err(CollectionError::Timeout { .. })
        ));

        // Failure on a later page must discard already accumulated hashes.
        let mut points = (100..1300)
            .map(|id| point(id.into(), Some("valid")))
            .collect::<Vec<_>>();
        points.push(point(1300.into(), None));
        upsert(&collection, points).await;
        assert!(matches!(
            scan(None).await,
            Err(CollectionError::BadRequest { .. })
        ));
    }
}
