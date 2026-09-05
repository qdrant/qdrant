use std::time::Duration;

use collection::operations::CollectionUpdateOperations;
use collection::operations::block_hashes::{BlockHashesRequest, BlockHashesResponse};
use collection::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorStructPersisted,
    WriteOrdering,
};
use common::counter::hardware_accumulator::HwMeasurementAcc;
use serde_json::json;

use crate::common::{TEST_OPTIMIZERS_CONFIG, collection_fixture};

#[derive(serde::Deserialize)]
struct TestRecord {
    id: segment::types::ExtendedPointId,
    payload: Option<segment::types::Payload>,
}

#[tokio::test(flavor = "multi_thread")]
async fn block_hashes_across_shards_and_segment_layouts() {
    let vectors: serde_json::Value =
        serde_json::from_str(include_str!("../../../../docs/block-hashes-v1.json")).unwrap();
    let case = &vectors["cases"][1];
    let expected: BlockHashesResponse = serde_json::from_value(case["expected"].clone()).unwrap();
    let request: BlockHashesRequest =
        serde_json::from_value(json!({"payload_key": "sync.fingerprint", "block_count": 16}))
            .unwrap();

    for (shards, segments) in [(1, 1), (1, 3), (3, 1), (3, 3)] {
        let dir = tempfile::tempdir().unwrap();
        let config = collection::optimizers_builder::OptimizersConfig {
            default_segment_number: segments,
            ..TEST_OPTIMIZERS_CONFIG.clone()
        };
        let collection = collection_fixture(dir.path(), shards, config).await;
        let mut records: Vec<TestRecord> = serde_json::from_value(case["points"].clone()).unwrap();
        if segments == 3 {
            records.reverse();
        }
        // Rewrite existing IDs at a later version; counts must remain logical.
        for version in 0..2 {
            let points = records
                .iter()
                .map(|p| PointStructPersisted {
                    id: p.id,
                    vector: VectorStructPersisted::Single(vec![1.0, 2.0, 3.0, 4.0]),
                    payload: if version == 0 {
                        Some(
                            serde_json::from_value(json!({"sync": {"fingerprint": "stale"}}))
                                .unwrap(),
                        )
                    } else {
                        p.payload.clone()
                    },
                })
                .collect();
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
        let result = collection
            .block_hashes(request.clone(), None, None, None, HwMeasurementAcc::new())
            .await
            .unwrap();
        assert_eq!(result, expected, "shards={shards}, segments={segments}");

        // Dropping a pending audit releases its state and cannot publish a result.
        let holder = collection.shards_holder();
        let guard = holder.write().await;
        let mut pending = Box::pin(collection.block_hashes(
            request.clone(),
            None,
            None,
            None,
            HwMeasurementAcc::new(),
        ));
        assert!(futures::poll!(pending.as_mut()).is_pending());
        drop(pending);
        drop(guard);
        assert_eq!(
            collection
                .block_hashes(request.clone(), None, None, None, HwMeasurementAcc::new())
                .await
                .unwrap(),
            expected
        );

        // A deadline covers the entire operation, including ready futures.
        let result = collection
            .block_hashes(
                request.clone(),
                None,
                None,
                Some(Duration::ZERO),
                HwMeasurementAcc::new(),
            )
            .await;
        assert!(matches!(
            result,
            Err(collection::operations::types::CollectionError::Timeout { .. })
        ));

        // A failure must discard work already accumulated in earlier pages.
        let points = (100..1300)
            .map(|id| PointStructPersisted {
                id: id.into(),
                vector: VectorStructPersisted::Single(vec![0.0; 4]),
                payload: Some(
                    serde_json::from_value(json!({"sync": {"fingerprint": "valid"}})).unwrap(),
                ),
            })
            .chain(std::iter::once(PointStructPersisted {
                id: 1300.into(),
                vector: VectorStructPersisted::Single(vec![0.0; 4]),
                payload: None,
            }))
            .collect();
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
        let result = collection
            .block_hashes(request.clone(), None, None, None, HwMeasurementAcc::new())
            .await;
        assert!(matches!(
            result,
            Err(collection::operations::types::CollectionError::BadRequest { .. })
        ));
    }
}
