use std::collections::BTreeMap;
use std::num::{NonZeroU32, NonZeroU64};

use collection::collection::Collection;
use collection::config::{CollectionConfig, CollectionParams, WalConfig};
use collection::operations::point_ops::{
    PointInsertOperations, PointOperations, PointStruct, WriteOrdering,
};
use collection::operations::types::{CollectionStatus, SearchRequest, VectorParams, VectorsConfig};
use collection::operations::CollectionUpdateOperations;
use collection::shards::channel_service::ChannelService;
use collection::shards::collection_shard_distribution::CollectionShardDistribution;
use collection::shards::replica_set::ReplicaState;
use rand::Rng;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::{NamedVector, NamedVectorStruct};
use segment::types::{Distance, ProductQuantizationConfig, ScalarQuantizationConfig, WithVector};
use tempfile::Builder;

use crate::common::{
    dummy_on_replica_failure, dummy_request_shard_transfer, TEST_OPTIMIZERS_CONFIG,
};

mod common;

#[tokio::test]
async fn quantization_snapshot_test() {
    const DIM: usize = 8;
    let vector_names = ["vec1", "vec2"];
    let mut rng = rand::thread_rng();
    let mut rand_vector = || {
        (0..DIM)
            .map(|_| rng.gen_range(0.0..1.0))
            .collect::<Vec<_>>()
    };

    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
    };

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Multi(BTreeMap::from([
            (
                vector_names[0].to_owned(),
                VectorParams {
                    size: NonZeroU64::new(DIM as u64).unwrap(),
                    distance: Distance::Dot,
                    hnsw_config: None,
                    quantization_config: Some(segment::types::QuantizationConfig::Scalar(
                        segment::types::ScalarQuantization {
                            scalar: ScalarQuantizationConfig {
                                quantile: None,
                                always_ram: None,
                                r#type: segment::types::ScalarType::Int8,
                            },
                        },
                    )),
                    on_disk: None,
                },
            ),
            (
                vector_names[1].to_owned(),
                VectorParams {
                    size: NonZeroU64::new(DIM as u64).unwrap(),
                    distance: Distance::Euclid,
                    hnsw_config: None,
                    quantization_config: Some(segment::types::QuantizationConfig::Product(
                        segment::types::ProductQuantization {
                            product: ProductQuantizationConfig {
                                compression: segment::types::CompressionRatio::X4,
                                always_ram: None,
                            },
                        },
                    )),
                    on_disk: None,
                },
            ),
        ])),
        shard_number: NonZeroU32::new(1).unwrap(),
        replication_factor: NonZeroU32::new(1).unwrap(),
        write_consistency_factor: NonZeroU32::new(1).unwrap(),
        on_disk_payload: false,
    };

    let mut optimizer_config = TEST_OPTIMIZERS_CONFIG.clone();
    optimizer_config.indexing_threshold = Some(500);
    optimizer_config.flush_interval_sec = 1;
    let config = CollectionConfig {
        params: collection_params,
        optimizer_config,
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
    };

    let snapshots_path = Builder::new().prefix("test_snapshots").tempdir().unwrap();
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
    let recover_dir = Builder::new()
        .prefix("test_collection_rec")
        .tempdir()
        .unwrap();
    let collection_name = "test".to_string();
    let collection_name_rec = "test_rec".to_string();

    let this_peer_id = 0;
    let shard_distribution = CollectionShardDistribution::all_local(
        Some(config.params.shard_number.into()),
        this_peer_id,
    );

    let mut collection = Collection::new(
        collection_name,
        this_peer_id,
        collection_dir.path(),
        snapshots_path.path(),
        &config,
        Default::default(),
        shard_distribution,
        ChannelService::default(),
        dummy_on_replica_failure(),
        dummy_request_shard_transfer(),
        None,
        None,
    )
    .await
    .unwrap();

    let local_shards = collection.get_local_shards().await;
    for shard_id in local_shards {
        collection
            .set_shard_replica_state(shard_id, 0, ReplicaState::Active, None)
            .await
            .unwrap();
    }

    // Upload 1000 random vectors to the collection
    let mut points = Vec::new();
    for i in 0..1_000 {
        points.push(PointStruct {
            id: i.into(),
            vector: NamedVectors::from([
                ("vec1".to_owned(), rand_vector()),
                ("vec2".to_owned(), rand_vector()),
            ])
            .into(),
            payload: None,
        });
    }
    let insert_points = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperations::PointsList(points),
    ));
    collection
        .update_from_client(insert_points, true, WriteOrdering::default())
        .await
        .unwrap();

    // Wait indexing
    while collection.info(None).await.unwrap().status == CollectionStatus::Green {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Take a snapshot
    let snapshots_tmp_dir = collection_dir.path().join("snapshots_tmp");
    std::fs::create_dir_all(&snapshots_tmp_dir).unwrap();
    let snapshot_description = collection
        .create_snapshot(&snapshots_tmp_dir, 0)
        .await
        .unwrap();

    // Restore the snapshot
    if let Err(err) = Collection::restore_snapshot(
        &snapshots_path.path().join(snapshot_description.name),
        recover_dir.path(),
        0,
        false,
    ) {
        collection.before_drop().await;
        panic!("Failed to restore snapshot: {err}")
    }

    let mut recovered_collection = Collection::load(
        collection_name_rec,
        this_peer_id,
        recover_dir.path(),
        snapshots_path.path(),
        Default::default(),
        ChannelService::default(),
        dummy_on_replica_failure(),
        dummy_request_shard_transfer(),
        None,
        None,
    )
    .await;

    for vector_name in &vector_names {
        let query_vector = NamedVectorStruct::Named(NamedVector {
            name: (*vector_name).to_owned(),
            vector: rand_vector(),
        });

        let full_search_request = SearchRequest {
            vector: query_vector.clone().into(),
            filter: None,
            limit: 10,
            offset: 0,
            with_payload: None,
            with_vector: Some(WithVector::Bool(true)),
            params: None,
            score_threshold: None,
        };

        let reference_result = collection
            .search(full_search_request.clone(), None, None)
            .await
            .unwrap();

        let recovered_result = recovered_collection
            .search(full_search_request, None, None)
            .await
            .unwrap();

        assert_eq!(reference_result.len(), recovered_result.len());

        for (reference, recovered) in reference_result.iter().zip(recovered_result.iter()) {
            assert_eq!(reference.id, recovered.id);
            assert_eq!(reference.vector, recovered.vector);
        }
    }

    collection.before_drop().await;
    recovered_collection.before_drop().await;
}
