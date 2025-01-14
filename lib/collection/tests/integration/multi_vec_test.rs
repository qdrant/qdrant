use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::path::Path;

use api::rest::SearchRequestInternal;
use collection::collection::Collection;
use collection::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use collection::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorStructPersisted,
    WriteOrdering,
};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{
    CollectionError, PointRequestInternal, RecommendRequestInternal, VectorsConfig,
};
use collection::operations::vector_params_builder::VectorParamsBuilder;
use collection::operations::CollectionUpdateOperations;
use collection::recommendations::recommend_by;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::{NamedVector, VectorStructInternal};
use segment::types::{Distance, WithPayloadInterface, WithVector};
use tempfile::Builder;

use crate::common::{new_local_collection, N_SHARDS, TEST_OPTIMIZERS_CONFIG};

const VEC_NAME1: &str = "vec1";
const VEC_NAME2: &str = "vec2";

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_vec() {
    test_multi_vec_with_shards(1).await;
    test_multi_vec_with_shards(N_SHARDS).await;
}

#[cfg(test)]
pub async fn multi_vec_collection_fixture(collection_path: &Path, shard_number: u32) -> Collection {
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
    };

    let vector_params1 = VectorParamsBuilder::new(4, Distance::Dot).build();
    let vector_params2 = VectorParamsBuilder::new(4, Distance::Dot).build();

    let mut vectors_config = BTreeMap::new();

    vectors_config.insert(VEC_NAME1.to_string(), vector_params1);
    vectors_config.insert(VEC_NAME2.to_string(), vector_params2);

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Multi(vectors_config),
        shard_number: NonZeroU32::new(shard_number).expect("Shard number can not be zero"),
        ..CollectionParams::empty()
    };

    let collection_config = CollectionConfigInternal {
        params: collection_params,
        optimizer_config: TEST_OPTIMIZERS_CONFIG.clone(),
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
        strict_mode_config: Default::default(),
        uuid: None,
    };

    let snapshot_path = collection_path.join("snapshots");

    // Default to a collection with all the shards local
    new_local_collection(
        "test".to_string(),
        collection_path,
        &snapshot_path,
        &collection_config,
    )
    .await
    .unwrap()
}

async fn test_multi_vec_with_shards(shard_number: u32) {
    let collection_dir = Builder::new()
        .prefix("test_multi_vec_with_shards")
        .tempdir()
        .unwrap();

    let collection = multi_vec_collection_fixture(collection_dir.path(), shard_number).await;

    // Upload 1000 random vectors to the collection
    let mut points = Vec::new();
    for i in 0..1000 {
        let mut vectors = NamedVectors::default();
        vectors.insert(VEC_NAME1.to_string(), vec![i as f32, 0.0, 0.0, 0.0].into());
        vectors.insert(VEC_NAME2.to_string(), vec![0.0, i as f32, 0.0, 0.0].into());

        points.push(PointStructPersisted {
            id: i.into(),
            vector: VectorStructPersisted::from(VectorStructInternal::from(vectors)),
            payload: Some(serde_json::from_str(r#"{"number": "John Doe"}"#).unwrap()),
        });
    }
    let insert_points = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(points),
    ));
    collection
        .update_from_client_simple(insert_points, true, WriteOrdering::default())
        .await
        .unwrap();

    let query_vector = vec![6.0, 0.0, 0.0, 0.0];

    let full_search_request = SearchRequestInternal {
        vector: NamedVector {
            name: VEC_NAME1.to_string(),
            vector: query_vector,
        }
        .into(),
        filter: None,
        limit: 10,
        offset: None,
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: Some(true.into()),
        params: None,
        score_threshold: None,
    };

    let hw_acc = HwMeasurementAcc::new();
    let result = collection
        .search(
            full_search_request.into(),
            None,
            &ShardSelectorInternal::All,
            None,
            hw_acc,
        )
        .await
        .unwrap();

    for hit in result {
        match hit.vector.unwrap() {
            VectorStructInternal::Single(_) => panic!("expected multi vector"),
            VectorStructInternal::MultiDense(_) => panic!("expected multi vector"),
            VectorStructInternal::Named(vectors) => {
                assert!(vectors.contains_key(VEC_NAME1));
                assert!(vectors.contains_key(VEC_NAME2));
            }
        }
    }

    let query_vector = vec![0.0, 2.0, 0.0, 0.0];

    let failed_search_request = SearchRequestInternal {
        vector: query_vector.clone().into(),
        filter: None,
        limit: 10,
        offset: None,
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: Some(true.into()),
        params: None,
        score_threshold: None,
    };

    let hw_acc = HwMeasurementAcc::new();
    let result = collection
        .search(
            failed_search_request.into(),
            None,
            &ShardSelectorInternal::All,
            None,
            hw_acc,
        )
        .await;

    assert!(
        matches!(result, Err(CollectionError::BadInput { .. })),
        "{result:?}"
    );

    let full_search_request = SearchRequestInternal {
        vector: NamedVector {
            name: VEC_NAME2.to_string(),
            vector: query_vector,
        }
        .into(),
        filter: None,
        limit: 10,
        offset: None,
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: Some(true.into()),
        params: None,
        score_threshold: None,
    };

    let hw_acc = HwMeasurementAcc::new();
    let result = collection
        .search(
            full_search_request.into(),
            None,
            &ShardSelectorInternal::All,
            None,
            hw_acc,
        )
        .await
        .unwrap();

    for hit in result {
        match hit.vector.unwrap() {
            VectorStructInternal::Single(_) => panic!("expected multi vector"),
            VectorStructInternal::MultiDense(_) => panic!("expected multi vector"),
            VectorStructInternal::Named(vectors) => {
                assert!(vectors.contains_key(VEC_NAME1));
                assert!(vectors.contains_key(VEC_NAME2));
            }
        }
    }

    let retrieve = collection
        .retrieve(
            PointRequestInternal {
                ids: vec![6.into()],
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: WithVector::Selector(vec![VEC_NAME1.to_string()]),
            },
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .unwrap();

    assert_eq!(retrieve.len(), 1);
    match retrieve[0].vector.as_ref().unwrap() {
        VectorStructInternal::Single(_) => panic!("expected multi vector"),
        VectorStructInternal::MultiDense(_) => panic!("expected multi vector"),
        VectorStructInternal::Named(vectors) => {
            assert!(vectors.contains_key(VEC_NAME1));
            assert!(!vectors.contains_key(VEC_NAME2));
        }
    }

    let hw_acc = HwMeasurementAcc::new();
    let recommend_result = recommend_by(
        RecommendRequestInternal {
            positive: vec![6.into()],
            with_payload: Some(WithPayloadInterface::Bool(false)),
            with_vector: Some(WithVector::Selector(vec![VEC_NAME2.to_string()])),
            limit: 10,
            ..Default::default()
        },
        &collection,
        |_name| async { unreachable!("should not be called in this test") },
        None,
        ShardSelectorInternal::All,
        None,
        hw_acc,
    )
    .await;

    match recommend_result {
        Ok(_) => panic!("Error expected"),
        Err(err) => match err {
            CollectionError::BadRequest { .. } => {}
            CollectionError::BadInput { .. } => {}
            error => panic!("Unexpected error {error}"),
        },
    }

    let hw_acc = HwMeasurementAcc::new();
    let recommend_result = recommend_by(
        RecommendRequestInternal {
            positive: vec![6.into()],
            with_payload: Some(WithPayloadInterface::Bool(false)),
            with_vector: Some(WithVector::Selector(vec![VEC_NAME2.to_string()])),
            limit: 10,
            using: Some(VEC_NAME1.to_string().into()),
            ..Default::default()
        },
        &collection,
        |_name| async { unreachable!("should not be called in this test") },
        None,
        ShardSelectorInternal::All,
        None,
        hw_acc,
    )
    .await
    .unwrap();

    assert_eq!(recommend_result.len(), 10);
    for hit in recommend_result {
        match hit.vector.as_ref().unwrap() {
            VectorStructInternal::Single(_) => panic!("expected multi vector"),
            VectorStructInternal::MultiDense(_) => panic!("expected multi vector"),
            VectorStructInternal::Named(vectors) => {
                assert!(!vectors.contains_key(VEC_NAME1));
                assert!(vectors.contains_key(VEC_NAME2));
            }
        }
    }
}
