#![allow(deprecated)]

use std::collections::BTreeMap;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::Path;

use collection::collection::Collection;
use collection::config::{
    CollectionConfig, CollectionParams, VectorParams, VectorsConfig, WalConfig,
};
use collection::operations::point_ops::{PointInsertOperations, PointOperations, PointStruct};
use collection::operations::types::{
    CollectionError, PointRequest, RecommendRequest, SearchRequest,
};
use collection::operations::CollectionUpdateOperations;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::{NamedVector, VectorStruct};
use segment::types::{Distance, WithPayloadInterface, WithVector};
use tempfile::Builder;
use tokio::runtime::Handle;

use crate::common::{new_local_collection, N_SHARDS, TEST_OPTIMIZERS_CONFIG};

mod common;

const VEC_NAME1: &str = "vec1";
const VEC_NAME2: &str = "vec2";

#[tokio::test]
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

    let vector_params1 = VectorParams {
        size: NonZeroU64::new(4).unwrap(),
        distance: Distance::Dot,
    };
    let vector_params2 = VectorParams {
        size: NonZeroU64::new(4).unwrap(),
        distance: Distance::Dot,
    };

    let mut vectors_config = BTreeMap::new();

    vectors_config.insert(VEC_NAME1.to_string(), vector_params1);
    vectors_config.insert(VEC_NAME2.to_string(), vector_params2);

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Multi(vectors_config),
        shard_number: NonZeroU32::new(shard_number).expect("Shard number can not be zero"),
        replication_factor: NonZeroU32::new(1).unwrap(),
        on_disk_payload: false,
    };

    let collection_config = CollectionConfig {
        params: collection_params,
        optimizer_config: TEST_OPTIMIZERS_CONFIG.clone(),
        wal_config,
        hnsw_config: Default::default(),
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

    let mut collection = multi_vec_collection_fixture(collection_dir.path(), shard_number).await;

    // Upload 1000 random vectors to the collection
    let mut points = Vec::new();
    for i in 0..1000 {
        let mut vectors = NamedVectors::default();
        vectors.insert(VEC_NAME1.to_string(), vec![i as f32, 0.0, 0.0, 0.0]);
        vectors.insert(VEC_NAME2.to_string(), vec![0.0, i as f32, 0.0, 0.0]);

        points.push(PointStruct {
            id: i.into(),
            vector: vectors.into(),
            payload: Some(serde_json::from_str(r#"{"number": "John Doe"}"#).unwrap()),
        });
    }
    let insert_points = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperations::PointsList(points),
    ));
    collection
        .update_from_client(insert_points, true)
        .await
        .unwrap();

    let query_vector = vec![6.0, 0.0, 0.0, 0.0];

    let full_search_request = SearchRequest {
        vector: NamedVector {
            name: VEC_NAME1.to_string(),
            vector: query_vector,
        }
        .into(),
        filter: None,
        limit: 10,
        offset: 0,
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: Some(true.into()),
        params: None,
        score_threshold: None,
    };

    let result = collection
        .search(full_search_request, &Handle::current(), None)
        .await
        .unwrap();

    for hit in result {
        match hit.vector.unwrap() {
            VectorStruct::Single(_) => panic!("expected multi vector"),
            VectorStruct::Multi(vectors) => {
                assert!(vectors.contains_key(VEC_NAME1));
                assert!(vectors.contains_key(VEC_NAME2));
            }
        }
    }

    let query_vector = vec![0.0, 2.0, 0.0, 0.0];

    let failed_search_request = SearchRequest {
        vector: query_vector.clone().into(),
        filter: None,
        limit: 10,
        offset: 0,
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: Some(true.into()),
        params: None,
        score_threshold: None,
    };

    let result = collection
        .search(failed_search_request, &Handle::current(), None)
        .await;

    assert!(
        matches!(result, Err(CollectionError::BadInput { .. })),
        "{:?}",
        result
    );

    let full_search_request = SearchRequest {
        vector: NamedVector {
            name: VEC_NAME2.to_string(),
            vector: query_vector,
        }
        .into(),
        filter: None,
        limit: 10,
        offset: 0,
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: Some(true.into()),
        params: None,
        score_threshold: None,
    };

    let result = collection
        .search(full_search_request, &Handle::current(), None)
        .await
        .unwrap();

    for hit in result {
        match hit.vector.unwrap() {
            VectorStruct::Single(_) => panic!("expected multi vector"),
            VectorStruct::Multi(vectors) => {
                assert!(vectors.contains_key(VEC_NAME1));
                assert!(vectors.contains_key(VEC_NAME2));
            }
        }
    }

    let retrieve = collection
        .retrieve(
            PointRequest {
                ids: vec![6.into()],
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: WithVector::Selector(vec![VEC_NAME1.to_string()]),
            },
            None,
        )
        .await
        .unwrap();

    assert_eq!(retrieve.len(), 1);
    match retrieve[0].vector.as_ref().unwrap() {
        VectorStruct::Single(_) => panic!("expected multi vector"),
        VectorStruct::Multi(vectors) => {
            assert!(vectors.contains_key(VEC_NAME1));
            assert!(!vectors.contains_key(VEC_NAME2));
        }
    }

    let recommend_result = collection
        .recommend_by(
            RecommendRequest {
                positive: vec![6.into()],
                negative: vec![],
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: Some(WithVector::Selector(vec![VEC_NAME2.to_string()])),
                score_threshold: None,
                limit: 10,
                offset: 0,
                filter: None,
                params: None,
                using: None,
            },
            &Handle::current(),
            None,
        )
        .await;

    match recommend_result {
        Ok(_) => panic!("Error expected"),
        Err(err) => match err {
            CollectionError::BadRequest { .. } => {}
            _ => panic!("Unexpected error"),
        },
    }

    let recommend_result = collection
        .recommend_by(
            RecommendRequest {
                positive: vec![6.into()],
                negative: vec![],
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: Some(WithVector::Selector(vec![VEC_NAME2.to_string()])),
                score_threshold: None,
                limit: 10,
                offset: 0,
                filter: None,
                params: None,
                using: Some(VEC_NAME1.to_string().into()),
            },
            &Handle::current(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(recommend_result.len(), 10);
    for hit in recommend_result {
        match hit.vector.as_ref().unwrap() {
            VectorStruct::Single(_) => panic!("expected multi vector"),
            VectorStruct::Multi(vectors) => {
                assert!(!vectors.contains_key(VEC_NAME1));
                assert!(vectors.contains_key(VEC_NAME2));
            }
        }
    }

    collection.before_drop().await;
}
