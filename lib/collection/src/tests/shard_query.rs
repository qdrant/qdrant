use std::sync::Arc;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::cpu::CpuBudget;
use segment::data_types::vectors::{NamedVectorStruct, VectorInternal, DEFAULT_VECTOR_NAME};
use segment::types::{PointIdType, WithPayloadInterface, WithVector};
use tempfile::Builder;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::operations::query_enum::QueryEnum;
use crate::operations::types::CollectionError;
use crate::operations::universal_query::shard_query::{
    FusionInternal, ScoringQuery, ShardPrefetch, ShardQueryRequest,
};
use crate::save_on_disk::SaveOnDisk;
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::ShardOperation;
use crate::tests::fixtures::*;

#[tokio::test(flavor = "multi_thread")]
async fn test_shard_query_rrf_rescoring() {
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

    let config = create_collection_config();

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
        payload_index_schema,
        current_runtime.clone(),
        current_runtime.clone(),
        CpuBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    let upsert_ops = upsert_operation();

    shard.update(upsert_ops.into(), true).await.unwrap();

    // RRF query without prefetches
    let query = ShardQueryRequest {
        prefetches: vec![],
        query: Some(ScoringQuery::Fusion(FusionInternal::Rrf)),
        filter: None,
        score_threshold: None,
        limit: 0,
        offset: 0,
        params: None,
        with_vector: WithVector::Bool(false),
        with_payload: WithPayloadInterface::Bool(false),
    };

    let hw_acc = HwMeasurementAcc::new();
    let sources_scores = shard
        .query_batch(Arc::new(vec![query]), &current_runtime, None, &hw_acc)
        .await;
    let expected_error =
        CollectionError::bad_request("cannot apply Fusion without prefetches".to_string());
    assert!(matches!(sources_scores, Err(err) if err == expected_error));

    // RRF query with single prefetch
    let nearest_query = QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
        VectorInternal::Dense(vec![1.0, 2.0, 3.0, 4.0]),
        DEFAULT_VECTOR_NAME,
    ));
    let inner_limit = 3;
    let nearest_query_prefetch = ShardPrefetch {
        prefetches: vec![], // no recursion here
        query: Some(ScoringQuery::Vector(nearest_query.clone())),
        limit: inner_limit,
        params: None,
        filter: None,
        score_threshold: None,
    };
    let outer_limit = 2;
    let query = ShardQueryRequest {
        prefetches: vec![nearest_query_prefetch.clone()],
        query: Some(ScoringQuery::Fusion(FusionInternal::Rrf)),
        filter: None,
        score_threshold: None,
        limit: outer_limit,
        offset: 0,
        params: None,
        with_vector: WithVector::Bool(false),
        with_payload: WithPayloadInterface::Bool(false),
    };

    let sources_scores = shard
        .query_batch(Arc::new(vec![query]), &current_runtime, None, &hw_acc)
        .await
        .unwrap()
        .pop()
        .unwrap();

    // one score per prefetch
    assert_eq!(sources_scores.len(), 1);

    // in case of top level RFF we need to propagate intermediate results
    // so the number of results is not limited by the outer limit at the shard level
    // the first source returned all its inner results
    assert_eq!(sources_scores[0].len(), inner_limit);
    // no payload/vector were requested
    sources_scores[0].iter().for_each(|scored_point| {
        assert_eq!(scored_point.vector, None);
        assert_eq!(scored_point.payload, None);
    });

    // RRF query with two prefetches
    let inner_limit = 3;
    let nearest_query_prefetch = ShardPrefetch {
        prefetches: vec![], // no recursion here
        query: Some(ScoringQuery::Vector(nearest_query.clone())),
        limit: inner_limit,
        params: None,
        filter: None,
        score_threshold: None,
    };
    let outer_limit = 2;
    let query = ShardQueryRequest {
        prefetches: vec![
            nearest_query_prefetch.clone(),
            nearest_query_prefetch.clone(),
        ],
        query: Some(ScoringQuery::Fusion(FusionInternal::Rrf)),
        filter: None,
        score_threshold: None,
        limit: outer_limit,
        offset: 0,
        params: None,
        with_vector: WithVector::Bool(false),
        with_payload: WithPayloadInterface::Bool(false),
    };

    let sources_scores = shard
        .query_batch(Arc::new(vec![query]), &current_runtime, None, &hw_acc)
        .await
        .unwrap()
        .pop()
        .unwrap();

    // one score per prefetch
    assert_eq!(sources_scores.len(), 2);

    // in case of top level RFF we need to propagate intermediate results
    // so the number of results is not limited by the outer limit at the shard level
    // the sources returned all inner results
    for source in sources_scores.iter() {
        assert_eq!(source.len(), inner_limit);
    }

    ////// Test that the order of prefetches is preserved in the response //////
    let query = ShardQueryRequest {
        prefetches: vec![
            ShardPrefetch {
                filter: Some(filter_single_id(1)),
                ..nearest_query_prefetch.clone()
            },
            ShardPrefetch {
                filter: Some(filter_single_id(2)),
                ..nearest_query_prefetch.clone()
            },
            ShardPrefetch {
                filter: Some(filter_single_id(3)),
                ..nearest_query_prefetch.clone()
            },
        ],
        query: Some(ScoringQuery::Fusion(FusionInternal::Rrf)),
        filter: None,
        score_threshold: None,
        limit: outer_limit,
        offset: 0,
        params: None,
        with_vector: WithVector::Bool(false),
        with_payload: WithPayloadInterface::Bool(false),
    };

    let sources_scores = shard
        .query_batch(Arc::new(vec![query]), &current_runtime, None, &hw_acc)
        .await
        .unwrap()
        .pop()
        .unwrap();

    // one score per prefetch
    assert_eq!(sources_scores.len(), 3);
    assert!(sources_scores.iter().all(|source| source.len() == 1));
    assert_eq!(sources_scores[0][0].id, PointIdType::NumId(1));
    assert_eq!(sources_scores[1][0].id, PointIdType::NumId(2));
    assert_eq!(sources_scores[2][0].id, PointIdType::NumId(3));
    hw_acc.discard();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shard_query_vector_rescoring() {
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

    let config = create_collection_config();

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
        payload_index_schema,
        current_runtime.clone(),
        current_runtime.clone(),
        CpuBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    let upsert_ops = upsert_operation();

    shard.update(upsert_ops.into(), true).await.unwrap();

    let nearest_query = QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
        VectorInternal::Dense(vec![1.0, 2.0, 3.0, 4.0]),
        DEFAULT_VECTOR_NAME,
    ));
    let inner_limit = 3;

    let nearest_query_prefetch = ShardPrefetch {
        prefetches: vec![], // no recursion here
        query: Some(ScoringQuery::Vector(nearest_query.clone())),
        limit: inner_limit,
        params: None,
        filter: None,
        score_threshold: None,
    };

    // rescoring against a vector without prefetches
    let outer_limit = 2;
    let query = ShardQueryRequest {
        prefetches: vec![],
        query: Some(ScoringQuery::Vector(nearest_query.clone())),
        filter: None,
        score_threshold: None,
        limit: outer_limit,
        offset: 0,
        params: None,
        with_vector: WithVector::Bool(false),
        with_payload: WithPayloadInterface::Bool(false),
    };

    let hw_acc = HwMeasurementAcc::new();
    let sources_scores = shard
        .query_batch(Arc::new(vec![query]), &current_runtime, None, &hw_acc)
        .await
        .unwrap()
        .pop()
        .unwrap();

    // only one inner result in absence of prefetches
    assert_eq!(sources_scores.len(), 1);
    // number of results is limited by the outer limit for rescoring
    assert_eq!(sources_scores[0].len(), outer_limit);

    // rescoring against a vector with single prefetch
    let outer_limit = 2;
    let query = ShardQueryRequest {
        prefetches: vec![nearest_query_prefetch.clone()],
        query: Some(ScoringQuery::Vector(nearest_query.clone())),
        filter: None,
        score_threshold: None,
        limit: outer_limit,
        offset: 0,
        params: None,
        with_vector: WithVector::Bool(false),
        with_payload: WithPayloadInterface::Bool(false),
    };

    let sources_scores = shard
        .query_batch(Arc::new(vec![query]), &current_runtime, None, &hw_acc)
        .await
        .unwrap()
        .pop()
        .unwrap();

    // only one inner result in absence of prefetches
    assert_eq!(sources_scores.len(), 1);
    // number of results is limited by the outer limit for vector rescoring
    assert_eq!(sources_scores[0].len(), outer_limit);

    // rescoring against a vector with two fetches
    let outer_limit = 2;
    let query = ShardQueryRequest {
        prefetches: vec![
            nearest_query_prefetch.clone(),
            nearest_query_prefetch.clone(),
        ],
        query: Some(ScoringQuery::Vector(nearest_query)),
        filter: None,
        score_threshold: None,
        limit: outer_limit,
        offset: 0,
        params: None,
        with_vector: WithVector::Bool(false),
        with_payload: WithPayloadInterface::Bool(false),
    };

    let sources_scores = shard
        .query_batch(Arc::new(vec![query]), &current_runtime, None, &hw_acc)
        .await
        .unwrap()
        .pop()
        .unwrap();

    // only one inner result in absence of fusion
    assert_eq!(sources_scores.len(), 1);
    // merging taking place
    // number of results is limited by the outer limit for vector rescoring
    assert_eq!(sources_scores[0].len(), outer_limit);
    hw_acc.discard();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shard_query_payload_vector() {
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

    let config = create_collection_config();

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
        payload_index_schema,
        current_runtime.clone(),
        current_runtime.clone(),
        CpuBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    let upsert_ops = upsert_operation();

    shard.update(upsert_ops.into(), true).await.unwrap();

    let nearest_query = QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
        VectorInternal::Dense(vec![1.0, 2.0, 3.0, 4.0]),
        DEFAULT_VECTOR_NAME,
    ));

    // rescoring against a vector without prefetches
    let outer_limit = 2;
    let query = ShardQueryRequest {
        prefetches: vec![],
        query: Some(ScoringQuery::Vector(nearest_query)),
        filter: None,
        score_threshold: None,
        limit: outer_limit,
        offset: 0,
        params: None,
        with_vector: WithVector::Bool(true), // requesting vector
        with_payload: WithPayloadInterface::Bool(true), // requesting payload
    };

    let hw_acc = HwMeasurementAcc::new();
    let sources_scores = shard
        .query_batch(Arc::new(vec![query]), &current_runtime, None, &hw_acc)
        .await
        .unwrap()
        .pop()
        .unwrap();
    hw_acc.discard();

    // only one inner result in absence of prefetches
    assert_eq!(sources_scores.len(), 1);
    // number of results is limited by the outer limit for rescoring
    assert_eq!(sources_scores[0].len(), outer_limit);
    // payload/vector were requested
    sources_scores[0].iter().for_each(|scored_point| {
        assert!(scored_point.vector.is_some());
        assert!(scored_point.payload.is_some());
    });
}
