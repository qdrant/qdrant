use std::sync::Arc;

use common::cpu::CpuBudget;
use segment::data_types::vectors::{NamedVectorStruct, Vector, DEFAULT_VECTOR_NAME};
use segment::types::{WithPayloadInterface, WithVector};
use tempfile::Builder;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::operations::query_enum::QueryEnum;
use crate::operations::types::CollectionError;
use crate::operations::universal_query::shard_query::{
    Fusion, ScoringQuery, ShardPrefetch, ShardQueryRequest,
};
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::ShardOperation;
use crate::tests::fixtures::*;

#[tokio::test(flavor = "multi_thread")]
async fn test_shard_query_rescoring() {
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

    let config = create_collection_config();

    let collection_name = "test".to_string();

    let current_runtime: Handle = Handle::current();

    let shard = LocalShard::build(
        0,
        collection_name.clone(),
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        Arc::new(Default::default()),
        current_runtime.clone(),
        CpuBudget::default(),
    )
    .await
    .unwrap();

    let upsert_ops = upsert_operation();

    shard.update(upsert_ops.into(), true).await.unwrap();

    // RFF query without prefetches
    let query = ShardQueryRequest {
        prefetches: vec![],
        query: ScoringQuery::Fusion(Fusion::Rrf),
        filter: None,
        score_threshold: None,
        limit: 0,
        offset: 0,
        params: None,
        with_vector: WithVector::Bool(false),
        with_payload: WithPayloadInterface::Bool(false),
    };

    let sources_scores = shard.query(Arc::new(query), &current_runtime).await;
    let expected_error =
        CollectionError::bad_request("cannot make RRF without prefetches".to_string());
    assert!(matches!(sources_scores, Err(err) if err == expected_error));

    // RFF query with prefetches
    let nearest_query = QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
        Vector::Dense(vec![1.0, 2.0, 3.0, 4.0]),
        DEFAULT_VECTOR_NAME,
    ));
    let inner_limit = 3;
    let nearest_query_prefetch = ShardPrefetch {
        prefetches: vec![], // no recursion here
        query: ScoringQuery::Vector(nearest_query.clone()),
        limit: inner_limit,
        params: None,
        filter: None,
        score_threshold: None,
    };
    let outer_limit = 2;
    let query = ShardQueryRequest {
        prefetches: vec![nearest_query_prefetch.clone()],
        query: ScoringQuery::Fusion(Fusion::Rrf),
        filter: None,
        score_threshold: None,
        limit: outer_limit,
        offset: 0,
        params: None,
        with_vector: WithVector::Bool(false),
        with_payload: WithPayloadInterface::Bool(false),
    };

    let sources_scores = shard
        .query(Arc::new(query), &current_runtime)
        .await
        .unwrap();

    // only one inner prefetch
    assert_eq!(sources_scores.len(), 1);
    // number of results is limited by the outer limit for rescoring
    assert_eq!(sources_scores[0].len(), outer_limit);

    // rescoring against a vector without prefetches
    let outer_limit = 2;
    let query = ShardQueryRequest {
        prefetches: vec![],
        query: ScoringQuery::Vector(nearest_query.clone()),
        filter: None,
        score_threshold: None,
        limit: outer_limit,
        offset: 0,
        params: None,
        with_vector: WithVector::Bool(false),
        with_payload: WithPayloadInterface::Bool(false),
    };

    let sources_scores = shard
        .query(Arc::new(query), &current_runtime)
        .await
        .unwrap();

    // only one inner result in absence of prefetches
    assert_eq!(sources_scores.len(), 1);
    // number of results is limited by the outer limit for rescoring
    assert_eq!(sources_scores[0].len(), outer_limit);

    // rescoring against a vector with prefetches
    let outer_limit = 2;
    let query = ShardQueryRequest {
        prefetches: vec![nearest_query_prefetch],
        query: ScoringQuery::Vector(nearest_query),
        filter: None,
        score_threshold: None,
        limit: outer_limit,
        offset: 0,
        params: None,
        with_vector: WithVector::Bool(false),
        with_payload: WithPayloadInterface::Bool(false),
    };

    let sources_scores = shard
        .query(Arc::new(query), &current_runtime)
        .await
        .unwrap();

    // only one inner result in absence of prefetches
    assert_eq!(sources_scores.len(), 1);
    // number of results is limited by the outer limit for rescoring
    assert_eq!(sources_scores[0].len(), outer_limit);
}
