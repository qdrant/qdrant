use std::collections::HashSet;

use collection::operations::payload_ops::{PayloadOps, SetPayload};
use collection::operations::point_ops::{Batch, PointOperations, PointStruct, WriteOrdering};
use collection::operations::types::{
    CountRequest, PointRequest, RecommendRequest, ScrollRequest, SearchRequest, UpdateStatus,
};
use collection::operations::CollectionUpdateOperations;
use collection::recommendations::recommend_by;
use itertools::Itertools;
use segment::data_types::vectors::VectorStruct;
use segment::types::{
    Condition, FieldCondition, Filter, HasIdCondition, Payload, PointIdType, WithPayloadInterface,
};
use tempfile::Builder;

use crate::common::{load_local_collection, simple_collection_fixture, N_SHARDS};

mod common;

#[tokio::test]
async fn test_collection_updater() {
    test_collection_updater_with_shards(1).await;
    test_collection_updater_with_shards(N_SHARDS).await;
}

async fn test_collection_updater_with_shards(shard_number: u32) {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();

    let mut collection = simple_collection_fixture(collection_dir.path(), shard_number).await;

    let insert_points = CollectionUpdateOperations::PointOperation(
        Batch {
            ids: vec![0, 1, 2, 3, 4]
                .into_iter()
                .map(|x| x.into())
                .collect_vec(),
            vectors: vec![
                vec![1.0, 0.0, 1.0, 1.0],
                vec![1.0, 0.0, 1.0, 0.0],
                vec![1.0, 1.0, 1.0, 1.0],
                vec![1.0, 1.0, 0.0, 1.0],
                vec![1.0, 0.0, 0.0, 0.0],
            ]
            .into(),
            payloads: None,
        }
        .into(),
    );

    let insert_result = collection
        .update_from_client(insert_points, true, WriteOrdering::default())
        .await;

    match insert_result {
        Ok(res) => {
            assert_eq!(res.status, UpdateStatus::Completed)
        }
        Err(err) => panic!("operation failed: {err:?}"),
    }

    let search_request = SearchRequest {
        vector: vec![1.0, 1.0, 1.0, 1.0].into(),
        with_payload: None,
        with_vector: None,
        filter: None,
        params: None,
        limit: 3,
        offset: 0,
        score_threshold: None,
    };

    let search_res = collection.search(search_request, None, None).await;

    match search_res {
        Ok(res) => {
            assert_eq!(res.len(), 3);
            assert_eq!(res[0].id, 2.into());
            assert!(res[0].payload.is_none());
        }
        Err(err) => panic!("search failed: {err:?}"),
    }
    collection.before_drop().await;
}

#[tokio::test]
async fn test_collection_search_with_payload_and_vector() {
    test_collection_search_with_payload_and_vector_with_shards(1).await;
    test_collection_search_with_payload_and_vector_with_shards(N_SHARDS).await;
}

async fn test_collection_search_with_payload_and_vector_with_shards(shard_number: u32) {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();

    let mut collection = simple_collection_fixture(collection_dir.path(), shard_number).await;

    let insert_points = CollectionUpdateOperations::PointOperation(
        Batch {
            ids: vec![0.into(), 1.into()],
            vectors: vec![vec![1.0, 0.0, 1.0, 1.0], vec![1.0, 0.0, 1.0, 0.0]].into(),
            payloads: serde_json::from_str(
                r#"[{ "k": { "type": "keyword", "value": "v1" } }, { "k": "v2" , "v": "v3"}]"#,
            )
            .unwrap(),
        }
        .into(),
    );

    let insert_result = collection
        .update_from_client(insert_points, true, WriteOrdering::default())
        .await;

    match insert_result {
        Ok(res) => {
            assert_eq!(res.status, UpdateStatus::Completed)
        }
        Err(err) => panic!("operation failed: {err:?}"),
    }

    let search_request = SearchRequest {
        vector: vec![1.0, 0.0, 1.0, 1.0].into(),
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: Some(true.into()),
        filter: None,
        params: None,
        limit: 3,
        offset: 0,
        score_threshold: None,
    };

    let search_res = collection.search(search_request, None, None).await;

    match search_res {
        Ok(res) => {
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].id, 0.into());
            assert_eq!(res[0].payload.as_ref().unwrap().len(), 1);
            match &res[0].vector {
                Some(VectorStruct::Single(v)) => assert_eq!(v, &vec![1.0, 0.0, 1.0, 1.0]),
                _ => panic!("vector is not returned"),
            }
        }
        Err(err) => panic!("search failed: {err:?}"),
    }

    let count_request = CountRequest {
        filter: Some(Filter::new_must(Condition::Field(FieldCondition {
            key: "k".to_string(),
            r#match: Some(serde_json::from_str(r#"{ "value": "v2" }"#).unwrap()),
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            values_count: None,
        }))),
        exact: true,
    };

    let count_res = collection.count(count_request, None).await.unwrap();
    assert_eq!(count_res.count, 1);

    collection.before_drop().await;
}

// FIXME: dos not work
#[tokio::test]
async fn test_collection_loading() {
    test_collection_loading_with_shards(1).await;
    test_collection_loading_with_shards(N_SHARDS).await;
}

async fn test_collection_loading_with_shards(shard_number: u32) {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();

    {
        let mut collection = simple_collection_fixture(collection_dir.path(), shard_number).await;
        let insert_points = CollectionUpdateOperations::PointOperation(
            Batch {
                ids: vec![0, 1, 2, 3, 4]
                    .into_iter()
                    .map(|x| x.into())
                    .collect_vec(),
                vectors: vec![
                    vec![1.0, 0.0, 1.0, 1.0],
                    vec![1.0, 0.0, 1.0, 0.0],
                    vec![1.0, 1.0, 1.0, 1.0],
                    vec![1.0, 1.0, 0.0, 1.0],
                    vec![1.0, 0.0, 0.0, 0.0],
                ]
                .into(),
                payloads: None,
            }
            .into(),
        );

        collection
            .update_from_client(insert_points, true, WriteOrdering::default())
            .await
            .unwrap();

        let payload: Payload = serde_json::from_str(r#"{"color":"red"}"#).unwrap();

        let assign_payload =
            CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(SetPayload {
                payload,
                points: Some(vec![2.into(), 3.into()]),
                filter: None,
            }));

        collection
            .update_from_client(assign_payload, true, WriteOrdering::default())
            .await
            .unwrap();
        collection.before_drop().await;
    }

    let collection_path = collection_dir.path();
    let mut loaded_collection = load_local_collection(
        "test".to_string(),
        collection_path,
        &collection_path.join("snapshots"),
    )
    .await;
    let request = PointRequest {
        ids: vec![1.into(), 2.into()],
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: true.into(),
    };
    let retrieved = loaded_collection
        .retrieve(request, None, None)
        .await
        .unwrap();

    assert_eq!(retrieved.len(), 2);

    for record in retrieved {
        if record.id == 2.into() {
            let non_empty_payload = record.payload.unwrap();

            assert_eq!(non_empty_payload.len(), 1)
        }
    }
    println!("Function end");
    loaded_collection.before_drop().await;
}

#[test]
fn test_deserialization() {
    let insert_points = CollectionUpdateOperations::PointOperation(
        Batch {
            ids: vec![0.into(), 1.into()],
            vectors: vec![vec![1.0, 0.0, 1.0, 1.0], vec![1.0, 0.0, 1.0, 0.0]].into(),
            payloads: None,
        }
        .into(),
    );
    let json_str = serde_json::to_string_pretty(&insert_points).unwrap();

    let _read_obj: CollectionUpdateOperations = serde_json::from_str(&json_str).unwrap();

    let crob_bytes = rmp_serde::to_vec(&insert_points).unwrap();

    let _read_obj2: CollectionUpdateOperations = rmp_serde::from_slice(&crob_bytes).unwrap();
}

#[test]
fn test_deserialization2() {
    let insert_points = CollectionUpdateOperations::PointOperation(
        vec![
            PointStruct {
                id: 0.into(),
                vector: vec![1.0, 0.0, 1.0, 1.0].into(),
                payload: None,
            },
            PointStruct {
                id: 1.into(),
                vector: vec![1.0, 0.0, 1.0, 0.0].into(),
                payload: None,
            },
        ]
        .into(),
    );

    let json_str = serde_json::to_string_pretty(&insert_points).unwrap();

    let _read_obj: CollectionUpdateOperations = serde_json::from_str(&json_str).unwrap();

    let raw_bytes = rmp_serde::to_vec(&insert_points).unwrap();

    let _read_obj2: CollectionUpdateOperations = rmp_serde::from_slice(&raw_bytes).unwrap();
}

// Request to find points sent to all shards but they might not have a particular id, so they will return an error
#[tokio::test]
async fn test_recommendation_api() {
    test_recommendation_api_with_shards(1).await;
    test_recommendation_api_with_shards(N_SHARDS).await;
}

async fn test_recommendation_api_with_shards(shard_number: u32) {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();
    let mut collection = simple_collection_fixture(collection_dir.path(), shard_number).await;

    let insert_points = CollectionUpdateOperations::PointOperation(
        Batch {
            ids: vec![0, 1, 2, 3, 4, 5, 6, 7, 8]
                .into_iter()
                .map(|x| x.into())
                .collect_vec(),
            vectors: vec![
                vec![0.0, 0.0, 1.0, 1.0],
                vec![1.0, 0.0, 0.0, 0.0],
                vec![1.0, 0.0, 0.0, 0.0],
                vec![0.0, 1.0, 0.0, 0.0],
                vec![0.0, 1.0, 0.0, 0.0],
                vec![0.0, 0.0, 1.0, 0.0],
                vec![0.0, 0.0, 1.0, 0.0],
                vec![0.0, 0.0, 0.0, 1.0],
                vec![0.0, 0.0, 0.0, 1.0],
            ]
            .into(),
            payloads: None,
        }
        .into(),
    );

    collection
        .update_from_client(insert_points, true, WriteOrdering::default())
        .await
        .unwrap();
    let result = recommend_by(
        RecommendRequest {
            positive: vec![0.into()],
            negative: vec![8.into()],
            limit: 5,
            ..Default::default()
        },
        &collection,
        |_name| async { unreachable!("Should not be called in this test") },
        None,
    )
    .await
    .unwrap();
    assert!(!result.is_empty());
    let top1 = &result[0];

    assert!(top1.id == 5.into() || top1.id == 6.into());
    collection.before_drop().await;
}

#[tokio::test]
async fn test_read_api() {
    test_read_api_with_shards(1).await;
    test_read_api_with_shards(N_SHARDS).await;
}

async fn test_read_api_with_shards(shard_number: u32) {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();
    let mut collection = simple_collection_fixture(collection_dir.path(), shard_number).await;

    let insert_points = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        Batch {
            ids: vec![0, 1, 2, 3, 4, 5, 6, 7, 8]
                .into_iter()
                .map(|x| x.into())
                .collect_vec(),
            vectors: vec![
                vec![0.0, 0.0, 1.0, 1.0],
                vec![1.0, 0.0, 0.0, 0.0],
                vec![1.0, 0.0, 0.0, 0.0],
                vec![0.0, 1.0, 0.0, 0.0],
                vec![0.0, 1.0, 0.0, 0.0],
                vec![0.0, 0.0, 1.0, 0.0],
                vec![0.0, 0.0, 1.0, 0.0],
                vec![0.0, 0.0, 0.0, 1.0],
                vec![0.0, 0.0, 0.0, 1.0],
            ]
            .into(),
            payloads: None,
        }
        .into(),
    ));

    collection
        .update_from_client(insert_points, true, WriteOrdering::default())
        .await
        .unwrap();

    let result = collection
        .scroll_by(
            ScrollRequest {
                offset: None,
                limit: Some(2),
                filter: None,
                with_payload: Some(WithPayloadInterface::Bool(true)),
                with_vector: false.into(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    assert_eq!(result.next_page_offset, Some(2.into()));
    assert_eq!(result.points.len(), 2);
    collection.before_drop().await;
}

#[tokio::test]
async fn test_collection_delete_points_by_filter() {
    test_collection_delete_points_by_filter_with_shards(1).await;
    test_collection_delete_points_by_filter_with_shards(N_SHARDS).await;
}

async fn test_collection_delete_points_by_filter_with_shards(shard_number: u32) {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();

    let mut collection = simple_collection_fixture(collection_dir.path(), shard_number).await;

    let insert_points = CollectionUpdateOperations::PointOperation(
        Batch {
            ids: vec![0, 1, 2, 3, 4]
                .into_iter()
                .map(|x| x.into())
                .collect_vec(),
            vectors: vec![
                vec![1.0, 0.0, 1.0, 1.0],
                vec![1.0, 0.0, 1.0, 0.0],
                vec![1.0, 1.0, 1.0, 1.0],
                vec![1.0, 1.0, 0.0, 1.0],
                vec![1.0, 0.0, 0.0, 0.0],
            ]
            .into(),
            payloads: None,
        }
        .into(),
    );

    let insert_result = collection
        .update_from_client(insert_points, true, WriteOrdering::default())
        .await;

    match insert_result {
        Ok(res) => {
            assert_eq!(res.status, UpdateStatus::Completed)
        }
        Err(err) => panic!("operation failed: {err:?}"),
    }

    // delete points with id (0, 3)
    let to_be_deleted: HashSet<PointIdType> = vec![0.into(), 3.into()].into_iter().collect();
    let delete_filter = segment::types::Filter {
        should: None,
        must: Some(vec![Condition::HasId(HasIdCondition::from(to_be_deleted))]),
        must_not: None,
    };

    let delete_points = CollectionUpdateOperations::PointOperation(
        PointOperations::DeletePointsByFilter(delete_filter),
    );

    let delete_result = collection
        .update_from_client(delete_points, true, WriteOrdering::default())
        .await;

    match delete_result {
        Ok(res) => {
            assert_eq!(res.status, UpdateStatus::Completed)
        }
        Err(err) => panic!("operation failed: {err:?}"),
    }

    let result = collection
        .scroll_by(
            ScrollRequest {
                offset: None,
                limit: Some(10),
                filter: None,
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: false.into(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // check if we only have 3 out of 5 points left and that the point id were really deleted
    assert_eq!(result.points.len(), 3);
    assert_eq!(result.points.get(0).unwrap().id, 1.into());
    assert_eq!(result.points.get(1).unwrap().id, 2.into());
    assert_eq!(result.points.get(2).unwrap().id, 4.into());
    collection.before_drop().await;
}

mod grouping {

    use collection::collection::Collection;
    use collection::grouping::group_by::{group_by, GroupRequest, SourceRequest};
    use collection::operations::consistency_params::ReadConsistency;
    use collection::shards::shard::ShardId;
    use rand::distributions::Uniform;
    use rand::rngs::ThreadRng;
    use rand::Rng;
    use segment::data_types::vectors::VectorType;
    use segment::types::WithVector;
    use serde_json::json;

    use super::*;

    struct Resources {
        request: GroupRequest,
        collection: Collection,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
    }

    fn rand_vector(rng: &mut ThreadRng, size: usize) -> VectorType {
        rng.sample_iter(Uniform::new(0.4, 0.6)).take(size).collect()
    }

    async fn setup(docs: u64, chunks: u64) -> Resources {
        let mut rng = rand::thread_rng();

        let source = SourceRequest::Search(SearchRequest {
            vector: vec![0.5, 0.5, 0.5, 0.5].into(),
            filter: None,
            params: None,
            limit: 4,
            offset: 0,
            with_payload: None,
            with_vector: None,
            score_threshold: None,
        });

        let request = GroupRequest::new(source, "docId".to_string(), 3);

        let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();

        let collection = simple_collection_fixture(collection_dir.path(), 1).await;

        let insert_points = CollectionUpdateOperations::PointOperation(
            Batch {
                // 8 docs, 4 chunks per doc
                ids: (0..docs * chunks).map(|x| x.into()).collect_vec(),
                vectors: (0..docs * chunks)
                    .map(|_| rand_vector(&mut rng, 4))
                    .collect_vec()
                    .into(),
                payloads: (0..docs)
                    .flat_map(|x| {
                        (0..chunks).map(move |_| {
                            Some(Payload::from(
                                json!({ "docId": x , "other_stuff": x.to_string() + "foo" }),
                            ))
                        })
                    })
                    .collect_vec()
                    .into(),
            }
            .into(),
        );

        let insert_result = collection
            .update_from_client(insert_points, true, WriteOrdering::default())
            .await
            .expect("insert failed");

        assert_eq!(insert_result.status, UpdateStatus::Completed);

        Resources {
            request,
            collection,
            read_consistency: None,
            shard_selection: None,
        }
    }

    #[tokio::test]
    async fn searching() {
        let resources = setup(16, 8).await;

        let result = group_by(
            resources.request.clone(),
            &resources.collection,
            Some(|_name| async { unreachable!() }),
            resources.read_consistency,
            resources.shard_selection,
        )
        .await;

        assert!(result.is_ok());

        let result = result.unwrap();

        let group_req = resources.request;

        assert_eq!(result.len(), group_req.groups);
        assert_eq!(result[0].hits.len(), group_req.top);

        // is sorted?
        let mut last_group_best_score = f32::MAX;
        for group in result {
            assert!(group.hits[0].score <= last_group_best_score);
            last_group_best_score = group.hits[0].score;

            let mut last_score = f32::MAX;
            for hit in group.hits {
                assert!(hit.score <= last_score);
                last_score = hit.score;
            }
        }
    }

    #[tokio::test]
    async fn recommending() {
        let resources = setup(16, 8).await;

        let request = GroupRequest::new(
            SourceRequest::Recommend(RecommendRequest {
                filter: None,
                params: None,
                limit: 4,
                offset: 0,
                with_payload: None,
                with_vector: None,
                score_threshold: None,
                positive: vec![1.into(), 2.into(), 3.into()],
                negative: Vec::new(),
                using: None,
                lookup_from: None,
            }),
            "docId".to_string(),
            2,
        );

        let result = group_by(
            request.clone(),
            &resources.collection,
            Some(|_name| async { unreachable!() }),
            resources.read_consistency,
            resources.shard_selection,
        )
        .await;

        assert!(result.is_ok());

        let result = result.unwrap();

        assert_eq!(result.len(), request.groups);

        let mut last_group_best_score = f32::MAX;
        for group in result {
            assert_eq!(group.hits.len(), request.top);

            // is sorted?
            assert!(group.hits[0].score <= last_group_best_score);
            last_group_best_score = group.hits[0].score;

            let mut last_score = f32::MAX;
            for hit in group.hits {
                assert!(hit.score <= last_score);
                last_score = hit.score;
            }
        }
    }

    #[tokio::test]
    async fn with_filter() {
        let resources = setup(16, 8).await;

        let filter: Filter = serde_json::from_value(json!({
            "must": [
                {
                    "key": "docId",
                    "range": {
                        "gte": 1,
                        "lte": 2
                    }
                }
            ]
        }))
        .unwrap();

        let group_by_request = GroupRequest::new(
            SourceRequest::Search(SearchRequest {
                vector: vec![0.5, 0.5, 0.5, 0.5].into(),
                filter: Some(filter.clone()),
                params: None,
                limit: 4,
                offset: 0,
                with_payload: None,
                with_vector: None,
                score_threshold: None,
            }),
            "docId".to_string(),
            3,
        );

        let result = group_by(
            group_by_request,
            &resources.collection,
            Some(|_name| async { unreachable!() }),
            resources.read_consistency,
            resources.shard_selection,
        )
        .await;

        assert!(result.is_ok());

        let result = result.unwrap();

        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn with_payload_and_vectors() {
        let resources = setup(16, 8).await;

        let group_by_request = GroupRequest::new(
            SourceRequest::Search(SearchRequest {
                vector: vec![0.5, 0.5, 0.5, 0.5].into(),
                filter: None,
                params: None,
                limit: 4,
                offset: 0,
                with_payload: Some(WithPayloadInterface::Bool(true)),
                with_vector: Some(WithVector::Bool(true)),
                score_threshold: None,
            }),
            "docId".to_string(),
            3,
        );

        let result = group_by(
            group_by_request.clone(),
            &resources.collection,
            Some(|_name| async { unreachable!() }),
            resources.read_consistency,
            resources.shard_selection,
        )
        .await;

        assert!(result.is_ok());

        let result = result.unwrap();

        assert_eq!(result.len(), 4);

        for group in result {
            assert_eq!(group.hits.len(), group_by_request.top);
            assert!(group.hits[0].payload.is_some());
            assert!(group.hits[0].vector.is_some());
        }
    }

    #[tokio::test]
    async fn group_by_string_field() {
        let Resources {
            collection,
            read_consistency,
            shard_selection,
            ..
        } = setup(16, 8).await;

        let group_by_request = GroupRequest::new(
            SourceRequest::Search(SearchRequest {
                vector: vec![0.5, 0.5, 0.5, 0.5].into(),
                filter: None,
                params: None,
                limit: 4,
                offset: 0,
                with_payload: Some(WithPayloadInterface::Bool(true)),
                with_vector: Some(WithVector::Bool(true)),
                score_threshold: None,
            }),
            "other_stuff".to_string(),
            3,
        );

        let result = group_by(
            group_by_request.clone(),
            &collection,
            Some(|_name| async { unreachable!() }),
            read_consistency,
            shard_selection,
        )
        .await;

        assert!(result.is_ok());

        let result = result.unwrap();

        assert_eq!(result.len(), 4);

        for group in result {
            assert_eq!(group.hits.len(), group_by_request.top);
        }
    }

    #[tokio::test]
    async fn zero_top_groups() {
        let Resources {
            collection,
            read_consistency,
            shard_selection,
            ..
        } = setup(16, 8).await;

        let group_by_request = GroupRequest::new(
            SourceRequest::Search(SearchRequest {
                vector: vec![0.5, 0.5, 0.5, 0.5].into(),
                filter: None,
                params: None,
                limit: 4,
                offset: 0,
                with_payload: None,
                with_vector: None,
                score_threshold: None,
            }),
            "docId".to_string(),
            0,
        );

        let result = group_by(
            group_by_request.clone(),
            &collection,
            Some(|_name| async { unreachable!() }),
            read_consistency,
            shard_selection,
        )
        .await;

        assert!(result.is_ok());

        let result = result.unwrap();

        assert_eq!(result.len(), 0);
    }

    #[tokio::test]
    async fn zero_limit_groups() {
        let Resources {
            collection,
            read_consistency,
            shard_selection,
            ..
        } = setup(16, 8).await;

        let group_by_request = GroupRequest::new(
            SourceRequest::Search(SearchRequest {
                vector: vec![0.5, 0.5, 0.5, 0.5].into(),
                filter: None,
                params: None,
                limit: 0,
                offset: 0,
                with_payload: None,
                with_vector: None,
                score_threshold: None,
            }),
            "docId".to_string(),
            3,
        );

        let result = group_by(
            group_by_request.clone(),
            &collection,
            Some(|_name| async { unreachable!() }),
            read_consistency,
            shard_selection,
        )
        .await;

        assert!(result.is_ok());

        let result = result.unwrap();

        assert_eq!(result.len(), 0);
    }

    #[tokio::test]
    #[ignore = "Takes more than second to run, too slow for CI"]
    async fn big_limit_groups() {
        let Resources {
            collection,
            read_consistency,
            shard_selection,
            ..
        } = setup(1000, 5).await;

        let group_by_request = GroupRequest::new(
            SourceRequest::Search(SearchRequest {
                vector: vec![0.5, 0.5, 0.5, 0.5].into(),
                filter: None,
                params: None,
                limit: 500,
                offset: 0,
                with_payload: None,
                with_vector: None,
                score_threshold: None,
            }),
            "docId".to_string(),
            3,
        );

        let result = group_by(
            group_by_request.clone(),
            &collection,
            Some(|_name| async { unreachable!() }),
            read_consistency,
            shard_selection,
        )
        .await;

        assert!(result.is_ok());

        let result = result.unwrap();

        assert_eq!(result.len(), group_by_request.groups);

        for group in result {
            assert_eq!(group.hits.len(), group_by_request.top);
        }
    }

    #[tokio::test]
    #[ignore = "Takes more than a second to run, too slow for CI"]
    async fn big_top_groups() {
        let Resources {
            collection,
            read_consistency,
            shard_selection,
            ..
        } = setup(10, 500).await;

        let group_by_request = GroupRequest::new(
            SourceRequest::Search(SearchRequest {
                vector: vec![0.5, 0.5, 0.5, 0.5].into(),
                filter: None,
                params: None,
                limit: 3,
                offset: 0,
                with_payload: None,
                with_vector: None,
                score_threshold: None,
            }),
            "docId".to_string(),
            400,
        );

        let result = group_by(
            group_by_request.clone(),
            &collection,
            Some(|_name| async { unreachable!() }),
            read_consistency,
            shard_selection,
        )
        .await;

        assert!(result.is_ok());

        let result = result.unwrap();

        assert_eq!(result.len(), group_by_request.groups);

        for group in result {
            assert_eq!(group.hits.len(), group_by_request.top);
        }
    }
}
