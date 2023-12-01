use std::collections::HashSet;
use std::fs::File;

use collection::operations::payload_ops::{PayloadOps, SetPayloadOp};
use collection::operations::point_ops::{Batch, PointOperations, PointStruct, WriteOrdering};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{
    CountRequestInternal, OrderByInterface, PointRequestInternal, RecommendRequestInternal,
    ScrollRequestInternal, SearchRequestInternal, UpdateStatus,
};
use collection::operations::CollectionUpdateOperations;
use collection::recommendations::recommend_by;
use collection::shards::replica_set::{ReplicaSetState, ReplicaState};
use itertools::Itertools;
use segment::data_types::vectors::VectorStruct;
use segment::types::{
    Condition, Direction, FieldCondition, Filter, HasIdCondition, OrderBy, Payload,
    PayloadFieldSchema, PayloadSchemaType, PointIdType, WithPayloadInterface,
};
use serde_json::Map;
use tempfile::Builder;

use crate::common::{load_local_collection, simple_collection_fixture, N_SHARDS};

#[tokio::test(flavor = "multi_thread")]
async fn test_collection_updater() {
    test_collection_updater_with_shards(1).await;
    test_collection_updater_with_shards(N_SHARDS).await;
}

async fn test_collection_updater_with_shards(shard_number: u32) {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();

    let collection = simple_collection_fixture(collection_dir.path(), shard_number).await;

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
        .update_from_client_simple(insert_points, true, WriteOrdering::default())
        .await;

    match insert_result {
        Ok(res) => {
            assert_eq!(res.status, UpdateStatus::Completed)
        }
        Err(err) => panic!("operation failed: {err:?}"),
    }

    let search_request = SearchRequestInternal {
        vector: vec![1.0, 1.0, 1.0, 1.0].into(),
        with_payload: None,
        with_vector: None,
        filter: None,
        params: None,
        limit: 3,
        offset: None,
        score_threshold: None,
    };

    let search_res = collection
        .search(
            search_request.into(),
            None,
            &ShardSelectorInternal::All,
            None,
        )
        .await;

    match search_res {
        Ok(res) => {
            assert_eq!(res.len(), 3);
            assert_eq!(res[0].id, 2.into());
            assert!(res[0].payload.is_none());
        }
        Err(err) => panic!("search failed: {err:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_collection_search_with_payload_and_vector() {
    test_collection_search_with_payload_and_vector_with_shards(1).await;
    test_collection_search_with_payload_and_vector_with_shards(N_SHARDS).await;
}

async fn test_collection_search_with_payload_and_vector_with_shards(shard_number: u32) {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();

    let collection = simple_collection_fixture(collection_dir.path(), shard_number).await;

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
        .update_from_client_simple(insert_points, true, WriteOrdering::default())
        .await;

    match insert_result {
        Ok(res) => {
            assert_eq!(res.status, UpdateStatus::Completed)
        }
        Err(err) => panic!("operation failed: {err:?}"),
    }

    let search_request = SearchRequestInternal {
        vector: vec![1.0, 0.0, 1.0, 1.0].into(),
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: Some(true.into()),
        filter: None,
        params: None,
        limit: 3,
        offset: None,
        score_threshold: None,
    };

    let search_res = collection
        .search(
            search_request.into(),
            None,
            &ShardSelectorInternal::All,
            None,
        )
        .await;

    match search_res {
        Ok(res) => {
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].id, 0.into());
            assert_eq!(res[0].payload.as_ref().unwrap().len(), 1);
            let vec = vec![1.0, 0.0, 1.0, 1.0];
            match &res[0].vector {
                Some(VectorStruct::Single(v)) => assert_eq!(v.clone(), vec),
                _ => panic!("vector is not returned"),
            }
        }
        Err(err) => panic!("search failed: {err:?}"),
    }

    let count_request = CountRequestInternal {
        filter: Some(Filter::new_must(Condition::Field(FieldCondition {
            key: "k".to_string(),
            r#match: Some(serde_json::from_str(r#"{ "value": "v2" }"#).unwrap()),
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            values_count: None,
            geo_polygon: None,
        }))),
        exact: true,
    };

    let count_res = collection
        .count(count_request, None, &ShardSelectorInternal::All)
        .await
        .unwrap();
    assert_eq!(count_res.count, 1);
}

// FIXME: does not work
#[tokio::test(flavor = "multi_thread")]
async fn test_collection_loading() {
    test_collection_loading_with_shards(1).await;
    test_collection_loading_with_shards(N_SHARDS).await;
}

async fn test_collection_loading_with_shards(shard_number: u32) {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();

    {
        let collection = simple_collection_fixture(collection_dir.path(), shard_number).await;
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
            .update_from_client_simple(insert_points, true, WriteOrdering::default())
            .await
            .unwrap();

        let payload: Payload = serde_json::from_str(r#"{"color":"red"}"#).unwrap();

        let assign_payload =
            CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(SetPayloadOp {
                payload,
                points: Some(vec![2.into(), 3.into()]),
                filter: None,
            }));

        collection
            .update_from_client_simple(assign_payload, true, WriteOrdering::default())
            .await
            .unwrap();
    }

    let collection_path = collection_dir.path();
    let loaded_collection = load_local_collection(
        "test".to_string(),
        collection_path,
        &collection_path.join("snapshots"),
    )
    .await;
    let request = PointRequestInternal {
        ids: vec![1.into(), 2.into()],
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: true.into(),
    };
    let retrieved = loaded_collection
        .retrieve(request, None, &ShardSelectorInternal::All)
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
#[tokio::test(flavor = "multi_thread")]
async fn test_recommendation_api() {
    test_recommendation_api_with_shards(1).await;
    test_recommendation_api_with_shards(N_SHARDS).await;
}

async fn test_recommendation_api_with_shards(shard_number: u32) {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = simple_collection_fixture(collection_dir.path(), shard_number).await;

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
        .update_from_client_simple(insert_points, true, WriteOrdering::default())
        .await
        .unwrap();
    let result = recommend_by(
        RecommendRequestInternal {
            positive: vec![0.into()],
            negative: vec![8.into()],
            limit: 5,
            ..Default::default()
        },
        &collection,
        |_name| async { unreachable!("Should not be called in this test") },
        None,
        ShardSelectorInternal::All,
        None,
    )
    .await
    .unwrap();
    assert!(!result.is_empty());
    let top1 = &result[0];

    assert!(top1.id == 5.into() || top1.id == 6.into());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_api() {
    test_read_api_with_shards(1).await;
    test_read_api_with_shards(N_SHARDS).await;
}

async fn test_read_api_with_shards(shard_number: u32) {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = simple_collection_fixture(collection_dir.path(), shard_number).await;

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
        .update_from_client_simple(insert_points, true, WriteOrdering::default())
        .await
        .unwrap();

    let result = collection
        .scroll_by(
            ScrollRequestInternal {
                offset: None,
                limit: Some(2),
                filter: None,
                with_payload: Some(WithPayloadInterface::Bool(true)),
                with_vector: false.into(),
                order_by: None,
            },
            None,
            &ShardSelectorInternal::All,
        )
        .await
        .unwrap();

    assert_eq!(result.next_page_offset, Some(2.into()));
    assert_eq!(result.points.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ordered_read_api() {
    test_ordered_scroll_api_with_shards(1).await;
    test_ordered_scroll_api_with_shards(N_SHARDS).await;
}

async fn test_ordered_scroll_api_with_shards(shard_number: u32) {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = simple_collection_fixture(collection_dir.path(), shard_number).await;

    fn get_float_payload(value: f64) -> Option<Payload> {
        let mut payload_map = Map::new();
        payload_map.insert("price".to_string(), (value).into());
        payload_map.insert("price_int".to_string(), (value as i64).into());
        Some(Payload(payload_map))
    }

    let payloads: Vec<Option<Payload>> = vec![
        get_float_payload(11.0),
        get_float_payload(10.0),
        get_float_payload(9.0),
        get_float_payload(8.0),
        get_float_payload(7.0),
        get_float_payload(6.0),
        get_float_payload(5.0),
        get_float_payload(5.0),
        get_float_payload(5.0),
        get_float_payload(5.0),
        get_float_payload(4.0),
        get_float_payload(3.0),
        get_float_payload(2.0),
        get_float_payload(1.0),
    ];
    let insert_points = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        Batch {
            ids: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
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
                vec![0.0, 1.0, 1.0, 1.0],
                vec![0.0, 1.0, 1.0, 1.0],
                vec![0.0, 1.0, 1.0, 1.0],
                vec![0.0, 1.0, 1.0, 1.0],
                vec![1.0, 1.0, 1.0, 1.0],
            ]
            .into(),
            payloads: Some(payloads),
        }
        .into(),
    ));

    collection
        .update_from_client_simple(insert_points, true, WriteOrdering::default())
        .await
        .unwrap();

    collection
        .create_payload_index_with_wait(
            "price".to_string(),
            PayloadFieldSchema::FieldType(PayloadSchemaType::Float),
            true,
        )
        .await
        .unwrap();

    collection
        .create_payload_index_with_wait(
            "price_int".to_string(),
            PayloadFieldSchema::FieldType(PayloadSchemaType::Integer),
            true,
        )
        .await
        .unwrap();

    for key in ["price", "price_int"] {
        let result_asc = collection
            .scroll_by(
                ScrollRequestInternal {
                    offset: None,
                    limit: Some(3),
                    filter: None,
                    with_payload: Some(WithPayloadInterface::Bool(true)),
                    with_vector: false.into(),
                    order_by: Some(OrderByInterface::Struct(OrderBy {
                        key: key.into(),
                        direction: Some(Direction::Asc),
                        value_offset: None,
                    })),
                },
                None,
                &ShardSelectorInternal::All,
            )
            .await
            .unwrap();

        assert_eq!(result_asc.points.len(), 3);
        assert_eq!(result_asc.next_page_offset, Some(10.into()));

        let result_desc = collection
            .scroll_by(
                ScrollRequestInternal {
                    offset: None,
                    limit: Some(5),
                    filter: None,
                    with_payload: Some(WithPayloadInterface::Bool(true)),
                    with_vector: false.into(),
                    order_by: Some(OrderByInterface::Struct(OrderBy {
                        key: key.into(),
                        direction: Some(Direction::Desc),
                        value_offset: None,
                    })),
                },
                None,
                &ShardSelectorInternal::All,
            )
            .await
            .unwrap();

        assert_eq!(result_desc.points.len(), 5);
        assert_eq!(result_desc.next_page_offset, Some(5.into()));

        let asc_second_page = collection
            .scroll_by(
                ScrollRequestInternal {
                    offset: Some(7.into()),
                    limit: Some(5),
                    filter: None,
                    with_payload: Some(WithPayloadInterface::Bool(true)),
                    with_vector: false.into(),
                    order_by: Some(OrderByInterface::Struct(OrderBy {
                        key: key.into(),
                        direction: Some(Direction::Asc),
                        value_offset: None,
                    })),
                },
                None,
                &ShardSelectorInternal::All,
            )
            .await
            .unwrap();

        assert_eq!(asc_second_page.points.len(), 5);
        assert_eq!(asc_second_page.next_page_offset, Some(3.into()));

        let desc_second_page = collection
            .scroll_by(
                ScrollRequestInternal {
                    offset: Some(7.into()),
                    limit: Some(4),
                    filter: None,
                    with_payload: Some(WithPayloadInterface::Bool(true)),
                    with_vector: false.into(),
                    order_by: Some(OrderByInterface::Struct(OrderBy {
                        key: key.into(),
                        direction: Some(Direction::Desc),
                        value_offset: None,
                    })),
                },
                None,
                &ShardSelectorInternal::All,
            )
            .await
            .unwrap();

        assert_eq!(desc_second_page.points.len(), 4);
        // Order is in (value, id), so should return ids [7, 6, 10, 11] and then offset id is 12
        assert_eq!(desc_second_page.next_page_offset, Some(12.into()));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_collection_delete_points_by_filter() {
    test_collection_delete_points_by_filter_with_shards(1).await;
    test_collection_delete_points_by_filter_with_shards(N_SHARDS).await;
}

async fn test_collection_delete_points_by_filter_with_shards(shard_number: u32) {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();

    let collection = simple_collection_fixture(collection_dir.path(), shard_number).await;

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
        .update_from_client_simple(insert_points, true, WriteOrdering::default())
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
        .update_from_client_simple(delete_points, true, WriteOrdering::default())
        .await;

    match delete_result {
        Ok(res) => {
            assert_eq!(res.status, UpdateStatus::Completed)
        }
        Err(err) => panic!("operation failed: {err:?}"),
    }

    let result = collection
        .scroll_by(
            ScrollRequestInternal {
                offset: None,
                limit: Some(10),
                filter: None,
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: false.into(),
                order_by: None,
            },
            None,
            &ShardSelectorInternal::All,
        )
        .await
        .unwrap();

    // check if we only have 3 out of 5 points left and that the point id were really deleted
    assert_eq!(result.points.len(), 3);
    assert_eq!(result.points.first().unwrap().id, 1.into());
    assert_eq!(result.points.get(1).unwrap().id, 2.into());
    assert_eq!(result.points.get(2).unwrap().id, 4.into());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_collection_local_load_initializing_not_stuck() {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();

    // Create and unload collection
    simple_collection_fixture(collection_dir.path(), 1).await;

    // Modify replica state file on disk, set state to Initializing
    // This is to simulate a situation where a collection was not fully created, we cannot create
    // this situation through our collection interface
    {
        let replica_state_path = collection_dir.path().join("0/replica_state.json");
        let replica_state_file = File::open(&replica_state_path).unwrap();
        let mut replica_set_state: ReplicaSetState =
            serde_json::from_reader(replica_state_file).unwrap();

        for peer_id in replica_set_state.peers().into_keys() {
            replica_set_state.set_peer_state(peer_id, ReplicaState::Initializing);
        }

        let replica_state_file = File::create(&replica_state_path).unwrap();
        serde_json::to_writer(replica_state_file, &replica_set_state).unwrap();
    }

    // Reload collection
    let collection_path = collection_dir.path();
    let loaded_collection = load_local_collection(
        "test".to_string(),
        collection_path,
        &collection_path.join("snapshots"),
    )
    .await;

    // Local replica must be in Active state after loading (all replicas are local)
    let loaded_state = loaded_collection.state().await;
    for shard_info in loaded_state.shards.values() {
        for replica_state in shard_info.replicas.values() {
            assert_eq!(replica_state, &ReplicaState::Active);
        }
    }
}
