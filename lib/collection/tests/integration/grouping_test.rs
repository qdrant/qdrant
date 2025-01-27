use collection::collection::Collection;
use collection::grouping::group_by::{GroupRequest, SourceRequest};
use collection::operations::point_ops::WriteOrdering;
use collection::operations::types::{RecommendRequestInternal, UpdateStatus};
use collection::operations::CollectionUpdateOperations;
use itertools::Itertools;
use rand::distributions::Uniform;
use rand::rngs::ThreadRng;
use rand::Rng;
use segment::data_types::vectors::DenseVector;
use segment::json_path::JsonPath;
use segment::types::{Filter, WithPayloadInterface, WithVector};
use serde_json::json;

use crate::common::simple_collection_fixture;

fn rand_dense_vector(rng: &mut ThreadRng, size: usize) -> DenseVector {
    rng.sample_iter(Uniform::new(0.4, 0.6)).take(size).collect()
}

mod group_by {
    use api::rest::SearchRequestInternal;
    use collection::grouping::GroupBy;
    use collection::operations::point_ops::{
        BatchPersisted, BatchVectorStructPersisted, PointInsertOperationsInternal, PointOperations,
    };
    use common::counter::hardware_accumulator::HwMeasurementAcc;
    use segment::payload_json;

    use super::*;

    struct Resources {
        request: GroupRequest,
        collection: Collection,
    }

    async fn setup(docs: u64, chunks: u64) -> Resources {
        let mut rng = rand::thread_rng();

        let source = SourceRequest::Search(SearchRequestInternal {
            vector: vec![0.5, 0.5, 0.5, 0.5].into(),
            filter: None,
            params: None,
            limit: 4,
            offset: None,
            with_payload: None,
            with_vector: None,
            score_threshold: None,
        });

        let request = GroupRequest::with_limit_from_request(source, JsonPath::new("docId"), 3);

        let collection_dir = tempfile::Builder::new()
            .prefix("collection")
            .tempdir()
            .unwrap();

        let collection = simple_collection_fixture(collection_dir.path(), 1).await;

        let batch = BatchPersisted {
            ids: (0..docs * chunks).map(|x| x.into()).collect_vec(),
            vectors: BatchVectorStructPersisted::Single(
                (0..docs * chunks)
                    .map(|_| rand_dense_vector(&mut rng, 4))
                    .collect_vec(),
            ),
            payloads: (0..docs)
                .flat_map(|x| {
                    (0..chunks).map(move |_| {
                        Some(payload_json! { "docId": x , "other_stuff": x.to_string() + "foo" })
                    })
                })
                .collect_vec()
                .into(),
        };

        let insert_points = CollectionUpdateOperations::PointOperation(
            PointOperations::UpsertPoints(PointInsertOperationsInternal::from(batch)),
        );

        let insert_result = collection
            .update_from_client_simple(insert_points, true, WriteOrdering::default())
            .await
            .expect("insert failed");

        assert_eq!(insert_result.status, UpdateStatus::Completed);

        Resources {
            request,
            collection,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn searching() {
        let resources = setup(16, 8).await;

        let hw_acc = HwMeasurementAcc::new();
        let group_by = GroupBy::new(
            resources.request.clone(),
            &resources.collection,
            |_| async { unreachable!() },
            hw_acc,
        );

        let result = group_by.execute().await;

        assert!(result.is_ok());

        let result = result.unwrap();

        let group_req = resources.request;

        assert_eq!(result.len(), group_req.limit);
        assert_eq!(result[0].hits.len(), group_req.group_size);

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

    #[tokio::test(flavor = "multi_thread")]
    async fn recommending() {
        let resources = setup(16, 8).await;

        let request = GroupRequest::with_limit_from_request(
            SourceRequest::Recommend(RecommendRequestInternal {
                strategy: Default::default(),
                filter: None,
                params: None,
                limit: 4,
                offset: None,
                with_payload: None,
                with_vector: None,
                score_threshold: None,
                positive: vec![1.into(), 2.into(), 3.into()],
                negative: Vec::new(),
                using: None,
                lookup_from: None,
            }),
            JsonPath::new("docId"),
            2,
        );

        let hw_acc = HwMeasurementAcc::new();
        let group_by = GroupBy::new(
            request.clone(),
            &resources.collection,
            |_| async { unreachable!() },
            hw_acc,
        );

        let result = group_by.execute().await;

        assert!(result.is_ok());

        let result = result.unwrap();

        assert_eq!(result.len(), request.limit);

        let mut last_group_best_score = f32::MAX;
        for group in result {
            assert_eq!(group.hits.len(), request.group_size);

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

    #[tokio::test(flavor = "multi_thread")]
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

        let group_by_request = GroupRequest::with_limit_from_request(
            SourceRequest::Search(SearchRequestInternal {
                vector: vec![0.5, 0.5, 0.5, 0.5].into(),
                filter: Some(filter.clone()),
                params: None,
                limit: 4,
                offset: None,
                with_payload: None,
                with_vector: None,
                score_threshold: None,
            }),
            JsonPath::new("docId"),
            3,
        );

        let hw_acc = HwMeasurementAcc::new();
        let group_by = GroupBy::new(
            group_by_request,
            &resources.collection,
            |_| async { unreachable!() },
            hw_acc,
        );

        let result = group_by.execute().await;

        assert!(result.is_ok());

        let result = result.unwrap();

        assert_eq!(result.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn with_payload_and_vectors() {
        let resources = setup(16, 8).await;

        let group_by_request = GroupRequest::with_limit_from_request(
            SourceRequest::Search(SearchRequestInternal {
                vector: vec![0.5, 0.5, 0.5, 0.5].into(),
                filter: None,
                params: None,
                limit: 4,
                offset: None,
                with_payload: Some(WithPayloadInterface::Bool(true)),
                with_vector: Some(WithVector::Bool(true)),
                score_threshold: None,
            }),
            JsonPath::new("docId"),
            3,
        );

        let hw_acc = HwMeasurementAcc::new();
        let group_by = GroupBy::new(
            group_by_request.clone(),
            &resources.collection,
            |_| async { unreachable!() },
            hw_acc,
        );

        let result = group_by.execute().await;

        assert!(result.is_ok());

        let result = result.unwrap();

        assert_eq!(result.len(), 4);

        for group in result {
            assert_eq!(group.hits.len(), group_by_request.group_size);
            assert!(group.hits[0].payload.is_some());
            assert!(group.hits[0].vector.is_some());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn group_by_string_field() {
        let Resources { collection, .. } = setup(16, 8).await;

        let group_by_request = GroupRequest::with_limit_from_request(
            SourceRequest::Search(SearchRequestInternal {
                vector: vec![0.5, 0.5, 0.5, 0.5].into(),
                filter: None,
                params: None,
                limit: 4,
                offset: None,
                with_payload: Some(WithPayloadInterface::Bool(true)),
                with_vector: Some(WithVector::Bool(true)),
                score_threshold: None,
            }),
            JsonPath::new("other_stuff"),
            3,
        );

        let hw_acc = HwMeasurementAcc::new();
        let group_by = GroupBy::new(
            group_by_request.clone(),
            &collection,
            |_| async { unreachable!() },
            hw_acc,
        );

        let result = group_by.execute().await;

        assert!(result.is_ok());

        let result = result.unwrap();

        assert_eq!(result.len(), 4);

        for group in result {
            assert_eq!(group.hits.len(), group_by_request.group_size);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn zero_group_size() {
        let Resources { collection, .. } = setup(16, 8).await;

        let group_by_request = GroupRequest::with_limit_from_request(
            SourceRequest::Search(SearchRequestInternal {
                vector: vec![0.5, 0.5, 0.5, 0.5].into(),
                filter: None,
                params: None,
                limit: 4,
                offset: None,
                with_payload: None,
                with_vector: None,
                score_threshold: None,
            }),
            JsonPath::new("docId"),
            0,
        );

        let hw_acc = HwMeasurementAcc::new();
        let group_by = GroupBy::new(
            group_by_request.clone(),
            &collection,
            |_| async { unreachable!() },
            hw_acc,
        );

        let result = group_by.execute().await;

        assert!(result.is_ok());

        let result = result.unwrap();

        assert_eq!(result.len(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn zero_limit_groups() {
        let Resources { collection, .. } = setup(16, 8).await;

        let group_by_request = GroupRequest::with_limit_from_request(
            SourceRequest::Search(SearchRequestInternal {
                vector: vec![0.5, 0.5, 0.5, 0.5].into(),
                filter: None,
                params: None,
                limit: 0,
                offset: None,
                with_payload: None,
                with_vector: None,
                score_threshold: None,
            }),
            JsonPath::new("docId"),
            3,
        );

        let hw_acc = HwMeasurementAcc::new();
        let group_by = GroupBy::new(
            group_by_request.clone(),
            &collection,
            |_| async { unreachable!() },
            hw_acc,
        );

        let result = group_by.execute().await;

        assert!(result.is_ok());

        let result = result.unwrap();

        assert_eq!(result.len(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn big_limit_groups() {
        let Resources { collection, .. } = setup(1000, 5).await;

        let group_by_request = GroupRequest::with_limit_from_request(
            SourceRequest::Search(SearchRequestInternal {
                vector: vec![0.5, 0.5, 0.5, 0.5].into(),
                filter: None,
                params: None,
                limit: 500,
                offset: None,
                with_payload: None,
                with_vector: None,
                score_threshold: None,
            }),
            JsonPath::new("docId"),
            3,
        );

        let hw_acc = HwMeasurementAcc::new();
        let group_by = GroupBy::new(
            group_by_request.clone(),
            &collection,
            |_| async { unreachable!() },
            hw_acc,
        );

        let result = group_by.execute().await;

        assert!(result.is_ok());

        let result = result.unwrap();

        assert_eq!(result.len(), group_by_request.limit);

        for group in result {
            assert_eq!(group.hits.len(), group_by_request.group_size);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn big_group_size_groups() {
        let Resources { collection, .. } = setup(10, 500).await;

        let group_by_request = GroupRequest::with_limit_from_request(
            SourceRequest::Search(SearchRequestInternal {
                vector: vec![0.5, 0.5, 0.5, 0.5].into(),
                filter: None,
                params: None,
                limit: 3,
                offset: None,
                with_payload: None,
                with_vector: None,
                score_threshold: None,
            }),
            JsonPath::new("docId"),
            400,
        );

        let hw_acc = HwMeasurementAcc::new();
        let group_by = GroupBy::new(
            group_by_request.clone(),
            &collection,
            |_| async { unreachable!() },
            hw_acc,
        );

        let result = group_by.execute().await;

        assert!(result.is_ok());

        let result = result.unwrap();

        assert_eq!(result.len(), group_by_request.limit);

        for group in result {
            assert_eq!(group.hits.len(), group_by_request.group_size);
        }
    }
}

/// Tests out the different features working together. The individual features are already tested in other places.
mod group_by_builder {
    use api::rest::SearchRequestInternal;
    use collection::grouping::GroupBy;
    use collection::lookup::types::PseudoId;
    use collection::lookup::WithLookup;
    use collection::operations::point_ops::{
        BatchPersisted, BatchVectorStructPersisted, PointInsertOperationsInternal, PointOperations,
    };
    use common::counter::hardware_accumulator::HwMeasurementAcc;
    use segment::json_path::JsonPath;
    use segment::payload_json;
    use tokio::sync::RwLock;

    use super::*;

    const BODY_TEXT: &str = "lorem ipsum dolor sit amet";

    struct Resources {
        request: GroupRequest,
        lookup_collection: RwLock<Collection>,
        collection: Collection,
    }

    /// Sets up two collections: one for chunks and one for docs.
    async fn setup(docs: u64, chunks_per_doc: u64) -> Resources {
        let mut rng = rand::thread_rng();

        let source_request = SourceRequest::Search(SearchRequestInternal {
            vector: vec![0.5, 0.5, 0.5, 0.5].into(),
            filter: None,
            params: None,
            limit: 4,
            offset: None,
            with_payload: None,
            with_vector: None,
            score_threshold: None,
        });

        let request =
            GroupRequest::with_limit_from_request(source_request, JsonPath::new("docId"), 3);

        let collection_dir = tempfile::Builder::new().prefix("chunks").tempdir().unwrap();
        let collection = simple_collection_fixture(collection_dir.path(), 1).await;

        // insert chunk points
        {
            let batch = BatchPersisted {
                ids: (0..docs * chunks_per_doc).map(|x| x.into()).collect_vec(),
                vectors: BatchVectorStructPersisted::Single(
                    (0..docs * chunks_per_doc)
                        .map(|_| rand_dense_vector(&mut rng, 4))
                        .collect_vec(),
                ),
                payloads: (0..docs)
                    .flat_map(|x| {
                        (0..chunks_per_doc).map(move |_| Some(payload_json! {"docId": x}))
                    })
                    .collect_vec()
                    .into(),
            };

            let insert_points = CollectionUpdateOperations::PointOperation(
                PointOperations::UpsertPoints(PointInsertOperationsInternal::from(batch)),
            );

            let insert_result = collection
                .update_from_client_simple(insert_points, true, WriteOrdering::default())
                .await
                .expect("insert failed");

            assert_eq!(insert_result.status, UpdateStatus::Completed);
        }

        let lookup_dir = tempfile::Builder::new().prefix("lookup").tempdir().unwrap();
        let lookup_collection = simple_collection_fixture(lookup_dir.path(), 1).await;

        // insert doc points
        {
            let batch = BatchPersisted {
                ids: (0..docs).map(|x| x.into()).collect_vec(),
                vectors: BatchVectorStructPersisted::Single(
                    (0..docs)
                        .map(|_| rand_dense_vector(&mut rng, 4))
                        .collect_vec(),
                ),
                payloads: (0..docs)
                    .map(|x| Some(payload_json! {"docId": x, "body": format!("{x} {BODY_TEXT}")}))
                    .collect_vec()
                    .into(),
            };

            let insert_points = CollectionUpdateOperations::PointOperation(
                PointOperations::UpsertPoints(PointInsertOperationsInternal::from(batch)),
            );
            let insert_result = lookup_collection
                .update_from_client_simple(insert_points, true, WriteOrdering::default())
                .await
                .expect("insert failed");

            assert_eq!(insert_result.status, UpdateStatus::Completed);
        }

        let lookup_collection = RwLock::new(lookup_collection);

        Resources {
            request,
            lookup_collection,
            collection,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn only_group_by() {
        let Resources {
            request,
            collection,
            ..
        } = setup(16, 8).await;

        let collection_by_name = |_: String| async { unreachable!() };

        let hw_acc = HwMeasurementAcc::new();
        let result = GroupBy::new(request.clone(), &collection, collection_by_name, hw_acc)
            .execute()
            .await;

        assert!(result.is_ok());

        let result = result.unwrap();

        // minimal assertion
        assert_eq!(result.len(), request.limit);
        for group in result {
            assert_eq!(group.hits.len(), request.group_size);
            assert!(group.lookup.is_none());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn group_by_with_lookup() {
        let Resources {
            mut request,
            collection,
            lookup_collection,
            ..
        } = setup(16, 8).await;

        request.with_lookup = Some(WithLookup {
            collection_name: "test".to_string(),
            with_payload: Some(true.into()),
            with_vectors: Some(true.into()),
        });

        let collection_by_name = |_: String| async { Some(lookup_collection.read().await) };

        let hw_acc = HwMeasurementAcc::new();
        let result = GroupBy::new(request.clone(), &collection, collection_by_name, hw_acc)
            .execute()
            .await;

        assert!(result.is_ok());

        let result = result.unwrap();

        assert_eq!(result.len(), request.limit);

        for group in result {
            assert_eq!(group.hits.len(), request.group_size);

            let lookup = group.lookup.expect("lookup not found");

            assert_eq!(PseudoId::from(group.id), PseudoId::from(lookup.id));

            let payload = lookup.payload.unwrap();
            let body = payload.0.get("body").unwrap().as_str().unwrap();
            assert_eq!(body, &format!("{} {BODY_TEXT}", lookup.id));
        }
    }
}
