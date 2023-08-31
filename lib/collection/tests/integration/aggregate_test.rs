use collection::operations::point_ops::{Batch, WriteOrdering};
use collection::operations::types::{
    SearchRequest, UpdateStatus,
};
use collection::operations::CollectionUpdateOperations;
use itertools::Itertools;

use tempfile::Builder;

use crate::common::simple_collection_fixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_aggregate_extract_args() {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();

    let collection = simple_collection_fixture(collection_dir.path(), 1).await;

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
            payloads: vec![0, 1, 2, 3, 4]
                .into_iter()
                .map(|i| serde_json::from_str(&format!("{{ \"foo\": {} }}", i)).unwrap())
                .collect()
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
        limit: 5,
        offset: 0,
        score_threshold: None,
        aggregate_function: Some("sum(foo)".to_string())
    };

    let search_res = collection.search(search_request, None, None).await;

    match search_res {
        Ok(res) => {
            assert!(res.iter().all(|p| p.aggregate_args.is_some()));
            assert!(res.iter().all(|p| p.aggregate_args.as_ref().unwrap().len() == 1));
        }
        Err(err) => panic!("search failed: {err:?}"),
    }
}