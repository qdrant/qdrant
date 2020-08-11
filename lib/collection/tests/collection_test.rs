mod common;

use collection::collection::Collection;
use collection::operations::CollectionUpdateOperations;
use collection::operations::point_ops::PointOps;

use crate::common::simple_collection_fixture;
use collection::operations::types::{UpdateStatus, SearchRequest};
use std::io::{stdout, Write};
use std::sync::Arc;


#[test]
fn test_collection_updater() {
    let (_rt, _tmp1, _tmp2, collection) = simple_collection_fixture();

    println!("here test_collection_updater");

    stdout().flush();

    let insert_points = CollectionUpdateOperations::PointOperation(
        PointOps::UpsertPoints {
            collection: "this".to_string(),
            ids: vec![0, 1, 2, 3, 4],
            vectors: vec![
                vec![1.0, 0.0, 1.0, 1.0],
                vec![1.0, 0.0, 1.0, 0.0],
                vec![1.0, 1.0, 1.0, 1.0],
                vec![1.0, 1.0, 0.0, 1.0],
                vec![1.0, 0.0, 0.0, 0.0],
            ],
        }
    );

    let insert_result = collection.update(insert_points, true);

    match insert_result {
        Ok(res) => {
            assert_eq!(res.status, UpdateStatus::Completed)
        },
        Err(err) => assert!(false, format!("operation failed: {:?}", err)),
    }

    let search_request = Arc::new(SearchRequest {
        vector: vec![1.0, 1.0, 1.0, 1.0],
        filter: None,
        params: None,
        top: 3
    });

    let search_res = collection.search(search_request);


    match search_res {
        Ok(res) => {
            assert_eq!(res.len(), 3);
            assert_eq!(res[0].idx, 2);
        },
        Err(err) => assert!(false, format!("search failed: {:?}", err)),
    }
}