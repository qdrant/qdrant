mod common;

use crate::common::simple_collection_fixture;
use collection::collection_builder::collection_loader::load_collection;
use collection::operations::point_ops::{PointInsertOperations, PointOperations};
use collection::operations::CollectionUpdateOperations;
use tempdir::TempDir;
use tokio::runtime::Handle;

#[tokio::test]
async fn test_collection_reloading() {
    let collection_dir = TempDir::new("collection").unwrap();

    {
        let _collection = simple_collection_fixture(collection_dir.path()).await;
    }

    for _i in 0..5 {
        let collection = load_collection(collection_dir.path(), Handle::current());
        let insert_points = CollectionUpdateOperations::PointOperation(
            PointOperations::UpsertPoints(PointInsertOperations::BatchPoints {
                ids: vec![0, 1],
                vectors: vec![vec![1.0, 0.0, 1.0, 1.0], vec![1.0, 0.0, 1.0, 0.0]],
                payloads: None,
            }),
        );
        collection.update(insert_points, true).await.unwrap();
    }

    let collection = load_collection(collection_dir.path(), Handle::current());
    assert_eq!(collection.info().unwrap().vectors_count, 2)
}
