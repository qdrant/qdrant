use collection::collection::distance_matrix::CollectionSearchMatrixRequest;
use collection::operations::point_ops::{
    BatchPersisted, BatchVectorStructPersisted, WriteOrdering,
};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use itertools::Itertools;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use tempfile::Builder;

use crate::common::simple_collection_fixture;

const SEED: u64 = 42;

#[tokio::test(flavor = "multi_thread")]
async fn distance_matrix_empty() {
    let collection_dir = Builder::new().prefix("storage").tempdir().unwrap();

    // empty collection
    let collection = simple_collection_fixture(collection_dir.path(), 1).await;

    let hw_acc = HwMeasurementAcc::new();
    let sample_size = 100;
    let limit_per_sample = 10;
    let request = CollectionSearchMatrixRequest {
        sample_size,
        limit_per_sample,
        filter: None,
        using: "".to_string(), // default vector name
    };
    let matrix = collection
        .search_points_matrix(request, ShardSelectorInternal::All, None, None, &hw_acc)
        .await
        .unwrap();
    hw_acc.discard();

    // assert all empty
    assert!(matrix.sample_ids.is_empty());
    assert!(matrix.nearests.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn distance_matrix_anonymous_vector() {
    let collection_dir = Builder::new().prefix("storage").tempdir().unwrap();

    let collection = simple_collection_fixture(collection_dir.path(), 1).await;

    let point_count = 2000;
    let ids = (0..point_count).map_into().collect();
    let mut rng = SmallRng::seed_from_u64(SEED);

    let vectors = (0..point_count)
        .map(|_| rng.gen::<[f32; 4]>().to_vec())
        .collect_vec();

    let batch = BatchPersisted {
        ids,
        vectors: BatchVectorStructPersisted::Single(vectors),
        payloads: None,
    };

    let upsert_points = collection::operations::CollectionUpdateOperations::PointOperation(
        collection::operations::point_ops::PointOperations::UpsertPoints(
            collection::operations::point_ops::PointInsertOperationsInternal::from(batch),
        ),
    );

    collection
        .update_from_client_simple(upsert_points, true, WriteOrdering::default())
        .await
        .unwrap();

    let hw_acc = HwMeasurementAcc::new();
    let sample_size = 100;
    let limit_per_sample = 10;
    let request = CollectionSearchMatrixRequest {
        sample_size,
        limit_per_sample,
        filter: None,
        using: "".to_string(), // default vector name
    };
    let matrix = collection
        .search_points_matrix(request, ShardSelectorInternal::All, None, None, &hw_acc)
        .await
        .unwrap();
    hw_acc.discard();

    assert_eq!(matrix.sample_ids.len(), sample_size);
    // no duplicate sample ids
    assert_eq!(
        matrix
            .sample_ids
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        sample_size
    );

    assert_eq!(matrix.nearests.len(), sample_size);
    for nearest in matrix.nearests {
        assert_eq!(nearest.len(), limit_per_sample);
        // assert each row sorted by scores
        nearest.iter().tuple_windows().for_each(|(prev, next)| {
            assert!(prev.score >= next.score);
        });
    }
}
