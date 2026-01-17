use std::collections::HashMap;
use std::path::PathBuf;
use std::time::SystemTime;

use api::rest::SearchRequestInternal;
use collection::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use collection::operations::CollectionUpdateOperations;
use collection::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorStructPersisted,
    WriteOrdering,
};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::CountRequestInternal;
use collection::operations::vector_params_builder::VectorParamsBuilder;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use fs_err as fs;
use segment::types::{Distance, WithPayloadInterface};
use tempfile::Builder;

use crate::common::{
    TEST_OPTIMIZERS_CONFIG, load_local_collection_read_only, new_local_collection,
};

/// Test that read-only mode allows search and count operations
/// but blocks all write operations (upsert, delete, etc.)
#[tokio::test(flavor = "multi_thread")]
async fn test_read_only_mode() {
    let _ = env_logger::builder().is_test(true).try_init();

    let collection_dir = Builder::new().prefix("test_read_only").tempdir().unwrap();
    let collection_path = collection_dir.path();
    let snapshots_path = collection_path.join("snapshots");

    // Create collection config
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
        wal_retain_closed: 1,
    };

    let collection_params = CollectionParams {
        vectors: VectorParamsBuilder::new(4, Distance::Dot).build().into(),
        shard_number: std::num::NonZeroU32::new(2).unwrap(),
        ..CollectionParams::empty()
    };

    let collection_config = CollectionConfigInternal {
        params: collection_params,
        optimizer_config: TEST_OPTIMIZERS_CONFIG.clone(),
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
        strict_mode_config: Default::default(),
        uuid: None,
        metadata: None,
    };

    // Create and populate the collection first (in normal write mode)
    {
        let collection = new_local_collection(
            "test".to_string(),
            collection_path,
            &snapshots_path,
            &collection_config,
        )
        .await
        .unwrap();

        // Insert some test points
        let points = vec![
            PointStructPersisted {
                id: 1.into(),
                vector: VectorStructPersisted::Single(vec![1.0, 0.0, 0.0, 0.0]),
                payload: None,
            },
            PointStructPersisted {
                id: 2.into(),
                vector: VectorStructPersisted::Single(vec![0.0, 1.0, 0.0, 0.0]),
                payload: None,
            },
            PointStructPersisted {
                id: 3.into(),
                vector: VectorStructPersisted::Single(vec![0.0, 0.0, 1.0, 0.0]),
                payload: None,
            },
        ];

        let insert_ops = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::PointsList(points),
        ));

        let hw_acc = HwMeasurementAcc::new();
        collection
            .update_from_client_simple(insert_ops, true, None, WriteOrdering::default(), hw_acc)
            .await
            .unwrap();

        // Flush all segments to disk before stopping
        // This is necessary because read-only mode doesn't replay WAL
        collection.full_flush_all_local_shards().await;

        // Now stop the collection
        collection.stop_gracefully().await;
    }

    // Now reload the collection in read-only mode
    let read_only_collection =
        load_local_collection_read_only("test".to_string(), collection_path, &snapshots_path).await;

    // Test 1: Search should work
    let search_request = SearchRequestInternal {
        vector: vec![1.0, 0.0, 0.0, 0.0].into(),
        limit: 10,
        offset: None,
        filter: None,
        params: None,
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: None,
        score_threshold: None,
    };

    let hw_acc = HwMeasurementAcc::new();
    let search_result = read_only_collection
        .search(
            search_request.into(),
            None,
            &ShardSelectorInternal::All,
            None,
            hw_acc.clone(),
        )
        .await;

    assert!(
        search_result.is_ok(),
        "Search should work in read-only mode"
    );
    let results = search_result.unwrap();
    assert!(!results.is_empty(), "Search should return results");

    // Test 2: Count should work
    let count_request = CountRequestInternal {
        filter: None,
        exact: true,
    };

    let count_result = read_only_collection
        .count(
            count_request,
            None,
            &ShardSelectorInternal::All,
            None,
            hw_acc.clone(),
        )
        .await;

    assert!(count_result.is_ok(), "Count should work in read-only mode");
    let count = count_result.unwrap();
    assert_eq!(count.count, 3, "Should have 3 points");

    // Test 3: Write operations should fail
    let insert_ops = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(vec![PointStructPersisted {
            id: 4.into(),
            vector: VectorStructPersisted::Single(vec![0.0, 0.0, 0.0, 1.0]),
            payload: None,
        }]),
    ));

    let hw_acc = HwMeasurementAcc::new();
    let write_result = read_only_collection
        .update_from_client_simple(insert_ops, true, None, WriteOrdering::default(), hw_acc)
        .await;

    assert!(
        write_result.is_err(),
        "Write operations should fail in read-only mode"
    );

    read_only_collection.stop_gracefully().await;
}

/// Test that loading in read-only mode doesn't modify files
#[tokio::test(flavor = "multi_thread")]
async fn test_read_only_no_file_modification() {
    let _ = env_logger::builder().is_test(true).try_init();

    fn collect_file_mtimes(
        path: &std::path::Path,
    ) -> std::io::Result<HashMap<PathBuf, SystemTime>> {
        let mut mtimes = HashMap::new();
        let mut stack = vec![path.to_path_buf()];

        while let Some(dir) = stack.pop() {
            for entry in fs::read_dir(&dir)? {
                let entry = entry?;
                let path = entry.path();
                let metadata = entry.metadata()?;
                if metadata.is_dir() {
                    stack.push(path);
                    continue;
                }
                if metadata.is_file() {
                    mtimes.insert(path, metadata.modified()?);
                }
            }
        }

        Ok(mtimes)
    }

    let collection_dir = Builder::new()
        .prefix("test_read_only_no_mod")
        .tempdir()
        .unwrap();
    let collection_path = collection_dir.path();
    let snapshots_path = collection_path.join("snapshots");

    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
        wal_retain_closed: 1,
    };

    let collection_params = CollectionParams {
        vectors: VectorParamsBuilder::new(4, Distance::Dot).build().into(),
        shard_number: std::num::NonZeroU32::new(1).unwrap(),
        ..CollectionParams::empty()
    };

    let collection_config = CollectionConfigInternal {
        params: collection_params,
        optimizer_config: TEST_OPTIMIZERS_CONFIG.clone(),
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
        strict_mode_config: Default::default(),
        uuid: None,
        metadata: None,
    };

    // Create collection
    {
        let collection = new_local_collection(
            "test".to_string(),
            collection_path,
            &snapshots_path,
            &collection_config,
        )
        .await
        .unwrap();

        collection.stop_gracefully().await;
    }

    let file_mtimes_before = collect_file_mtimes(collection_path).unwrap();

    // Load in read-only mode - should not panic or fail
    let read_only_collection =
        load_local_collection_read_only("test".to_string(), collection_path, &snapshots_path).await;

    let file_mtimes_after = collect_file_mtimes(collection_path).unwrap();
    assert_eq!(
        file_mtimes_before.len(),
        file_mtimes_after.len(),
        "File set changed in read-only mode"
    );
    for (path, original_mtime) in file_mtimes_before {
        let current_mtime = file_mtimes_after
            .get(&path)
            .unwrap_or_else(|| panic!("File {path:?} was removed in read-only mode"));
        assert_eq!(
            original_mtime, *current_mtime,
            "File {path:?} was modified in read-only mode"
        );
    }

    // Verify it loads successfully
    let local_shards = read_only_collection.get_local_shards().await;
    assert!(!local_shards.is_empty(), "Should have local shards");

    read_only_collection.stop_gracefully().await;
}
