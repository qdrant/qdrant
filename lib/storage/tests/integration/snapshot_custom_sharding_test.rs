// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

//! Regression for <https://github.com/qdrant/qdrant/issues/10135>:
//! restoring a snapshot of a custom-sharded collection into a new collection
//! must recreate the shard keys and restore the points.

use std::num::NonZeroUsize;
use std::sync::Arc;

use collection::operations::point_ops::{
    BatchPersisted, BatchVectorStructPersisted, PointInsertOperationsInternal, PointOperations,
    WriteOrdering,
};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::snapshot_ops::SnapshotRecover;
use collection::operations::types::CountRequestInternal;
use collection::operations::vector_params_builder::VectorParamsBuilder;
use collection::operations::CollectionUpdateOperations;
use collection::optimizers_builder::OptimizersConfig;
use collection::shards::channel_service::ChannelService;
use collection::shards::shard::ShardsPlacement;
use collection::shards::shard_trait::WaitUntil;
use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::load_concurrency::LoadConcurrencyConfig;
use common::mmap;
use segment::types::{Distance, ShardKey};
use storage::content_manager::collection_meta_ops::{
    CollectionMetaOperations, CreateCollection, CreateCollectionOperation, CreateShardKey,
    DeleteCollectionOperation,
};
use storage::content_manager::consensus::operation_sender::OperationSender;
use storage::content_manager::snapshots::recover::do_recover_from_snapshot;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use storage::rbac::{Access, AccessRequirements, Auth};
use storage::types::{PerformanceConfig, StorageConfig};
use tempfile::Builder;

const FULL_ACCESS: Auth = Auth::new_internal(Access::full("For test"));

#[test]
fn test_snapshot_restore_recreates_custom_shard_keys() {
    let storage_dir = Builder::new().prefix("storage").tempdir().unwrap();

    let config = StorageConfig {
        storage_path: storage_dir.path().to_path_buf(),
        snapshots_path: storage_dir.path().join("snapshots"),
        snapshots_config: Default::default(),
        temp_path: None,
        on_disk_payload: false,
        payload: None,
        optimizers: OptimizersConfig {
            deleted_threshold: 0.5,
            vacuum_min_vector_number: 100,
            default_segment_number: 2,
            max_segment_size: None,
            #[expect(deprecated)]
            memmap_threshold: Some(100),
            indexing_threshold: Some(100),
            flush_interval_sec: 2,
            max_optimization_threads: Some(2),
            prevent_unoptimized: None,
        },
        optimizers_overwrite: None,
        wal: Default::default(),
        performance: PerformanceConfig {
            max_search_threads: 1,
            max_optimization_runtime_threads: 1,
            optimizer_cpu_budget: 0,
            optimizer_io_budget: 0,
            update_rate_limit: None,
            search_timeout_sec: None,
            incoming_shard_transfers_limit: Some(1),
            outgoing_shard_transfers_limit: Some(1),
            async_scorer: None,
            io_uring: None,
            load_concurrency: LoadConcurrencyConfig::default(),
        },
        hnsw_index: Default::default(),
        hnsw_global_config: Default::default(),
        mmap_advice: mmap::Advice::Random,
        low_memory_mode: Default::default(),
        node_type: Default::default(),
        update_queue_size: Default::default(),
        handle_collection_load_errors: false,
        recovery_mode: None,
        update_concurrency: Some(NonZeroUsize::new(2).unwrap()),
        shard_transfer_method: None,
        collection: None,
        max_collections: None,
        quotas: Default::default(),
    };

    let (propose_sender, _propose_receiver) = std::sync::mpsc::channel();
    let propose_operation_sender = OperationSender::new(propose_sender);

    let toc = Arc::new(
        TableOfContent::new(
            &config,
            ResourceBudget::default(),
            ChannelService::new(6333, false, None, None),
            0,
            Some(propose_operation_sender),
        )
        .unwrap(),
    );
    let handle = toc.general_runtime_handle().clone();
    let dispatcher = Dispatcher::new(toc.clone());

    let shard_key = ShardKey::Keyword("k1".into());

    // Custom-sharded collection: no shards until a shard key is created
    handle
        .block_on(dispatcher.submit_collection_meta_op(
            CollectionMetaOperations::CreateCollection(
                CreateCollectionOperation::new(
                    "test".to_string(),
                    CreateCollection {
                        vectors: VectorParamsBuilder::new(4, Distance::Cosine)
                            .build()
                            .into(),
                        sparse_vectors: None,
                        hnsw_config: None,
                        wal_config: None,
                        optimizers_config: None,
                        shard_number: Some(1.try_into().unwrap()),
                        on_disk_payload: None,
                        payload: None,
                        replication_factor: None,
                        write_consistency_factor: None,
                        quantization_config: None,
                        sharding_method: Some(collection::config::ShardingMethod::Custom),
                        strict_mode_config: None,
                        uuid: None,
                        metadata: None,
                    },
                )
                .unwrap(),
            ),
            FULL_ACCESS,
            None,
        ))
        .unwrap();

    let placement: ShardsPlacement = vec![vec![0]];
    handle
        .block_on(dispatcher.submit_collection_meta_op(
            CollectionMetaOperations::CreateShardKey(CreateShardKey {
                collection_name: "test".to_string(),
                shard_key: shard_key.clone(),
                placement,
                initial_state: None,
            }),
            FULL_ACCESS,
            None,
        ))
        .unwrap();

    // Insert two points under the shard key
    {
        let pass = FULL_ACCESS
            .check_global_access(AccessRequirements::new().manage(), "snapshot test")
            .unwrap()
            .issue_pass("test");
        let collection = handle.block_on(toc.get_collection(&pass)).unwrap();

        let batch = BatchPersisted {
            ids: vec![1, 2].into_iter().map(Into::into).collect(),
            vectors: BatchVectorStructPersisted::Single(vec![vec![1.0; 4], vec![0.5; 4]]),
            payloads: None,
        };
        let upsert = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::from(batch),
        ));
        handle
            .block_on(collection.update_from_client(
                upsert,
                WaitUntil::from(true),
                None,
                WriteOrdering::default(),
                Some(shard_key.clone()),
                HwMeasurementAcc::new(),
            ))
            .unwrap();
    }

    // Snapshot, delete the collection, then restore from the snapshot
    let snapshot_name = {
        let multipass = FULL_ACCESS
            .check_global_access(AccessRequirements::new().manage(), "snapshot test")
            .unwrap();
        let pass = multipass.issue_pass("test");
        handle.block_on(toc.create_snapshot(&pass)).unwrap().name
    };
    let snapshot_path = config
        .snapshots_path
        .join("test")
        .join(&snapshot_name);

    handle
        .block_on(dispatcher.submit_collection_meta_op(
            CollectionMetaOperations::DeleteCollection(DeleteCollectionOperation(
                "test".to_string(),
            )),
            FULL_ACCESS,
            None,
        ))
        .unwrap();

    let location = reqwest::Url::parse(&format!("file://{}", snapshot_path.display())).unwrap();
    handle
        .block_on(do_recover_from_snapshot(
            &dispatcher,
            "test",
            SnapshotRecover {
                location,
                priority: None,
                checksum: None,
                api_key: None,
            },
            FULL_ACCESS,
            reqwest::Client::new(),
        ))
        .unwrap();

    // The recreated collection must have the snapshot's shard layout and its points
    let multipass = FULL_ACCESS
        .check_global_access(AccessRequirements::new().manage(), "snapshot test")
        .unwrap();
    let pass = multipass.issue_pass("test");
    let collection = handle.block_on(toc.get_collection(&pass)).unwrap();

    let state = handle.block_on(collection.state());
    assert_eq!(
        state.shards.len(),
        1,
        "restored collection must have the snapshot's shard"
    );

    let count = handle
        .block_on(collection.count(
            CountRequestInternal {
                filter: None,
                exact: true,
            },
            None,
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        ))
        .unwrap();
    assert_eq!(count.count, 2, "restored points must survive the restore");
}


/// Restoring into an existing custom-sharded collection whose shard keys do
/// not match the snapshot's must fail loudly instead of restoring the data
/// under the wrong keys.
#[test]
fn test_snapshot_restore_rejects_mismatched_shard_keys() {
    let storage_dir = Builder::new().prefix("storage").tempdir().unwrap();

    let config = StorageConfig {
        storage_path: storage_dir.path().to_path_buf(),
        snapshots_path: storage_dir.path().join("snapshots"),
        snapshots_config: Default::default(),
        temp_path: None,
        on_disk_payload: false,
        payload: None,
        optimizers: OptimizersConfig {
            deleted_threshold: 0.5,
            vacuum_min_vector_number: 100,
            default_segment_number: 2,
            max_segment_size: None,
            #[expect(deprecated)]
            memmap_threshold: Some(100),
            indexing_threshold: Some(100),
            flush_interval_sec: 2,
            max_optimization_threads: Some(2),
            prevent_unoptimized: None,
        },
        optimizers_overwrite: None,
        wal: Default::default(),
        performance: PerformanceConfig {
            max_search_threads: 1,
            max_optimization_runtime_threads: 1,
            optimizer_cpu_budget: 0,
            optimizer_io_budget: 0,
            update_rate_limit: None,
            search_timeout_sec: None,
            incoming_shard_transfers_limit: Some(1),
            outgoing_shard_transfers_limit: Some(1),
            async_scorer: None,
            io_uring: None,
            load_concurrency: LoadConcurrencyConfig::default(),
        },
        hnsw_index: Default::default(),
        hnsw_global_config: Default::default(),
        mmap_advice: mmap::Advice::Random,
        low_memory_mode: Default::default(),
        node_type: Default::default(),
        update_queue_size: Default::default(),
        handle_collection_load_errors: false,
        recovery_mode: None,
        update_concurrency: Some(NonZeroUsize::new(2).unwrap()),
        shard_transfer_method: None,
        collection: None,
        max_collections: None,
        quotas: Default::default(),
    };

    let (propose_sender, _propose_receiver) = std::sync::mpsc::channel();
    let propose_operation_sender = OperationSender::new(propose_sender);

    let toc = Arc::new(
        TableOfContent::new(
            &config,
            ResourceBudget::default(),
            ChannelService::new(6334, false, None, None),
            0,
            Some(propose_operation_sender),
        )
        .unwrap(),
    );
    let handle = toc.general_runtime_handle().clone();
    let dispatcher = Dispatcher::new(toc.clone());

    let create_collection = || {
        dispatcher.submit_collection_meta_op(
            CollectionMetaOperations::CreateCollection(
                CreateCollectionOperation::new(
                    "test".to_string(),
                    CreateCollection {
                        vectors: VectorParamsBuilder::new(4, Distance::Cosine)
                            .build()
                            .into(),
                        sparse_vectors: None,
                        hnsw_config: None,
                        wal_config: None,
                        optimizers_config: None,
                        shard_number: Some(1.try_into().unwrap()),
                        on_disk_payload: None,
                        payload: None,
                        replication_factor: None,
                        write_consistency_factor: None,
                        quantization_config: None,
                        sharding_method: Some(collection::config::ShardingMethod::Custom),
                        strict_mode_config: None,
                        uuid: None,
                        metadata: None,
                    },
                )
                .unwrap(),
            ),
            FULL_ACCESS,
            None,
        )
    };

    let create_shard_key = |key: &str| {
        dispatcher.submit_collection_meta_op(
            CollectionMetaOperations::CreateShardKey(CreateShardKey {
                collection_name: "test".to_string(),
                shard_key: ShardKey::Keyword(key.into()),
                placement: vec![vec![0]],
                initial_state: None,
            }),
            FULL_ACCESS,
            None,
        )
    };

    // Snapshot a collection sharded by "k1"
    handle.block_on(create_collection()).unwrap();
    handle.block_on(create_shard_key("k1")).unwrap();

    let snapshot_name = {
        let multipass = FULL_ACCESS
            .check_global_access(AccessRequirements::new().manage(), "snapshot test")
            .unwrap();
        let pass = multipass.issue_pass("test");
        handle.block_on(toc.create_snapshot(&pass)).unwrap().name
    };
    let snapshot_path = config.snapshots_path.join("test").join(&snapshot_name);

    handle
        .block_on(dispatcher.submit_collection_meta_op(
            CollectionMetaOperations::DeleteCollection(DeleteCollectionOperation(
                "test".to_string(),
            )),
            FULL_ACCESS,
            None,
        ))
        .unwrap();

    // Recreate the collection sharded by "k2" instead
    handle.block_on(create_collection()).unwrap();
    handle.block_on(create_shard_key("k2")).unwrap();

    let location = reqwest::Url::parse(&format!("file://{}", snapshot_path.display())).unwrap();
    let result = handle.block_on(do_recover_from_snapshot(
        &dispatcher,
        "test",
        SnapshotRecover {
            location,
            priority: None,
            checksum: None,
            api_key: None,
        },
        FULL_ACCESS,
        reqwest::Client::new(),
    ));

    assert!(
        result.is_err(),
        "restoring a snapshot with shard key k1 into a collection keyed by k2 must fail"
    );
}
