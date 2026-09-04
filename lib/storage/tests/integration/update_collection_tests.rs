// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use collection::collection::Collection;
use collection::operations::config_diff::HnswConfigDiff;
use collection::operations::types::{VectorParamsDiff, VectorsConfigDiff};
use collection::operations::vector_params_builder::VectorParamsBuilder;
use collection::operations::verification::new_unchecked_verification_pass;
use collection::optimizers_builder::OptimizersConfig;
use collection::shards::channel_service::ChannelService;
use common::budget::ResourceBudget;
use common::load_concurrency::LoadConcurrencyConfig;
use common::mmap;
use segment::types::Distance;
use storage::content_manager::collection_meta_ops::{
    CollectionMetaOperations, CreateCollection, CreateCollectionOperation, UpdateCollection,
    UpdateCollectionOperation,
};
use storage::content_manager::consensus::operation_sender::OperationSender;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use storage::rbac::{Access, AccessRequirements, Auth};
use storage::types::{PerformanceConfig, StorageConfig};
use tempfile::{Builder, TempDir};
use tokio::runtime::Handle;

const FULL_ACCESS: Auth = Auth::new_internal(Access::full("For test"));

#[test]
fn update_collection_reject_mid_operation() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();

    create_collection(&handle, &dispatcher, "test");

    let collection = get_collection(&handle, &dispatcher, "test");
    let hnsw_config = handle.block_on(collection.config()).hnsw_config;

    // Second diff names a vector that does not exist, so the operation is rejected
    let update = UpdateCollection {
        hnsw_config: Some(HnswConfigDiff {
            m: Some(hnsw_config.m + 1),
            ..Default::default()
        }),
        vectors: Some(VectorsConfigDiff(BTreeMap::from([(
            "missing".into(),
            VectorParamsDiff {
                hnsw_config: None,
                quantization_config: None,
                on_disk: None,
                memory: None,
            },
        )]))),
        params: None,
        optimizers_config: None,
        quantization_config: None,
        sparse_vectors: None,
        strict_mode_config: None,
        metadata: None,
    };

    let error = handle
        .block_on(dispatcher.submit_collection_meta_op(
            CollectionMetaOperations::UpdateCollection(
                UpdateCollectionOperation::new("test".to_string(), update).unwrap(),
            ),
            FULL_ACCESS,
            None,
        ))
        .unwrap_err();

    assert!(
        matches!(error, StorageError::BadInput { .. }),
        "updating a missing vector should be rejected as bad input, got {error:?}"
    );

    // Rejected operation must not save the HNSW diff it carries
    assert_eq!(
        handle.block_on(collection.config()).hnsw_config,
        hnsw_config
    );
}

fn new_dispatcher() -> (TempDir, Handle, Dispatcher) {
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

    (storage_dir, handle, Dispatcher::new(toc))
}

fn create_collection(handle: &Handle, dispatcher: &Dispatcher, collection_name: &str) {
    handle
        .block_on(
            dispatcher.submit_collection_meta_op(
                CollectionMetaOperations::CreateCollection(
                    CreateCollectionOperation::new(
                        collection_name.to_string(),
                        CreateCollection {
                            vectors: VectorParamsBuilder::new(10, Distance::Cosine)
                                .build()
                                .into(),
                            sparse_vectors: None,
                            hnsw_config: None,
                            wal_config: None,
                            optimizers_config: None,
                            shard_number: Some(1),
                            on_disk_payload: None,
                            payload: None,
                            replication_factor: None,
                            write_consistency_factor: None,
                            quantization_config: None,
                            sharding_method: None,
                            strict_mode_config: None,
                            uuid: None,
                            metadata: None,
                        },
                    )
                    .unwrap(),
                ),
                FULL_ACCESS,
                None,
            ),
        )
        .unwrap();
}

fn get_collection(
    handle: &Handle,
    dispatcher: &Dispatcher,
    collection_name: &str,
) -> Arc<Collection> {
    // Nothing to verify here.
    let pass = new_unchecked_verification_pass();

    handle
        .block_on(
            dispatcher.toc(&FULL_ACCESS, &pass).get_collection(
                &FULL_ACCESS
                    .check_collection_access(collection_name, AccessRequirements::new(), "test")
                    .unwrap(),
            ),
        )
        .unwrap()
}
