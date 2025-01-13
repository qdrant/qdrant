use std::num::NonZeroUsize;
use std::sync::Arc;

use collection::operations::vector_params_builder::VectorParamsBuilder;
use collection::operations::verification::new_unchecked_verification_pass;
use collection::optimizers_builder::OptimizersConfig;
use collection::shards::channel_service::ChannelService;
use common::cpu::CpuBudget;
use memory::madvise;
use segment::types::Distance;
use storage::content_manager::collection_meta_ops::{
    ChangeAliasesOperation, CollectionMetaOperations, CreateAlias, CreateCollection,
    CreateCollectionOperation, DeleteAlias, RenameAlias,
};
use storage::content_manager::consensus::operation_sender::OperationSender;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use storage::rbac::{Access, AccessRequirements};
use storage::types::{PerformanceConfig, StorageConfig};
use tempfile::Builder;
use tokio::runtime::Runtime;

const FULL_ACCESS: Access = Access::full("For test");

#[test]
fn test_alias_operation() {
    let storage_dir = Builder::new().prefix("storage").tempdir().unwrap();

    let config = StorageConfig {
        storage_path: storage_dir.path().to_str().unwrap().to_string(),
        snapshots_path: storage_dir
            .path()
            .join("snapshots")
            .to_str()
            .unwrap()
            .to_string(),
        snapshots_config: Default::default(),
        temp_path: None,
        on_disk_payload: false,
        on_disk_payload_uses_mmap: false,
        optimizers: OptimizersConfig {
            deleted_threshold: 0.5,
            vacuum_min_vector_number: 100,
            default_segment_number: 2,
            max_segment_size: None,
            memmap_threshold: Some(100),
            indexing_threshold: Some(100),
            flush_interval_sec: 2,
            max_optimization_threads: Some(2),
        },
        optimizers_overwrite: None,
        wal: Default::default(),
        performance: PerformanceConfig {
            max_search_threads: 1,
            max_optimization_threads: 1,
            optimizer_cpu_budget: 0,
            update_rate_limit: None,
            search_timeout_sec: None,
            incoming_shard_transfers_limit: Some(1),
            outgoing_shard_transfers_limit: Some(1),
            async_scorer: None,
        },
        hnsw_index: Default::default(),
        mmap_advice: madvise::Advice::Random,
        node_type: Default::default(),
        update_queue_size: Default::default(),
        handle_collection_load_errors: false,
        recovery_mode: None,
        update_concurrency: Some(NonZeroUsize::new(2).unwrap()),
        // update_concurrency: None,
        shard_transfer_method: None,
        collection: None,
    };

    let search_runtime = Runtime::new().unwrap();
    let handle = search_runtime.handle().clone();

    let update_runtime = Runtime::new().unwrap();

    let general_runtime = Runtime::new().unwrap();

    let (propose_sender, _propose_receiver) = std::sync::mpsc::channel();
    let propose_operation_sender = OperationSender::new(propose_sender);

    let toc = Arc::new(TableOfContent::new(
        &config,
        search_runtime,
        update_runtime,
        general_runtime,
        CpuBudget::default(),
        ChannelService::new(6333, None),
        0,
        Some(propose_operation_sender),
    ));
    let dispatcher = Dispatcher::new(toc);

    handle
        .block_on(
            dispatcher.submit_collection_meta_op(
                CollectionMetaOperations::CreateCollection(CreateCollectionOperation::new(
                    "test".to_string(),
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
                        replication_factor: None,
                        write_consistency_factor: None,
                        init_from: None,
                        quantization_config: None,
                        sharding_method: None,
                        strict_mode_config: None,
                        uuid: None,
                    },
                )),
                FULL_ACCESS,
                None,
            ),
        )
        .unwrap();

    handle
        .block_on(dispatcher.submit_collection_meta_op(
            CollectionMetaOperations::ChangeAliases(ChangeAliasesOperation {
                actions: vec![CreateAlias {
                        collection_name: "test".to_string(),
                        alias_name: "test_alias".to_string(),
                    }
                    .into()],
            }),
            FULL_ACCESS,
            None,
        ))
        .unwrap();

    handle
        .block_on(dispatcher.submit_collection_meta_op(
            CollectionMetaOperations::ChangeAliases(ChangeAliasesOperation {
                actions: vec![
                        CreateAlias {
                            collection_name: "test".to_string(),
                            alias_name: "test_alias2".to_string(),
                        }
                        .into(),
                        DeleteAlias {
                            alias_name: "test_alias".to_string(),
                        }
                        .into(),
                        RenameAlias {
                            old_alias_name: "test_alias2".to_string(),
                            new_alias_name: "test_alias3".to_string(),
                        }
                        .into(),
                    ],
            }),
            FULL_ACCESS,
            None,
        ))
        .unwrap();

    // Nothing to verify here.
    let pass = new_unchecked_verification_pass();

    let _ = handle
        .block_on(
            dispatcher.toc(&FULL_ACCESS, &pass).get_collection(
                &FULL_ACCESS
                    .check_collection_access("test_alias3", AccessRequirements::new())
                    .unwrap(),
            ),
        )
        .unwrap();
}
