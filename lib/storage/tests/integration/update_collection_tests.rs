// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::rest::MaxOptimizationThreads;
use collection::collection::Collection;
use collection::operations::CollectionUpdateOperations;
use collection::operations::config_diff::{HnswConfigDiff, OptimizersConfigDiff};
use collection::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorStructPersisted,
};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{CollectionStatus, VectorParamsDiff, VectorsConfigDiff};
use collection::operations::vector_params_builder::VectorParamsBuilder;
use collection::operations::verification::new_unchecked_verification_pass;
use collection::optimizers_builder::OptimizersConfig;
use collection::shards::channel_service::ChannelService;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::shard_trait::WaitUntil;
use common::ambient::{self, AmbientContext, AmbientFutureExt};
use common::budget::ResourceBudget;
use common::load_concurrency::LoadConcurrencyConfig;
use common::mmap;
use segment::types::Distance;
use storage::content_manager::CollectionContainer as _;
use storage::content_manager::collection_meta_ops::{
    CollectionMetaOperations, CreateCollection, CreateCollectionOperation, UpdateCollection,
    UpdateCollectionOperation,
};
use storage::content_manager::consensus::operation_sender::OperationSender;
use storage::content_manager::consensus_state_machine::{Action, CollectionConfigDiff};
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

    create_collection(&handle, &dispatcher, "test", None);

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

/// After a restart, optimizations wait for a trigger, so the collection is grey. Re-sending an
/// unchanged config is how users trigger them: it must start them, although it doesn't recreate
/// the optimizers.
#[test]
fn unchanged_update_triggers_optimizers_after_restart() {
    let _scope = ambient::test_guard();
    check_update_after_restart(|handle, dispatcher| {
        let update = UpdateCollection {
            optimizers_config: Some(unchanged_optimizers()),
            ..empty_update()
        };
        submit_update(handle, dispatcher, update);
    });
}

/// As [`unchanged_update_triggers_optimizers_after_restart`], with the action consensus applies
/// for the update in distributed mode.
#[test]
fn unchanged_update_action_triggers_optimizers_after_restart() {
    let _scope = ambient::test_guard();
    check_update_after_restart(|_handle, dispatcher| {
        let pass = new_unchecked_verification_pass();
        dispatcher
            .toc(&FULL_ACCESS, &pass)
            .apply_action(Action::UpdateCollectionConfig {
                collection: "test".into(),
                diff: Box::new(CollectionConfigDiff::Optimizers(unchanged_optimizers())),
            })
            .unwrap();
    });
}

/// Restarts a collection with pending optimizations, so that it's grey, sends an update with
/// `send_update`, and checks that the update starts the optimizations, so that it's yellow.
fn check_update_after_restart(send_update: impl FnOnce(&Handle, &Dispatcher)) {
    let storage_dir = Builder::new().prefix("storage").tempdir().unwrap();

    {
        let (handle, dispatcher) = dispatcher_at(storage_dir.path());
        // Optimizations are planned but never run, so they are still pending after the restart
        let optimizers_config = OptimizersConfigDiff {
            indexing_threshold: Some(10),
            max_optimization_threads: Some(MaxOptimizationThreads::Threads(0)),
            ..unchanged_optimizers()
        };
        create_collection(&handle, &dispatcher, "test", Some(optimizers_config));
        // Consensus activates a new replica, and there is none here
        let pass = new_unchecked_verification_pass();
        dispatcher
            .toc(&FULL_ACCESS, &pass)
            .apply_action(Action::SetReplicaState {
                collection: "test".into(),
                shard_id: 0,
                peer_id: 0,
                state: ReplicaState::Active,
            })
            .unwrap();
        upsert_points(&handle, &dispatcher, "test", 1000);
        // Stop the workers, so the shard releases its WAL for the restart
        let collection = get_collection(&handle, &dispatcher, "test");
        handle.block_on(collection.stop_gracefully());
    }

    // Restart
    let (handle, dispatcher) = dispatcher_at(storage_dir.path());
    let collection = get_collection(&handle, &dispatcher, "test");
    assert_eq!(
        collection_status(&handle, &collection),
        CollectionStatus::Grey
    );

    send_update(&handle, &dispatcher);

    // A trigger reaches the optimizers through the update worker, asynchronously
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut status = collection_status(&handle, &collection);
    while status == CollectionStatus::Grey && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
        status = collection_status(&handle, &collection);
    }
    assert_eq!(status, CollectionStatus::Yellow);
}

/// An update with no diffs: `{}`.
fn empty_update() -> UpdateCollection {
    UpdateCollection {
        hnsw_config: None,
        vectors: None,
        params: None,
        optimizers_config: None,
        quantization_config: None,
        sparse_vectors: None,
        strict_mode_config: None,
        metadata: None,
    }
}

fn submit_update(handle: &Handle, dispatcher: &Dispatcher, update: UpdateCollection) {
    handle
        .block_on(dispatcher.submit_collection_meta_op(
            CollectionMetaOperations::UpdateCollection(
                UpdateCollectionOperation::new("test".to_string(), update).unwrap(),
            ),
            FULL_ACCESS,
            None,
        ))
        .unwrap();
}

fn new_dispatcher() -> (TempDir, Handle, Dispatcher) {
    let storage_dir = Builder::new().prefix("storage").tempdir().unwrap();
    let (handle, dispatcher) = dispatcher_at(storage_dir.path());
    (storage_dir, handle, dispatcher)
}

/// A dispatcher over the storage at `storage_path`, loading the collections already there.
fn dispatcher_at(storage_path: &Path) -> (Handle, Dispatcher) {
    let config = StorageConfig {
        storage_path: storage_path.to_path_buf(),
        snapshots_path: storage_path.join("snapshots"),
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

    (handle, Dispatcher::new(toc))
}

fn create_collection(
    handle: &Handle,
    dispatcher: &Dispatcher,
    collection_name: &str,
    optimizers_config: Option<OptimizersConfigDiff>,
) {
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
                            optimizers_config,
                            shard_number: Some(1),
                            on_disk_payload: None,
                            payload: None,
                            id_tracker: None,
                            replication_factor: None,
                            write_consistency_factor: None,
                            quantization_config: None,
                            sharding_method: None,
                            strict_mode_config: None,
                            uuid: None,
                            metadata: None,
                            created_at: None,
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

/// Upserts `count` points with 10-dimensional vectors, waiting until they are visible.
fn upsert_points(handle: &Handle, dispatcher: &Dispatcher, collection_name: &str, count: u64) {
    let collection = get_collection(handle, dispatcher, collection_name);
    let pass = new_unchecked_verification_pass();
    let metrics = dispatcher
        .toc(&FULL_ACCESS, &pass)
        .get_collection_hw_metrics(collection_name.into());
    let points = (0..count)
        .map(|id| PointStructPersisted {
            id: id.into(),
            vector: VectorStructPersisted::Single(vec![
                id as f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
            ]),
            payload: None,
        })
        .collect();
    let operation = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(points),
    ));
    handle
        .block_on(
            collection
                .update_all_local(operation, WaitUntil::Visible, true)
                .measured(AmbientContext::request("test", metrics)),
        )
        .unwrap();
}

/// A diff that changes nothing, as `{"optimizers_config": {}}`.
fn unchanged_optimizers() -> OptimizersConfigDiff {
    OptimizersConfigDiff {
        deleted_threshold: None,
        vacuum_min_vector_number: None,
        default_segment_number: None,
        max_segment_size: None,
        memmap_threshold: None,
        indexing_threshold: None,
        flush_interval_sec: None,
        max_optimization_threads: None,
        prevent_unoptimized: None,
    }
}

fn collection_status(handle: &Handle, collection: &Collection) -> CollectionStatus {
    handle
        .block_on(collection.info(&ShardSelectorInternal::All))
        .unwrap()
        .status
}
