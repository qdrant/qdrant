use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use collection::collection::Collection;
use collection::config::ShardingMethod;
use collection::optimizers_builder::OptimizersConfig;
use collection::shards::channel_service::ChannelService;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use common::budget::ResourceBudget;
use common::load_concurrency::LoadConcurrencyConfig;
use common::mmap;
use tempfile::{Builder, TempDir};

use super::TableOfContent;
use crate::content_manager::collection_meta_ops::{
    CollectionMetaOperations, CreateCollection, CreateCollectionOperation,
};
use crate::content_manager::consensus::operation_sender::OperationSender;
use crate::rbac::{Access, AccessRequirements};
use crate::types::{PerformanceConfig, StorageConfig};

const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn snapshot_reconciliation_does_not_block_unrelated_lookups() {
    let (_storage_dir, toc) = new_toc();
    create_collection(&toc, "blocked");
    create_collection(&toc, "unrelated");
    let mut snapshot = toc.collections_snapshot_sync();
    let metadata = serde_json::from_value(serde_json::json!({"recovered": true})).unwrap();
    snapshot
        .collections
        .get_mut("blocked")
        .unwrap()
        .config
        .metadata = Some(metadata);
    let expected = snapshot.collections["blocked"].clone();
    let blocked = get_collection(&toc, "blocked");
    let shard_holder = blocked.shards_holder();
    let shard_guard = toc.general_runtime.block_on(shard_holder.read());

    let apply_toc = Arc::clone(&toc);
    let apply = std::thread::spawn(move || apply_toc.apply_collections_snapshot(snapshot));

    // State application saves configuration before taking the shard holder write
    // lock. Observing the new metadata proves it reached reconciliation, while the
    // read guard keeps shard reconciliation paused during the lookup probes.
    let deadline = Instant::now() + WAIT_TIMEOUT;
    let application_started = loop {
        let config = toc.general_runtime.block_on(blocked.config());
        if config.metadata == expected.config.metadata {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(1));
    };

    let access = Access::full("snapshot lookup test");
    let pass = access
        .check_collection_access("unrelated", AccessRequirements::new())
        .unwrap();
    let lookup = toc
        .general_runtime
        .block_on(async { tokio::time::timeout(WAIT_TIMEOUT, toc.get_collection(&pass)).await });
    let listing = toc
        .general_runtime
        .block_on(async { tokio::time::timeout(WAIT_TIMEOUT, toc.all_collections(&access)).await });
    let still_applying = !apply.is_finished();

    // Release the paused collection and join before asserting, including on failure.
    drop(shard_guard);
    let applied = apply.join().unwrap();

    assert!(
        application_started,
        "snapshot application did not reach the paused collection"
    );
    assert!(
        still_applying,
        "snapshot application must remain paused during the probes"
    );
    assert!(
        lookup.is_ok_and(|collection| collection.is_ok()),
        "unrelated lookup was blocked"
    );
    assert!(
        listing.is_ok_and(|collections| collections.len() == 2),
        "collection listing was blocked"
    );
    applied.unwrap();
    assert_eq!(toc.general_runtime.block_on(blocked.state()), expected);
}

#[test]
fn snapshot_creates_collection_with_disabled_local_replicas() {
    let (_storage_dir, toc) = new_toc();
    create_collection(&toc, "seed");
    let mut snapshot = toc.collections_snapshot_sync();
    let mut state = snapshot.collections["seed"].clone();
    for shard in state.shards.values_mut() {
        shard
            .replicas
            .insert(toc.this_peer_id, ReplicaState::Active);
        shard.replicas.insert(1, ReplicaState::Active);
    }
    snapshot
        .collections
        .insert("new".to_string(), state.clone());
    snapshot
        .aliases
        .insert("new_alias".to_string(), "new".to_string());

    toc.apply_collections_snapshot(snapshot).unwrap();

    let collection = get_collection(&toc, "new_alias");
    let actual = toc.general_runtime.block_on(collection.state());
    assert_eq!(actual, state);
    toc.general_runtime.block_on(async {
        let shard_holder = collection.shards_holder().read_owned().await;
        for replica_set in shard_holder.all_shards() {
            assert_eq!(
                replica_set.peer_state(toc.this_peer_id),
                Some(ReplicaState::Active)
            );
            assert!(
                !replica_set.active_shards(false).contains(&toc.this_peer_id),
                "empty local replica must be disabled"
            );
        }
    });
}

#[test]
fn snapshot_recreates_collection_with_different_uuid_and_removes_obsolete_collection() {
    let (_storage_dir, toc) = new_toc();
    create_collection(&toc, "recreated");
    create_collection(&toc, "removed");
    let mut snapshot = toc.collections_snapshot_sync();
    let state = snapshot.collections.get_mut("recreated").unwrap();
    state.config.uuid = Some(uuid::Uuid::new_v4());
    let expected = state.clone();
    snapshot.collections.remove("removed");
    snapshot
        .aliases
        .insert("alias".to_string(), "recreated".to_string());

    toc.apply_collections_snapshot(snapshot.clone()).unwrap();
    toc.apply_collections_snapshot(snapshot).unwrap();

    assert_eq!(toc.all_collections_sync(), vec!["recreated".to_string()]);
    let collection = get_collection(&toc, "alias");
    assert_eq!(toc.general_runtime.block_on(collection.state()), expected);
    assert!(!toc.get_collection_path("removed").exists());
}

#[test]
fn snapshot_recreates_collection_with_incompatible_config() {
    let (_storage_dir, toc) = new_toc();
    create_collection(&toc, "recreated");
    let mut snapshot = toc.collections_snapshot_sync();
    let state = snapshot.collections.get_mut("recreated").unwrap();
    state.config.params.sharding_method = Some(ShardingMethod::Custom);
    state.shards.clear();
    let expected = state.clone();

    toc.apply_collections_snapshot(snapshot).unwrap();

    let collection = get_collection(&toc, "recreated");
    assert_eq!(toc.general_runtime.block_on(collection.state()), expected);
}

#[test]
fn snapshot_state_failure_removes_unpublished_collection_and_allows_retry() {
    let (_storage_dir, toc) = new_toc();
    create_collection(&toc, "new");
    let mut snapshot = toc.collections_snapshot_sync();
    let state = snapshot.collections.get_mut("new").unwrap();
    state.config.uuid = Some(uuid::Uuid::new_v4());
    let expected = state.clone();
    let path = toc.get_collection_path("new");

    // Reproduce the state after construction but before publication, with a valid config
    // on disk. A UUID mismatch forces apply_state to fail without filesystem timing races.
    let error = toc.general_runtime.block_on(async {
        let _create_guard = toc.collection_create_lock.lock().await;
        let collection = toc.collections.write().await.remove("new").unwrap();
        toc.apply_collection_snapshot_state("new", collection, &expected, true)
            .await
            .err()
            .expect("state application must fail")
    });

    assert!(error.to_string().contains("UUID mismatch"), "{error}");
    assert!(toc.all_collections_sync().is_empty());
    assert!(
        !path.exists(),
        "failed unpublished collection must be removed"
    );

    toc.apply_collections_snapshot(snapshot).unwrap();

    let collection = get_collection(&toc, "new");
    assert_eq!(toc.general_runtime.block_on(collection.state()), expected);
}

#[test]
fn snapshot_state_failure_keeps_published_collection() {
    let (_storage_dir, toc) = new_toc();
    create_collection(&toc, "existing");
    let mut snapshot = toc.collections_snapshot_sync();
    let state = snapshot.collections.get_mut("existing").unwrap();
    state.config.uuid = Some(uuid::Uuid::new_v4());
    let collection = get_collection(&toc, "existing");
    let original_state = toc.general_runtime.block_on(collection.state());

    let error = toc.general_runtime.block_on(async {
        toc.apply_collection_snapshot_state("existing", collection, state, false)
            .await
            .err()
            .expect("state application must fail")
    });

    assert!(error.to_string().contains("UUID mismatch"), "{error}");
    let collection = get_collection(&toc, "existing");
    assert_eq!(
        toc.general_runtime.block_on(collection.state()),
        original_state
    );
    assert!(toc.get_collection_path("existing").exists());
}

fn create_collection(toc: &TableOfContent, name: &str) {
    let config: CreateCollection = serde_json::from_value(serde_json::json!({
        "vectors": {"size": 4, "distance": "Dot"},
        "shard_number": 1,
    }))
    .unwrap();
    let operation = CreateCollectionOperation::new(name.to_string(), config).unwrap();
    toc.perform_collection_meta_op_sync(CollectionMetaOperations::CreateCollection(operation))
        .unwrap();
}

fn get_collection(toc: &TableOfContent, name: &str) -> Arc<Collection> {
    let access = Access::full("snapshot test");
    let pass = access
        .check_collection_access(name, AccessRequirements::new())
        .unwrap();
    toc.general_runtime
        .block_on(toc.get_collection(&pass))
        .unwrap()
}

fn new_toc() -> (TempDir, Arc<TableOfContent>) {
    let storage_dir = Builder::new().prefix("storage").tempdir().unwrap();

    let config = StorageConfig {
        storage_path: storage_dir.path().to_path_buf(),
        snapshots_path: storage_dir.path().join("snapshots"),
        snapshots_config: Default::default(),
        temp_path: None,
        #[expect(deprecated)]
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

    (storage_dir, toc)
}
