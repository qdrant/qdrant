use std::collections::HashMap;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use collection::shards::channel_service::ChannelService;
use common::budget::ResourceBudget;
use storage::content_manager::CollectionContainer;
use storage::content_manager::collection_meta_ops::{
    CollectionMetaOperations, CreateCollection, CreateCollectionOperation,
};
use storage::content_manager::consensus::operation_sender::OperationSender;
use storage::content_manager::consensus_manager::CollectionsSnapshot;
use storage::content_manager::consensus_ops::ConsensusOperations;
use storage::content_manager::toc::TableOfContent;
use storage::rbac::{Access, AccessRequirements};
use storage::types::StorageConfig;
use tempfile::Builder;
use tokio::runtime::Runtime;
use uuid::Uuid;

const SNAPSHOT_COLLECTIONS: usize = 16;
const SHARDS_PER_COLLECTION: u32 = 2;
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);
const APPLY_TIMEOUT: Duration = Duration::from_secs(180);

fn test_storage_config(storage_dir: &std::path::Path) -> StorageConfig {
    let json = serde_json::json!({
        "storage_path": storage_dir.to_string_lossy().into_owned(),
        "snapshots_path": storage_dir.join("snapshots").to_string_lossy().into_owned(),
        "temp_path": null,
        "on_disk_payload": false,
        "optimizers": {
            "deleted_threshold": 0.5,
            "vacuum_min_vector_number": 100,
            "default_segment_number": 2,
            "max_segment_size": null,
            "memmap_threshold": 100,
            "indexing_threshold": 100,
            "flush_interval_sec": 2,
            "max_optimization_threads": 2,
        },
        "wal": { "wal_capacity_mb": 32, "wal_segments_ahead": 0 },
        "performance": {
            "max_search_threads": 1,
            "max_optimization_runtime_threads": 2,
            "optimizer_cpu_budget": 0,
            "optimizer_io_budget": 0,
            "incoming_shard_transfers_limit": 1,
            "outgoing_shard_transfers_limit": 1,
        },
        "hnsw_index": { "m": 16, "ef_construct": 100, "full_scan_threshold": 10000 },
        "update_concurrency": 2,
    });
    serde_json::from_value(json).expect("valid storage config")
}

fn test_toc(config: &StorageConfig) -> (Arc<TableOfContent>, mpsc::Receiver<ConsensusOperations>) {
    let (propose_sender, propose_receiver) = mpsc::channel();
    let toc = Arc::new(
        TableOfContent::new(
            config,
            ResourceBudget::default(),
            ChannelService::new(6333, false, None, None),
            0,
            Some(OperationSender::new(propose_sender)),
        )
        .unwrap(),
    );
    (toc, propose_receiver)
}

fn create_seed_collection(toc: &TableOfContent) {
    let create_collection: CreateCollection = serde_json::from_value(serde_json::json!({
        "vectors": { "size": 4, "distance": "Dot" },
        "shard_number": SHARDS_PER_COLLECTION,
        "hnsw_config": null,
        "wal_config": null,
        "optimizers_config": null,
        "sparse_vectors": null,
        "strict_mode_config": null,
    }))
    .expect("valid create collection request");
    CollectionContainer::perform_collection_meta_op(
        toc,
        CollectionMetaOperations::CreateCollection(
            CreateCollectionOperation::new("seed".to_string(), create_collection).unwrap(),
        ),
    )
    .unwrap();
}

#[test]
fn raft_snapshot_apply_keeps_collection_reads_available() {
    let storage_dir = Builder::new().prefix("storage").tempdir().unwrap();
    let config = test_storage_config(storage_dir.path());
    let (toc, _propose_receiver) = test_toc(&config);
    create_seed_collection(toc.as_ref());

    let mut seed_state = CollectionContainer::collections_snapshot(toc.as_ref())
        .collections
        .get("seed")
        .cloned()
        .expect("seed collection state must exist");
    seed_state.config.metadata =
        Some(serde_json::from_value(serde_json::json!({ "snapshot_test": true })).unwrap());

    let mut collections = HashMap::with_capacity(SNAPSHOT_COLLECTIONS + 1);
    collections.insert("seed".to_string(), seed_state.clone());
    for i in 0..SNAPSHOT_COLLECTIONS {
        collections.insert(format!("snapshot-{i}"), seed_state.clone());
    }
    let snapshot = CollectionsSnapshot {
        collections,
        aliases: Default::default(),
    };

    let probe_rt = Runtime::new().unwrap();
    let access = Access::full("snapshot apply availability test");
    let pass = access
        .check_collection_access("seed", AccessRequirements::new())
        .unwrap();

    let baseline_all = probe_rt.block_on(toc.all_collections(&access));
    let seed_collection = probe_rt.block_on(toc.get_collection(&pass)).unwrap();
    assert_eq!(baseline_all.len(), 1);

    // 分片读锁使状态应用在写锁处暂停，避免依赖机器速度来保证探针与快照重叠。
    let shards_holder = seed_collection.shards_holder();
    let shard_guard = probe_rt.block_on(shards_holder.clone().read_owned());

    let apply_toc = Arc::clone(&toc);
    let apply = thread::spawn(move || {
        CollectionContainer::apply_collections_snapshot(apply_toc.as_ref(), snapshot)
    });

    probe_rt
        .block_on(async {
            tokio::time::timeout(APPLY_TIMEOUT, async {
                // 持有读锁时，新读请求进入 Pending，说明状态应用的写者已排队。
                loop {
                    let read = std::pin::pin!(shards_holder.read());
                    if futures::poll!(read).is_pending() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
        })
        .expect("snapshot application did not reach shard reconciliation");
    assert!(!apply.is_finished());

    let (listing, lookup) = probe_rt.block_on(async {
        tokio::join!(
            tokio::time::timeout(PROBE_TIMEOUT, toc.all_collections(&access)),
            tokio::time::timeout(PROBE_TIMEOUT, toc.get_collection(&pass)),
        )
    });

    drop(shard_guard);
    apply.join().unwrap().unwrap();

    let collections_after = probe_rt.block_on(toc.all_collections(&access));

    listing.expect("collection listing stalled during snapshot application");
    lookup
        .expect("collection lookup stalled during snapshot application")
        .unwrap();
    assert_eq!(collections_after.len(), SNAPSHOT_COLLECTIONS + 1);
}

#[test]
fn raft_snapshot_apply_recreates_collection_with_different_uuid() {
    let storage_dir = Builder::new().prefix("storage").tempdir().unwrap();
    let config = test_storage_config(storage_dir.path());
    let (toc, _propose_receiver) = test_toc(&config);
    create_seed_collection(toc.as_ref());

    let mut snapshot = CollectionContainer::collections_snapshot(toc.as_ref());
    let state = snapshot.collections.get_mut("seed").unwrap();
    let replacement_uuid = Uuid::from_u128(42);
    assert_ne!(state.config.uuid, Some(replacement_uuid));
    state.config.uuid = Some(replacement_uuid);

    let apply_toc = Arc::clone(&toc);
    let (result_sender, result_receiver) = mpsc::channel();
    let apply = thread::spawn(move || {
        let result = CollectionContainer::apply_collections_snapshot(apply_toc.as_ref(), snapshot);
        result_sender.send(result).unwrap();
    });
    result_receiver
        .recv_timeout(Duration::from_secs(30))
        .expect("snapshot recreation retained a reference to the deleted collection")
        .unwrap();
    apply.join().unwrap();

    let restored = CollectionContainer::collections_snapshot(toc.as_ref());
    assert_eq!(restored.collections.len(), 1);
    assert_eq!(
        restored.collections["seed"].config.uuid,
        Some(replacement_uuid)
    );
}
