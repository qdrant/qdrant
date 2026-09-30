use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use collection::shards::channel_service::ChannelService;
use common::budget::ResourceBudget;
use storage::content_manager::CollectionContainer;
use storage::content_manager::collection_meta_ops::{
    CollectionMetaOperations, CreateCollection, CreateCollectionOperation,
};
use storage::content_manager::consensus::operation_sender::OperationSender;
use storage::content_manager::consensus_manager::CollectionsSnapshot;
use storage::content_manager::toc::TableOfContent;
use storage::rbac::{Access, AccessRequirements};
use storage::types::StorageConfig;
use tempfile::Builder;
use tokio::runtime::Runtime;

const SNAPSHOT_COLLECTIONS: usize = 16;
const SHARDS_PER_COLLECTION: u32 = 2;
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);
const PROBE_INTERVAL: Duration = Duration::from_millis(10);

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

#[test]
fn raft_snapshot_apply_keeps_collection_reads_available() {
    let storage_dir = Builder::new().prefix("storage").tempdir().unwrap();
    let config = test_storage_config(storage_dir.path());

    let (propose_sender, _propose_receiver) = std::sync::mpsc::channel();
    let toc = Arc::new(
        TableOfContent::new(
            &config,
            ResourceBudget::default(),
            ChannelService::new(6333, false, None, None),
            0,
            Some(OperationSender::new(propose_sender)),
        )
        .unwrap(),
    );

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
        toc.as_ref(),
        CollectionMetaOperations::CreateCollection(
            CreateCollectionOperation::new("seed".to_string(), create_collection).unwrap(),
        ),
    )
    .unwrap();

    let seed_state = CollectionContainer::collections_snapshot(toc.as_ref())
        .collections
        .get("seed")
        .cloned()
        .expect("seed collection state must exist");

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
    let baseline_get = probe_rt.block_on(toc.get_collection(&pass));
    assert_eq!(baseline_all.len(), 1);
    assert!(baseline_get.is_ok());

    let apply_toc = Arc::clone(&toc);
    let apply_started = Instant::now();
    let apply = thread::spawn(move || {
        CollectionContainer::apply_collections_snapshot(apply_toc.as_ref(), snapshot)
    });

    let mut completed_all = 0usize;
    let mut completed_get = 0usize;
    let mut timed_out_all = 0usize;
    let mut timed_out_get = 0usize;
    let deadline = Instant::now() + Duration::from_secs(180);
    while !apply.is_finished() && Instant::now() < deadline {
        if probe_rt
            .block_on(async {
                tokio::time::timeout(PROBE_TIMEOUT, toc.all_collections(&access)).await
            })
            .is_ok()
        {
            completed_all += 1;
        } else {
            timed_out_all += 1;
        }

        if probe_rt
            .block_on(async {
                tokio::time::timeout(PROBE_TIMEOUT, toc.get_collection(&pass)).await
            })
            .is_ok_and(|result| result.is_ok())
        {
            completed_get += 1;
        } else {
            timed_out_get += 1;
        }

        thread::sleep(PROBE_INTERVAL);
    }

    let apply_elapsed = apply_started.elapsed();
    assert!(
        apply.is_finished(),
        "snapshot application did not finish in time"
    );
    apply.join().unwrap().unwrap();

    let collections_after = probe_rt.block_on(toc.all_collections(&access));

    assert!(apply_elapsed >= Duration::from_millis(200));
    assert!(completed_all > 0 && completed_get > 0);
    assert_eq!(
        timed_out_all, 0,
        "collection listing stalled during snapshot application"
    );
    assert_eq!(
        timed_out_get, 0,
        "collection lookup stalled during snapshot application"
    );
    assert_eq!(collections_after.len(), SNAPSHOT_COLLECTIONS + 1);
}
