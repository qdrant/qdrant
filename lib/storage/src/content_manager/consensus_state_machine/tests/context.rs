//! Scraping [`NodeContext`] out of this node's storage config

use std::num::NonZeroUsize;
use std::path::PathBuf;

use collection::config::PayloadStorageParams;
use collection::shards::transfer::ShardTransferMethod;
use common::mmap;
use segment::data_types::collection_defaults::CollectionConfigDefaults;
use segment::types::Memory;

use super::*;
use crate::types::{PerformanceConfig, StorageConfig};

#[test]
fn from_storage_config() {
    let expected = NodeContext {
        peer_id: PEER_ID,
        is_distributed: true,
        collection_defaults: Some(collection_defaults()),
        default_shard_transfer_method: Some(ShardTransferMethod::StreamRecords),
        max_collections: Some(7),
        wal: wal_config(),
        optimizers: optimizers_config(),
        hnsw_index: hnsw_config(),
        payload: Some(payload_params()),
        on_disk_payload: true,
    };

    let context = NodeContext::from_storage_config(&storage_config(), PEER_ID, true);

    assert_eq!(context, expected);
}

/// Every option an operation reads is set away from its default.
///
/// Two pairs of read and ignored options share a type — `on_disk_payload` with
/// `handle_collection_load_errors`, `max_collections` with `update_queue_size` — so each of the
/// four gets a value that tells it apart from the other three.
fn storage_config() -> StorageConfig {
    StorageConfig {
        collection: Some(collection_defaults()),
        shard_transfer_method: Some(ShardTransferMethod::StreamRecords),
        max_collections: Some(7),
        wal: wal_config(),
        optimizers: optimizers_config(),
        hnsw_index: hnsw_config(),
        payload: Some(payload_params()),
        #[expect(deprecated)]
        on_disk_payload: true,

        storage_path: PathBuf::from("storage"),
        snapshots_path: PathBuf::from("snapshots"),
        snapshots_config: Default::default(),
        temp_path: None,
        optimizers_overwrite: None,
        performance: PerformanceConfig {
            max_search_threads: 1,
            max_optimization_runtime_threads: 1,
            update_rate_limit: None,
            search_timeout_sec: None,
            optimizer_cpu_budget: 0,
            optimizer_io_budget: 0,
            incoming_shard_transfers_limit: None,
            outgoing_shard_transfers_limit: None,
            async_scorer: None,
            io_uring: None,
            load_concurrency: Default::default(),
        },
        hnsw_global_config: Default::default(),
        mmap_advice: mmap::Advice::Random,
        low_memory_mode: Default::default(),
        node_type: Default::default(),
        update_queue_size: Some(9),
        handle_collection_load_errors: false,
        recovery_mode: None,
        update_concurrency: NonZeroUsize::new(3),
        quotas: None,
    }
}

fn collection_defaults() -> CollectionConfigDefaults {
    CollectionConfigDefaults {
        vectors: None,
        quantization: None,
        shard_number: Some(3),
        shard_number_per_node: None,
        replication_factor: Some(2),
        write_consistency_factor: None,
        strict_mode: None,
    }
}

fn wal_config() -> WalConfig {
    WalConfig {
        wal_capacity_mb: 16,
        wal_segments_ahead: 1,
        wal_retain_closed: 2,
    }
}

fn hnsw_config() -> HnswConfig {
    HnswConfig {
        m: 32,
        ..Default::default()
    }
}

fn payload_params() -> PayloadStorageParams {
    PayloadStorageParams {
        memory: Some(Memory::Pinned),
    }
}
