//! Tests that loading a local shard from disk does not hold the collection config lock.

use std::sync::{Arc, mpsc};

use common::ambient;
use common::budget::ResourceBudget;
use common::save_on_disk::SaveOnDisk;
use parking_lot::RwLock as ParkingRwLock;
use segment::pending_changes::PersistedProxyChanges;
use tempfile::Builder;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::ShardOperation;
use crate::tests::fixtures::create_collection_config;

/// A collection config write must not wait for the segment phase of [`LocalShard::load`], which
/// only reads the config to snapshot it before it touches the disk.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_does_not_hold_config_lock_during_segment_load() {
    let _scope = ambient::async_test_guard();

    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
    let payload_index_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
    let payload_index_schema_file = payload_index_schema_dir.path().join("payload-schema.json");
    let payload_index_schema =
        Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file).unwrap());

    let config = create_collection_config();
    let collection_config = Arc::new(RwLock::new(config.clone()));

    let update_runtime = Handle::current();
    let search_runtime = AdaptiveSearchHandle::current_for_tests();

    // Build a shard with a single segment, then close it to load it back from disk below.
    let shard = LocalShard::build(
        0,
        "test".to_string(),
        collection_dir.path(),
        Arc::clone(&collection_config),
        Arc::new(Default::default()),
        Arc::clone(&payload_index_schema),
        update_runtime.clone(),
        search_runtime.clone(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();
    shard.stop_gracefully().await;

    // Loading a segment reads the payload index schema to rebuild payload indices. Hold it
    // exclusively in another thread, so the segment phase of the load below stays blocked.
    let (locked_tx, locked_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let schema_for_blocker = Arc::clone(&payload_index_schema);
    let blocker = std::thread::spawn(move || {
        let _guard = ParkingRwLock::write(&schema_for_blocker);
        locked_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    locked_rx.recv().unwrap();

    let load = LocalShard::load(
        0,
        "test".to_string(),
        collection_dir.path(),
        Arc::clone(&collection_config),
        config.optimizer_config.clone(),
        Arc::new(Default::default()),
        Arc::clone(&payload_index_schema),
        true,
        PersistedProxyChanges::Replay,
        update_runtime,
        search_runtime,
        ResourceBudget::default(),
    );
    tokio::pin!(load);

    // A single poll runs the load into the segment phase, which is blocked on the schema, so the
    // load is pending here without relying on a timeout.
    assert!(futures::poll!(load.as_mut()).is_pending());

    // The load is in the segment phase now: writing the collection config must not wait for it.
    assert!(
        collection_config.try_write().is_ok(),
        "collection config must not be locked while the shard loads segments"
    );

    let mut config_write = collection_config.write().await;
    config_write.optimizer_config.flush_interval_sec = 42;
    drop(config_write);

    // Release the segment phase and let the load finish.
    release_tx.send(()).unwrap();
    blocker.join().unwrap();

    let loaded = load.await.unwrap();
    {
        let loaded_config = loaded.collection_config.read().await;
        assert_eq!(loaded_config.optimizer_config.flush_interval_sec, 42);
    }
    loaded.stop_gracefully().await;
}
