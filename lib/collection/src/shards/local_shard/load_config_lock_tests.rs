//! Tests and test hooks for collection config locking in [`LocalShard::load`].

pub mod load_hooks {
    use tokio::sync::oneshot;

    struct PauseAfterSegmentsLoaded {
        collection: String,
        paused: Option<oneshot::Sender<()>>,
        release: Option<oneshot::Receiver<()>>,
    }

    static PAUSE_AFTER_SEGMENTS_LOADED: std::sync::Mutex<Option<PauseAfterSegmentsLoaded>> =
        std::sync::Mutex::new(None);

    /// Pause the next [`LocalShard::load`] of `collection` once its segments are loaded.
    ///
    /// `paused` is signalled when the load reaches that point, and the load continues once
    /// `release` resolves.
    pub fn install_pause_after_segments_loaded(
        collection: String,
        paused: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    ) {
        let mut hook = PAUSE_AFTER_SEGMENTS_LOADED.lock().unwrap();
        *hook = Some(PauseAfterSegmentsLoaded {
            collection,
            paused: Some(paused),
            release: Some(release),
        });
    }

    pub fn clear_pause_after_segments_loaded() {
        let mut hook = PAUSE_AFTER_SEGMENTS_LOADED.lock().unwrap();
        *hook = None;
    }

    pub async fn pause_after_segments_loaded(collection_name: &str) {
        let release = {
            let mut hook = PAUSE_AFTER_SEGMENTS_LOADED.lock().unwrap();
            let Some(hook) = hook.as_mut() else {
                return;
            };

            if collection_name != hook.collection {
                return;
            }

            if let Some(paused) = hook.paused.take() {
                let _ = paused.send(());
            }
            hook.release.take()
        };

        if let Some(release) = release {
            let _ = release.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use common::ambient::{AmbientContext, AmbientFutureExt};
    use common::budget::ResourceBudget;
    use common::save_on_disk::SaveOnDisk;
    use segment::pending_changes::PersistedProxyChanges;
    use tempfile::Builder;
    use tokio::runtime::Handle;
    use tokio::sync::{RwLock, oneshot};

    use super::load_hooks::{
        clear_pause_after_segments_loaded, install_pause_after_segments_loaded,
    };
    use crate::common::adaptive_handle::AdaptiveSearchHandle;
    use crate::shards::local_shard::LocalShard;
    use crate::shards::shard_trait::{ShardOperation, WaitUntil};
    use crate::tests::fixtures::{create_collection_config, upsert_operation};

    /// A collection config writer must not wait for a shard to load its segments.
    ///
    /// The load is paused right after its segments are loaded, which is in the middle of the
    /// disk phase that used to run under the collection config read guard.
    #[tokio::test(flavor = "multi_thread")]
    async fn load_does_not_block_config_writer_while_loading_segments() {
        let collection_name = "load_does_not_block_config_writer".to_string();

        let collection_dir = Builder::new().prefix("load_config_lock").tempdir().unwrap();
        let payload_index_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
        let payload_index_schema_file = payload_index_schema_dir.path().join("payload-schema.json");
        let payload_index_schema =
            Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file).unwrap());

        let config = create_collection_config();

        let update_runtime = Handle::current();
        let search_runtime = AdaptiveSearchHandle::current_for_tests();

        // Persist a shard with some points, so there are segments to load
        let shard = LocalShard::build(
            0,
            collection_name.clone(),
            collection_dir.path(),
            Arc::new(RwLock::new(config.clone())),
            Arc::new(Default::default()),
            payload_index_schema.clone(),
            update_runtime.clone(),
            search_runtime.clone(),
            ResourceBudget::default(),
            config.optimizer_config.clone(),
        )
        .await
        .unwrap();

        shard
            .update(upsert_operation().into(), WaitUntil::Visible, None)
            .measured(AmbientContext::new())
            .await
            .unwrap();
        let points_before = shard.info().await.unwrap().points_count.unwrap_or(0);
        assert_eq!(points_before, 5);

        shard.stop_gracefully().await;

        let (paused_tx, paused_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        install_pause_after_segments_loaded(collection_name.clone(), paused_tx, release_rx);

        let collection_config = Arc::new(RwLock::new(config.clone()));

        let load_task = {
            let collection_name = collection_name.clone();
            let shard_path = collection_dir.path().to_path_buf();
            let collection_config = collection_config.clone();
            let payload_index_schema = payload_index_schema.clone();
            let optimizers_config = config.optimizer_config.clone();
            tokio::spawn(async move {
                LocalShard::load(
                    0,
                    collection_name,
                    &shard_path,
                    collection_config,
                    optimizers_config,
                    Arc::new(Default::default()),
                    payload_index_schema,
                    true,
                    PersistedProxyChanges::Replay,
                    update_runtime,
                    search_runtime,
                    ResourceBudget::default(),
                )
                .await
            })
        };

        tokio::time::timeout(Duration::from_secs(30), paused_rx)
            .await
            .expect("shard load should reach the point after loading segments")
            .expect("load hook should signal the pause");

        // The load is paused after loading its segments. A config writer must get the lock now,
        // without waiting for the load to finish.
        let write_result = collection_config.try_write().map(|mut config| {
            config.optimizer_config.indexing_threshold = Some(1234);
        });

        // Let the load continue regardless of the outcome, so a failing assertion below doesn't
        // leave it hanging
        let _ = release_tx.send(());
        clear_pause_after_segments_loaded();

        assert!(
            write_result.is_ok(),
            "collection config writer is blocked while the shard loads its segments",
        );

        let shard = tokio::time::timeout(Duration::from_secs(30), load_task)
            .await
            .expect("shard load should finish after it is released")
            .unwrap()
            .unwrap();

        // The shard loaded completely
        let points_after = shard.info().await.unwrap().points_count.unwrap_or(0);
        assert_eq!(points_after, points_before);

        shard.stop_gracefully().await;
    }
}
