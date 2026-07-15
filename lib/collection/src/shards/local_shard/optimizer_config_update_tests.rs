//! Tests and test hooks for local shard optimizer config updates.

pub mod worker_restart_hooks {
    use std::sync::Arc;

    use crate::shards::local_shard::LocalShard;

    struct WorkerRestartHook {
        pause_collection: String,
        fail_collection: String,
        paused: Option<tokio::sync::oneshot::Sender<()>>,
        release: Option<tokio::sync::oneshot::Receiver<()>>,
        fail_after_pause: Arc<tokio::sync::Notify>,
    }

    static WORKER_RESTART_HOOK: std::sync::Mutex<Option<WorkerRestartHook>> =
        std::sync::Mutex::new(None);

    pub fn install_worker_restart_hook(
        pause_collection: String,
        fail_collection: String,
        paused: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut hook = WORKER_RESTART_HOOK.lock().unwrap();
        *hook = Some(WorkerRestartHook {
            pause_collection,
            fail_collection,
            paused: Some(paused),
            release: Some(release),
            fail_after_pause: Arc::new(tokio::sync::Notify::new()),
        });
    }

    pub fn clear_worker_restart_hook() {
        let mut hook = WORKER_RESTART_HOOK.lock().unwrap();
        *hook = None;
    }

    pub fn worker_restart_fail_after_pause(
        collection_name: &str,
    ) -> Option<Arc<tokio::sync::Notify>> {
        let hook = WORKER_RESTART_HOOK.lock().unwrap();
        let hook = hook.as_ref()?;

        (collection_name == hook.fail_collection).then(|| hook.fail_after_pause.clone())
    }

    pub async fn worker_restart_pause_after_stop(collection_name: &str) {
        let release = {
            let mut hook = WORKER_RESTART_HOOK.lock().unwrap();
            let Some(hook) = hook.as_mut() else {
                return;
            };

            if collection_name != hook.pause_collection {
                return;
            }

            if let Some(paused) = hook.paused.take() {
                let _ = paused.send(());
            }
            hook.fail_after_pause.notify_one();
            hook.release.take()
        };

        if let Some(release) = release {
            let _ = release.await;
        }
    }

    pub async fn update_handler_is_stopped(shard: &LocalShard) -> bool {
        let update_handler = shard.update_handler.clone();
        tokio::task::spawn_blocking(move || update_handler.blocking_lock().is_stopped())
            .await
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use common::budget::ResourceBudget;
    use common::save_on_disk::SaveOnDisk;
    use futures::future;
    use tempfile::Builder;
    use tokio::runtime::Handle;
    use tokio::sync::{RwLock, oneshot};

    use super::worker_restart_hooks::{
        clear_worker_restart_hook, install_worker_restart_hook, update_handler_is_stopped,
    };
    use crate::common::adaptive_handle::AdaptiveSearchHandle;
    use crate::shards::local_shard::LocalShard;
    use crate::shards::shard_trait::ShardOperation;
    use crate::tests::fixtures::create_collection_config;

    #[tokio::test(flavor = "multi_thread")]
    async fn optimizer_config_update_refreshes_prevent_unoptimized_flag() {
        let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
        let payload_index_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
        let payload_index_schema_file = payload_index_schema_dir.path().join("payload-schema.json");
        let payload_index_schema =
            Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file).unwrap());

        let mut config = create_collection_config();
        config.optimizer_config.prevent_unoptimized = Some(false);

        let update_runtime = Handle::current();
        let search_runtime = AdaptiveSearchHandle::current_for_tests();
        let shard = LocalShard::build(
            0,
            "test".to_string(),
            collection_dir.path(),
            Arc::new(RwLock::new(config.clone())),
            Arc::new(Default::default()),
            payload_index_schema,
            update_runtime.clone(),
            search_runtime.clone(),
            ResourceBudget::default(),
            config.optimizer_config.clone(),
        )
        .await
        .unwrap();

        assert!(!shard.update_handler.lock().await.prevent_unoptimized);

        {
            let mut shard_config = shard.collection_config.write().await;
            shard_config.optimizer_config.prevent_unoptimized = Some(true);
        }

        shard.on_optimizer_config_update().await.unwrap();

        assert!(shard.update_handler.lock().await.prevent_unoptimized);

        {
            let mut shard_config = shard.collection_config.write().await;
            shard_config.optimizer_config.prevent_unoptimized = Some(false);
        }

        shard.on_optimizer_config_update().await.unwrap();

        assert!(!shard.update_handler.lock().await.prevent_unoptimized);

        shard.stop_gracefully().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn optimizer_config_update_clears_optimizer_errors() {
        let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
        let payload_index_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
        let payload_index_schema_file = payload_index_schema_dir.path().join("payload-schema.json");
        let payload_index_schema =
            Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file).unwrap());

        let config = create_collection_config();

        let update_runtime = Handle::current();
        let search_runtime = AdaptiveSearchHandle::current_for_tests();
        let shard = LocalShard::build(
            0,
            "test".to_string(),
            collection_dir.path(),
            Arc::new(RwLock::new(config.clone())),
            Arc::new(Default::default()),
            payload_index_schema,
            update_runtime.clone(),
            search_runtime.clone(),
            ResourceBudget::default(),
            config.optimizer_config.clone(),
        )
        .await
        .unwrap();

        assert!(shard.segments.read().optimizer_errors.is_none());

        shard
            .segments
            .write()
            .report_optimizer_error("test optimizer error");
        assert!(shard.segments.read().optimizer_errors.is_some());

        shard.on_optimizer_config_update().await.unwrap();

        assert!(
            shard.segments.read().optimizer_errors.is_none(),
            "optimizer errors should be cleared after config update"
        );

        shard.stop_gracefully().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_join_all_completes_sibling_restart_after_workers_stop() {
        let pause_collection = "pause_after_workers_stop".to_string();
        let fail_collection = "fail_after_pause".to_string();

        let pause_collection_dir = Builder::new()
            .prefix("pause-workers-stop")
            .tempdir()
            .unwrap();
        let fail_collection_dir = Builder::new().prefix("fail-after-pause").tempdir().unwrap();
        let payload_index_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
        let payload_index_schema_file = payload_index_schema_dir.path().join("payload-schema.json");
        let payload_index_schema =
            Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file).unwrap());

        let config = create_collection_config();

        let update_runtime = Handle::current();
        let search_runtime = AdaptiveSearchHandle::current_for_tests();
        let paused_shard = LocalShard::build(
            0,
            pause_collection.clone(),
            pause_collection_dir.path(),
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
        let failing_shard = LocalShard::build(
            1,
            fail_collection.clone(),
            fail_collection_dir.path(),
            Arc::new(RwLock::new(config.clone())),
            Arc::new(Default::default()),
            payload_index_schema,
            update_runtime.clone(),
            search_runtime.clone(),
            ResourceBudget::default(),
            config.optimizer_config.clone(),
        )
        .await
        .unwrap();

        assert!(!update_handler_is_stopped(&paused_shard).await);
        assert!(!update_handler_is_stopped(&failing_shard).await);

        let (paused_tx, paused_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        install_worker_restart_hook(pause_collection, fail_collection, paused_tx, release_rx);

        let release_task = tokio::spawn(async move {
            paused_rx
                .await
                .expect("paused shard should reach the window after wait_workers_stops");
            let _ = release_tx.send(());
        });

        // Generous timeout: the two-shard stop/rebuild cycle can exceed 5s on loaded
        // CI runners (notably Windows). The regression check is not weakened by this:
        // if join_all drops the sibling restart again, the paused shard is never
        // released and the timeout still fails the test.
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            future::join_all(vec![
                paused_shard.on_optimizer_config_update(),
                failing_shard.on_optimizer_config_update(),
            ]),
        )
        .await
        .expect("worker restart should complete after the paused shard is released");
        release_task.await.expect("release task should not panic");
        clear_worker_restart_hook();

        assert!(
            result.iter().any(|shard_result| shard_result.is_err()),
            "the injected sibling failure should be reported"
        );
        assert!(
            !update_handler_is_stopped(&paused_shard).await,
            "join_all must let the paused shard finish run_workers even when a sibling fails"
        );
        assert!(
            !update_handler_is_stopped(&failing_shard).await,
            "the injected sibling error happens before the failing shard stops its own workers"
        );

        paused_shard.stop_gracefully().await;
        failing_shard.stop_gracefully().await;
    }
}
