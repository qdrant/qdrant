use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use ahash::AHashMap;
use common::ambient::{AmbientContext, AmbientFutureExt};
use common::budget::ResourceBudget;
use fs_err as fs;
use rstest::rstest;
use segment::types::StrictModeConfig;
use shard::snapshots::snapshot_data::SnapshotData;
use shard::snapshots::snapshot_manifest::RecoveryType;
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::collection::Collection;
use crate::operations::types::CollectionError;
use crate::shards::channel_service::ChannelService;
use crate::shards::collection_shard_distribution::CollectionShardDistribution;
use crate::shards::replica_set::snapshots::install_restore_local_replica_before_flag_hook;
use crate::shards::shard_initializing_flag_path;
use crate::shards::shard_trait::WaitUntil;
use crate::tests::fixtures::{create_collection_config, upsert_operation};

type PreparationHook = (oneshot::Sender<()>, oneshot::Receiver<()>);

// Restoring a packed shard can take longer when other collection tests run in parallel.
const RESTORE_COMPLETION_TIMEOUT: Duration = Duration::from_secs(30);

static PREPARATION_HOOKS: LazyLock<Mutex<HashMap<PathBuf, PreparationHook>>> =
    LazyLock::new(Mutex::default);

pub(super) async fn pause_preparation(temp_dir: &Path) {
    let hook = PREPARATION_HOOKS.lock().unwrap().remove(temp_dir);

    if let Some((reached, release)) = hook {
        let _ = reached.send(());
        let _ = release.await;
    }
}

#[derive(Clone, Copy)]
enum ShardChange {
    Keep,
    Remove,
    Replace,
    Restore,
    RemoveDuringRestore,
    ReplaceDuringRestore,
    Cancel,
    DropFuture,
}

#[rstest]
#[case::packed(ShardChange::Keep, false)]
#[case::unpacked(ShardChange::Keep, true)]
#[case::removed(ShardChange::Remove, false)]
#[case::replaced(ShardChange::Replace, false)]
#[case::restoring(ShardChange::Restore, false)]
#[case::removed_during_restore(ShardChange::RemoveDuringRestore, false)]
#[case::replaced_during_restore(ShardChange::ReplaceDuringRestore, false)]
#[case::cancelled(ShardChange::Cancel, false)]
#[case::dropped(ShardChange::DropFuture, false)]
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_preparation_releases_shard_holder(
    #[case] change: ShardChange,
    #[case] unpacked: bool,
) {
    let collection_dir = tempfile::tempdir().unwrap();
    let snapshots_dir = tempfile::tempdir().unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let config = create_collection_config();
    let distribution = CollectionShardDistribution {
        shards: AHashMap::from([(0, HashSet::from([1]))]),
    };
    let collection = Arc::new(
        Collection::new(
            "test".to_string(),
            1,
            collection_dir.path(),
            snapshots_dir.path(),
            &config,
            Arc::new(Default::default()),
            distribution,
            None,
            ChannelService::default(),
            Arc::new(|_, _, _| {}),
            Arc::new(|_| {}),
            Arc::new(|_, _| {}),
            None,
            None,
            ResourceBudget::default(),
            None,
        )
        .await
        .unwrap(),
    );

    let original = collection
        .shards_holder()
        .read()
        .await
        .get_shard(0)
        .cloned()
        .unwrap();
    original
        .update_local(upsert_operation().into(), WaitUntil::Visible, None, false)
        .measured(AmbientContext::new())
        .await
        .unwrap();
    let original_points = original.info(true).await.unwrap().points_count;
    let snapshot = collection
        .create_shard_snapshot(0, temp_dir.path())
        .await
        .unwrap();
    let snapshot_path = snapshots_dir.path().join("shards/0").join(snapshot.name);
    let snapshot_data = if unpacked {
        let snapshot_dir = tempfile::tempdir().unwrap();
        common::tar_unpack::tar_unpack_file(&snapshot_path, snapshot_dir.path()).unwrap();
        SnapshotData::Unpacked(snapshot_dir)
    } else {
        SnapshotData::new_packed_persistent(snapshot_path)
    };
    let cancel = cancel::CancellationToken::new();

    let (reached_tx, reached_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    PREPARATION_HOOKS
        .lock()
        .unwrap()
        .insert(temp_dir.path().to_path_buf(), (reached_tx, release_rx));

    let mut restore = Box::pin(async {
        collection
            .restore_shard_snapshot(
                0,
                snapshot_data,
                RecoveryType::Full,
                1,
                false,
                temp_dir.path(),
                None,
                cancel.clone(),
            )
            .await?
            .await
    });
    tokio::select! {
        result = &mut restore => panic!("restore finished before preparation paused: {result:?}"),
        reached = timeout(Duration::from_secs(10), reached_rx) => reached.unwrap().unwrap(),
    }

    // A real writer and a subsequent reader must finish while preparation is paused.
    let strict_mode = StrictModeConfig {
        enabled: Some(true),
        ..Default::default()
    };
    timeout(
        Duration::from_secs(10),
        collection.update_strict_mode_config(strict_mode),
    )
    .await
    .expect("configuration update blocked during snapshot preparation")
    .unwrap();
    timeout(Duration::from_secs(10), collection.state())
        .await
        .expect("collection reader blocked during snapshot preparation");

    if matches!(change, ShardChange::Remove | ShardChange::Replace) {
        collection
            .shards_holder()
            .write()
            .await
            .drop_and_remove_shard(0)
            .await
            .unwrap();
    }
    if matches!(change, ShardChange::Replace) {
        let replacement = collection
            .create_replica_set(0, None, &[1], None)
            .await
            .unwrap();
        collection
            .shards_holder()
            .write()
            .await
            .add_shards(vec![(0, replacement)], None)
            .await
            .unwrap();
    }

    if matches!(change, ShardChange::DropFuture) {
        drop(restore);

        // Preparation runs in the caller: dropping it must release the paused future.
        assert!(
            release_tx.is_closed(),
            "preparation continued in a detached task"
        );
        assert_eq!(
            original.info(true).await.unwrap().points_count,
            original_points
        );
        collection.stop_gracefully().await;
        return;
    }

    if matches!(change, ShardChange::Cancel) {
        cancel.cancel();
    }

    let during_restore = matches!(
        change,
        ShardChange::Restore | ShardChange::RemoveDuringRestore | ShardChange::ReplaceDuringRestore
    );
    let restore_hook = if during_restore {
        let (reached_tx, reached_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        install_restore_local_replica_before_flag_hook(
            shard_initializing_flag_path(collection_dir.path(), 0),
            reached_tx,
            release_rx,
        );
        Some((reached_rx, release_tx))
    } else {
        None
    };

    release_tx.send(()).unwrap();

    let removal = if let Some((reached_rx, release_tx)) = restore_hook {
        tokio::select! {
            result = &mut restore => panic!("restore finished before restore hook: {result:?}"),
            reached = timeout(Duration::from_secs(10), reached_rx) => reached.unwrap().unwrap(),
        }

        // The disk restore holds only the replica set lock; holder writers and
        // subsequent readers must still make progress.
        let shards_holder = collection.shards_holder();
        drop(
            timeout(Duration::from_secs(10), shards_holder.write())
                .await
                .expect("shard holder writer blocked during snapshot restoration"),
        );
        timeout(Duration::from_secs(10), collection.state())
            .await
            .expect("collection reader blocked during snapshot restoration");

        let removal = if matches!(
            change,
            ShardChange::RemoveDuringRestore | ShardChange::ReplaceDuringRestore
        ) {
            let collection = Arc::clone(&collection);
            let (locked_tx, locked_rx) = oneshot::channel();
            let removal = tokio::spawn(async move {
                let shards_holder = collection.shards_holder();
                let mut holder = shards_holder.write().await;
                let _ = locked_tx.send(());
                holder.drop_and_remove_shard(0).await
            });
            timeout(Duration::from_secs(10), locked_rx)
                .await
                .expect("removal did not acquire shard holder")
                .unwrap();
            Some(removal)
        } else {
            None
        };

        release_tx.send(()).unwrap();
        removal
    } else {
        None
    };

    let result = timeout(RESTORE_COMPLETION_TIMEOUT, restore).await.unwrap();
    if let Some(removal) = removal {
        timeout(RESTORE_COMPLETION_TIMEOUT, removal)
            .await
            .expect("removal blocked after restoration")
            .unwrap()
            .unwrap();
        assert!(!shard_initializing_flag_path(collection_dir.path(), 0).exists());
        assert!(!collection_dir.path().join("0").exists());
    }

    if matches!(change, ShardChange::ReplaceDuringRestore) {
        let replacement = collection
            .create_replica_set(0, None, &[1], None)
            .await
            .unwrap();
        collection
            .shards_holder()
            .write()
            .await
            .add_shards(vec![(0, replacement)], None)
            .await
            .unwrap();
    }

    match change {
        ShardChange::DropFuture => unreachable!(),
        ShardChange::Keep | ShardChange::Restore => {
            result.unwrap();
            assert_eq!(
                original.info(true).await.unwrap().points_count,
                original_points
            );
        }
        ShardChange::Cancel => {
            assert!(matches!(result, Err(CollectionError::Cancelled { .. })));
            assert_eq!(
                original.info(true).await.unwrap().points_count,
                original_points
            );
        }
        ShardChange::Remove => {
            assert!(matches!(result, Err(CollectionError::NotFound { .. })));
            assert!(!collection.shards_holder().read().await.contains_shard(0));
        }
        ShardChange::RemoveDuringRestore => {
            result.unwrap();
            assert!(!collection.shards_holder().read().await.contains_shard(0));
        }
        ShardChange::Replace => {
            assert!(matches!(result, Err(CollectionError::BadRequest { .. })));
            let replacement = collection
                .shards_holder()
                .read()
                .await
                .get_shard(0)
                .cloned()
                .unwrap();
            assert_eq!(replacement.info(true).await.unwrap().points_count, Some(0));
        }
        ShardChange::ReplaceDuringRestore => {
            result.unwrap();
            let replacement = collection
                .shards_holder()
                .read()
                .await
                .get_shard(0)
                .cloned()
                .unwrap();
            assert_eq!(replacement.info(true).await.unwrap().points_count, Some(0));
            assert!(!shard_initializing_flag_path(collection_dir.path(), 0).exists());
        }
    }

    collection.stop_gracefully().await;
}

type ExtractionHook = (oneshot::Sender<PathBuf>, oneshot::Receiver<()>);

static EXTRACTION_HOOKS: LazyLock<Mutex<HashMap<PathBuf, ExtractionHook>>> =
    LazyLock::new(Mutex::default);

pub(super) fn pause_extraction(snapshot_temp_dir: &Path) {
    let parent = snapshot_temp_dir.parent().unwrap();
    let hook = EXTRACTION_HOOKS.lock().unwrap().remove(parent);

    if let Some((reached, release)) = hook {
        let _ = reached.send(snapshot_temp_dir.to_path_buf());
        if release.blocking_recv().is_ok() {
            // Writing after the caller exits must still be safe: the task owns the directory.
            fs::write(snapshot_temp_dir.join("after-cancellation"), b"test").unwrap();
        }
    }
}

#[rstest]
#[case::drop_future(false)]
#[case::cancel_token(true)]
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_preparation_keeps_temp_dir_until_extraction_finishes(#[case] cancel_token: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let snapshot_dir = tempfile::tempdir().unwrap();
    fs::write(snapshot_dir.path().join("data"), b"snapshot").unwrap();
    let cancel = cancel::CancellationToken::new();
    let task_cancel = cancel.clone();
    let temp_path = temp_dir.path().to_path_buf();

    let (reached_tx, reached_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    EXTRACTION_HOOKS
        .lock()
        .unwrap()
        .insert(temp_path.clone(), (reached_tx, release_rx));

    let preparation = tokio::spawn(async move {
        super::ShardHolder::prepare_shard_snapshot(
            SnapshotData::Unpacked(snapshot_dir),
            "test",
            0,
            1,
            true,
            &temp_path,
            None,
            task_cancel,
        )
        .await
    });
    let prepared_path = timeout(Duration::from_secs(10), reached_rx)
        .await
        .unwrap()
        .unwrap();

    if cancel_token {
        cancel.cancel();
        let result = timeout(Duration::from_secs(10), preparation)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(CollectionError::Cancelled { .. })));
    } else {
        preparation.abort();
        let error = timeout(Duration::from_secs(10), preparation)
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.is_cancelled());
    }

    assert!(
        prepared_path.is_dir(),
        "caller deleted the running task's directory"
    );
    assert_eq!(fs::read(prepared_path.join("data")).unwrap(), b"snapshot");

    release_tx.send(()).unwrap();
    timeout(Duration::from_secs(10), async {
        while prepared_path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("temporary directory was not cleaned up after extraction finished");
}
