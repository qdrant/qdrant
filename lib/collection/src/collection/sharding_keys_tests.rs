use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use ahash::AHashMap;
use common::ambient::{AmbientContext, AmbientFutureExt};
use common::budget::ResourceBudget;
use segment::types::ShardKey;
use tokio::time::timeout;

use super::Collection;
use crate::config::ShardingMethod;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::CountRequestInternal;
use crate::shards::channel_service::ChannelService;
use crate::shards::collection_shard_distribution::CollectionShardDistribution;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::shard_holder::shard_mapping::ShardKeyMapping;
use crate::shards::shard_holder::{SHARD_KEY_MAPPING_FILE, ShardHolder};
use crate::shards::shard_trait::WaitUntil;
use crate::tests::fixtures::{create_collection_config, upsert_operation};

async fn custom_collection(path: &std::path::Path) -> Collection {
    let mut config = create_collection_config();
    config.params.sharding_method = Some(ShardingMethod::Custom);
    let mut mapping = ShardKeyMapping::default();
    mapping.insert(ShardKey::from("removed"), HashSet::from([0, 1]));
    mapping.insert(ShardKey::from("retained"), HashSet::from([2]));
    Collection::new(
        "shard-key-removal".to_string(),
        1,
        path,
        &path.join("snapshots"),
        &config,
        Arc::default(),
        CollectionShardDistribution {
            shards: AHashMap::from([
                (0, HashSet::from([1])),
                (1, HashSet::from([1])),
                (2, HashSet::from([1])),
            ]),
        },
        Some(mapping),
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
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_shard_key_releases_holder_during_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let collection = custom_collection(dir.path()).await;
    let holder = collection.shards_holder();
    let (removed, retained) = {
        let holder = holder.read().await;
        (
            Arc::clone(holder.get_shard(0).unwrap()),
            Arc::clone(holder.get_shard(2).unwrap()),
        )
    };
    retained
        .set_replica_state(1, ReplicaState::Active)
        .await
        .unwrap();
    let update = retained
        .update_local(upsert_operation().into(), WaitUntil::Visible, None, false)
        .measured(AmbientContext::new())
        .await
        .unwrap();
    assert!(
        update.is_some(),
        "the retained shard must accept the test points"
    );

    // Hold shutdown at its first await without relying on disk speed or a large dataset.
    let local_guard = removed.lock_local_for_test().await;
    let mut removal = Box::pin(collection.drop_shard_key(ShardKey::from("removed")));
    assert!(futures::poll!(&mut removal).is_pending());

    let retained_ids = timeout(
        Duration::from_secs(2),
        collection.get_shard_ids(&ShardKey::from("retained")),
    )
    .await;

    let retained_count = timeout(
        Duration::from_secs(2),
        collection
            .count(
                CountRequestInternal {
                    filter: None,
                    exact: true,
                },
                None,
                None,
                &ShardSelectorInternal::ShardKey(ShardKey::from("retained")),
                None,
            )
            .measured(AmbientContext::new()),
    )
    .await;

    // Release shutdown even on the broken implementation, so failure does not leave workers running.
    drop(local_guard);
    timeout(Duration::from_secs(10), &mut removal)
        .await
        .unwrap()
        .unwrap();
    drop(removal);

    let registered: Vec<_> = holder.read().await.get_shards().map(|(id, _)| id).collect();
    let persisted = ShardHolder::new(dir.path(), ShardingMethod::Custom).unwrap();
    collection.stop_gracefully().await;

    assert_eq!(
        retained_ids
            .expect("unrelated shard-key reads blocked during shutdown")
            .unwrap(),
        vec![2]
    );
    assert_eq!(
        retained_count
            .expect("unrelated count blocked during shutdown")
            .unwrap()
            .count,
        5
    );
    assert_eq!(registered, vec![2]);
    assert!(!dir.path().join("0").exists());
    assert!(!dir.path().join("1").exists());
    assert!(dir.path().join("2").exists());
    assert!(
        !persisted
            .get_shard_key_to_ids_mapping()
            .contains_key(&ShardKey::from("removed"))
    );
    assert!(dir.path().join(SHARD_KEY_MAPPING_FILE).exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_shard_key_retries_failed_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let collection = custom_collection(dir.path()).await;
    let holder = collection.shards_holder();
    let removed = Arc::clone(holder.read().await.get_shard(0).unwrap());
    removed.stop_gracefully().await;
    let shard_path = dir.path().join("0");
    let backup = dir.path().join("saved-shard");
    fs_err::rename(&shard_path, &backup).unwrap();
    fs_err::write(&shard_path, []).unwrap();

    assert!(
        collection
            .drop_shard_key(ShardKey::from("removed"))
            .await
            .is_err()
    );
    let registered_after_failure: Vec<_> =
        holder.read().await.get_shards().map(|(id, _)| id).collect();

    fs_err::remove_file(&shard_path).unwrap();
    fs_err::rename(&backup, &shard_path).unwrap();
    collection
        .drop_shard_key(ShardKey::from("removed"))
        .await
        .unwrap();
    collection
        .drop_shard_key(ShardKey::from("removed"))
        .await
        .unwrap();
    let registered: Vec<_> = holder.read().await.get_shards().map(|(id, _)| id).collect();
    collection.stop_gracefully().await;

    assert_eq!(registered_after_failure, vec![2]);
    assert_eq!(registered, vec![2]);
    assert!(!shard_path.exists());
    assert!(!dir.path().join("1").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_shard_key_mapping_failure_keeps_shards_registered() {
    let dir = tempfile::tempdir().unwrap();
    let collection = custom_collection(dir.path()).await;
    let mapping_path = dir.path().join(SHARD_KEY_MAPPING_FILE);
    let backup = dir.path().join("saved-mapping");
    fs_err::rename(&mapping_path, &backup).unwrap();
    fs_err::create_dir(&mapping_path).unwrap();

    assert!(
        collection
            .drop_shard_key(ShardKey::from("removed"))
            .await
            .is_err()
    );
    let registered: Vec<_> = collection
        .shards_holder()
        .read()
        .await
        .get_shards()
        .map(|(id, _)| id)
        .collect();
    let ids = collection
        .get_shard_ids(&ShardKey::from("removed"))
        .await
        .unwrap();

    fs_err::remove_dir(&mapping_path).unwrap();
    fs_err::rename(&backup, &mapping_path).unwrap();
    collection
        .drop_shard_key(ShardKey::from("removed"))
        .await
        .unwrap();
    collection.stop_gracefully().await;

    assert_eq!(registered, vec![0, 1, 2]);
    assert_eq!(ids.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_shard_key_can_resume_cancelled_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let collection = custom_collection(dir.path()).await;
    let holder = collection.shards_holder();
    let removed = Arc::clone(holder.read().await.get_shard(0).unwrap());
    let local_guard = removed.lock_local_for_test().await;
    let mut removal = Box::pin(collection.drop_shard_key(ShardKey::from("removed")));
    assert!(futures::poll!(&mut removal).is_pending());
    drop(removal);
    drop(local_guard);

    collection
        .drop_shard_key(ShardKey::from("removed"))
        .await
        .unwrap();
    let registered: Vec<_> = holder.read().await.get_shards().map(|(id, _)| id).collect();
    collection.stop_gracefully().await;

    assert_eq!(registered, vec![2]);
    assert!(!dir.path().join("0").exists());
    assert!(!dir.path().join("1").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_creation_finishes_cancelled_removal_before_reusing_ids() {
    let dir = tempfile::tempdir().unwrap();
    let collection = custom_collection(dir.path()).await;
    let holder = collection.shards_holder();
    let removed = Arc::clone(holder.read().await.get_shard(0).unwrap());
    let local_guard = removed.lock_local_for_test().await;
    let mut removal = Box::pin(collection.drop_shard_key(ShardKey::from("removed")));
    assert!(futures::poll!(&mut removal).is_pending());
    drop(removal);
    drop(local_guard);

    collection
        .create_and_register_shards(
            Some(ShardKey::from("replacement")),
            &[(0, vec![1], ReplicaState::Active)],
        )
        .await
        .unwrap();
    collection
        .drop_shard_key(ShardKey::from("removed"))
        .await
        .unwrap();
    let replacement_ids = collection
        .get_shard_ids(&ShardKey::from("replacement"))
        .await
        .unwrap();
    let replacement_info = collection
        .info(&ShardSelectorInternal::ShardKey(ShardKey::from(
            "replacement",
        )))
        .await
        .unwrap();
    collection.stop_gracefully().await;

    assert_eq!(replacement_ids, vec![0]);
    assert_eq!(replacement_info.points_count, Some(0));
    assert!(dir.path().join("0").exists());
    assert!(!dir.path().join("1").exists());
}
