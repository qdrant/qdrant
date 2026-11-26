use std::sync::{Arc, mpsc};

use collection::operations::types::PeerMetadata;
use collection::operations::verification::new_unchecked_verification_pass;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use serde_json::json;
use storage::content_manager::CollectionContainer;
use storage::content_manager::consensus::operation_sender::OperationSender;
use storage::content_manager::consensus::persistent::Persistent;
use storage::content_manager::consensus_manager::ConsensusManager;
use storage::content_manager::consensus_state_machine::Action;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
use storage::quota::{QUOTA_CONFIG_FILE, QuotaConfig, QuotaManager};
use tempfile::TempDir;

use super::alias_tests::{FULL_ACCESS, create_collection, new_dispatcher};

#[test]
fn add_peer_action_replay() {
    let (storage_dir, _handle, dispatcher) = new_dispatcher();
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let manager = consensus_manager(&storage_dir, toc.clone());
    let unrelated_uri = "http://127.0.0.1:7335".parse().unwrap();
    manager.add_peer(2, unrelated_uri).unwrap();

    // Replacing an existing address must update the shared channel-service map too
    manager
        .add_peer(1, "http://127.0.0.1:6335".parse().unwrap())
        .unwrap();
    let uri = "http://127.0.0.1:8335".parse().unwrap();
    let action = Action::AddPeer { peer_id: 1, uri };

    for _ in 0..2 {
        manager.apply_action(action.clone()).unwrap();

        let expected = manager.persistent.read().peer_address_by_id();
        assert_eq!(*toc.get_channel_service().id_to_address.read(), expected);
        let reloaded = Persistent::load_or_init(storage_dir.path(), true, false, None).unwrap();
        assert_eq!(reloaded.peer_address_by_id(), expected);
        assert_eq!(
            expected[&1],
            "http://127.0.0.1:8335".parse::<http::Uri>().unwrap()
        );
        assert_eq!(
            expected[&2],
            "http://127.0.0.1:7335".parse::<http::Uri>().unwrap()
        );
    }
}

#[test]
fn add_peer_action_save_failure_retry() {
    let (storage_dir, _handle, dispatcher) = new_dispatcher();
    let pass = new_unchecked_verification_pass();
    let manager = consensus_manager(&storage_dir, dispatcher.toc(&FULL_ACCESS, &pass).clone());
    let action = Action::AddPeer {
        peer_id: 1,
        uri: "http://127.0.0.1:6335".parse().unwrap(),
    };
    let path = manager.persistent.read().path.clone();
    let backup = path.with_extension("backup");
    fs_err::rename(&path, &backup).unwrap();
    fs_err::create_dir(&path).unwrap();

    let error = manager.apply_action(action.clone()).unwrap_err();

    assert!(matches!(error, StorageError::ServiceError { .. }));
    fs_err::remove_dir(&path).unwrap();
    fs_err::rename(backup, path).unwrap();

    manager.apply_action(action).unwrap();

    let reloaded = Persistent::load_or_init(storage_dir.path(), true, false, None).unwrap();
    assert!(reloaded.peer_address_by_id().contains_key(&1));
}

#[test]
fn remove_peer_action_replay() {
    for peer_id in [0, 1] {
        let (storage_dir, handle, dispatcher) = new_dispatcher();
        create_collection(&handle, &dispatcher, "test");
        let pass = new_unchecked_verification_pass();
        let toc = dispatcher.toc(&FULL_ACCESS, &pass);
        let manager = consensus_manager(&storage_dir, toc.clone());
        for id in 0..3 {
            manager
                .add_peer(
                    id,
                    format!("http://127.0.0.1:{}", 6335 + id).parse().unwrap(),
                )
                .unwrap();
            manager
                .persistent
                .write()
                .update_peer_metadata(id, PeerMetadata::current())
                .unwrap();
        }
        toc.apply_action(Action::SetReplicaState {
            collection: "test".into(),
            shard_id: 0,
            peer_id: 1,
            state: ReplicaState::Active,
        })
        .unwrap();
        let collection = toc.collection_state("test").unwrap();
        let mut expected_addresses = manager.persistent.read().peer_address_by_id();
        let mut expected_metadata = manager.persistent.read().peer_metadata_by_id();
        if peer_id == 0 {
            expected_addresses.clear();
        } else {
            expected_addresses.remove(&peer_id);
        }
        expected_metadata.remove(&peer_id);
        let mut expected_loaded_metadata = expected_metadata.clone();
        expected_loaded_metadata.retain(|id, _| expected_addresses.contains_key(id));

        for _ in 0..2 {
            manager
                .apply_action(Action::RemovePeer { peer_id })
                .unwrap();

            assert_eq!(toc.collection_state("test").unwrap(), collection);
            assert_eq!(
                *toc.get_channel_service().id_to_address.read(),
                expected_addresses
            );
            assert_eq!(
                *toc.get_channel_service().id_to_metadata.read(),
                expected_metadata
            );
            let path = manager.persistent.read().path.clone();
            let persisted: Persistent =
                serde_json::from_slice(&fs_err::read(path).unwrap()).unwrap();
            assert_eq!(persisted.peer_metadata_by_id(), expected_metadata);

            // Startup removes metadata for addresses forgotten during self-removal
            let reloaded = Persistent::load_or_init(storage_dir.path(), true, false, None).unwrap();
            assert_eq!(reloaded.peer_address_by_id(), expected_addresses);
            assert_eq!(reloaded.peer_metadata_by_id(), expected_loaded_metadata);
        }
    }
}

#[test]
fn remove_peer_action_save_failure_retry() {
    let (storage_dir, _handle, dispatcher) = new_dispatcher();
    let pass = new_unchecked_verification_pass();
    let manager = consensus_manager(&storage_dir, dispatcher.toc(&FULL_ACCESS, &pass).clone());
    manager
        .add_peer(1, "http://127.0.0.1:6335".parse().unwrap())
        .unwrap();
    manager
        .persistent
        .write()
        .update_peer_metadata(1, PeerMetadata::current())
        .unwrap();
    let path = manager.persistent.read().path.clone();
    let backup = path.with_extension("backup");
    fs_err::rename(&path, &backup).unwrap();
    fs_err::create_dir(&path).unwrap();
    let action = Action::RemovePeer { peer_id: 1 };

    let error = manager.apply_action(action.clone()).unwrap_err();

    assert!(matches!(error, StorageError::ServiceError { .. }));
    assert!(
        !manager
            .persistent
            .read()
            .peer_address_by_id()
            .contains_key(&1)
    );
    assert!(
        !manager
            .persistent
            .read()
            .peer_metadata_by_id()
            .contains_key(&1)
    );
    fs_err::remove_dir(&path).unwrap();
    fs_err::rename(backup, path).unwrap();

    manager.apply_action(action).unwrap();

    let reloaded = Persistent::load_or_init(storage_dir.path(), true, false, None).unwrap();
    assert!(!reloaded.peer_address_by_id().contains_key(&1));
    assert!(!reloaded.peer_metadata_by_id().contains_key(&1));
}

#[test]
fn set_peer_metadata_action_replay() {
    let (storage_dir, _handle, dispatcher) = new_dispatcher();
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let manager = consensus_manager(&storage_dir, toc.clone());
    let old_metadata = PeerMetadata::new("1.0.0".parse().unwrap());
    for peer_id in [1, 2] {
        manager
            .add_peer(
                peer_id,
                format!("http://127.0.0.1:{}", 6335 + peer_id)
                    .parse()
                    .unwrap(),
            )
            .unwrap();
        manager
            .persistent
            .write()
            .update_peer_metadata(peer_id, old_metadata.clone())
            .unwrap();
    }
    let addresses = manager.persistent.read().peer_address_by_id();
    let metadata = PeerMetadata::new("1.2.3".parse().unwrap());
    let action = Action::SetPeerMetadata {
        peer_id: 1,
        metadata: metadata.clone(),
    };
    let expected = [(1, metadata), (2, old_metadata)].into();

    for _ in 0..2 {
        manager.apply_action(action.clone()).unwrap();

        assert_eq!(*toc.get_channel_service().id_to_metadata.read(), expected);
        assert_eq!(*toc.get_channel_service().id_to_address.read(), addresses);
        let reloaded = Persistent::load_or_init(storage_dir.path(), true, false, None).unwrap();
        assert_eq!(reloaded.peer_metadata_by_id(), expected);
    }
}

#[test]
fn set_peer_metadata_action_save_failure_retry() {
    let (storage_dir, _handle, dispatcher) = new_dispatcher();
    let pass = new_unchecked_verification_pass();
    let manager = consensus_manager(&storage_dir, dispatcher.toc(&FULL_ACCESS, &pass).clone());
    manager
        .add_peer(1, "http://127.0.0.1:6335".parse().unwrap())
        .unwrap();
    let metadata = PeerMetadata::current();
    let action = Action::SetPeerMetadata {
        peer_id: 1,
        metadata: metadata.clone(),
    };

    let reloaded = retry_after_failed_save(&storage_dir, &manager, action);

    assert_eq!(reloaded.peer_metadata_by_id()[&1], metadata);
}

#[test]
fn set_cluster_metadata_key_action_replay() {
    let (storage_dir, _handle, dispatcher) = new_dispatcher();
    let pass = new_unchecked_verification_pass();
    let manager = consensus_manager(&storage_dir, dispatcher.toc(&FULL_ACCESS, &pass).clone());
    manager
        .persistent
        .write()
        .update_cluster_metadata_key("other".into(), json!({"preserved": true}));

    for value in [json!({"nested": [1, null, "value"]}), json!(null)] {
        let action = Action::SetClusterMetadataKey {
            key: "target".into(),
            value: value.clone(),
        };

        for _ in 0..2 {
            manager.apply_action(action.clone()).unwrap();

            let reloaded = Persistent::load_or_init(storage_dir.path(), true, false, None).unwrap();
            assert_eq!(
                reloaded.cluster_metadata,
                manager.persistent.read().cluster_metadata
            );
            assert_eq!(reloaded.get_cluster_metadata_key("target"), value);
            assert_eq!(
                reloaded.cluster_metadata.contains_key("target"),
                !value.is_null()
            );
            assert_eq!(
                reloaded.get_cluster_metadata_key("other"),
                json!({"preserved": true})
            );
        }
    }
}

#[test]
fn set_cluster_metadata_key_action_save_failure_retry() {
    let (storage_dir, _handle, dispatcher) = new_dispatcher();
    let pass = new_unchecked_verification_pass();
    let manager = consensus_manager(&storage_dir, dispatcher.toc(&FULL_ACCESS, &pass).clone());

    for value in [json!({"owner": "updated"}), json!(null)] {
        let action = Action::SetClusterMetadataKey {
            key: "target".into(),
            value: value.clone(),
        };

        let reloaded = retry_after_failed_save(&storage_dir, &manager, action);

        assert_eq!(reloaded.get_cluster_metadata_key("target"), value);
        assert_eq!(
            reloaded.cluster_metadata.contains_key("target"),
            !value.is_null()
        );
    }
}

#[test]
fn set_quota_config_action_replay() {
    let (storage_dir, _handle, dispatcher) = new_dispatcher();
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let manager = consensus_manager(&storage_dir, toc.clone());
    let config = QuotaConfig {
        enabled: true,
        max_resident_memory_percent: Some(80),
        max_disk_usage_percent: Some(90),
        release_margin_percent: Some(7),
    };

    for config in [config, QuotaConfig::default()] {
        for _ in 0..2 {
            manager
                .apply_action(Action::SetQuotaConfig { config })
                .unwrap();

            assert_eq!(toc.quota_config(), config);
            let reloaded =
                QuotaManager::load_or_init(storage_dir.path(), QuotaConfig::default()).unwrap();
            assert_eq!(reloaded.config(), config);
        }
    }
}

#[test]
fn set_quota_config_action_save_failure_retry() {
    let (storage_dir, _handle, dispatcher) = new_dispatcher();
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let manager = consensus_manager(&storage_dir, toc.clone());
    let initial = QuotaConfig {
        enabled: true,
        max_disk_usage_percent: Some(90),
        ..Default::default()
    };
    manager
        .apply_action(Action::SetQuotaConfig { config: initial })
        .unwrap();
    let updated = QuotaConfig {
        max_disk_usage_percent: Some(95),
        ..initial
    };

    // Matching values must still reach set_config, which clears recorded quota verdicts
    for config in [initial, updated] {
        let before = toc.quota_config();
        let path = storage_dir.path().join(QUOTA_CONFIG_FILE);
        let backup = path.with_extension("backup");
        fs_err::rename(&path, &backup).unwrap();
        fs_err::create_dir(&path).unwrap();
        let action = Action::SetQuotaConfig { config };

        let error = manager.apply_action(action.clone()).unwrap_err();

        assert!(matches!(error, StorageError::ServiceError { .. }));
        assert_eq!(toc.quota_config(), before);
        fs_err::remove_dir(&path).unwrap();
        fs_err::rename(backup, path).unwrap();

        manager.apply_action(action).unwrap();

        assert_eq!(toc.quota_config(), config);
        let reloaded =
            QuotaManager::load_or_init(storage_dir.path(), QuotaConfig::default()).unwrap();
        assert_eq!(reloaded.config(), config);
    }
}

#[test]
fn set_quota_config_action_invalid_config() {
    let (storage_dir, _handle, dispatcher) = new_dispatcher();
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let manager = consensus_manager(&storage_dir, toc.clone());
    let before = toc.quota_config();
    let invalid = QuotaConfig {
        max_disk_usage_percent: Some(0),
        ..Default::default()
    };

    let error = manager
        .apply_action(Action::SetQuotaConfig { config: invalid })
        .unwrap_err();

    assert!(matches!(error, StorageError::ServiceError { .. }));
    assert_eq!(toc.quota_config(), before);
}

#[test]
fn collection_action_delegation() {
    let (storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let manager = consensus_manager(&storage_dir, toc.clone());
    let action = Action::UpdateAliases {
        set: [("alias".into(), "test".into())].into(),
        remove: Default::default(),
    };

    manager.apply_action(action.clone()).unwrap();
    manager.apply_action(action).unwrap();

    assert_eq!(toc.alias_mapping().get("alias"), Some(&"test".to_string()));

    let error = manager
        .apply_action(Action::SetShardNumber {
            collection: "missing".into(),
            shard_number: 2.try_into().unwrap(),
        })
        .unwrap_err();
    assert!(matches!(error, StorageError::ServiceError { .. }));
}

fn retry_after_failed_save(
    storage_dir: &TempDir,
    manager: &ConsensusManager<TableOfContent>,
    action: Action,
) -> Persistent {
    let path = manager.persistent.read().path.clone();
    let backup = path.with_extension("backup");
    fs_err::rename(&path, &backup).unwrap();
    fs_err::create_dir(&path).unwrap();

    let error = manager.apply_action(action.clone()).unwrap_err();

    assert!(matches!(error, StorageError::ServiceError { .. }));
    fs_err::remove_dir(&path).unwrap();
    fs_err::rename(backup, path).unwrap();

    // The failed save already changed memory. Replay must still write that state to disk.
    manager.apply_action(action).unwrap();

    Persistent::load_or_init(storage_dir.path(), true, false, None).unwrap()
}

pub(super) fn consensus_manager(
    storage_dir: &TempDir,
    toc: Arc<TableOfContent>,
) -> ConsensusManager<TableOfContent> {
    let mut persistent =
        Persistent::load_or_init(storage_dir.path(), true, false, Some(0)).unwrap();
    persistent.peer_address_by_id = toc.get_channel_service().id_to_address.clone();
    persistent.peer_metadata_by_id = toc.get_channel_service().id_to_metadata.clone();
    persistent.save().unwrap();
    let (sender, _receiver) = mpsc::channel();
    ConsensusManager::new(
        persistent,
        toc,
        OperationSender::new(sender),
        storage_dir.path(),
    )
    .unwrap()
}
