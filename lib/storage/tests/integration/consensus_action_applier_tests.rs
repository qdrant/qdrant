use std::sync::{Arc, mpsc};

use collection::operations::verification::new_unchecked_verification_pass;
use storage::content_manager::CollectionContainer;
use storage::content_manager::consensus::operation_sender::OperationSender;
use storage::content_manager::consensus::persistent::Persistent;
use storage::content_manager::consensus_manager::ConsensusManager;
use storage::content_manager::consensus_state_machine::Action;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
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

fn consensus_manager(
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
