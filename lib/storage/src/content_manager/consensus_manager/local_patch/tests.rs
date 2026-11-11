//! Patch consensus state through `ConsensusManager`, backed by a test container

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};

use collection::collection_state::{ShardInfo, State};
use collection::operations::types::PeerMetadata;
use collection::shards::CollectionId;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::shard::PeerId;
use parking_lot::Mutex;
use serde_json::{Value, json};
use tempfile::{Builder, TempDir};

use super::*;
use crate::content_manager::alias_mapping::AliasMapping;
use crate::content_manager::collection_meta_ops::CollectionMetaOperations;
use crate::content_manager::consensus::operation_sender::OperationSender;
use crate::content_manager::consensus::persistent::Persistent;
use crate::content_manager::consensus_manager::CollectionsSnapshot;
use crate::content_manager::consensus_state_machine::tests::{
    PEER_ID, collection_state, node_context,
};
use crate::content_manager::consensus_state_machine::{Action, NodeContext};
use crate::content_manager::errors::StorageError;
use crate::quota::QuotaConfig;

const COLLECTION: &str = "books";
const OTHER_PEER_ID: PeerId = PEER_ID + 1;

#[test]
fn dry_run_changes_nothing() {
    let dir = tempdir();
    let container = Arc::new(container());
    let manager = manager(container.clone(), dir.path());

    let report = patch(&manager, replica_and_address_diff(), PatchMode::DryRun);

    assert_eq!(report.changes.len(), 2);
    assert_eq!(replica_state(&container), ReplicaState::Active);
    assert_eq!(
        manager.peer_address(OTHER_PEER_ID).unwrap().to_string(),
        "http://peer-2:6335/",
    );
    assert!(!manager.state_machine_needs_reload());
}

#[test]
fn apply_changes_collections_and_persists_peer_state() {
    let dir = tempdir();
    let container = Arc::new(container());
    let manager = manager(container.clone(), dir.path());

    let report = patch(&manager, replica_and_address_diff(), PatchMode::Apply);

    assert_eq!(report.changes.len(), 2);
    assert_eq!(replica_state(&container), ReplicaState::Dead);
    assert!(manager.state_machine_needs_reload());

    drop(manager);
    let persistent = Persistent::load_or_init(dir.path(), false, false, None).unwrap();
    assert_eq!(
        persistent.peer_address_by_id.read()[&OTHER_PEER_ID].to_string(),
        "http://peer-2-new:6335/",
    );
}

#[test]
fn empty_patch_keeps_state_machine() {
    let dir = tempdir();
    let container = Arc::new(container());
    let manager = manager(container.clone(), dir.path());

    let report = patch(&manager, json!({}), PatchMode::Apply);

    assert!(report.changes.is_empty());
    assert!(!manager.state_machine_needs_reload());
}

#[test]
fn quota_config_is_set_only_if_changed() {
    let dir = tempdir();
    let container = Arc::new(container());
    let manager = manager(container.clone(), dir.path());

    let unchanged_quota = json!({
        "cluster_metadata": { "owner": "support" },
        "quota_config": QuotaConfig::default(),
    });
    patch(&manager, unchanged_quota, PatchMode::Apply);
    assert_eq!(container.quota_config_updates.load(Ordering::Relaxed), 0);

    let mut quota_config = QuotaConfig::default();
    quota_config.enabled = !quota_config.enabled;

    patch(
        &manager,
        json!({ "quota_config": quota_config }),
        PatchMode::Apply,
    );
    assert_eq!(container.quota_config_updates.load(Ordering::Relaxed), 1);
    assert_eq!(*container.quota_config.lock(), quota_config);
}

/// Mark this peer's replica of shard 0 dead and change the other peer's address
fn replica_and_address_diff() -> Value {
    json!({
        "collections": {
            COLLECTION: { "update": { "shards": { "0": { "replicas": { PEER_ID.to_string(): "Dead" } } } } },
        },
        "peer_addresses": { OTHER_PEER_ID.to_string(): "http://peer-2-new:6335" },
    })
}

fn patch(manager: &ConsensusManager<Container>, diff: Value, mode: PatchMode) -> diff::PatchReport {
    let diff = serde_json::from_value(diff).expect("valid diff");
    manager
        .patch_consensus_state(diff, mode)
        .expect("patch applied")
}

fn replica_state(container: &Container) -> ReplicaState {
    container.collections.lock()[COLLECTION].shards[&0].replicas[&PEER_ID]
}

fn tempdir() -> TempDir {
    Builder::new()
        .prefix("consensus-state-patch")
        .tempdir()
        .expect("temp dir")
}

fn manager(container: Arc<Container>, path: &Path) -> ConsensusManager<Container> {
    let (sender, _receiver) = mpsc::channel();

    ConsensusManager::new(
        persistent(path),
        container,
        OperationSender::new(sender),
        path,
    )
    .expect("manager initialized")
}

/// `Persistent` state with two peers, metadata of this peer and one cluster metadata key
fn persistent(path: &Path) -> Persistent {
    let mut persistent =
        Persistent::load_or_init(path, true, false, Some(PEER_ID)).expect("state initialized");

    for (peer_id, address) in [
        (PEER_ID, "http://peer-1:6335"),
        (OTHER_PEER_ID, "http://peer-2:6335"),
    ] {
        persistent
            .insert_peer(peer_id, address.parse().expect("valid uri"))
            .expect("peer inserted");
    }

    persistent
        .update_peer_metadata(PEER_ID, PeerMetadata::current())
        .expect("metadata updated");
    persistent.update_cluster_metadata_key("owner".to_string(), json!("qdrant"));

    persistent
}

/// `Container` with one collection with one shard, replicated on both peers
fn container() -> Container {
    let mut collection = collection_state(Vec::new());
    let replicas = HashMap::from([
        (PEER_ID, ReplicaState::Active),
        (OTHER_PEER_ID, ReplicaState::Active),
    ]);
    collection.shards.insert(0, ShardInfo { replicas });

    Container {
        collections: Mutex::new(HashMap::from([(COLLECTION.to_string(), collection)])),
        aliases: Mutex::new(AliasMapping::default()),
        quota_config: Mutex::new(QuotaConfig::default()),
        quota_config_updates: AtomicUsize::new(0),
    }
}

/// Test `TableOfContent` stand-in, which applies Raft snapshot state in memory
struct Container {
    collections: Mutex<HashMap<CollectionId, State>>,
    aliases: Mutex<AliasMapping>,
    quota_config: Mutex<QuotaConfig>,
    quota_config_updates: AtomicUsize,
}

impl CollectionContainer for Container {
    fn collections_snapshot(&self) -> CollectionsSnapshot {
        CollectionsSnapshot {
            collections: self.collections.lock().clone(),
            aliases: self.aliases.lock().clone(),
        }
    }

    fn apply_collections_snapshot(&self, data: CollectionsSnapshot) -> Result<(), StorageError> {
        let CollectionsSnapshot {
            collections,
            aliases,
        } = data;

        *self.collections.lock() = collections;
        *self.aliases.lock() = aliases;

        Ok(())
    }

    fn quota_config(&self) -> QuotaConfig {
        *self.quota_config.lock()
    }

    fn set_quota_config(&self, config: QuotaConfig) -> Result<(), StorageError> {
        self.quota_config_updates.fetch_add(1, Ordering::Relaxed);
        *self.quota_config.lock() = config;
        Ok(())
    }

    fn node_context(&self) -> NodeContext {
        node_context()
    }

    // Remaining `CollectionContainer` methods are not exercised by these tests

    fn apply_action(&self, _action: Action) -> Result<(), StorageError> {
        unimplemented!()
    }

    fn perform_collection_meta_op(
        &self,
        _operation: CollectionMetaOperations,
    ) -> Result<bool, StorageError> {
        unimplemented!()
    }

    fn remove_peer(&self, _peer_id: PeerId) -> Result<(), StorageError> {
        unimplemented!()
    }

    fn peer_has_shards(&self, _peer_id: PeerId) -> bool {
        unimplemented!()
    }

    fn remove_peer_channels(&self, _peer_id: PeerId) -> Result<(), StorageError> {
        unimplemented!()
    }

    fn sync_local_state(&self) -> Result<(), StorageError> {
        unimplemented!()
    }

    fn collection_names(&self) -> BTreeSet<CollectionId> {
        unimplemented!()
    }

    fn alias_mapping(&self) -> AliasMapping {
        unimplemented!()
    }

    fn collection_state(&self, _collection: &str) -> Option<State> {
        unimplemented!()
    }

    fn take_dirty_collections(&self) -> BTreeSet<CollectionId> {
        unimplemented!()
    }
}
