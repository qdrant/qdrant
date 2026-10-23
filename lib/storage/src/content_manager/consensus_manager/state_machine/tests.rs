//! Exercise storage execution, cached-state maintenance and independent validation

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};

use collection::collection_state;
use collection::collection_state::ShardInfo;
use collection::operations::types::PeerMetadata;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::shard::{PeerId, ShardId};
use parking_lot::Mutex;
use raft::eraftpb::{ConfState, Entry as RaftEntry, Snapshot, SnapshotMetadata};
use segment::types::PayloadSchemaType;
use serde_json::json;
use tempfile::{Builder, TempDir};

use super::*;
use crate::content_manager::alias_mapping::AliasMapping;
use crate::content_manager::collection_meta_ops::{
    ChangeAliasesOperation, CollectionMetaOperations, CreateAlias, DropPayloadIndex,
};
use crate::content_manager::consensus::operation_sender::OperationSender;
use crate::content_manager::consensus_manager::SnapshotData;
use crate::content_manager::consensus_state_machine::tests::{
    PEER_ID, collection_state, node_context,
};
use crate::content_manager::consensus_state_machine::{Action, NodeContext};

const COLLECTION: &str = "books";
const ALIAS: &str = "novels";
const OTHER_ALIAS: &str = "crime";
const OTHER_PEER_ID: PeerId = PEER_ID + 1;
const METADATA_KEY: &str = "owner";

#[test]
fn matching_entry() {
    let container = Arc::new(container());
    let dir = tempdir();
    let manager = manager(container, dir.path());
    let operation = ConsensusOperations::UpdateClusterMetadata {
        key: "answer".to_string(),
        value: json!(42),
    };

    manager.apply_normal_entry(&entry(&operation)).unwrap();

    assert_eq!(
        manager
            .state_machine
            .lock()
            .machine
            .state()
            .cluster_metadata["answer"],
        json!(42)
    );
    assert_eq!(
        manager.persistent.read().cluster_metadata["answer"],
        json!(42)
    );
}

#[test]
fn manager_initializes_state_machine_on_construction() {
    let container = Arc::new(container());
    let dir = tempdir();
    let manager = manager(container.clone(), dir.path());

    assert_eq!(container.snapshots(), 1);

    manager.apply_normal_entry(&entry(&nop())).unwrap();

    assert_eq!(container.snapshots(), 1);
    assert!(container.actions.lock().is_empty());
}

#[test]
fn planning_does_not_advance_cache_or_execute_actions() {
    let container = Arc::new(indexed_container());
    let dir = tempdir();
    let manager = manager(container.clone(), dir.path());
    let before = manager.state_machine.lock().machine.state().clone();

    let plan = manager
        .plan_operation(&drop_payload_index(COLLECTION))
        .unwrap();

    assert!(!plan.actions.is_empty());
    assert_eq!(manager.state_machine.lock().machine.state(), &before);
    assert!(container.actions.lock().is_empty());
    assert_eq!(*container.collections.lock(), before.collections);
}

#[test]
fn successful_execution_advances_cache() {
    let container = Arc::new(indexed_container());
    let dir = tempdir();
    let manager = manager(container.clone(), dir.path());

    manager
        .apply_normal_entry(&entry(&drop_payload_index(COLLECTION)))
        .unwrap();

    let cached = manager.state_machine.lock();
    assert!(
        cached.machine.state().collections[COLLECTION]
            .payload_index_schema
            .schema
            .is_empty()
    );
    assert_eq!(
        cached.machine.state().collections,
        *container.collections.lock()
    );
    assert_eq!(container.actions.lock().len(), 1);
}

#[test]
fn failed_action_preserves_cache_and_reloads_partial_writes() {
    let container = Arc::new(indexed_container());
    let dir = tempdir();
    let manager = manager(container.clone(), dir.path());
    let before = manager.state_machine.lock().machine.state().clone();
    *container.error_after_write.lock() = Some(StorageError::bad_request("failure after write"));
    let entry = entry(&drop_payload_index(COLLECTION));

    let error = manager.apply_normal_entry(&entry).unwrap_err();

    assert!(matches!(error, StorageError::ServiceError { .. }));
    assert_eq!(manager.state_machine.lock().machine.state(), &before);
    assert!(manager.state_machine.lock().needs_reload);
    assert!(
        container.collections.lock()[COLLECTION]
            .payload_index_schema
            .schema
            .is_empty()
    );
    container.error_after_write.lock().take();

    manager.apply_normal_entry(&entry).unwrap();

    assert_eq!(container.snapshots(), 2);
    assert!(!manager.state_machine.lock().needs_reload);
    assert_eq!(
        manager.state_machine.lock().machine.state().collections,
        *container.collections.lock()
    );
}

#[test]
fn batch_planning_projects_earlier_operations_without_advancing_cache() {
    let container = Arc::new(container());
    let dir = tempdir();
    let manager = manager(container.clone(), dir.path());
    let operations = [add_alias_op(), drop_payload_index(OTHER_ALIAS)];
    let before = manager.state_machine.lock().machine.state().clone();

    let plans = manager.plan_operations(&operations).unwrap();

    assert_eq!(plans.len(), 2);
    assert!(
        matches!(&plans[1].actions[..], [Action::DropPayloadIndex { collection, .. }] if collection == COLLECTION)
    );
    assert_eq!(manager.state_machine.lock().machine.state(), &before);
    assert!(container.actions.lock().is_empty());
}

#[test]
fn rejected_batch_leaves_cache_and_storage_unchanged() {
    let container = Arc::new(container());
    let dir = tempdir();
    let manager = manager(container.clone(), dir.path());
    let before = manager.state_machine.lock().machine.state().clone();
    let operations = [add_alias_op(), drop_payload_index("missing")];

    let error = manager.plan_operations(&operations).unwrap_err();

    assert!(matches!(error, StorageError::NotFound { .. }));
    assert_eq!(manager.state_machine.lock().machine.state(), &before);
    assert_eq!(*container.aliases.lock(), before.aliases);
    assert!(container.actions.lock().is_empty());
}

#[test]
fn dirty_collection_resync_before_planning() {
    let container = Arc::new(container());
    let dir = tempdir();
    let manager = manager(container.clone(), dir.path());
    container.add_shard(0);
    container.mark_dirty();

    manager.plan_operation(&nop()).unwrap();

    assert_eq!(
        manager.state_machine.lock().machine.state().collections,
        *container.collections.lock()
    );
    assert_eq!(container.snapshots(), 1);
}

#[test]
fn dirty_collection_resync_after_execution() {
    let container = Arc::new(container());
    let dir = tempdir();
    let manager = manager(container.clone(), dir.path());
    let operation = drop_payload_index(COLLECTION);
    let plan = manager.plan_operation(&operation).unwrap();
    container.add_shard(0);
    container.mark_dirty();

    manager.finish_state_machine(&[operation], &[plan]);

    assert_eq!(
        manager.state_machine.lock().machine.state().collections,
        *container.collections.lock()
    );
    assert_eq!(container.snapshots(), 1);
}

#[test]
#[should_panic(expected = "aliases")]
fn dirty_collection_refresh_keeps_unrelated_divergence() {
    let container = Arc::new(container());
    let dir = tempdir();
    let manager = manager(container.clone(), dir.path());
    let operation = nop();
    let plan = manager.plan_operation(&operation).unwrap();
    container.add_shard(0);
    container.mark_dirty();
    container.add_alias(OTHER_ALIAS);

    manager.finish_state_machine(&[operation], &[plan]);
}

#[test]
fn discovery_invalidation_survives_execution_completion() {
    let container = Arc::new(container());
    let dir = tempdir();
    let manager = manager(container.clone(), dir.path());
    let operation = nop();
    let plan = manager.plan_operation(&operation).unwrap();
    manager
        .add_peer(OTHER_PEER_ID, "http://localhost:7000".parse().unwrap())
        .unwrap();

    manager.finish_state_machine(&[operation], &[plan]);

    assert!(manager.state_machine.lock().needs_reload);
    manager.plan_operation(&nop()).unwrap();
    assert!(
        manager
            .state_machine
            .lock()
            .machine
            .state()
            .peer_address_by_id
            .contains_key(&OTHER_PEER_ID)
    );
    assert_eq!(container.snapshots(), 2);
}

#[test]
fn snapshot_invalidates() {
    let container = Arc::new(container());
    let dir = tempdir();
    let manager = manager(container.clone(), dir.path());
    manager.apply_normal_entry(&entry(&nop())).unwrap();

    manager.apply_snapshot(&snapshot()).unwrap().unwrap();
    manager.apply_normal_entry(&entry(&nop())).unwrap();

    assert_eq!(container.snapshots(), 2);
    assert!(
        manager
            .state_machine
            .lock()
            .machine
            .state()
            .collections
            .is_empty()
    );
}

#[test]
#[should_panic(expected = "aliases")]
fn manager_panics_on_divergence() {
    let container = Arc::new(container());
    let dir = tempdir();
    let manager = manager(container.clone(), dir.path());
    container.add_alias(OTHER_ALIAS);

    manager.apply_normal_entry(&entry(&nop())).unwrap();
}

#[test]
fn comparison_observes_collection_without_invalidating_cache() {
    let container = Arc::new(container());
    let dir = tempdir();
    let manager = manager(container.clone(), dir.path());
    container.add_shard(0);
    let persistent = manager.persistent.read();
    let cached = manager.state_machine.lock();

    let report = validation::compare(
        cached.machine.state(),
        container.as_ref(),
        &persistent,
        &[drop_payload_index(COLLECTION)],
    );

    assert_eq!(report.as_deref(), Some("collections[books].shards"));
    assert!(!cached.needs_reload);
}

#[test]
fn comparison_resolves_aliases() {
    let container = Arc::new(container());
    let dir = tempdir();
    let manager = manager(container.clone(), dir.path());
    container.add_shard(0);
    let persistent = manager.persistent.read();
    let cached = manager.state_machine.lock();

    let report = validation::compare(
        cached.machine.state(),
        container.as_ref(),
        &persistent,
        &[drop_payload_index(ALIAS)],
    );

    assert_eq!(report.as_deref(), Some("collections[books].shards"));
}

#[test]
fn peer_removal_compares_all_collections() {
    let container = Arc::new(container());
    container.add_shard_for(0, OTHER_PEER_ID);
    let dir = tempdir();
    let manager = manager(container.clone(), dir.path());
    let operation = ConsensusOperations::RemovePeer(OTHER_PEER_ID);
    let plan = manager.plan_operation(&operation).unwrap();
    let persistent = manager.persistent.read();
    let mut cached = manager.state_machine.lock();
    cached.machine.apply_actions(&plan.actions);

    let report = validation::compare(
        cached.machine.state(),
        container.as_ref(),
        &persistent,
        &[operation],
    );

    assert_eq!(report.as_deref(), Some("collections[books].shards"));
}

fn indexed_container() -> Container {
    let container = container();
    container
        .collections
        .lock()
        .get_mut(COLLECTION)
        .unwrap()
        .payload_index_schema
        .schema
        .insert("city".parse().unwrap(), PayloadSchemaType::Keyword.into());
    container
}

fn add_alias_op() -> ConsensusOperations {
    ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::ChangeAliases(
        ChangeAliasesOperation {
            actions: vec![
                CreateAlias {
                    collection_name: COLLECTION.into(),
                    alias_name: OTHER_ALIAS.into(),
                }
                .into(),
            ],
        },
    )))
}

#[test]
fn read_cluster_state() {
    let dir = tempdir();
    let persistent = persistent(dir.path());
    let container = container();

    let state = super::read_cluster_state(&container, &persistent);

    assert_eq!(state.collections, *container.collections.lock());
    assert_eq!(state.aliases, *container.aliases.lock());
    assert_eq!(state.quota_config, container.quota_config);
    assert_eq!(
        state.peer_address_by_id,
        *persistent.peer_address_by_id.read(),
    );
    assert_eq!(
        state.peer_metadata_by_id,
        *persistent.peer_metadata_by_id.read(),
    );
    assert_eq!(state.cluster_metadata, persistent.cluster_metadata);
}

/// `read_shallow_state` should return applied state with collection names instead of full
/// collection state; all other fields should match `read_cluster_state`
#[test]
fn read_shallow_state() {
    let dir = tempdir();
    let persistent = persistent(dir.path());
    let container = container();

    let state = validation::read_shallow_state(&container, &persistent);

    assert_eq!(state.collections, BTreeSet::from([COLLECTION.to_string()]));
    assert_eq!(state.aliases, *container.aliases.lock());
    assert_eq!(state.quota_config, container.quota_config);
    assert_eq!(
        state.peer_address_by_id,
        *persistent.peer_address_by_id.read(),
    );
    assert_eq!(
        state.peer_metadata_by_id,
        *persistent.peer_metadata_by_id.read(),
    );
    assert_eq!(state.cluster_metadata, persistent.cluster_metadata);
}

fn tempdir() -> TempDir {
    Builder::new()
        .prefix("consensus-state-machine")
        .tempdir()
        .expect("temp dir")
}

/// Build `ConsensusManager` with the test `Container`
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

fn entry(operation: &ConsensusOperations) -> RaftEntry {
    RaftEntry {
        data: serde_cbor::to_vec(operation).expect("operation serialized"),
        ..Default::default()
    }
}

/// Snapshot of an empty cluster, with this peer as its only voter
fn snapshot() -> Snapshot {
    let data = SnapshotData {
        collections_data: CollectionsSnapshot::default(),
        address_by_id: PeerAddressById::new(),
        metadata_by_id: PeerMetadataById::new(),
        cluster_metadata: HashMap::new(),
        quota_config: None,
    };

    let conf_state = ConfState {
        voters: vec![PEER_ID],
        ..Default::default()
    };

    Snapshot {
        data: serde_cbor::to_vec(&data).expect("snapshot serialized"),
        metadata: Some(SnapshotMetadata {
            conf_state: Some(conf_state),
            index: 1,
            term: 1,
        }),
    }
}

fn nop() -> ConsensusOperations {
    ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::Nop { token: 0 }))
}

/// Covered operation that names `collection`, causing validation to compare that collection
fn drop_payload_index(collection: &str) -> ConsensusOperations {
    let operation = DropPayloadIndex {
        collection_name: collection.to_string(),
        field_name: "city".parse().expect("valid field name"),
    };

    ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::DropPayloadIndex(
        operation,
    )))
}

/// `Persistent` state containing this peer's address and metadata plus one cluster metadata key
fn persistent(path: &Path) -> Persistent {
    let mut persistent =
        Persistent::load_or_init(path, true, false, Some(PEER_ID)).expect("state initialized");

    let address = "http://localhost:6335".parse().expect("valid uri");
    persistent
        .insert_peer(PEER_ID, address)
        .expect("peer inserted");
    persistent
        .update_peer_metadata(PEER_ID, PeerMetadata::current())
        .expect("metadata updated");
    persistent.update_cluster_metadata_key(METADATA_KEY.to_string(), json!("qdrant"));

    persistent
}

/// `Container` with one collection, one alias, and non-default quota config
fn container() -> Container {
    let mut aliases = AliasMapping::default();
    aliases.insert(ALIAS.to_string(), COLLECTION.to_string());

    Container {
        collections: Mutex::new(HashMap::from([(
            COLLECTION.to_string(),
            collection_state(Vec::new()),
        )])),
        aliases: Mutex::new(aliases),
        quota_config: QuotaConfig {
            enabled: true,
            ..Default::default()
        },
        error_after_write: Mutex::new(None),
        actions: Mutex::new(Vec::new()),
        snapshots: AtomicUsize::new(0),
        dirty_collections: Mutex::new(BTreeSet::new()),
    }
}

/// Test `TableOfContent` stand-in backed by state configured for each test.
///
/// Locks let tests mutate state as if another task changed it while a Raft entry was applied.
struct Container {
    collections: Mutex<HashMap<CollectionId, collection_state::State>>,
    aliases: Mutex<AliasMapping>,
    quota_config: QuotaConfig,
    error_after_write: Mutex<Option<StorageError>>,
    actions: Mutex<Vec<Action>>,
    /// Number of full collection-state reads, used to distinguish resync from rebuild
    snapshots: AtomicUsize,
    /// Collections changed outside consensus and waiting to be resynced
    dirty_collections: Mutex<BTreeSet<CollectionId>>,
}

impl Container {
    fn snapshots(&self) -> usize {
        self.snapshots.load(Ordering::Relaxed)
    }

    /// Mark collection as changed outside consensus, as partial snapshot recovery does
    fn mark_dirty(&self) {
        self.dirty_collections.lock().insert(COLLECTION.to_string());
    }

    /// Add alias to `Container` without updating consensus state machine
    fn add_alias(&self, alias: &str) {
        self.aliases
            .lock()
            .insert(alias.to_string(), COLLECTION.to_string());
    }

    /// Add shard to `Container` without updating consensus state machine
    fn add_shard(&self, shard_id: ShardId) {
        self.add_shard_for(shard_id, PEER_ID);
    }

    fn add_shard_for(&self, shard_id: ShardId, peer_id: PeerId) {
        let replicas = HashMap::from([(peer_id, ReplicaState::Active)]);

        self.collections
            .lock()
            .get_mut(COLLECTION)
            .expect("collection exists")
            .shards
            .insert(shard_id, ShardInfo { replicas });
    }
}

impl CollectionContainer for Container {
    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "This container implements only the actions exercised by these tests"
    )]
    fn apply_action(&self, action: Action) -> StorageResult<()> {
        self.actions.lock().push(action.clone());
        match action {
            Action::DropPayloadIndex {
                collection,
                field_name,
            } => {
                self.collections
                    .lock()
                    .get_mut(&collection)
                    .unwrap()
                    .payload_index_schema
                    .schema
                    .remove(&field_name);
            }
            Action::DropCollection { collection } => {
                self.collections.lock().remove(&collection);
            }
            Action::UpdateAliases { set, remove } => {
                let mut aliases = self.aliases.lock();
                for alias in remove {
                    aliases.remove(&alias);
                }
                for (alias, collection) in set {
                    aliases.insert(alias, collection);
                }
            }
            action => panic!("unexpected collection action: {action:?}"),
        }

        match self.error_after_write.lock().as_ref() {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    fn collections_snapshot(&self) -> CollectionsSnapshot {
        self.snapshots.fetch_add(1, Ordering::Relaxed);

        CollectionsSnapshot {
            collections: self.collections.lock().clone(),
            aliases: self.aliases.lock().clone(),
        }
    }

    fn collection_state(&self, collection: &str) -> Option<collection_state::State> {
        self.collections.lock().get(collection).cloned()
    }

    fn collection_names(&self) -> BTreeSet<CollectionId> {
        self.collections.lock().keys().cloned().collect()
    }

    fn alias_mapping(&self) -> AliasMapping {
        self.aliases.lock().clone()
    }

    fn take_dirty_collections(&self) -> BTreeSet<CollectionId> {
        std::mem::take(&mut self.dirty_collections.lock())
    }

    fn node_context(&self) -> NodeContext {
        node_context()
    }

    fn quota_config(&self) -> QuotaConfig {
        self.quota_config
    }

    fn perform_collection_meta_op(
        &self,
        _operation: CollectionMetaOperations,
    ) -> StorageResult<bool> {
        panic!("CSM application must not invoke the retained operation handlers")
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

    fn remove_peer(&self, _peer_id: PeerId) -> StorageResult<()> {
        unimplemented!()
    }

    fn remove_peer_channels(&self, _peer_id: PeerId) -> StorageResult<()> {
        Ok(())
    }

    // Remaining `CollectionContainer` methods are not exercised by these tests

    fn peer_has_shards(&self, _peer_id: PeerId) -> bool {
        unimplemented!()
    }

    fn sync_local_state(&self) -> Result<(), StorageError> {
        unimplemented!()
    }

    fn set_quota_config(&self, _config: QuotaConfig) -> Result<(), StorageError> {
        unimplemented!()
    }
}
