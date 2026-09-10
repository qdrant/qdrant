//! Run consensus state machine alongside a stand-in for `TableOfContent`.
//!
//! Most tests use `ShadowStateMachine` directly, so they can inspect divergence reports.
//! Tests using `ConsensusManager` only check that applying a Raft entry runs validation
//! and panics on a divergence in panic mode.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};

use collection::collection_state;
use collection::collection_state::ShardInfo;
use collection::operations::types::PeerMetadata;
use collection::shards::CollectionId;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::shard::{PeerId, ShardId};
use raft::eraftpb::{ConfState, Entry as RaftEntry, Snapshot, SnapshotMetadata};
use segment::types::PayloadSchemaType;
use serde_json::json;
use tempfile::{Builder, TempDir};

use super::*;
use crate::content_manager::alias_mapping::AliasMapping;
use crate::content_manager::collection_meta_ops::{
    CollectionMetaOperations, CreatePayloadIndex, DropPayloadIndex, SetShardReplicaState,
};
use crate::content_manager::consensus::operation_sender::OperationSender;
use crate::content_manager::consensus_manager::{ConsensusManager, SnapshotData};
use crate::content_manager::consensus_state_machine::NodeContext;
use crate::content_manager::consensus_state_machine::tests::{
    PEER_ID, collection_state, node_context,
};
use crate::quota::QuotaConfig;
use crate::types::{PeerAddressById, PeerMetadataById};

const COLLECTION: &str = "books";
const ALIAS: &str = "novels";
const OTHER_ALIAS: &str = "crime";
const METADATA_KEY: &str = "owner";
/// Collection absent from consensus state machine and `Container`
const MISSING: &str = "outis";

/// Consensus state machine and operation handler should apply the same cluster metadata update.
/// This test uses `ConsensusManager` because the operation handler writes the applied value to
/// `Persistent`.
#[test]
fn matching_entry() {
    let container = Arc::new(container());
    let dir = tempdir();
    let manager = manager(container, ShadowMode::Panic, dir.path());

    let operation = ConsensusOperations::UpdateClusterMetadata {
        key: "answer".to_string(),
        value: json!(42),
    };

    manager
        .apply_normal_entry(&entry(&operation))
        .expect("entry applied");
}

/// `ConsensusManager` should initialize consensus state machine before running operation handler
#[test]
fn manager_initializes_state_machine_before_handler() {
    let container = Arc::new(container());
    let dir = tempdir();
    let manager = manager(container.clone(), ShadowMode::Panic, dir.path());

    manager.apply_normal_entry(&entry(&nop())).expect("nop");

    assert_eq!(container.snapshots_before_handler(), 1);
}

/// `ConsensusManager` should return operation handler's boolean result unchanged
#[test]
fn manager_preserves_handler_result() {
    let mut container = container();
    container.handler_result = false;

    let container = Arc::new(container);
    let dir = tempdir();
    let manager = manager(container, ShadowMode::Disabled, dir.path());

    assert!(!manager.apply_normal_entry(&entry(&nop())).expect("nop"));
}

/// Consensus state machine and `Container` contain the same collection, but with different shard
/// state; validation should report the difference
#[test]
fn diverged_collection() {
    let shadow = Shadow::new(ShadowMode::Panic);

    assert_eq!(shadow.apply(&nop()), None);

    shadow.container.add_shard(0);

    assert_eq!(
        shadow.apply(&drop_payload_index(COLLECTION)).as_deref(),
        Some(format!("collections[{COLLECTION}].shards").as_str()),
    );
}

/// When an operation names an alias, validation should resolve it to a collection and report
/// differences under that collection's name
#[test]
fn diverged_collection_under_alias() {
    let shadow = Shadow::new(ShadowMode::Panic);

    assert_eq!(shadow.apply(&nop()), None);

    shadow.container.add_shard(0);

    assert_eq!(
        shadow.apply(&drop_payload_index(ALIAS)).as_deref(),
        Some(format!("collections[{COLLECTION}].shards").as_str()),
    );
}

/// Consensus state machine and operation handler should both reject a missing collection with
/// the same error class
#[test]
fn matching_rejection() {
    let shadow = Shadow::new(ShadowMode::Panic);

    let rejection = Err(StorageError::not_found(format!(
        "Collection `{MISSING}` doesn't exist!"
    )));

    assert_eq!(
        shadow.apply_with(&create_payload_index(MISSING), &rejection),
        None,
    );
}

/// Consensus state machine rejects a missing collection as not-found, while operation handler
/// rejects it as bad-request. Validation should report the difference because the error class
/// reaches the client.
#[test]
fn differing_rejection() {
    let shadow = Shadow::new(ShadowMode::Panic);
    let rejection = Err(StorageError::bad_request("no"));

    assert!(
        shadow
            .apply_with(&create_payload_index(MISSING), &rejection)
            .is_some()
    );
}

/// Validation should report when consensus state machine accepts an operation rejected by the
/// operation handler, even though neither state changes
#[test]
fn rejected_by_apply_only() {
    let shadow = Shadow::new(ShadowMode::Panic);
    let rejection = Err(StorageError::bad_request("no"));

    assert!(shadow.apply_with(&nop(), &rejection).is_some());
}

/// An uncovered operation that names no collection may have changed any collection,
/// so validation should invalidate the entire consensus state machine
#[test]
fn not_covered_invalidates() {
    invalidates(&remove_peer(), &Ok(true));
}

/// When an uncovered operation names a collection, validation should reload only that collection
/// instead of rebuilding the entire consensus state machine
#[test]
fn not_covered_resync() {
    let shadow = Shadow::new(ShadowMode::Panic);

    // Initialization reads all collections; resync below should only read the named collection
    assert_eq!(shadow.apply(&nop()), None);

    // Add shard directly to `Container`, so its collection state no longer matches state machine
    shadow.container.add_shard(0);

    // `set_replica_state` is not covered by consensus state machine yet, so it should trigger
    // resync of the named collection
    assert_eq!(shadow.apply(&set_replica_state()), None);

    // `drop_payload_index` makes `apply` compare collection states, which should match after resync
    assert_eq!(shadow.apply(&drop_payload_index(COLLECTION)), None);

    // Full collection state should have been read only once during initialization;
    // resync should have read only the named collection
    assert_eq!(shadow.container.snapshots(), 1);
}

/// Resyncing one collection should not hide a divergence in unrelated state
#[test]
fn not_covered_keeps_the_rest() {
    let shadow = Shadow::new(ShadowMode::Panic);

    assert_eq!(shadow.apply(&nop()), None);

    shadow.container.add_alias(OTHER_ALIAS);
    assert_eq!(shadow.apply(&set_replica_state()), None);

    assert_eq!(shadow.apply(&nop()).as_deref(), Some("aliases"));
}

/// Partial snapshot recovery can rewrite collection payload index schema without a consensus
/// operation. Consensus state machine reloads affected collection instead of reporting recovered
/// state as a divergence.
#[test]
fn dirty_collection_resync() {
    let shadow = Shadow::new(ShadowMode::Panic);

    assert_eq!(shadow.apply(&nop()), None);

    shadow.container.add_shard(0);
    shadow.container.mark_dirty();

    // The operation names the collection, so validation should read its state after resync
    assert_eq!(shadow.apply(&drop_payload_index(COLLECTION)), None);
}

/// Partial snapshot recovery can finish after consensus state machine applies an operation but
/// before validation compares state. A second resync should load recovered collection state.
#[test]
fn dirty_collection_resync_mid_apply() {
    let shadow = Shadow::new(ShadowMode::Panic);

    assert_eq!(shadow.apply(&nop()), None);

    let report = shadow.apply_between(&drop_payload_index(COLLECTION), &Ok(true), |container| {
        container.add_shard(0);
        container.mark_dirty();
    });

    assert_eq!(report, None);
}

/// A service error may leave the operation handler's writes partially applied,
/// so consensus state machine should be invalidated
#[test]
fn service_error_invalidates() {
    let failed = Err(StorageError::service_error("out of disk"));

    invalidates(&nop(), &failed);
}

/// Verify that validation invalidates consensus state machine when it cannot compare an operation.
///
/// Alias added directly to `Container` would be reported as a divergence if state machine is not
/// invalidated.
fn invalidates(operation: &ConsensusOperations, result: &StorageResult<bool>) {
    let shadow = Shadow::new(ShadowMode::Panic);

    assert_eq!(shadow.apply(&nop()), None);

    shadow.container.add_alias(OTHER_ALIAS);
    assert_eq!(shadow.apply_with(operation, result), None);

    assert_eq!(shadow.apply(&nop()), None);
}

/// Snapshot recovery replaces applied state without updating consensus state machine,
/// so next Raft entry should rebuild it
#[test]
fn snapshot_invalidates() {
    let container = Arc::new(container());

    let dir = tempdir();
    let manager = manager(container, ShadowMode::Panic, dir.path());

    manager.apply_normal_entry(&entry(&nop())).expect("nop");

    // Snapshot removes the existing collection and its alias
    manager
        .apply_snapshot(&snapshot())
        .expect("snapshot applied")
        .expect("snapshot applied");

    manager.apply_normal_entry(&entry(&nop())).expect("nop");
}

/// `ConsensusManager` should panic on a divergence when consensus state machine validation runs
/// in panic mode
#[test]
#[should_panic(expected = "aliases")]
fn manager_panics_on_divergence() {
    let container = Arc::new(container());
    let dir = tempdir();
    let manager = manager(container.clone(), ShadowMode::Panic, dir.path());

    manager.apply_normal_entry(&entry(&nop())).expect("nop");

    container.add_alias(OTHER_ALIAS);

    manager.apply_normal_entry(&entry(&nop())).expect("nop");
}

/// With consensus state machine validation disabled, `ConsensusManager` should not panic on
/// the same divergence
#[test]
fn manager_disabled() {
    let container = Arc::new(container());
    let dir = tempdir();
    let manager = manager(container.clone(), ShadowMode::Disabled, dir.path());

    manager.apply_normal_entry(&entry(&nop())).expect("nop");

    container.add_alias(OTHER_ALIAS);

    manager.apply_normal_entry(&entry(&nop())).expect("nop");
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

    let state = super::read_shallow_state(&container, &persistent);

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

/// Test wrapper that applies operations to `ShadowStateMachine` and compares its state with
/// `Container` directly, without `ConsensusManager`
struct Shadow {
    machine: Mutex<ShadowStateMachine>,
    container: Container,
    persistent: Persistent,
    /// Holds the state directory alive for as long as `persistent` reads it
    _dir: TempDir,
}

impl Shadow {
    fn new(mode: ShadowMode) -> Self {
        let dir = tempdir();

        Self {
            machine: mode.build().expect("state machine enabled"),
            container: container(),
            persistent: persistent(dir.path()),
            _dir: dir,
        }
    }

    /// Apply `operation` and compare as if the operation handler returned `Ok`
    fn apply(&self, operation: &ConsensusOperations) -> Option<String> {
        self.apply_with(operation, &Ok(true))
    }

    /// Apply `operation` and compare with the operation handler's `result`
    fn apply_with(
        &self,
        operation: &ConsensusOperations,
        result: &StorageResult<bool>,
    ) -> Option<String> {
        self.apply_between(operation, result, |_| ())
    }

    /// Apply `operation`, let `between` simulate a concurrent change to `Container`,
    /// then compare with the operation handler's `result`
    fn apply_between(
        &self,
        operation: &ConsensusOperations,
        result: &StorageResult<bool>,
        between: impl FnOnce(&Container),
    ) -> Option<String> {
        let mut machine = self.machine.lock();
        let outcome = machine.apply(&self.container, &self.persistent, operation);

        between(&self.container);

        machine.diff(
            &self.container,
            &self.persistent,
            operation,
            &outcome,
            result,
        )
    }
}

fn tempdir() -> TempDir {
    Builder::new().prefix("shadow").tempdir().expect("temp dir")
}

/// Build `ConsensusManager` with the test `Container` and consensus state machine validation
/// running in `mode`
fn manager(
    container: Arc<Container>,
    mode: ShadowMode,
    path: &Path,
) -> ConsensusManager<Container> {
    let (sender, _receiver) = mpsc::channel();

    ConsensusManager::new(
        persistent(path),
        container,
        OperationSender::new(sender),
        path,
    )
    .expect("manager initialized")
    .with_shadow(mode)
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

/// Covered operation that names `collection`, which is absent from the consensus state machine
/// and `Container`
fn create_payload_index(collection: &str) -> ConsensusOperations {
    let operation = CreatePayloadIndex {
        collection_name: collection.to_string(),
        field_name: "city".parse().expect("valid field name"),
        field_schema: PayloadSchemaType::Keyword.into(),
    };

    ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::CreatePayloadIndex(
        operation,
    )))
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

/// Operation not covered by the consensus state machine that names no collection
fn remove_peer() -> ConsensusOperations {
    ConsensusOperations::RemovePeer(PEER_ID)
}

/// Operation not yet covered by the consensus state machine that names the collection it changes
fn set_replica_state() -> ConsensusOperations {
    let operation = SetShardReplicaState {
        collection_name: COLLECTION.to_string(),
        shard_id: 0,
        peer_id: PEER_ID,
        state: ReplicaState::Active,
        from_state: None,
    };

    ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::SetShardReplicaState(
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
        handler_result: true,
        snapshots: AtomicUsize::new(0),
        snapshots_before_handler: AtomicUsize::new(0),
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
    handler_result: bool,
    /// Number of full collection-state reads, used to distinguish resync from rebuild
    snapshots: AtomicUsize,
    /// Number of full collection-state reads observed when operation handler ran
    snapshots_before_handler: AtomicUsize,
    /// Collections changed outside consensus and waiting to be resynced
    dirty_collections: Mutex<BTreeSet<CollectionId>>,
}

impl Container {
    fn snapshots(&self) -> usize {
        self.snapshots.load(Ordering::Relaxed)
    }

    fn snapshots_before_handler(&self) -> usize {
        self.snapshots_before_handler.load(Ordering::Relaxed)
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
        let replicas = HashMap::from([(PEER_ID, ReplicaState::Active)]);

        self.collections
            .lock()
            .get_mut(COLLECTION)
            .expect("collection exists")
            .shards
            .insert(shard_id, ShardInfo { replicas });
    }
}

impl CollectionContainer for Container {
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

    /// `ConsensusManager` calls this method for collection meta operations.
    /// Manager tests only pass `Nop` here; other tests mutate `Container` directly,
    /// so this stand-in can return success without applying the operation.
    fn perform_collection_meta_op(
        &self,
        _operation: CollectionMetaOperations,
    ) -> Result<bool, StorageError> {
        self.snapshots_before_handler
            .store(self.snapshots(), Ordering::Relaxed);

        Ok(self.handler_result)
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

    // Remaining `CollectionContainer` methods are not exercised by these tests

    fn remove_peer(&self, _peer_id: PeerId) -> Result<(), StorageError> {
        unimplemented!()
    }

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
