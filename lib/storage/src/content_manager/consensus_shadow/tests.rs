//! Running the shadow alongside a stand-in for `TableOfContent`.
//!
//! What the shadow decides is asserted on the report [`ShadowStateMachine::diff`] returns.
//! Driving an entry through `ConsensusManager` only covers the wiring, where the one thing a
//! test can observe is whether the peer died.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, mpsc};

use collection::collection_state;
use collection::collection_state::ShardInfo;
use collection::operations::types::PeerMetadata;
use collection::shards::CollectionId;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::shard::{PeerId, ShardId};
use raft::eraftpb::Entry as RaftEntry;
use serde_json::json;
use tempfile::{Builder, TempDir};

use super::*;
use crate::content_manager::alias_mapping::AliasMapping;
use crate::content_manager::collection_meta_ops::{
    CollectionMetaOperations, DropPayloadIndex, SetShardReplicaState,
};
use crate::content_manager::consensus::operation_sender::OperationSender;
use crate::content_manager::consensus_manager::ConsensusManager;
use crate::content_manager::consensus_state_machine::NodeContext;
use crate::content_manager::consensus_state_machine::tests::{
    PEER_ID, collection_state, node_context,
};
use crate::quota::QuotaConfig;

const COLLECTION: &str = "books";
const ALIAS: &str = "novels";
const OTHER_ALIAS: &str = "crime";
const METADATA_KEY: &str = "owner";

/// Both sides record the same cluster metadata key: the machine in its own state, the apply
/// path in `Persistent`, which the compare reads back. Needs the manager, since the applied
/// side is what the handler writes.
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

/// A collection differing inside is a divergence, so the compare reads collection state and not
/// just the names
#[test]
fn diverged_collection() {
    let shadow = Shadow::new(ShadowMode::Panic);

    assert_eq!(shadow.apply(&nop()), None);

    shadow.container.add_shard(0);

    assert!(shadow.apply(&drop_payload_index()).is_some());
}

/// An operation the machine does not model leaves state it cannot predict
#[test]
fn not_covered_invalidates() {
    invalidates(&set_replica_state(), &Ok(true));
}

/// A service error kills the consensus thread, and what the failed apply wrote before it gave
/// up is not something the machine predicts
#[test]
fn service_error_invalidates() {
    let failed = Err(StorageError::service_error("out of disk"));

    invalidates(&nop(), &failed);
}

/// An entry the machine cannot be compared against drops the machine, so the next one builds
/// a machine out of what the apply path holds.
///
/// The alias makes the two disagree, so a machine that survived the entry reports it.
fn invalidates(operation: &ConsensusOperations, result: &StorageResult<bool>) {
    let shadow = Shadow::new(ShadowMode::Panic);

    assert_eq!(shadow.apply(&nop()), None);

    shadow.container.add_alias(OTHER_ALIAS);
    assert_eq!(shadow.apply_with(operation, result), None);

    assert_eq!(shadow.apply(&nop()), None);
}

/// The peer dies on a divergence in panic mode, which is what the consensus test suite runs
#[test]
#[should_panic]
fn manager_panics_on_divergence() {
    let container = Arc::new(container());
    let dir = tempdir();
    let manager = manager(container.clone(), ShadowMode::Panic, dir.path());

    manager.apply_normal_entry(&entry(&nop())).expect("nop");

    container.add_alias(OTHER_ALIAS);

    manager.apply_normal_entry(&entry(&nop())).expect("nop");
}

/// Same divergence with the shadow off, so a shadow that runs anyway fails this
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
fn scrape_cluster_state() {
    let dir = tempdir();
    let persistent = persistent(dir.path());
    let container = container();

    let state = super::scrape_cluster_state(&container, &persistent);

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

/// Shadow over a container, without the manager in between
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
            machine: mode.build().expect("shadow enabled"),
            container: container(),
            persistent: persistent(dir.path()),
            _dir: dir,
        }
    }

    /// Plan `operation` and compare, as an entry the apply path answered `Ok` to
    fn apply(&self, operation: &ConsensusOperations) -> Option<String> {
        self.apply_with(operation, &Ok(true))
    }

    /// Plan `operation` and compare, as an entry the apply path answered `result` to
    fn apply_with(
        &self,
        operation: &ConsensusOperations,
        result: &StorageResult<bool>,
    ) -> Option<String> {
        let mut machine = self.machine.lock();
        let outcome = machine.apply(&self.container, &self.persistent, operation);

        machine.diff(&self.container, &self.persistent, &outcome, result)
    }
}

fn tempdir() -> TempDir {
    Builder::new().prefix("shadow").tempdir().expect("temp dir")
}

/// Manager over `container`, applying entries with the shadow running in `mode`
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

fn nop() -> ConsensusOperations {
    ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::Nop { token: 0 }))
}

/// Covered operation naming the collection, so the compare after it reads that collection
fn drop_payload_index() -> ConsensusOperations {
    let operation = DropPayloadIndex {
        collection_name: COLLECTION.to_string(),
        field_name: "city".parse().expect("valid field name"),
    };

    ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::DropPayloadIndex(
        operation,
    )))
}

/// Operation the machine does not model yet
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

/// Peer state holding this peer's address and metadata, and one cluster metadata key
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

/// Container holding one collection under one alias, and a quota config away from its default
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
    }
}

/// `TableOfContent` stand-in answering out of the state a test gives it.
///
/// The state is behind locks, so a test can change it the way something other than the entry
/// being applied would.
struct Container {
    collections: Mutex<HashMap<CollectionId, collection_state::State>>,
    aliases: Mutex<AliasMapping>,
    quota_config: QuotaConfig,
}

impl Container {
    /// Point another alias at the collection, as an operation the machine never saw would
    fn add_alias(&self, alias: &str) {
        self.aliases
            .lock()
            .insert(alias.to_string(), COLLECTION.to_string());
    }

    /// Add a shard to the collection, as an operation the machine does not model would
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
        CollectionsSnapshot {
            collections: self.collections.lock().clone(),
            aliases: self.aliases.lock().clone(),
        }
    }

    fn node_context(&self) -> NodeContext {
        node_context()
    }

    fn quota_config(&self) -> QuotaConfig {
        self.quota_config
    }

    /// Answers and changes nothing: every test applies an operation whose effect on the
    /// container a test writes by hand, or none at all
    fn perform_collection_meta_op(
        &self,
        _operation: CollectionMetaOperations,
    ) -> Result<bool, StorageError> {
        Ok(true)
    }

    // Never reached: no test recovers a snapshot, removes a peer or writes a quota config

    fn apply_collections_snapshot(&self, _data: CollectionsSnapshot) -> Result<(), StorageError> {
        unimplemented!()
    }

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
