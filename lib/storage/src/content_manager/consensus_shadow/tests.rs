//! Running the shadow alongside a stand-in for `TableOfContent`

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

use super::fixtures::*;
use super::*;
use crate::content_manager::alias_mapping::AliasMapping;
use crate::content_manager::collection_meta_ops::{
    CollectionMetaOperations, CreatePayloadIndex, DropPayloadIndex, SetShardReplicaState,
};
use crate::content_manager::consensus::operation_sender::OperationSender;
use crate::content_manager::consensus_manager::{ConsensusManager, SnapshotData};
use crate::content_manager::consensus_state_machine::NodeContext;
use crate::quota::QuotaConfig;
use crate::types::{PeerAddressById, PeerMetadataById};

const ALIAS: &str = "novels";
const OTHER_ALIAS: &str = "crime";
const METADATA_KEY: &str = "owner";
const MISSING: &str = "outis";

/// Both sides record the same cluster metadata key: the machine in its own state, the apply
/// path in `Persistent`, which the compare reads back
#[test]
fn matching_entry() {
    let dir = tempdir();
    let container = Arc::new(container());
    let manager = manager(container, panicking_shadow(), dir.path());

    let operation = ConsensusOperations::UpdateClusterMetadata {
        key: "answer".to_string(),
        value: json!(42),
    };

    manager
        .apply_normal_entry(&entry(&operation))
        .expect("entry applied");
}

/// An alias appearing in `TableOfContent` without an operation is the shape of the bug the
/// shadow looks for: the machine has no way to know about it
#[test]
#[should_panic(expected = "aliases")]
fn diverged_aliases() {
    let dir = tempdir();
    let container = Arc::new(container());
    let manager = manager(container.clone(), panicking_shadow(), dir.path());

    // Builds the machine out of the container, so the two start out equal
    manager.apply_normal_entry(&entry(&nop())).expect("nop");

    container
        .aliases
        .lock()
        .insert(OTHER_ALIAS.to_string(), COLLECTION.to_string());

    manager.apply_normal_entry(&entry(&nop())).expect("nop");
}

/// Same divergence as above, with the shadow off. Panicking is left on, so a shadow that runs
/// anyway fails this test.
#[test]
fn disabled() {
    let dir = tempdir();
    let container = Arc::new(container());
    let config = ShadowConfig {
        enabled: false,
        ..panicking_shadow()
    };
    let manager = manager(container.clone(), config, dir.path());

    manager.apply_normal_entry(&entry(&nop())).expect("nop");

    container
        .aliases
        .lock()
        .insert(OTHER_ALIAS.to_string(), COLLECTION.to_string());

    manager.apply_normal_entry(&entry(&nop())).expect("nop");
}

/// Snapshot recovery leaves state no operation asked for, so the machine goes and the next
/// entry builds a new one. Here the snapshot empties the container while the machine still
/// holds a collection and its alias.
#[test]
fn snapshot_invalidates() {
    let dir = tempdir();
    let container = Arc::new(container());
    let manager = manager(container, panicking_shadow(), dir.path());

    manager.apply_normal_entry(&entry(&nop())).expect("nop");

    manager
        .apply_snapshot(&snapshot())
        .expect("snapshot applied")
        .expect("snapshot applied");

    manager.apply_normal_entry(&entry(&nop())).expect("nop");
}

/// An operation the machine does not model reads the collections it names back, rather than
/// rebuilding the machine from every collection
#[test]
fn not_covered_resync() {
    let dir = tempdir();
    let container = Arc::new(container());
    let manager = manager(container.clone(), panicking_shadow(), dir.path());

    // Builds the machine, the one full read of the state this test expects
    manager.apply_normal_entry(&entry(&nop())).expect("nop");

    container.add_shard(0);
    manager
        .apply_normal_entry(&entry(&set_replica_state(COLLECTION)))
        .expect("replica state");

    // Names the collection, so its state is compared: the shard has to be in both by now.
    // Dropping an index neither side has changes nothing either side records.
    manager
        .apply_normal_entry(&entry(&drop_payload_index(COLLECTION)))
        .expect("index dropped");

    assert_eq!(container.snapshots(), 1);
}

/// Reading one collection back does not paper over a divergence somewhere else
#[test]
#[should_panic(expected = "aliases")]
fn not_covered_keeps_the_rest() {
    let dir = tempdir();
    let container = Arc::new(container());
    let manager = manager(container.clone(), panicking_shadow(), dir.path());

    manager.apply_normal_entry(&entry(&nop())).expect("nop");

    container
        .aliases
        .lock()
        .insert(OTHER_ALIAS.to_string(), COLLECTION.to_string());
    manager
        .apply_normal_entry(&entry(&set_replica_state(COLLECTION)))
        .expect("replica state");

    manager.apply_normal_entry(&entry(&nop())).expect("nop");
}

/// A rejection of the same class on both sides is a match, whatever the state says
#[test]
fn matching_rejection() {
    let dir = tempdir();
    let rejection = StorageError::not_found(format!("Collection `{MISSING}` doesn't exist!"));
    let container = Arc::new(container_answering(Err(rejection)));
    let manager = manager(container, panicking_shadow(), dir.path());

    manager
        .apply_normal_entry(&entry(&create_payload_index(MISSING)))
        .expect_err("both sides reject a missing collection");
}

#[test]
#[should_panic(expected = "rejected it differently")]
fn differing_rejection() {
    let dir = tempdir();
    let container = Arc::new(container_answering(Err(StorageError::bad_request("no"))));
    let manager = manager(container, panicking_shadow(), dir.path());

    let _ = manager.apply_normal_entry(&entry(&create_payload_index(MISSING)));
}

#[test]
#[should_panic(expected = "machine accepted, apply rejected")]
fn rejected_by_apply_only() {
    let dir = tempdir();
    let container = Arc::new(container_answering(Err(StorageError::bad_request("no"))));
    let manager = manager(container, panicking_shadow(), dir.path());

    let _ = manager.apply_normal_entry(&entry(&nop()));
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

#[test]
fn scrape_actual_state() {
    let dir = tempdir();
    let persistent = persistent(dir.path());
    let container = container();

    let actual = super::scrape_actual_state(&container, &persistent);

    assert_eq!(actual.collections, BTreeSet::from([COLLECTION.to_string()]));
    assert_eq!(actual.aliases, *container.aliases.lock());
    assert_eq!(actual.quota_config, container.quota_config);
    assert_eq!(
        actual.peer_address_by_id,
        *persistent.peer_address_by_id.read(),
    );
    assert_eq!(
        actual.peer_metadata_by_id,
        *persistent.peer_metadata_by_id.read(),
    );
    assert_eq!(actual.cluster_metadata, persistent.cluster_metadata);
}

fn tempdir() -> TempDir {
    Builder::new().prefix("shadow").tempdir().expect("temp dir")
}

/// Manager over `container`, applying entries with the shadow `config` describes
fn manager(
    container: Arc<Container>,
    config: ShadowConfig,
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
    .with_shadow(config)
}

fn panicking_shadow() -> ShadowConfig {
    ShadowConfig {
        enabled: true,
        on_divergence: OnDivergence::Panic,
    }
}

fn entry(operation: &ConsensusOperations) -> RaftEntry {
    RaftEntry {
        data: serde_cbor::to_vec(operation).expect("operation serialized"),
        ..Default::default()
    }
}

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

fn drop_payload_index(collection: &str) -> ConsensusOperations {
    let operation = DropPayloadIndex {
        collection_name: collection.to_string(),
        field_name: "city".parse().expect("valid field name"),
    };

    ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::DropPayloadIndex(
        operation,
    )))
}

fn set_replica_state(collection: &str) -> ConsensusOperations {
    let operation = SetShardReplicaState {
        collection_name: collection.to_string(),
        shard_id: 0,
        peer_id: PEER_ID,
        state: ReplicaState::Active,
        from_state: None,
    };

    ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::SetShardReplicaState(
        operation,
    )))
}

fn nop() -> ConsensusOperations {
    ConsensusOperations::CollectionMeta(Box::new(CollectionMetaOperations::Nop { token: 0 }))
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

fn container() -> Container {
    let mut aliases = AliasMapping::default();
    aliases.insert(ALIAS.to_string(), COLLECTION.to_string());

    Container {
        collections: Mutex::new(HashMap::from([(
            COLLECTION.to_string(),
            collection_state(),
        )])),
        aliases: Mutex::new(aliases),
        quota_config: QuotaConfig {
            enabled: true,
            ..Default::default()
        },
        meta_op_result: Ok(true),
        snapshots: AtomicUsize::new(0),
    }
}

/// Container whose apply path answers `result` for every collection meta operation
fn container_answering(result: StorageResult<bool>) -> Container {
    Container {
        meta_op_result: result,
        ..container()
    }
}

/// `TableOfContent` stand-in answering out of the state a test gives it.
///
/// Aliases are behind a lock, so a test can change them the way something other than consensus
/// would.
struct Container {
    collections: Mutex<HashMap<CollectionId, collection_state::State>>,
    aliases: Mutex<AliasMapping>,
    quota_config: QuotaConfig,
    /// What the apply path answers for a collection meta operation
    meta_op_result: StorageResult<bool>,
    /// How many times the whole state was read back, to tell a resync from a rebuild
    snapshots: AtomicUsize,
}

impl Container {
    fn snapshots(&self) -> usize {
        self.snapshots.load(Ordering::Relaxed)
    }

    /// Add a shard to the collection, the way an operation the machine does not model would
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

    fn node_context(&self) -> NodeContext {
        node_context()
    }

    fn quota_config(&self) -> QuotaConfig {
        self.quota_config
    }

    /// Answers and changes nothing: every test either applies a nop, or an operation the
    /// answer rejects
    fn perform_collection_meta_op(
        &self,
        _operation: CollectionMetaOperations,
    ) -> Result<bool, StorageError> {
        self.meta_op_result.clone()
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

    // Never reached: no test removes a peer or writes a quota config

    fn remove_peer(&self, _peer_id: PeerId) -> Result<(), StorageError> {
        unimplemented!()
    }

    fn sync_local_state(&self) -> Result<(), StorageError> {
        unimplemented!()
    }

    fn set_quota_config(&self, _config: QuotaConfig) -> Result<(), StorageError> {
        unimplemented!()
    }
}
