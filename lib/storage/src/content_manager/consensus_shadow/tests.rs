//! Running the shadow alongside a stand-in for `TableOfContent`

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::{Arc, mpsc};

use collection::collection_state;
use collection::operations::types::PeerMetadata;
use collection::shards::CollectionId;
use collection::shards::shard::PeerId;
use raft::eraftpb::Entry as RaftEntry;
use serde_json::json;
use tempfile::{Builder, TempDir};

use super::fixtures::*;
use super::*;
use crate::content_manager::alias_mapping::AliasMapping;
use crate::content_manager::collection_meta_ops::CollectionMetaOperations;
use crate::content_manager::consensus::operation_sender::OperationSender;
use crate::content_manager::consensus_manager::ConsensusManager;
use crate::content_manager::consensus_state_machine::NodeContext;
use crate::quota::QuotaConfig;

const ALIAS: &str = "novels";
const OTHER_ALIAS: &str = "crime";
const METADATA_KEY: &str = "owner";

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

#[test]
fn scrape_cluster_state() {
    let dir = tempdir();
    let persistent = persistent(dir.path());
    let container = container();

    let state = super::scrape_cluster_state(&container, &persistent);

    assert_eq!(state.collections, container.collections);
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
        collections: HashMap::from([(COLLECTION.to_string(), collection_state())]),
        aliases: Mutex::new(aliases),
        quota_config: QuotaConfig {
            enabled: true,
            ..Default::default()
        },
    }
}

/// `TableOfContent` stand-in answering out of the state a test gives it.
///
/// Aliases are behind a lock, so a test can change them the way something other than consensus
/// would.
struct Container {
    collections: HashMap<CollectionId, collection_state::State>,
    aliases: Mutex<AliasMapping>,
    quota_config: QuotaConfig,
}

impl CollectionContainer for Container {
    fn collections_snapshot(&self) -> CollectionsSnapshot {
        CollectionsSnapshot {
            collections: self.collections.clone(),
            aliases: self.aliases.lock().clone(),
        }
    }

    fn collection_state(&self, collection: &str) -> Option<collection_state::State> {
        self.collections.get(collection).cloned()
    }

    fn collection_names(&self) -> BTreeSet<CollectionId> {
        self.collections.keys().cloned().collect()
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

    fn perform_collection_meta_op(
        &self,
        operation: CollectionMetaOperations,
    ) -> Result<bool, StorageError> {
        assert!(
            matches!(operation, CollectionMetaOperations::Nop { .. }),
            "no test applies {operation:?} through this container",
        );

        Ok(true)
    }

    // Never reached: no test recovers a snapshot, removes a peer or writes a quota config

    fn apply_collections_snapshot(&self, _data: CollectionsSnapshot) -> Result<(), StorageError> {
        unimplemented!()
    }

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
