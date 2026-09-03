//! Reading state back out of `TableOfContent`

use std::collections::{BTreeSet, HashMap};
use std::path::Path;

use collection::collection_state;
use collection::operations::types::PeerMetadata;
use collection::shards::CollectionId;
use collection::shards::shard::PeerId;
use serde_json::json;
use tempfile::Builder;

use super::fixtures::*;
use super::*;
use crate::content_manager::alias_mapping::AliasMapping;
use crate::content_manager::collection_meta_ops::CollectionMetaOperations;
use crate::content_manager::errors::StorageError;
use crate::quota::QuotaConfig;

const ALIAS: &str = "novels";
const METADATA_KEY: &str = "owner";

#[test]
fn scrape_cluster_state() {
    let dir = Builder::new().prefix("shadow").tempdir().unwrap();
    let persistent = persistent(dir.path());
    let container = container();

    let state = super::scrape_cluster_state(&container, &persistent);

    assert_eq!(state.collections, container.collections);
    assert_eq!(state.aliases, container.aliases);
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
    let dir = Builder::new().prefix("shadow").tempdir().unwrap();
    let persistent = persistent(dir.path());
    let container = container();

    let actual = super::scrape_actual_state(&container, &persistent);

    assert_eq!(actual.collections, BTreeSet::from([COLLECTION.to_string()]));
    assert_eq!(actual.aliases, container.aliases);
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
        aliases,
        quota_config: QuotaConfig {
            enabled: true,
            ..Default::default()
        },
    }
}

/// `TableOfContent` stand-in answering out of the state a test gives it
struct Container {
    collections: HashMap<CollectionId, collection_state::State>,
    aliases: AliasMapping,
    quota_config: QuotaConfig,
}

impl CollectionContainer for Container {
    fn collections_snapshot(&self) -> CollectionsSnapshot {
        CollectionsSnapshot {
            collections: self.collections.clone(),
            aliases: self.aliases.clone(),
        }
    }

    fn collection_state(&self, collection: &str) -> Option<collection_state::State> {
        self.collections.get(collection).cloned()
    }

    fn collection_names(&self) -> BTreeSet<CollectionId> {
        self.collections.keys().cloned().collect()
    }

    fn alias_mapping(&self) -> AliasMapping {
        self.aliases.clone()
    }

    fn quota_config(&self) -> QuotaConfig {
        self.quota_config
    }

    // Reading state back is all these tests do

    fn perform_collection_meta_op(
        &self,
        _operation: CollectionMetaOperations,
    ) -> Result<bool, StorageError> {
        unimplemented!()
    }

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
