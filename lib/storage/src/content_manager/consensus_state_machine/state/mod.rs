mod apply;
mod plan;

use std::collections::HashMap;

use collection::collection_state;
use collection::shards::CollectionId;
use semver::Version;

use super::Action;
use crate::content_manager::alias_mapping::AliasMapping;
use crate::content_manager::errors::{StorageError, StorageResult};
use crate::quota::QuotaConfig;
use crate::types::{PeerAddressById, PeerMetadataById};

/// Cluster state modeled by consensus state machine. Mirrors [`SnapshotData`].
///
/// [`SnapshotData`]: crate::content_manager::consensus_manager::SnapshotData
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ClusterState {
    pub collections: HashMap<CollectionId, collection_state::State>,
    pub aliases: AliasMapping,
    pub peer_address_by_id: PeerAddressById,
    pub peer_metadata_by_id: PeerMetadataById,
    pub cluster_metadata: HashMap<String, serde_json::Value>,
    pub quota_config: QuotaConfig,
}

impl ClusterState {
    pub fn collection(&self, collection: &str) -> Option<&collection_state::State> {
        self.collections.get(collection)
    }

    pub fn has_collection(&self, collection: &str) -> bool {
        self.collections.contains_key(collection)
    }

    /// Resolve a name that may be an alias, and check that the collection exists
    pub fn resolve_collection(&self, collection: &str) -> StorageResult<String> {
        let resolved = match self.aliases.get(collection) {
            Some(collection) => collection.clone(),
            None => collection.to_string(),
        };

        if !self.has_collection(&resolved) {
            return Err(StorageError::not_found(format!(
                "Collection `{resolved}` doesn't exist!"
            )));
        }

        Ok(resolved)
    }

    /// Whether every known peer runs at least `version`.
    /// Implementation intentionally matches `ChannelService::all_peers_at_version`.
    pub fn all_peers_at_version(&self, version: &Version) -> bool {
        // More peer addresses than metadata means at least one version is unknown
        if self.peer_address_by_id.len() > self.peer_metadata_by_id.len() {
            return false;
        }

        self.peer_metadata_by_id
            .values()
            .all(|metadata| metadata.version() >= version)
    }
}
