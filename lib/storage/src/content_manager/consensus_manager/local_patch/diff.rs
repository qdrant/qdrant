mod shard_layout;

use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::hash::{BuildHasher, Hash};
use std::str::FromStr;

use collection::collection_state::{ShardInfo, State};
use collection::config::CollectionConfigInternal;
use collection::operations::types::PeerMetadata;
use collection::shards::CollectionId;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::resharding::ReshardState;
use collection::shards::shard::{PeerId, ShardId};
use collection::shards::shard_holder::shard_mapping::ShardKeyMapping;
use collection::shards::transfer::ShardTransfer;
use segment::types::{PayloadFieldSchema, PayloadKeyType};
use serde::{Deserialize, Serialize};
use tonic::transport::Uri;

use crate::content_manager::alias_mapping::AliasMapping;
use crate::content_manager::consensus_manager::{CollectionsSnapshot, SnapshotData};
use crate::content_manager::errors::{StorageError, StorageResult};
use crate::quota::QuotaConfig;
use crate::types::PeerAddressById;

/// Changes to consensus state, applied to a single peer without going through consensus.
///
/// `None` fields are left unchanged.
/// Map entries are set to the given value, or removed when the value is `null`.
/// Values below a map entry are replaced whole.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsensusStateDiff {
    pub collections: Option<HashMap<CollectionId, CollectionDiff>>,
    pub aliases: Option<HashMap<String, Option<CollectionId>>>,
    /// URI edits of known peers only. Peers can't be added or removed.
    pub peer_addresses: Option<HashMap<PeerId, String>>,
    pub peer_metadata: Option<HashMap<PeerId, Option<PeerMetadata>>>,
    pub cluster_metadata: Option<HashMap<String, Option<serde_json::Value>>>,
    pub quota_config: Option<QuotaConfig>,
}

/// Change to an existing collection. Collections can't be created.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectionDiff {
    Update(Box<CollectionStateDiff>),
    /// Also removes aliases of the collection
    Delete,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionStateDiff {
    /// UUID and incompatible params can't be changed: Raft snapshot application would
    /// recreate the collection empty
    pub config: Option<CollectionConfigInternal>,
    pub shards: Option<HashMap<ShardId, Option<ShardInfoDiff>>>,
    /// Outer `None` = unchanged, `Some(None)` = clear resharding
    #[serde(default, with = "serde_with::rust::double_option")]
    pub resharding: Option<Option<ReshardState>>,
    pub transfers: Option<HashSet<ShardTransfer>>,
    pub shards_key_mapping: Option<ShardKeyMapping>,
    pub payload_index_schema: Option<HashMap<PayloadKeyType, Option<PayloadFieldSchema>>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardInfoDiff {
    pub replicas: HashMap<PeerId, Option<ReplicaState>>,
}

/// Changes made by applying a [`ConsensusStateDiff`]
#[derive(Debug, Default, Serialize)]
pub struct PatchReport {
    pub changes: Vec<Change>,
}

#[derive(Debug, Serialize)]
pub struct Change {
    pub path: String,
    /// `None` if the value didn't exist
    pub old: Option<serde_json::Value>,
    /// `None` if the value was removed
    pub new: Option<serde_json::Value>,
}

impl ConsensusStateDiff {
    pub fn apply(self, state: &mut SnapshotData, report: &mut PatchReport) -> StorageResult<()> {
        let Self {
            collections,
            aliases,
            peer_addresses,
            peer_metadata,
            cluster_metadata,
            quota_config,
        } = self;

        let SnapshotData {
            collections_data:
                CollectionsSnapshot {
                    collections: state_collections,
                    aliases: state_aliases,
                },
            address_by_id: state_addresses,
            metadata_by_id: state_metadata,
            cluster_metadata: state_cluster_metadata,
            quota_config: state_quota_config,
        } = state;

        for (name, diff) in collections.into_iter().flatten() {
            let path = format!("collections.{name}");

            match diff {
                CollectionDiff::Update(diff) => {
                    let Some(collection) = state_collections.get_mut(&name) else {
                        return Err(StorageError::bad_request(format!(
                            "can't update collection {name}: collection doesn't exist",
                        )));
                    };

                    diff.apply(collection, state_addresses, &path, report)?;
                }

                CollectionDiff::Delete => {
                    let Some(collection) = state_collections.remove(&name) else {
                        return Err(StorageError::bad_request(format!(
                            "can't delete collection {name}: collection doesn't exist",
                        )));
                    };

                    report.push(path, Some(&collection), None)?;

                    let collection_aliases: Vec<_> =
                        state_aliases.collection_aliases(&name).collect();

                    for alias in collection_aliases {
                        patch_alias(state_aliases, alias, None, report)?;
                    }
                }
            }
        }

        for (alias, collection) in aliases.into_iter().flatten() {
            if let Some(collection) = &collection {
                if !state_collections.contains_key(collection) {
                    return Err(StorageError::bad_request(format!(
                        "can't set alias {alias}: collection {collection} doesn't exist",
                    )));
                }

                // Same check as regular alias creation, otherwise the alias would shadow
                // the collection
                if state_collections.contains_key(&alias) {
                    return Err(StorageError::bad_request(format!(
                        "can't set alias {alias}: collection with the same name exists",
                    )));
                }
            }

            patch_alias(state_aliases, alias, collection, report)?;
        }

        for (peer_id, uri) in peer_addresses.into_iter().flatten() {
            let Some(address) = state_addresses.get_mut(&peer_id) else {
                return Err(StorageError::bad_request(format!(
                    "can't update address of peer {peer_id}: peer doesn't exist",
                )));
            };

            let uri = Uri::from_str(&uri).map_err(|err| {
                StorageError::bad_request(format!("invalid URI {uri} of peer {peer_id}: {err}"))
            })?;

            // `Uri` also accepts a path alone, which can't be connected to
            if uri.scheme().is_none() || uri.authority().is_none() {
                return Err(StorageError::bad_request(format!(
                    "invalid URI {uri} of peer {peer_id}: URI must have scheme and authority",
                )));
            }

            if *address != uri {
                let path = format!("peer_addresses.{peer_id}");
                report.push(path, Some(&address.to_string()), Some(&uri.to_string()))?;
                *address = uri;
            }
        }

        if let Some(peer_metadata) = peer_metadata {
            if let Some(peer_id) = peer_metadata
                .keys()
                .find(|peer_id| !state_addresses.contains_key(peer_id))
            {
                return Err(StorageError::bad_request(format!(
                    "can't update metadata of peer {peer_id}: peer doesn't exist",
                )));
            }

            patch_map(state_metadata, peer_metadata, "peer_metadata", report)?;
        }

        // Metadata of a removed peer remains until restart, if the peer's metadata update
        // was committed after its removal. Applying the patch drops it, so report it.
        let orphan_peers: Vec<_> = state_metadata
            .keys()
            .filter(|peer_id| !state_addresses.contains_key(peer_id))
            .copied()
            .collect();

        for peer_id in orphan_peers {
            let metadata = state_metadata.remove(&peer_id);
            report.push(format!("peer_metadata.{peer_id}"), metadata.as_ref(), None)?;
        }

        if let Some(cluster_metadata) = cluster_metadata {
            patch_map(
                state_cluster_metadata,
                cluster_metadata,
                "cluster_metadata",
                report,
            )?;
        }

        if let Some(quota_config) = quota_config {
            replace(
                state_quota_config,
                Some(quota_config),
                "quota_config".to_string(),
                report,
            )?;
        }

        Ok(())
    }
}

impl CollectionStateDiff {
    fn apply(
        self,
        state: &mut State,
        peers: &PeerAddressById,
        path: &str,
        report: &mut PatchReport,
    ) -> StorageResult<()> {
        let Self {
            config,
            shards,
            resharding,
            transfers,
            shards_key_mapping,
            payload_index_schema,
        } = self;

        // Pre-existing inconsistencies must not block unrelated changes,
        // so only reject inconsistencies introduced by the diff
        let shard_layout_violations = shard_layout::violations(state);

        let State {
            config: state_config,
            shards: state_shards,
            resharding: state_resharding,
            transfers: state_transfers,
            shards_key_mapping: state_shards_key_mapping,
            payload_index_schema: state_payload_index_schema,
        } = state;

        if let Some(config) = config {
            if config.uuid != state_config.uuid {
                return Err(StorageError::bad_request(format!(
                    "can't update {path}: collection UUID can't be changed",
                )));
            }

            state_config
                .params
                .check_compatible(&config.params)
                .map_err(|err| {
                    StorageError::bad_request(format!(
                        "can't update {path}: incompatible collection params: {err}",
                    ))
                })?;

            replace(state_config, config, format!("{path}.config"), report)?;
        }

        for (shard_id, diff) in shards.into_iter().flatten() {
            let path = format!("{path}.shards.{shard_id}");

            match diff {
                Some(ShardInfoDiff { replicas }) => {
                    let is_new_shard = !state_shards.contains_key(&shard_id);

                    let shard = state_shards.entry(shard_id).or_insert_with(|| ShardInfo {
                        replicas: HashMap::new(),
                    });

                    if is_new_shard {
                        report.push(path.clone(), None, Some(&*shard))?;
                    }

                    // Existing replicas of unknown peers can still be changed or removed
                    let unknown_peer = replicas.iter().find(|&(peer_id, state)| {
                        state.is_some()
                            && !peers.contains_key(peer_id)
                            && !shard.replicas.contains_key(peer_id)
                    });

                    if let Some((peer_id, _)) = unknown_peer {
                        return Err(StorageError::bad_request(format!(
                            "can't add replica to {path}: peer {peer_id} doesn't exist",
                        )));
                    }

                    patch_map(
                        &mut shard.replicas,
                        replicas,
                        &format!("{path}.replicas"),
                        report,
                    )?;

                    if is_new_shard && shard.replicas.is_empty() {
                        return Err(StorageError::bad_request(format!(
                            "can't create {path}: shard must have replicas",
                        )));
                    }
                }

                None => {
                    if let Some(shard) = state_shards.remove(&shard_id) {
                        report.push(path, Some(&shard), None)?;
                    }
                }
            }
        }

        if let Some(resharding) = resharding {
            replace(
                state_resharding,
                resharding,
                format!("{path}.resharding"),
                report,
            )?;
        }

        if let Some(transfers) = transfers {
            replace(
                state_transfers,
                transfers,
                format!("{path}.transfers"),
                report,
            )?;
        }

        if let Some(shards_key_mapping) = shards_key_mapping {
            replace(
                state_shards_key_mapping,
                shards_key_mapping,
                format!("{path}.shards_key_mapping"),
                report,
            )?;
        }

        if let Some(payload_index_schema) = payload_index_schema {
            patch_map(
                &mut state_payload_index_schema.schema,
                payload_index_schema,
                &format!("{path}.payload_index_schema"),
                report,
            )?;
        }

        let new_violation = shard_layout::violations(state)
            .into_iter()
            .find(|violation| !shard_layout_violations.contains(violation));

        if let Some(violation) = new_violation {
            return Err(StorageError::bad_request(format!(
                "can't update {path}: {violation}",
            )));
        }

        Ok(())
    }
}

impl PatchReport {
    fn push<T: Serialize>(
        &mut self,
        path: String,
        old: Option<&T>,
        new: Option<&T>,
    ) -> StorageResult<()> {
        let change = Change {
            path,
            old: old.map(to_json).transpose()?,
            new: new.map(to_json).transpose()?,
        };

        self.changes.push(change);
        Ok(())
    }
}

/// Replace `target` with `value`, if they differ
fn replace<T: PartialEq + Serialize>(
    target: &mut T,
    value: T,
    path: String,
    report: &mut PatchReport,
) -> StorageResult<()> {
    if *target != value {
        report.push(path, Some(&*target), Some(&value))?;
        *target = value;
    }

    Ok(())
}

/// Set each entry of `map` to the given value, or remove it if the value is `None`
fn patch_map<K, V, S>(
    map: &mut HashMap<K, V, S>,
    diff: HashMap<K, Option<V>>,
    path: &str,
    report: &mut PatchReport,
) -> StorageResult<()>
where
    K: Eq + Hash + Display,
    V: PartialEq + Serialize,
    S: BuildHasher,
{
    for (key, value) in diff {
        if map.get(&key) == value.as_ref() {
            continue;
        }

        let path = format!("{path}.{key}");

        match value {
            Some(value) => {
                report.push(path, map.get(&key), Some(&value))?;
                map.insert(key, value);
            }

            None => {
                let old = map.remove(&key);
                report.push(path, old.as_ref(), None)?;
            }
        }
    }

    Ok(())
}

fn patch_alias(
    aliases: &mut AliasMapping,
    alias: String,
    collection: Option<CollectionId>,
    report: &mut PatchReport,
) -> StorageResult<()> {
    let old = aliases.get(&alias).cloned();
    if old == collection {
        return Ok(());
    }

    report.push(
        format!("aliases.{alias}"),
        old.as_ref(),
        collection.as_ref(),
    )?;

    match collection {
        Some(collection) => aliases.insert(alias, collection),
        None => aliases.remove(&alias),
    }

    Ok(())
}

fn to_json(value: &impl Serialize) -> StorageResult<serde_json::Value> {
    serde_json::to_value(value).map_err(|err| {
        StorageError::service_error(format!("failed to serialize consensus state: {err}"))
    })
}

#[cfg(test)]
mod tests;
