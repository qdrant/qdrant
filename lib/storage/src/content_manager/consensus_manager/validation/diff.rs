//! Report field-level differences between consensus state machine and applied state

use std::collections::{BTreeSet, HashMap};
use std::fmt;

use collection::collection_state;
use collection::shards::CollectionId;
use collection::shards::shard::PeerId;
use serde::Serialize;
use serde_json::Value;

use crate::content_manager::alias_mapping::AliasMapping;
use crate::content_manager::consensus_state_machine::ClusterState;
use crate::quota::QuotaConfig;
use crate::types::{PeerAddressById, PeerMetadataById};

/// Lightweight view of applied consensus state.
///
/// Mirrors [`ClusterState`], but stores only collection names instead of full collection state,
/// so validation can check cluster fields without reading every collection.
///
/// Full state is loaded on demand for collections the operation may have changed.
#[derive(Clone, Debug)]
pub struct ShallowState {
    pub collections: BTreeSet<CollectionId>,
    pub aliases: AliasMapping,
    pub peer_address_by_id: PeerAddressById,
    pub peer_metadata_by_id: PeerMetadataById,
    pub cluster_metadata: HashMap<String, serde_json::Value>,
    pub quota_config: QuotaConfig,
}

/// One field that differs between the state machine and applied state
#[derive(Clone, Debug)]
pub struct Difference {
    /// Field path, such as `collections[books].shards`
    pub field: String,
    /// Differing values inside the field, as `path: expected X, applied Y`.
    ///
    /// Expected values come from the state machine.
    pub values: String,
}

impl fmt::Display for Difference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.field, self.values)
    }
}

/// Compare collection names and cluster fields
pub fn cluster(state_machine: &ClusterState, applied: &ShallowState) -> Vec<Difference> {
    let ClusterState {
        collections,
        aliases,
        peer_address_by_id,
        peer_metadata_by_id,
        cluster_metadata,
        quota_config,
    } = state_machine;

    let ShallowState {
        collections: applied_collections,
        aliases: applied_aliases,
        peer_address_by_id: applied_peer_address_by_id,
        peer_metadata_by_id: applied_peer_metadata_by_id,
        cluster_metadata: applied_cluster_metadata,
        quota_config: applied_quota_config,
    } = applied;

    let mut diff = Vec::new();

    let collections: BTreeSet<_> = collections.keys().cloned().collect();
    for collection in collections.symmetric_difference(applied_collections) {
        let values = if collections.contains(collection) {
            "expected present, applied absent"
        } else {
            "expected absent, applied present"
        };

        diff.push(Difference {
            field: format!("collections[{collection}]"),
            values: values.into(),
        });
    }

    if aliases != applied_aliases {
        diff.push(json_difference("aliases", aliases, applied_aliases));
    }

    if peer_address_by_id != applied_peer_address_by_id {
        diff.push(json_difference(
            "peer_address_by_id",
            &uris_to_strings(peer_address_by_id),
            &uris_to_strings(applied_peer_address_by_id),
        ));
    }

    if peer_metadata_by_id != applied_peer_metadata_by_id {
        diff.push(json_difference(
            "peer_metadata_by_id",
            peer_metadata_by_id,
            applied_peer_metadata_by_id,
        ));
    }

    if cluster_metadata != applied_cluster_metadata {
        diff.push(json_difference(
            "cluster_metadata",
            cluster_metadata,
            applied_cluster_metadata,
        ));
    }

    if quota_config != applied_quota_config {
        diff.push(json_difference(
            "quota_config",
            quota_config,
            applied_quota_config,
        ));
    }

    diff
}

/// Compare collection states
pub fn collection(
    name: &str,
    state_machine: &collection_state::State,
    applied: &collection_state::State,
) -> Vec<Difference> {
    let collection_state::State {
        config,
        shards,
        resharding,
        transfers,
        shards_key_mapping,
        payload_index_schema,
    } = state_machine;

    let collection_state::State {
        config: applied_config,
        shards: applied_shards,
        resharding: applied_resharding,
        transfers: applied_transfers,
        shards_key_mapping: applied_shards_key_mapping,
        payload_index_schema: applied_payload_index_schema,
    } = applied;

    let field = |field: &str| format!("collections[{name}].{field}");
    let mut diff = Vec::new();

    if config != applied_config {
        diff.push(json_difference(&field("config"), config, applied_config));
    }

    if shards != applied_shards {
        diff.push(json_difference(&field("shards"), shards, applied_shards));
    }

    if resharding != applied_resharding {
        diff.push(json_difference(
            &field("resharding"),
            resharding,
            applied_resharding,
        ));
    }

    if transfers != applied_transfers {
        diff.push(json_difference(
            &field("transfers"),
            transfers,
            applied_transfers,
        ));
    }

    if shards_key_mapping != applied_shards_key_mapping {
        diff.push(json_difference(
            &field("shards_key_mapping"),
            shards_key_mapping,
            applied_shards_key_mapping,
        ));
    }

    if payload_index_schema != applied_payload_index_schema {
        diff.push(json_difference(
            &field("payload_index_schema"),
            payload_index_schema,
            applied_payload_index_schema,
        ));
    }

    diff
}

fn uris_to_strings(peer_address_by_id: &PeerAddressById) -> HashMap<PeerId, String> {
    peer_address_by_id
        .iter()
        .map(|(&peer_id, uri)| (peer_id, uri.to_string()))
        .collect()
}

/// Describe a differing field by the JSON values that differ inside it.
///
/// Objects are compared key by key, so a changed replica state reports only that replica.
/// Arrays are reported whole, with sorted elements, because sets such as transfers serialize
/// in arbitrary order.
fn json_difference<T>(field: &str, expected: &T, applied: &T) -> Difference
where
    T: Serialize + fmt::Debug,
{
    let values = match (
        serde_json::to_value(expected),
        serde_json::to_value(applied),
    ) {
        (Ok(expected), Ok(applied)) => {
            let mut changes = Vec::new();
            json_changes("", Some(&expected), Some(&applied), &mut changes);
            changes.join("; ")
        }

        // Fall back to the whole values, e.g. when a map key does not serialize as a JSON key
        _ => format!("expected {expected:?}, applied {applied:?}"),
    };

    Difference {
        field: field.to_string(),
        values,
    }
}

fn json_changes(
    path: &str,
    expected: Option<&Value>,
    applied: Option<&Value>,
    changes: &mut Vec<String>,
) {
    if expected == applied {
        return;
    }

    if let (Some(Value::Object(expected)), Some(Value::Object(applied))) = (expected, applied) {
        let keys: BTreeSet<_> = expected.keys().chain(applied.keys()).collect();
        for key in keys {
            let path = if path.is_empty() {
                key.clone()
            } else {
                format!("{path}.{key}")
            };
            json_changes(&path, expected.get(key), applied.get(key), changes);
        }

        return;
    }

    let prefix = if path.is_empty() {
        String::new()
    } else {
        format!("{path}: ")
    };
    changes.push(format!(
        "{prefix}expected {}, applied {}",
        render_json(expected),
        render_json(applied),
    ));
}

fn render_json(value: Option<&Value>) -> String {
    match value {
        None => "absent".into(),

        Some(Value::Array(elements)) => {
            let mut elements: Vec<_> = elements.iter().map(Value::to_string).collect();
            elements.sort();
            format!("[{}]", elements.join(","))
        }

        Some(value) => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroU32;

    use collection::collection_state::ShardInfo;
    use collection::operations::cluster_ops::ReshardingDirection;
    use collection::operations::types::PeerMetadata;
    use collection::shards::replica_set::replica_set_state::ReplicaState;
    use collection::shards::resharding::ReshardState;
    use collection::shards::transfer::ShardTransfer;
    use segment::types::PayloadSchemaType;
    use serde_json::json;
    use uuid::Uuid;

    use super::*;
    use crate::content_manager::alias_mapping::AliasMapping;
    use crate::content_manager::consensus_state_machine::tests::{PEER_ID, collection_state};
    use crate::quota::QuotaConfig;

    const COLLECTION: &str = "books";
    const ALIAS: &str = "novels";

    /// A change to one field of a state, and the field name the compare reports for it
    type Mutation<T> = (&'static str, fn(&mut T));

    #[test]
    fn cluster_match() {
        let (expected, actual) = cluster_states();
        let diff = cluster(&expected, &actual);

        assert!(diff.is_empty(), "states match, got {diff:?}");
    }

    /// A collection one side does not hold is reported by name, whatever is inside it
    #[test]
    fn cluster_collection_names() {
        let (expected, mut actual) = cluster_states();
        actual.collections.clear();

        let diff = cluster(&expected, &actual);

        assert_eq!(fields(&diff), [format!("collections[{COLLECTION}]")]);
        assert_eq!(diff[0].values, "expected present, applied absent");
    }

    #[test]
    fn cluster_every_field() {
        let mutations: Vec<Mutation<ShallowState>> = vec![
            ("aliases", |actual| actual.aliases.remove(ALIAS)),
            ("peer_address_by_id", |actual| {
                actual.peer_address_by_id.clear();
            }),
            ("peer_metadata_by_id", |actual| {
                actual.peer_metadata_by_id.clear();
            }),
            ("cluster_metadata", |actual| {
                actual.cluster_metadata.clear();
            }),
            ("quota_config", |actual| {
                actual.quota_config.enabled = !actual.quota_config.enabled;
            }),
        ];

        for (field, mutate) in mutations {
            let (expected, mut actual) = cluster_states();
            mutate(&mut actual);

            assert_eq!(
                fields(&cluster(&expected, &actual)),
                [field],
                "mutated {field}"
            );
        }
    }

    /// Matching state-machine and applied states used as the mutation-test baseline
    fn cluster_states() -> (ClusterState, ShallowState) {
        let mut aliases = AliasMapping::default();
        aliases.insert(ALIAS.to_string(), COLLECTION.to_string());

        let peer_address_by_id =
            HashMap::from([(PEER_ID, "http://localhost:6335".parse().expect("valid URI"))]);
        let peer_metadata_by_id = HashMap::from([(PEER_ID, PeerMetadata::current())]);
        let cluster_metadata = HashMap::from([("owner".to_string(), json!("qdrant"))]);
        let quota_config = QuotaConfig::default();

        let expected = ClusterState {
            collections: HashMap::from([(COLLECTION.to_string(), collection_state(Vec::new()))]),
            aliases: aliases.clone(),
            peer_address_by_id: peer_address_by_id.clone(),
            peer_metadata_by_id: peer_metadata_by_id.clone(),
            cluster_metadata: cluster_metadata.clone(),
            quota_config,
        };

        let actual = ShallowState {
            collections: BTreeSet::from([COLLECTION.to_string()]),
            aliases,
            peer_address_by_id,
            peer_metadata_by_id,
            cluster_metadata,
            quota_config,
        };

        (expected, actual)
    }

    #[test]
    fn collection_match() {
        let state = collection_state(Vec::new());
        let diff = collection(COLLECTION, &state, &state);

        assert!(diff.is_empty(), "states match, got {diff:?}");
    }

    #[test]
    fn collection_every_field() {
        let mutations: Vec<Mutation<collection_state::State>> = vec![
            ("config", |state| {
                state.config.params.shard_number = NonZeroU32::new(2).expect("non-zero");
            }),
            ("shards", |state| {
                let replicas = HashMap::new();
                state.shards.insert(0, ShardInfo { replicas });
            }),
            ("resharding", |state| {
                let resharding =
                    ReshardState::new(Uuid::nil(), ReshardingDirection::Up, PEER_ID, 1, None);
                state.resharding = Some(resharding);
            }),
            ("transfers", |state| {
                state.transfers.insert(ShardTransfer {
                    shard_id: 0,
                    to_shard_id: None,
                    from: PEER_ID,
                    to: PEER_ID + 1,
                    sync: false,
                    method: None,
                    filter: None,
                });
            }),
            ("shards_key_mapping", |state| {
                state
                    .shards_key_mapping
                    .insert("keyword".into(), [0].into_iter().collect());
            }),
            ("payload_index_schema", |state| {
                let field = "city".parse().expect("valid field name");
                state
                    .payload_index_schema
                    .schema
                    .insert(field, PayloadSchemaType::Keyword.into());
            }),
        ];

        for (field, mutate) in mutations {
            let mut actual = collection_state(Vec::new());
            mutate(&mut actual);

            assert_eq!(
                fields(&collection(
                    COLLECTION,
                    &collection_state(Vec::new()),
                    &actual
                )),
                [format!("collections[{COLLECTION}].{field}")],
                "mutated {field}",
            );
        }
    }

    /// Values name the differing replica or config field, not the whole collection state
    #[test]
    fn collection_values() {
        let mut expected = collection_state(Vec::new());
        let replicas = HashMap::from([(PEER_ID, ReplicaState::Active)]);
        expected.shards.insert(0, ShardInfo { replicas });

        let mut actual = expected.clone();
        let replicas = HashMap::from([(PEER_ID, ReplicaState::Dead)]);
        actual.shards.insert(0, ShardInfo { replicas });
        actual.config.params.shard_number = NonZeroU32::new(2).expect("non-zero");

        let diff = collection(COLLECTION, &expected, &actual);

        let values: Vec<_> = diff.iter().map(ToString::to_string).collect();
        assert_eq!(
            values,
            [
                format!(
                    "collections[{COLLECTION}].config \
                     (params.shard_number: expected 1, applied 2)"
                ),
                format!(
                    "collections[{COLLECTION}].shards \
                     (0.replicas.{PEER_ID}: expected \"Active\", applied \"Dead\")"
                ),
            ],
        );
    }

    /// Sets serialize in arbitrary order, so their elements are sorted before reporting
    #[test]
    fn collection_values_sorted() {
        let transfer = |to| ShardTransfer {
            shard_id: 0,
            to_shard_id: None,
            from: PEER_ID,
            to,
            sync: false,
            method: None,
            filter: None,
        };
        let expected = collection_state(Vec::new());
        let mut actual = expected.clone();
        actual
            .transfers
            .extend([transfer(PEER_ID + 2), transfer(PEER_ID + 1)]);

        let diff = collection(COLLECTION, &expected, &actual);

        let first = serde_json::to_string(&transfer(PEER_ID + 1)).expect("serializable");
        let second = serde_json::to_string(&transfer(PEER_ID + 2)).expect("serializable");
        assert_eq!(diff.len(), 1);
        assert_eq!(
            diff[0].values,
            format!("expected [], applied [{first},{second}]"),
        );
    }

    fn fields(diff: &[Difference]) -> Vec<&str> {
        diff.iter()
            .map(|difference| difference.field.as_str())
            .collect()
    }
}
