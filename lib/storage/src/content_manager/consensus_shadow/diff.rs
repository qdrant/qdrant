//! Report field-level differences between consensus state machine and applied state

use std::collections::{BTreeSet, HashMap};
use std::mem;

use collection::collection_state;
use collection::shards::CollectionId;

use crate::content_manager::alias_mapping::AliasMapping;
use crate::content_manager::consensus_state_machine::{ApplyOutcome, ClusterState};
use crate::content_manager::errors::StorageResult;
use crate::quota::QuotaConfig;
use crate::types::{PeerAddressById, PeerMetadataById};

/// Lightweight view of applied consensus state.
///
/// Mirrors [`ClusterState`], but stores only collection names instead of full collection state,
/// because validation reads it after each covered consensus operation.
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

/// Compare collection names and cluster fields
pub fn cluster(state_machine: &ClusterState, applied: &ShallowState) -> Vec<String> {
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

    // TODO: Include values into diff!?
    let mut diff = Vec::new();

    let collections: BTreeSet<_> = collections.keys().cloned().collect();
    for collection in collections.symmetric_difference(applied_collections) {
        diff.push(format!("collections[{collection}]"));
    }

    if aliases != applied_aliases {
        diff.push("aliases".into());
    }

    if peer_address_by_id != applied_peer_address_by_id {
        diff.push("peer_address_by_id".into());
    }

    if peer_metadata_by_id != applied_peer_metadata_by_id {
        diff.push("peer_metadata_by_id".into());
    }

    if cluster_metadata != applied_cluster_metadata {
        diff.push("cluster_metadata".into());
    }

    if quota_config != applied_quota_config {
        diff.push("quota_config".into());
    }

    diff
}

/// Compare collection states
pub fn collection(
    name: &str,
    state_machine: &collection_state::State,
    applied: &collection_state::State,
) -> Vec<String> {
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

    // TODO: Include values into diff!?
    let mut fields = Vec::new(); // TODO: `SmallVec`?

    if config != applied_config {
        fields.push("config");
    }

    if shards != applied_shards {
        fields.push("shards");
    }

    if resharding != applied_resharding {
        fields.push("resharding");
    }

    if transfers != applied_transfers {
        fields.push("transfers");
    }

    if shards_key_mapping != applied_shards_key_mapping {
        fields.push("shards_key_mapping");
    }

    if payload_index_schema != applied_payload_index_schema {
        fields.push("payload_index_schema");
    }

    fields
        .into_iter()
        .map(|field| format!("collections[{name}].{field}"))
        .collect()
}

/// Compare outcomes from consensus state machine and operation handler.
/// Errors match by variant, error messages are ignored.
pub fn outcome(outcome: &ApplyOutcome, result: &StorageResult<bool>) -> Option<String> {
    match (outcome, result) {
        (ApplyOutcome::Accepted(_), Ok(_)) => {
            log::debug!("Consensus state machine and operation handler accepted operation");
            None
        }

        (ApplyOutcome::Rejected(state_machine), Err(handler))
            if mem::discriminant(state_machine) == mem::discriminant(handler) =>
        {
            log::debug!(
                "Consensus state machine and operation handler rejected operation: \
                 `{state_machine}` and `{handler}`"
            );

            None
        }

        (ApplyOutcome::Accepted(_), Err(handler)) => Some(format!(
            "state machine accepted operation, \
             but operation handler returned error `{handler}`"
        )),

        (ApplyOutcome::Rejected(state_machine), Ok(_)) => Some(format!(
            "state machine rejected operation with `{state_machine}`, \
             but operation handler applied it successfully"
        )),

        (ApplyOutcome::Rejected(state_machine), Err(handler)) => Some(format!(
            "state machine and operation handler rejected operation differently: \
             `{state_machine}` vs `{handler}`"
        )),

        (ApplyOutcome::NotCovered, _) => {
            log::debug!("Consensus state machine does not cover operation");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroU32;

    use collection::collection_state::ShardInfo;
    use collection::operations::cluster_ops::ReshardingDirection;
    use collection::operations::types::PeerMetadata;
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
        let (shadow, actual) = cluster_states();
        let diff = cluster(&shadow, &actual);

        assert!(diff.is_empty(), "states match, got {diff:?}");
    }

    /// A collection one side does not hold is reported by name, whatever is inside it
    #[test]
    fn cluster_collection_names() {
        let (shadow, mut actual) = cluster_states();
        actual.collections.clear();

        assert_eq!(
            cluster(&shadow, &actual),
            [format!("collections[{COLLECTION}]")],
        );
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
            let (shadow, mut actual) = cluster_states();
            mutate(&mut actual);

            assert_eq!(cluster(&shadow, &actual), [field], "mutated {field}");
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

        let shadow = ClusterState {
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

        (shadow, actual)
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
                collection(COLLECTION, &collection_state(Vec::new()), &actual),
                [format!("collections[{COLLECTION}].{field}")],
                "mutated {field}",
            );
        }
    }
}
