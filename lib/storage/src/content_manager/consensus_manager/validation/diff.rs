//! Report field-level differences between consensus state machine and applied state

use std::collections::{BTreeSet, HashMap};
use std::fmt;

use collection::collection_state;
use collection::shards::CollectionId;

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

    let mut diff = Vec::new();

    let collections: BTreeSet<_> = collections.keys().cloned().collect();

    for collection in collections.difference(applied_collections) {
        diff.push(format!("collections[{collection}]: only in state machine"));
    }

    for collection in applied_collections.difference(&collections) {
        diff.push(format!("collections[{collection}]: only in applied state"));
    }

    push_diff(&mut diff, "aliases", aliases, applied_aliases);
    push_diff(
        &mut diff,
        "peer_address_by_id",
        peer_address_by_id,
        applied_peer_address_by_id,
    );
    push_diff(
        &mut diff,
        "peer_metadata_by_id",
        peer_metadata_by_id,
        applied_peer_metadata_by_id,
    );
    push_diff(
        &mut diff,
        "cluster_metadata",
        cluster_metadata,
        applied_cluster_metadata,
    );
    push_diff(
        &mut diff,
        "quota_config",
        quota_config,
        applied_quota_config,
    );

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

    let mut diff = Vec::new();
    let field = |field: &str| format!("collections[{name}].{field}");

    push_diff(&mut diff, field("config"), config, applied_config);
    push_diff(&mut diff, field("shards"), shards, applied_shards);
    push_diff(
        &mut diff,
        field("resharding"),
        resharding,
        applied_resharding,
    );
    push_diff(&mut diff, field("transfers"), transfers, applied_transfers);
    push_diff(
        &mut diff,
        field("shards_key_mapping"),
        shards_key_mapping,
        applied_shards_key_mapping,
    );
    push_diff(
        &mut diff,
        field("payload_index_schema"),
        payload_index_schema,
        applied_payload_index_schema,
    );

    diff
}

/// Report `field` with both values if they differ
fn push_diff<T: PartialEq + fmt::Debug>(
    diff: &mut Vec<String>,
    field: impl fmt::Display,
    state_machine: &T,
    applied: &T,
) {
    if state_machine != applied {
        diff.push(format!(
            "{field}: state machine {state_machine:?}, applied {applied:?}"
        ));
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
        let (expected, actual) = cluster_states();
        let diff = cluster(&expected, &actual);

        assert!(diff.is_empty(), "states match, got {diff:?}");
    }

    /// A collection one side does not hold is reported by name, whatever is inside it
    #[test]
    fn cluster_collection_names() {
        let (expected, mut actual) = cluster_states();
        actual.collections.clear();

        assert_eq!(
            cluster(&expected, &actual),
            [format!("collections[{COLLECTION}]: only in state machine")],
        );

        let (expected, mut actual) = cluster_states();
        actual.collections.insert("extra".to_string());

        assert_eq!(
            cluster(&expected, &actual),
            ["collections[extra]: only in applied state"],
        );
    }

    /// A differing field is reported with both values
    #[test]
    fn cluster_field_values() {
        let (expected, mut actual) = cluster_states();
        actual.cluster_metadata.clear();

        assert_eq!(
            cluster(&expected, &actual),
            [r#"cluster_metadata: state machine {"owner": String("qdrant")}, applied {}"#],
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
            let (expected, mut actual) = cluster_states();
            mutate(&mut actual);

            assert_reports_field(&cluster(&expected, &actual), field);
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

            assert_reports_field(
                &collection(COLLECTION, &collection_state(Vec::new()), &actual),
                &format!("collections[{COLLECTION}].{field}"),
            );
        }
    }

    /// The diff reports exactly one field, with both values
    fn assert_reports_field(diff: &[String], field: &str) {
        let [difference] = diff else {
            panic!("mutated {field}, expected one difference, got {diff:?}");
        };

        assert!(
            difference.starts_with(&format!("{field}: state machine ")),
            "mutated {field}, got {difference}",
        );
        assert!(
            difference.contains(", applied "),
            "mutated {field}, got {difference}",
        );
    }
}
