//! Compare the shadow state against the state `TableOfContent` holds.
//!
//! Both sides are destructured field by field, so a field added to either one stops compiling
//! until the compare covers it.

use std::collections::{BTreeSet, HashMap};

use collection::collection_state;
use collection::shards::CollectionId;

use crate::content_manager::alias_mapping::AliasMapping;
use crate::content_manager::consensus_state_machine::ClusterState;
use crate::quota::QuotaConfig;
use crate::types::{PeerAddressById, PeerMetadataById};

/// Cluster state read back from `TableOfContent` for one compare.
///
/// Carries collection names only. Reading a collection's state is the expensive part, so
/// [`collection`] compares one collection at a time.
#[derive(Clone, Debug)]
pub struct ActualState {
    pub collections: BTreeSet<CollectionId>,
    pub aliases: AliasMapping,
    pub peer_address_by_id: PeerAddressById,
    pub peer_metadata_by_id: PeerMetadataById,
    pub cluster_metadata: HashMap<String, serde_json::Value>,
    pub quota_config: QuotaConfig,
}

/// Fields where `shadow` differs from `actual`, named as paths into [`ClusterState`].
///
/// A collection only one side holds is reported as `collections[name]`
pub fn cluster(shadow: &ClusterState, actual: &ActualState) -> Vec<String> {
    let ClusterState {
        collections,
        aliases,
        peer_address_by_id,
        peer_metadata_by_id,
        cluster_metadata,
        quota_config,
    } = shadow;

    let ActualState {
        collections: actual_collections,
        aliases: actual_aliases,
        peer_address_by_id: actual_peer_address_by_id,
        peer_metadata_by_id: actual_peer_metadata_by_id,
        cluster_metadata: actual_cluster_metadata,
        quota_config: actual_quota_config,
    } = actual;

    let mut diff = Vec::new();

    let collections: BTreeSet<_> = collections.keys().cloned().collect();
    for collection in collections.symmetric_difference(actual_collections) {
        diff.push(format!("collections[{collection}]"));
    }

    if aliases != actual_aliases {
        diff.push("aliases".to_string());
    }

    if peer_address_by_id != actual_peer_address_by_id {
        diff.push("peer_address_by_id".to_string());
    }

    if peer_metadata_by_id != actual_peer_metadata_by_id {
        diff.push("peer_metadata_by_id".to_string());
    }

    if cluster_metadata != actual_cluster_metadata {
        diff.push("cluster_metadata".to_string());
    }

    if quota_config != actual_quota_config {
        diff.push("quota_config".to_string());
    }

    diff
}

/// Fields where the two states of one collection differ, named as paths into [`ClusterState`].
///
/// Both sides hold the collection: [`cluster`] compares which collections exist
pub fn collection(
    name: &str,
    shadow: &collection_state::State,
    actual: &collection_state::State,
) -> Vec<String> {
    let collection_state::State {
        config,
        shards,
        resharding,
        transfers,
        shards_key_mapping,
        payload_index_schema,
    } = shadow;

    let collection_state::State {
        config: actual_config,
        shards: actual_shards,
        resharding: actual_resharding,
        transfers: actual_transfers,
        shards_key_mapping: actual_shards_key_mapping,
        payload_index_schema: actual_payload_index_schema,
    } = actual;

    let mut fields = Vec::new();

    if config != actual_config {
        fields.push("config");
    }

    if shards != actual_shards {
        fields.push("shards");
    }

    if resharding != actual_resharding {
        fields.push("resharding");
    }

    if transfers != actual_transfers {
        fields.push("transfers");
    }

    if shards_key_mapping != actual_shards_key_mapping {
        fields.push("shards_key_mapping");
    }

    if payload_index_schema != actual_payload_index_schema {
        fields.push("payload_index_schema");
    }

    fields
        .into_iter()
        .map(|field| format!("collections[{name}].{field}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use collection::collection_state::ShardInfo;
    use collection::config::{CollectionConfigInternal, CollectionParams};
    use collection::operations::cluster_ops::ReshardingDirection;
    use collection::operations::types::{PeerMetadata, VectorsConfig};
    use collection::optimizers_builder::OptimizersConfig;
    use collection::shards::resharding::ReshardState;
    use collection::shards::shard::PeerId;
    use collection::shards::transfer::ShardTransfer;
    use segment::types::PayloadSchemaType;
    use serde_json::json;
    use uuid::Uuid;

    use super::*;

    const COLLECTION: &str = "books";
    const PEER_ID: PeerId = 42;

    /// A change to one field of a state, and the field name the compare reports for it
    type Mutation<T> = (&'static str, fn(&mut T));

    #[test]
    fn cluster_match() {
        let (shadow, actual) = cluster_states();
        let diff = cluster(&shadow, &actual);

        assert!(diff.is_empty(), "states match, got {diff:?}");
    }

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
        let mutations: Vec<Mutation<ActualState>> = vec![
            ("aliases", |actual| actual.aliases.remove("novels")),
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

    /// A shadow state and the read-back matching it, both filled in every field
    fn cluster_states() -> (ClusterState, ActualState) {
        let mut aliases = AliasMapping::default();
        aliases.insert("novels".to_string(), COLLECTION.to_string());

        let peer_address_by_id =
            HashMap::from([(PEER_ID, "http://localhost:6335".parse().expect("valid uri"))]);
        let peer_metadata_by_id = HashMap::from([(PEER_ID, PeerMetadata::current())]);
        let cluster_metadata = HashMap::from([("owner".to_string(), json!("qdrant"))]);
        let quota_config = QuotaConfig::default();

        let shadow = ClusterState {
            collections: HashMap::from([(COLLECTION.to_string(), collection_state())]),
            aliases: aliases.clone(),
            peer_address_by_id: peer_address_by_id.clone(),
            peer_metadata_by_id: peer_metadata_by_id.clone(),
            cluster_metadata: cluster_metadata.clone(),
            quota_config,
        };

        let actual = ActualState {
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
        let state = collection_state();
        let diff = collection(COLLECTION, &state, &state);

        assert!(diff.is_empty(), "states match, got {diff:?}");
    }

    #[test]
    fn collection_every_field() {
        let mutations: Vec<Mutation<collection_state::State>> = vec![
            ("config", |state| {
                state.config.params.shard_number = NonZeroU32::new(2).unwrap();
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
            let mut actual = collection_state();
            mutate(&mut actual);

            assert_eq!(
                collection(COLLECTION, &collection_state(), &actual),
                [format!("collections[{COLLECTION}].{field}")],
                "mutated {field}",
            );
        }
    }

    /// Collection state with every field left empty, for a test to fill the one it covers
    fn collection_state() -> collection_state::State {
        collection_state::State {
            config: collection_config(),
            shards: Default::default(),
            resharding: None,
            transfers: Default::default(),
            shards_key_mapping: Default::default(),
            payload_index_schema: Default::default(),
        }
    }

    /// Config is fixed: the compare only reads whether two configs are equal
    fn collection_config() -> CollectionConfigInternal {
        let params = CollectionParams {
            vectors: VectorsConfig::Multi(Default::default()),
            sparse_vectors: None,
            shard_number: NonZeroU32::new(1).unwrap(),
            sharding_method: None,
            #[expect(deprecated)]
            on_disk_payload: Some(false),
            payload: None,
            replication_factor: NonZeroU32::new(1).unwrap(),
            write_consistency_factor: NonZeroU32::new(1).unwrap(),
            read_fan_out_factor: None,
            read_fan_out_delay_ms: None,
        };

        CollectionConfigInternal {
            params,
            hnsw_config: Default::default(),
            optimizer_config: OptimizersConfig {
                deleted_threshold: 0.1,
                vacuum_min_vector_number: 1000,
                default_segment_number: 0,
                max_segment_size: None,
                #[expect(deprecated)]
                memmap_threshold: None,
                indexing_threshold: Some(100_000),
                flush_interval_sec: 60,
                max_optimization_threads: Some(0),
                prevent_unoptimized: None,
            },
            wal_config: Default::default(),
            quantization_config: None,
            strict_mode_config: None,
            uuid: None,
            metadata: None,
        }
    }
}
